//! Caller-supplied keys for table files and spills.
//!
//! The embedder unwraps through [`sekai_keys::KeyProvider`]. Mizuchi never
//! stores plaintext keys. Wrapped DEKs travel in Parquet key metadata and in
//! lake table JSON. Spill files use an ephemeral per-query DEK that is dropped
//! when the query ends.

use std::collections::{BTreeMap, HashMap};
use std::io::{ErrorKind, Read, Write};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use arrow::datatypes::Schema;
use parquet::arrow::ArrowSchemaConverter;
use parquet::encryption::decrypt::{FileDecryptionProperties, KeyRetriever};
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::file::properties::WriterProperties;
use rand::RngCore;
pub use sekai_keys::{
    Dek, KeyError, KeyId, KeyProvider, KeyPurpose, MemoryKeyProvider, RuntimeProfile, TenantId,
    UnwrapGrant, WrappedDek,
};

use mizuchi_core::error::{Error, Result};

const SPILL_MAGIC: &[u8; 4] = b"MZS1";
const SPILL_ID_LEN: usize = 16;
/// Unauthenticated `u32` length on disk is capped before `Vec` allocation.
pub const SPILL_FRAME_MAX: usize = 64 * 1024 * 1024;
/// Max IPC plaintext that still seals under [`SPILL_FRAME_MAX`] (nonce + tag).
pub const SPILL_PLAINTEXT_MAX: usize = SPILL_FRAME_MAX - 12 - 16;
const ENGINE: &str = "mizuchi";

/// Identity and frame count written after `MZS1`. Bound into each frame AAD.
pub struct SpillHeader {
    file_id: [u8; SPILL_ID_LEN],
    frame_count: u32,
}

impl SpillHeader {
    pub fn frame_count(&self) -> u32 {
        self.frame_count
    }
}

/// Unwrap grant plus provider used for one write or read.
#[derive(Clone)]
pub struct EncryptionSession {
    provider: Arc<dyn KeyProvider + Send + Sync>,
    tenant: TenantId,
    grant: UnwrapGrant,
    columns: Vec<String>,
}

impl std::fmt::Debug for EncryptionSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionSession")
            .field("tenant", &self.tenant)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

impl EncryptionSession {
    /// `columns` name Parquet fields that get their own DEK. Other fields in
    /// the written file still encrypt with the footer key. Empty encrypts
    /// every column with the footer key only.
    pub fn new(
        provider: Arc<dyn KeyProvider + Send + Sync>,
        tenant: TenantId,
        grant: UnwrapGrant,
        columns: Vec<String>,
    ) -> Self {
        Self {
            provider,
            tenant,
            grant,
            columns,
        }
    }

    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    pub fn kek_id(&self) -> KeyId {
        self.provider.describe().kek_id
    }

    /// Rewrite dedicated-column names from logical lake fields to physical
    /// Parquet paths so a rename still assigns the requested DEK.
    pub fn with_physical_names(&self, physical: &BTreeMap<String, String>) -> Self {
        if self.columns.is_empty() {
            return self.clone();
        }
        let columns = self
            .columns
            .iter()
            .map(|name| map_physical_column(name, physical))
            .collect();
        Self {
            provider: Arc::clone(&self.provider),
            tenant: self.tenant.clone(),
            grant: self.grant.clone(),
            columns,
        }
    }

    /// Parquet writer properties for `schema`. If `columns` is non-empty,
    /// parquet encrypts only named column keys, so every physical leaf gets
    /// a key: listed names (and their nested leaves) mint a DEK, the rest
    /// reuse the footer DEK.
    pub fn writer_properties(&self, schema: &Schema) -> Result<WriterProperties> {
        let footer = self.mint(KeyPurpose::Store)?;
        let footer_meta = encode_wrapped(&footer.wrapped)?;
        let mut builder = FileEncryptionProperties::builder(footer.dek.as_bytes().to_vec())
            .with_footer_key_metadata(footer_meta.clone());
        if !self.columns.is_empty() {
            let descr = ArrowSchemaConverter::new()
                .convert(schema)
                .map_err(|e| Error::Io(format!("parquet schema: {e}")))?;
            let mut dedicated: HashMap<&str, Minted> = HashMap::new();
            for name in &self.columns {
                dedicated.insert(name.as_str(), self.mint(KeyPurpose::Store)?);
            }
            for col in descr.columns() {
                let path = col.path().string();
                if let Some(name) = matching_column_key(&path, dedicated.keys().copied()) {
                    let minted = &dedicated[name];
                    builder = builder.with_column_key_and_metadata(
                        &path,
                        minted.dek.as_bytes().to_vec(),
                        encode_wrapped(&minted.wrapped)?,
                    );
                } else {
                    builder = builder.with_column_key_and_metadata(
                        &path,
                        footer.dek.as_bytes().to_vec(),
                        footer_meta.clone(),
                    );
                }
            }
        }
        let enc = builder
            .build()
            .map_err(|e| Error::Io(format!("parquet encryption: {e}")))?;
        Ok(WriterProperties::builder()
            .with_file_encryption_properties(enc)
            .build())
    }

    pub fn decryption_properties(&self) -> Result<Arc<FileDecryptionProperties>> {
        FileDecryptionProperties::with_key_retriever(Arc::new(Retriever {
            provider: Arc::clone(&self.provider),
            grant: self.grant.clone(),
        }))
        .build()
        .map_err(|e| Error::Io(format!("parquet decryption: {e}")))
    }

    fn mint(&self, purpose: KeyPurpose) -> Result<Minted> {
        let dek = Dek::generate();
        let wrapped = self
            .provider
            .wrap(&self.tenant, purpose, &dek)
            .map_err(key_err)?;
        Ok(Minted { dek, wrapped })
    }

    /// Lake metadata: key ids only, never DEK bytes. Delta `configuration`
    /// and Iceberg `properties` are string maps, so this is JSON text.
    pub fn lake_key_record(&self) -> String {
        let d = self.provider.describe();
        serde_json::json!({
            "kek_id": d.kek_id.as_str(),
            "tenant_id": self.tenant.as_str(),
            "purpose": "store",
        })
        .to_string()
    }
}

struct Minted {
    dek: Dek,
    wrapped: WrappedDek,
}

struct Retriever {
    provider: Arc<dyn KeyProvider + Send + Sync>,
    grant: UnwrapGrant,
}

impl KeyRetriever for Retriever {
    fn retrieve_key(&self, key_metadata: &[u8]) -> parquet::errors::Result<Vec<u8>> {
        let wrapped = decode_wrapped(key_metadata).map_err(|e| {
            parquet::errors::ParquetError::General(format!("wrapped DEK metadata: {e}"))
        })?;
        let now = unix_now();
        let out = self
            .provider
            .unwrap(&wrapped, &self.grant, ENGINE, now)
            .map_err(|e| parquet::errors::ParquetError::General(e.to_string()))?;
        // ADR 0113: unwrap yields an audit event the embedder records.
        // This engine is not an audit store. Wrap KeyProvider to persist it.
        let _audit = out.audit;
        Ok(out.dek.as_bytes().to_vec())
    }
}

fn map_physical_column(name: &str, physical: &BTreeMap<String, String>) -> String {
    if let Some(mapped) = physical.get(name) {
        return mapped.clone();
    }
    name.split_once('.')
        .and_then(|(head, rest)| physical.get(head).map(|mapped| format!("{mapped}.{rest}")))
        .unwrap_or_else(|| name.to_string())
}

fn encode_wrapped(wrapped: &WrappedDek) -> Result<Vec<u8>> {
    serde_json::to_vec(wrapped).map_err(|e| Error::Io(e.to_string()))
}

fn decode_wrapped(bytes: &[u8]) -> Result<WrappedDek> {
    serde_json::from_slice(bytes).map_err(|e| Error::Io(e.to_string()))
}

pub fn key_err(err: KeyError) -> Error {
    Error::Io(err.to_string())
}

pub fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Ephemeral spill DEK. Dropped when the query ends; never written to disk.
pub struct SpillKey(Dek);

impl SpillKey {
    pub fn generate() -> Self {
        Self(Dek::generate())
    }
}

impl std::fmt::Debug for SpillKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SpillKey(<redacted>)")
    }
}

/// Encrypt Arrow IPC bytes. Layout: magic, 12-byte nonce, ciphertext.
pub fn seal_spill(key: &SpillKey, plaintext: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::from(*SPILL_MAGIC);
    out.extend_from_slice(&seal_body(key, plaintext, SPILL_MAGIC)?);
    Ok(out)
}

pub fn open_spill(key: &SpillKey, sealed: &[u8]) -> Result<Vec<u8>> {
    if sealed.len() < 4 + 12 + 16 || sealed[..4] != *SPILL_MAGIC {
        return Err(Error::Io("spill file is not encrypted".into()));
    }
    open_body(key, &sealed[4..], SPILL_MAGIC)
}

pub fn is_sealed_spill(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[..4] == *SPILL_MAGIC
}

pub fn write_spill_header<W: Write>(
    out: &mut W,
    key: &SpillKey,
    frame_count: u32,
) -> Result<SpillHeader> {
    let mut file_id = [0u8; SPILL_ID_LEN];
    rand::thread_rng().fill_bytes(&mut file_id);
    let header = SpillHeader {
        file_id,
        frame_count,
    };
    out.write_all(SPILL_MAGIC)
        .map_err(|e| Error::Io(e.to_string()))?;
    out.write_all(&file_id)
        .map_err(|e| Error::Io(e.to_string()))?;
    out.write_all(&frame_count.to_be_bytes())
        .map_err(|e| Error::Io(e.to_string()))?;
    let auth = seal_body(key, b"", &header_aad(&header))?;
    out.write_all(&auth).map_err(|e| Error::Io(e.to_string()))?;
    Ok(header)
}

pub fn read_spill_header<R: Read>(src: &mut R, key: &SpillKey) -> Result<SpillHeader> {
    let mut file_id = [0u8; SPILL_ID_LEN];
    src.read_exact(&mut file_id).map_err(truncated_spill)?;
    let mut count = [0u8; 4];
    src.read_exact(&mut count).map_err(truncated_spill)?;
    let header = SpillHeader {
        file_id,
        frame_count: u32::from_be_bytes(count),
    };
    let mut auth = [0u8; 12 + 16];
    src.read_exact(&mut auth).map_err(truncated_spill)?;
    open_body(key, &auth, &header_aad(&header))?;
    Ok(header)
}

fn header_aad(header: &SpillHeader) -> [u8; 24] {
    let mut aad = [0u8; 24];
    aad[..4].copy_from_slice(SPILL_MAGIC);
    aad[4..20].copy_from_slice(&header.file_id);
    aad[20..24].copy_from_slice(&header.frame_count.to_be_bytes());
    aad
}

/// One independently authenticated IPC payload. File layout after magic:
/// 16-byte file id, `u32be frame count`, then that many frames of
/// `u32be length` + nonce + ciphertext. AAD binds id, index, and count.
pub fn append_spill_frame<W: Write>(
    out: &mut W,
    key: &SpillKey,
    plaintext: &[u8],
    header: &SpillHeader,
    index: u32,
) -> Result<()> {
    let body = seal_body(key, plaintext, &frame_aad(header, index))?;
    if body.len() > SPILL_FRAME_MAX {
        return Err(Error::Io("spill frame too large".into()));
    }
    let len = u32::try_from(body.len()).map_err(|_| Error::Io("spill frame too large".into()))?;
    out.write_all(&len.to_be_bytes())
        .map_err(|e| Error::Io(e.to_string()))?;
    out.write_all(&body).map_err(|e| Error::Io(e.to_string()))
}

pub fn read_spill_frame<R: Read>(
    src: &mut R,
    key: &SpillKey,
    header: &SpillHeader,
    index: u32,
) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    src.read_exact(&mut len_buf).map_err(truncated_spill)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len < 12 + 16 {
        return Err(Error::Io("spill frame too small".into()));
    }
    if len > SPILL_FRAME_MAX {
        return Err(Error::Io("spill frame too large".into()));
    }
    let mut body = vec![0u8; len];
    src.read_exact(&mut body).map_err(truncated_spill)?;
    open_body(key, &body, &frame_aad(header, index))
}

fn frame_aad(header: &SpillHeader, index: u32) -> [u8; 28] {
    let mut aad = [0u8; 28];
    aad[..4].copy_from_slice(SPILL_MAGIC);
    aad[4..20].copy_from_slice(&header.file_id);
    aad[20..24].copy_from_slice(&index.to_be_bytes());
    aad[24..28].copy_from_slice(&header.frame_count.to_be_bytes());
    aad
}

fn truncated_spill(err: std::io::Error) -> Error {
    if err.kind() == ErrorKind::UnexpectedEof {
        Error::Io("truncated spill".into())
    } else {
        Error::Io(err.to_string())
    }
}

fn seal_body(key: &SpillKey, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key.0.as_bytes())
        .map_err(|e| Error::Io(format!("spill cipher: {e}")))?;
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(
            nonce,
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| Error::Io("spill encrypt failed".into()))?;
    let mut out = Vec::with_capacity(12 + ciphertext.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

fn open_body(key: &SpillKey, body: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
    if body.len() < 12 + 16 {
        return Err(Error::Io("spill frame too small".into()));
    }
    let cipher = Aes256Gcm::new_from_slice(key.0.as_bytes())
        .map_err(|e| Error::Io(format!("spill cipher: {e}")))?;
    let nonce = Nonce::from_slice(&body[..12]);
    cipher
        .decrypt(
            nonce,
            Payload {
                msg: &body[12..],
                aad,
            },
        )
        .map_err(|_| Error::Io("spill decrypt failed".into()))
}

/// Longest listed column name that covers `path` (`path` itself or a parent).
fn matching_column_key<'a>(
    path: &str,
    names: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    names
        .into_iter()
        .filter(|name| path == *name || path.starts_with(&format!("{name}.")))
        .max_by_key(|name| name.len())
}

/// Writer properties when `enc` is set; `None` leaves the file plaintext.
pub fn parquet_writer_properties(
    enc: Option<&EncryptionSession>,
    schema: &Schema,
) -> Result<Option<WriterProperties>> {
    match enc {
        Some(s) => Ok(Some(s.writer_properties(schema)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sekai_keys::{MemoryKeyProvider, RuntimeProfile};

    #[test]
    fn overlapping_column_keys_prefer_the_longest_path() {
        assert_eq!(
            matching_column_key("profile.ssn", ["profile", "profile.ssn"]),
            Some("profile.ssn")
        );
        assert_eq!(
            matching_column_key("profile.ssn", ["profile.ssn", "profile"]),
            Some("profile.ssn")
        );
        assert_eq!(
            matching_column_key("profile.name", ["profile", "profile.ssn"]),
            Some("profile")
        );
        assert_eq!(matching_column_key("note", ["profile"]), None);
    }

    #[test]
    fn physical_names_map_logical_and_nested_paths() {
        let provider = Arc::new(MemoryKeyProvider::new(RuntimeProfile::Development).unwrap());
        let tenant = TenantId::new("acme").unwrap();
        let grant = UnwrapGrant {
            tenant_id: tenant.clone(),
            purpose: KeyPurpose::Store,
            expires_unix_secs: unix_now() + 3_600,
        };
        let keys = EncryptionSession::new(
            provider,
            tenant,
            grant,
            vec!["sensitive".into(), "person.ssn".into()],
        );
        let mut physical = BTreeMap::new();
        physical.insert("sensitive".into(), "secret".into());
        physical.insert("person".into(), "profile".into());
        let mapped = keys.with_physical_names(&physical);
        assert_eq!(mapped.columns, vec!["secret", "profile.ssn"]);
    }

    #[test]
    fn spill_round_trip_hides_plaintext() {
        let key = SpillKey::generate();
        let plain = b"employee_ssn=123-45-6789";
        let sealed = seal_spill(&key, plain).unwrap();
        assert!(!sealed.windows(plain.len()).any(|w| w == plain));
        assert_eq!(open_spill(&key, &sealed).unwrap(), plain);
    }

    #[test]
    fn oversized_spill_frame_length_is_refused() {
        use std::io::Cursor;
        let key = SpillKey::generate();
        let mut buf = Vec::new();
        write_spill_header(&mut buf, &key, 1).unwrap();
        buf.extend_from_slice(&u32::MAX.to_be_bytes());
        buf.extend_from_slice(&[0u8; 40]);
        let mut cur = Cursor::new(&buf[4..]);
        let header = read_spill_header(&mut cur, &key).unwrap();
        let err = read_spill_frame(&mut cur, &key, &header, 0).unwrap_err();
        assert!(err.to_string().contains("spill frame too large"), "{err}");
    }

    #[test]
    fn revoked_key_cannot_unwrap_metadata() {
        let provider = Arc::new(MemoryKeyProvider::new(RuntimeProfile::Development).unwrap());
        let tenant = TenantId::new("acme").unwrap();
        let dek = Dek::generate();
        let wrapped = provider.wrap(&tenant, KeyPurpose::Store, &dek).unwrap();
        provider.revoke_dek(&wrapped.dek_id).unwrap();
        let grant = UnwrapGrant {
            tenant_id: tenant,
            purpose: KeyPurpose::Store,
            expires_unix_secs: unix_now() + 3_600,
        };
        let err = provider
            .unwrap(&wrapped, &grant, ENGINE, unix_now())
            .unwrap_err();
        assert_eq!(err, KeyError::Revoked);
    }
}
