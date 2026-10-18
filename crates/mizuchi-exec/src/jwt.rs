//! JWT/JWKS bearer verification for the query API (HS256, embedder keys).
//!
//! HS256 only. The embedder supplies the secret or a JWKS document of `oct`
//! keys. There is no network fetch.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, KeyInit, Mac};
use serde_json::Value;
use sha2::Sha256;
use subtle::ConstantTimeEq;

use mizuchi_core::error::{Error, Result};

type HmacSha256 = Hmac<Sha256>;

/// Identity extracted from a verified JWT.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenIdentity {
    /// Tenant claim.
    pub tenant: String,
    /// Principal (`sub`, else `principal`).
    pub principal: String,
    /// Actor (`actor`, else innermost `act.sub`, else principal).
    pub actor: String,
}

/// HS256 JWT verifier with optional issuer, audience, and JWKS key set.
#[derive(Clone)]
pub struct JwtVerifier {
    issuer: Option<String>,
    audience: Option<String>,
    keys: JwtKeys,
}

#[derive(Clone)]
enum JwtKeys {
    Hs256(Vec<u8>),
    Jwks(BTreeMap<String, Vec<u8>>),
}

impl std::fmt::Debug for JwtVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtVerifier")
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("keys", &"_")
            .finish()
    }
}

impl JwtVerifier {
    /// One HMAC secret. Header `kid` is ignored.
    pub fn hs256(secret: impl Into<Vec<u8>>) -> Result<Self> {
        let secret = secret.into();
        if secret.is_empty() {
            return Err(Error::Plan("JWT HS256 secret must be non-empty".into()));
        }
        Ok(Self {
            issuer: None,
            audience: None,
            keys: JwtKeys::Hs256(secret),
        })
    }

    /// JWKS object of `oct` keys. RSA/EC keys are ignored; verify fails when
    /// no HMAC key matches.
    pub fn from_jwks(jwks: &str) -> Result<Self> {
        let v: Value = serde_json::from_str(jwks).map_err(|e| Error::Plan(format!("JWKS: {e}")))?;
        let keys = v
            .get("keys")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::Plan("JWKS missing keys".into()))?;
        let mut map = BTreeMap::new();
        for key in keys {
            if key.get("kty").and_then(Value::as_str) != Some("oct") {
                continue;
            }
            if let Some(alg) = key.get("alg").and_then(Value::as_str)
                && alg != "HS256"
            {
                continue;
            }
            let k = key
                .get("k")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Plan("JWKS oct key missing k".into()))?;
            let secret = b64url_decode(k).map_err(|e| Error::Plan(format!("JWKS k: {e}")))?;
            if secret.is_empty() {
                return Err(Error::Plan("JWKS oct key is empty".into()));
            }
            let kid = key
                .get("kid")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            map.insert(kid, secret);
        }
        if map.is_empty() {
            return Err(Error::Plan("JWKS has no HS256 oct keys".into()));
        }
        Ok(Self {
            issuer: None,
            audience: None,
            keys: JwtKeys::Jwks(map),
        })
    }

    /// Required `iss` claim when set.
    pub fn issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = Some(issuer.into());
        self
    }

    /// Required `aud` claim when set (`aud` may be a string or array).
    pub fn audience(mut self, audience: impl Into<String>) -> Self {
        self.audience = Some(audience.into());
        self
    }

    /// Verify compact HS256 JWT and return tenant/principal/actor.
    pub fn verify(&self, token: &str) -> Result<TokenIdentity> {
        let (header, payload, sig) = split_jwt(token)?;
        let header_json: Value =
            serde_json::from_slice(&header).map_err(|e| Error::Plan(format!("JWT header: {e}")))?;
        let alg = header_json.get("alg").and_then(Value::as_str).unwrap_or("");
        if alg != "HS256" {
            return Err(Error::Plan(format!("JWT alg {alg} is not HS256")));
        }
        let kid = header_json.get("kid").and_then(Value::as_str);
        let key = self.lookup_key(kid)?;
        let signing = token
            .rsplit_once('.')
            .map(|(h, _)| h)
            .ok_or_else(|| Error::Plan("JWT is malformed".into()))?;
        let mut mac = HmacSha256::new_from_slice(key)
            .map_err(|_| Error::Plan("JWT HMAC key rejected".into()))?;
        mac.update(signing.as_bytes());
        let expected = mac.finalize().into_bytes();
        if expected.len() != sig.len() || !bool::from(expected.as_slice().ct_eq(sig.as_slice())) {
            return Err(Error::Plan("JWT signature mismatch".into()));
        }
        let claims: Value = serde_json::from_slice(&payload)
            .map_err(|e| Error::Plan(format!("JWT payload: {e}")))?;
        self.check_claims(&claims)?;
        identity_from_claims(&claims)
    }

    fn lookup_key(&self, kid: Option<&str>) -> Result<&[u8]> {
        match &self.keys {
            JwtKeys::Hs256(secret) => Ok(secret.as_slice()),
            JwtKeys::Jwks(map) => {
                if let Some(kid) = kid
                    && let Some(k) = map.get(kid)
                {
                    return Ok(k.as_slice());
                }
                if kid.is_none() && map.len() == 1 {
                    return Ok(map.values().next().expect("len 1").as_slice());
                }
                if let Some(k) = map.get("")
                    && kid.is_none()
                {
                    return Ok(k.as_slice());
                }
                Err(Error::Plan("JWT kid does not match JWKS".into()))
            }
        }
    }

    fn check_claims(&self, claims: &Value) -> Result<()> {
        if let Some(iss) = &self.issuer {
            match claims.get("iss").and_then(Value::as_str) {
                Some(got) if got == iss => {}
                _ => return Err(Error::Plan("JWT issuer mismatch".into())),
            }
        }
        if let Some(aud) = &self.audience
            && !audience_matches(claims.get("aud"), aud)
        {
            return Err(Error::Plan("JWT audience mismatch".into()));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        if let Some(exp) = claim_time(claims, "exp")?
            && now >= exp
        {
            return Err(Error::Plan("JWT expired".into()));
        }
        if let Some(nbf) = claim_time(claims, "nbf")?
            && now < nbf
        {
            return Err(Error::Plan("JWT not yet valid".into()));
        }
        Ok(())
    }
}

fn identity_from_claims(claims: &Value) -> Result<TokenIdentity> {
    let tenant = claims
        .get("tenant")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Plan("JWT missing tenant".into()))?
        .to_string();
    if tenant.is_empty() {
        return Err(Error::Plan("JWT tenant is empty".into()));
    }
    let principal = claims
        .get("sub")
        .and_then(Value::as_str)
        .or_else(|| claims.get("principal").and_then(Value::as_str))
        .ok_or_else(|| Error::Plan("JWT missing sub".into()))?
        .to_string();
    if principal.is_empty() {
        return Err(Error::Plan("JWT principal is empty".into()));
    }
    let actor = claims
        .get("actor")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| actor_chain(claims.get("act")))
        .unwrap_or_else(|| principal.clone());
    Ok(TokenIdentity {
        tenant,
        principal,
        actor,
    })
}

fn actor_chain(act: Option<&Value>) -> Option<String> {
    let mut cur = act?;
    let mut last = None;
    for _ in 0..8 {
        if let Some(sub) = cur.get("sub").and_then(Value::as_str)
            && !sub.is_empty()
        {
            last = Some(sub.to_string());
        }
        match cur.get("act") {
            Some(next) => cur = next,
            None => break,
        }
    }
    last
}

fn audience_matches(aud: Option<&Value>, want: &str) -> bool {
    match aud {
        Some(Value::String(s)) => s == want,
        Some(Value::Array(items)) => items.iter().any(|v| v.as_str() == Some(want)),
        _ => false,
    }
}

fn claim_time(claims: &Value, name: &str) -> Result<Option<f64>> {
    match claims.get(name) {
        None => Ok(None),
        Some(Value::Number(n)) => n
            .as_f64()
            .map(Some)
            .ok_or_else(|| Error::Plan(format!("JWT {name} is not numeric"))),
        Some(_) => Err(Error::Plan(format!("JWT {name} is not numeric"))),
    }
}

fn split_jwt(token: &str) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let mut parts = token.split('.');
    let h = parts
        .next()
        .ok_or_else(|| Error::Plan("JWT is malformed".into()))?;
    let p = parts
        .next()
        .ok_or_else(|| Error::Plan("JWT is malformed".into()))?;
    let s = parts
        .next()
        .ok_or_else(|| Error::Plan("JWT is malformed".into()))?;
    if parts.next().is_some() || h.is_empty() || p.is_empty() || s.is_empty() {
        return Err(Error::Plan("JWT is malformed".into()));
    }
    Ok((
        b64url_decode(h).map_err(|e| Error::Plan(format!("JWT header: {e}")))?,
        b64url_decode(p).map_err(|e| Error::Plan(format!("JWT payload: {e}")))?,
        b64url_decode(s).map_err(|e| Error::Plan(format!("JWT signature: {e}")))?,
    ))
}

fn b64url_decode(input: &str) -> std::result::Result<Vec<u8>, &'static str> {
    let mut s = input.replace('-', "+").replace('_', "/");
    while !s.len().is_multiple_of(4) {
        s.push('=');
    }
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i + 4 <= bytes.len() {
        let a = b64_val(bytes[i])?;
        let b = b64_val(bytes[i + 1])?;
        let c = b64_val(bytes[i + 2])?;
        let d = b64_val(bytes[i + 3])?;
        out.push((a << 2) | (b >> 4));
        if bytes[i + 2] != b'=' {
            out.push((b << 4) | (c >> 2));
        }
        if bytes[i + 3] != b'=' {
            out.push((c << 6) | d);
        }
        i += 4;
    }
    Ok(out)
}

fn b64_val(b: u8) -> std::result::Result<u8, &'static str> {
    match b {
        b'A'..=b'Z' => Ok(b - b'A'),
        b'a'..=b'z' => Ok(b - b'a' + 26),
        b'0'..=b'9' => Ok(b - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        b'=' => Ok(0),
        _ => Err("invalid base64"),
    }
}

#[cfg(test)]
fn b64url_encode(data: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    let mut i = 0;
    while i < data.len() {
        let b0 = data[i];
        let b1 = data.get(i + 1).copied();
        let b2 = data.get(i + 2).copied();
        out.push(T[(b0 >> 2) as usize] as char);
        out.push(T[(((b0 & 0x03) << 4) | (b1.unwrap_or(0) >> 4)) as usize] as char);
        if b1.is_some() {
            out.push(
                T[(((b1.unwrap_or(0) & 0x0f) << 2) | (b2.unwrap_or(0) >> 6)) as usize] as char,
            );
        }
        if b2.is_some() {
            out.push(T[(b2.unwrap_or(0) & 0x3f) as usize] as char);
        }
        i += 3;
    }
    out
}

/// Compact HS256 JWT for tests.
#[cfg(test)]
pub(crate) fn mint_hs256(secret: &[u8], claims: &Value) -> Result<String> {
    if secret.is_empty() {
        return Err(Error::Plan("JWT HS256 secret must be non-empty".into()));
    }
    let header = serde_json::json!({"alg":"HS256","typ":"JWT"});
    let h = b64url_encode(header.to_string().as_bytes());
    let p = b64url_encode(claims.to_string().as_bytes());
    let signing = format!("{h}.{p}");
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|_| Error::Plan("JWT HMAC key rejected".into()))?;
    mac.update(signing.as_bytes());
    let sig = b64url_encode(&mac.finalize().into_bytes());
    Ok(format!("{signing}.{sig}"))
}

/// Compact HS256 JWT with a `kid` header, for JWKS tests.
#[cfg(test)]
pub(crate) fn mint_hs256_kid(secret: &[u8], kid: &str, claims: &Value) -> Result<String> {
    if secret.is_empty() {
        return Err(Error::Plan("JWT HS256 secret must be non-empty".into()));
    }
    let header = serde_json::json!({"alg":"HS256","typ":"JWT","kid":kid});
    let h = b64url_encode(header.to_string().as_bytes());
    let p = b64url_encode(claims.to_string().as_bytes());
    let signing = format!("{h}.{p}");
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|_| Error::Plan("JWT HMAC key rejected".into()))?;
    mac.update(signing.as_bytes());
    let sig = b64url_encode(&mac.finalize().into_bytes());
    Ok(format!("{signing}.{sig}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims() -> Value {
        serde_json::json!({
            "tenant": "a",
            "sub": "user-1",
            "iss": "mizuchi",
            "aud": "query-api",
        })
    }

    #[test]
    fn hs256_round_trip_extracts_identity() {
        let v = JwtVerifier::hs256(b"secret")
            .unwrap()
            .issuer("mizuchi")
            .audience("query-api");
        let token = mint_hs256(b"secret", &claims()).unwrap();
        let id = v.verify(&token).unwrap();
        assert_eq!(
            id,
            TokenIdentity {
                tenant: "a".into(),
                principal: "user-1".into(),
                actor: "user-1".into(),
            }
        );
    }

    #[test]
    fn actor_chain_uses_innermost_sub() {
        let v = JwtVerifier::hs256(b"secret").unwrap();
        let token = mint_hs256(
            b"secret",
            &serde_json::json!({
                "tenant": "a",
                "sub": "user-1",
                "act": {"sub": "svc", "act": {"sub": "job"}}
            }),
        )
        .unwrap();
        assert_eq!(v.verify(&token).unwrap().actor, "job");
    }

    #[test]
    fn wrong_secret_or_alg_none_is_refused() {
        let v = JwtVerifier::hs256(b"secret").unwrap();
        let token = mint_hs256(b"other", &claims()).unwrap();
        assert!(v.verify(&token).is_err());
        let none = "eyJhbGciOiJub25lIn0.eyJ0ZW5hbnQiOiJhIiwic3ViIjoidSJ9.";
        assert!(v.verify(none).is_err());
    }

    #[test]
    fn issuer_audience_and_tenant_are_required_when_configured() {
        let v = JwtVerifier::hs256(b"secret")
            .unwrap()
            .issuer("mizuchi")
            .audience("query-api");
        let mut c = claims();
        c["iss"] = Value::String("other".into());
        assert!(v.verify(&mint_hs256(b"secret", &c).unwrap()).is_err());
        let mut c = claims();
        c.as_object_mut().unwrap().remove("tenant");
        assert!(v.verify(&mint_hs256(b"secret", &c).unwrap()).is_err());
    }

    #[test]
    fn jwks_oct_key_by_kid() {
        let secret = b"jwks-secret";
        let k = b64url_encode(secret);
        let jwks = format!(r#"{{"keys":[{{"kty":"oct","kid":"k1","alg":"HS256","k":"{k}"}}]}}"#);
        let v = JwtVerifier::from_jwks(&jwks)
            .unwrap()
            .issuer("mizuchi")
            .audience("query-api");
        let token = mint_hs256_kid(secret, "k1", &claims()).unwrap();
        assert_eq!(v.verify(&token).unwrap().tenant, "a");
        let wrong_kid = mint_hs256_kid(secret, "nope", &claims()).unwrap();
        assert!(v.verify(&wrong_kid).is_err());
    }

    #[test]
    fn empty_secret_is_rejected() {
        assert!(JwtVerifier::hs256(Vec::<u8>::new()).is_err());
        assert!(JwtVerifier::from_jwks(r#"{"keys":[]}"#).is_err());
    }

    #[test]
    fn fractional_exp_and_nbf_are_honoured() {
        let v = JwtVerifier::hs256(b"secret").unwrap();
        let mut expired = claims();
        expired["exp"] = serde_json::json!(0.5);
        let err = v
            .verify(&mint_hs256(b"secret", &expired).unwrap())
            .unwrap_err();
        assert!(err.to_string().contains("expired"), "{err}");
        let mut future = claims();
        future["nbf"] = serde_json::json!(4_102_444_800.5);
        let err = v
            .verify(&mint_hs256(b"secret", &future).unwrap())
            .unwrap_err();
        assert!(err.to_string().contains("not yet valid"), "{err}");
        let mut bad = claims();
        bad["exp"] = Value::String("soon".into());
        assert!(v.verify(&mint_hs256(b"secret", &bad).unwrap()).is_err());
    }
}
