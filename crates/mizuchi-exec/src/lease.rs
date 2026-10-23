//! Coordinator leadership: a lease in a shared store with a fencing token.
//!
//! One coordinator holds the lease at a time. Every takeover raises the
//! fencing token, every task assignment carries it, and a worker refuses a
//! token lower than the highest it has seen. Lease timing alone decides who
//! may *start* work; the token is what keeps a paused or partitioned former
//! leader from finishing any, whatever the clocks say.
//!
//! [`MemoryLeaseStore`] serves tests and single-process hosts.
//! [`DirLeaseStore`] keeps the lease in a directory that every coordinator
//! mounts (the compose and Kubernetes layouts share one volume).

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mizuchi_core::error::{Error, Result};

/// How long a damaged lease file still counts as live after its last write.
const TORN_GRACE: Duration = Duration::from_secs(60);

/// One grant of the lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub holder: String,
    /// Raised on every takeover; never reused.
    pub token: u64,
    /// Wall-clock expiry in milliseconds since the Unix epoch.
    pub expires_at_ms: u64,
}

/// Where coordinators contend for the lease.
pub trait LeaseStore: Send + Sync {
    /// Renews the lease when `holder` holds the current token, or takes it
    /// with a new, higher token when it is free or expired. `None` while
    /// another holder's lease is live.
    fn acquire(&self, holder: &str, ttl: Duration, now_ms: u64) -> Result<Option<Lease>>;

    /// Ends `token` early so a standby need not wait for it to expire. A
    /// token that is no longer current is left alone.
    fn release(&self, holder: &str, token: u64) -> Result<()>;
}

/// A store shared by several coordinators in one process.
impl<T: LeaseStore + ?Sized> LeaseStore for std::sync::Arc<T> {
    fn acquire(&self, holder: &str, ttl: Duration, now_ms: u64) -> Result<Option<Lease>> {
        (**self).acquire(holder, ttl, now_ms)
    }

    fn release(&self, holder: &str, token: u64) -> Result<()> {
        (**self).release(holder, token)
    }
}

/// In-process lease, for tests and single-process hosts.
#[derive(Debug, Default)]
pub struct MemoryLeaseStore {
    current: Mutex<Option<Lease>>,
}

impl LeaseStore for MemoryLeaseStore {
    fn acquire(&self, holder: &str, ttl: Duration, now_ms: u64) -> Result<Option<Lease>> {
        let mut current = self.current.lock().unwrap_or_else(|e| e.into_inner());
        let next = next_grant(current.as_ref(), holder, ttl, now_ms);
        if let Some(lease) = &next {
            *current = Some(lease.clone());
        }
        Ok(next)
    }

    fn release(&self, holder: &str, token: u64) -> Result<()> {
        let mut current = self.current.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(lease) = current.as_mut()
            && lease.holder == holder
            && lease.token == token
        {
            lease.expires_at_ms = 0;
        }
        Ok(())
    }
}

/// The grant `holder` gets against `current`: a renewal of its own token, a
/// takeover with the next token, or none while someone else's lease is live.
fn next_grant(current: Option<&Lease>, holder: &str, ttl: Duration, now_ms: u64) -> Option<Lease> {
    let expires_at_ms = now_ms.saturating_add(ttl.as_millis() as u64);
    match current {
        Some(lease) if lease.holder == holder && lease.expires_at_ms > now_ms => Some(Lease {
            expires_at_ms,
            ..lease.clone()
        }),
        Some(lease) if lease.expires_at_ms > now_ms => None,
        other => Some(Lease {
            holder: holder.to_string(),
            token: other.map_or(1, |lease| lease.token + 1),
            expires_at_ms,
        }),
    }
}

/// A lease kept in a shared directory as a sequence of version files.
///
/// Every transition (a takeover, a renewal, a release) claims the next
/// version, `lease-<version>`, by publishing a synced staging file with a hard
/// link, which fails if that version exists. Two coordinators acting on the
/// same version therefore cannot both succeed: a renewal and a takeover that
/// race are decided by the link, and the loser re-reads the winner's lease.
/// The highest version is the current lease.
#[derive(Debug, Clone)]
pub struct DirLeaseStore {
    dir: PathBuf,
}

/// The lease at one version.
struct Versioned {
    version: u64,
    lease: Lease,
}

impl DirLeaseStore {
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(|e| io(&dir, e))?;
        Ok(Self { dir })
    }

    fn path(&self, version: u64) -> PathBuf {
        self.dir.join(format!("lease-{version:020}"))
    }

    fn versions(&self) -> Result<Vec<u64>> {
        let mut versions = Vec::new();
        for entry in std::fs::read_dir(&self.dir).map_err(|e| io(&self.dir, e))? {
            let entry = entry.map_err(|e| io(&self.dir, e))?;
            if let Some(version) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix("lease-"))
                .and_then(|version| version.parse::<u64>().ok())
            {
                versions.push(version);
            }
        }
        versions.sort_unstable();
        Ok(versions)
    }

    /// The highest version and its lease.
    ///
    /// A takeover's token is the version it claims, and a renewal keeps its
    /// token, so no token ever exceeds its version: the highest version is an
    /// upper bound on every token granted. Versions are published whole, so
    /// one that cannot be parsed is damaged; it counts as a live lease with
    /// its version as token until [`TORN_GRACE`] after it was written, so it
    /// delays a takeover instead of blocking it, and the next token still
    /// exceeds every earlier one. A version pruned between listing and
    /// reading is not damage: the directory is listed again.
    fn current(&self) -> Result<Option<Versioned>> {
        loop {
            let versions = self.versions()?;
            let Some(&version) = versions.last() else {
                return Ok(None);
            };
            let path = self.path(version);
            let text = match std::fs::read_to_string(&path) {
                Ok(text) => text,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(io(&path, e)),
            };
            if let Some(lease) = parse(&text) {
                return Ok(Some(Versioned { version, lease }));
            }
            let expires_at_ms = std::fs::metadata(&path)
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|written| written.duration_since(UNIX_EPOCH).ok())
                .map_or(u64::MAX, |written| {
                    (written + TORN_GRACE).as_millis() as u64
                });
            return Ok(Some(Versioned {
                version,
                lease: Lease {
                    holder: String::new(),
                    token: version,
                    expires_at_ms,
                },
            }));
        }
    }

    /// Writes `lease` to a new temporary file and syncs it. The name carries
    /// the holder and a timestamp, and the file is created exclusively, so
    /// coordinators in separate containers (same pid, same counter) never
    /// share a staging file.
    fn stage(&self, lease: &Lease) -> Result<PathBuf> {
        use std::io::Write;
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let holder: String = lease
            .holder
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let tmp = self.dir.join(format!(
            ".lease-{}-{holder}-{}-{stamp}-{}.tmp",
            lease.token,
            std::process::id(),
            unique()
        ));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .map_err(|e| io(&tmp, e))?;
        file.write_all(
            format!(
                "{}\n{}\n{}\n",
                lease.holder, lease.token, lease.expires_at_ms
            )
            .as_bytes(),
        )
        .and_then(|()| file.sync_all())
        .map_err(|e| io(&tmp, e))?;
        Ok(tmp)
    }

    /// Publishes `lease` as `version`. False when another coordinator
    /// published that version first.
    fn claim(&self, version: u64, lease: &Lease) -> Result<bool> {
        let tmp = self.stage(lease)?;
        let target = self.path(version);
        let linked = std::fs::hard_link(&tmp, &target);
        let _ = std::fs::remove_file(&tmp);
        match linked {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
            Err(e) => return Err(io(&target, e)),
        }
        // Old versions are pruned, so a contender paused long enough could
        // re-create one. A version below the highest is a stale claim: it is
        // withdrawn and lost, and the current lease is untouched.
        if self
            .versions()?
            .last()
            .is_some_and(|&highest| highest > version)
        {
            let _ = std::fs::remove_file(&target);
            return Ok(false);
        }
        // Durable before acknowledged, so a grant survives a crash of the
        // host that serves the directory.
        std::fs::File::open(&self.dir)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| io(&self.dir, e))?;
        for old in version.saturating_sub(16)..version.saturating_sub(8) {
            let _ = std::fs::remove_file(self.path(old));
        }
        Ok(true)
    }

    /// The current fencing token, or 0 before any lease. Workers that mount
    /// the lease directory check assignments against it, so a restarted
    /// worker cannot admit a token that was superseded while it was down.
    pub fn current_token(&self) -> Result<u64> {
        Ok(self.current()?.map_or(0, |current| current.lease.token))
    }
}

impl LeaseStore for DirLeaseStore {
    fn acquire(&self, holder: &str, ttl: Duration, now_ms: u64) -> Result<Option<Lease>> {
        if holder.is_empty() || holder.contains('\n') {
            return Err(Error::Plan(
                "lease holder must be one non-empty line".into(),
            ));
        }
        let current = self.current()?;
        let Some(mut next) = next_grant(current.as_ref().map(|c| &c.lease), holder, ttl, now_ms)
        else {
            return Ok(None);
        };
        let version = current.as_ref().map_or(1, |current| current.version + 1);
        if current.is_none_or(|current| current.lease.token != next.token) {
            // A takeover's token is its version, above every token granted.
            next.token = version;
        }
        // A competing renewal or takeover that published this version first
        // wins; this coordinator re-reads and acts on that lease next time.
        Ok(self.claim(version, &next)?.then_some(next))
    }

    fn release(&self, holder: &str, token: u64) -> Result<()> {
        if let Some(current) = self.current()?
            && current.lease.holder == holder
            && current.lease.token == token
        {
            let released = Lease {
                expires_at_ms: 0,
                ..current.lease
            };
            // Losing this race means the lease already moved on.
            self.claim(current.version + 1, &released)?;
        }
        Ok(())
    }
}

/// A version file's lease: holder, token and expiry, one per line.
fn parse(text: &str) -> Option<Lease> {
    let mut lines = text.lines();
    let holder = lines.next()?.to_string();
    let token = lines.next()?.parse().ok()?;
    let expires_at_ms = lines.next()?.parse().ok()?;
    Some(Lease {
        holder,
        token,
        expires_at_ms,
    })
}

fn io(path: &Path, error: std::io::Error) -> Error {
    Error::Io(format!("lease {}: {error}", path.display()))
}

fn unique() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

/// What a coordinator reports about its leadership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderStatus {
    pub leader: bool,
    /// The fencing token last held, or 0 if never.
    pub token: u64,
    /// Time since the lease was last renewed, while leader.
    pub lease_age: Option<Duration>,
}

/// One coordinator's side of the lease.
///
/// A `Leadership` leads under one generation: the first token it is granted.
/// Once it loses that lease it stays fenced, even if the lease later comes
/// free again; leading again takes a new `Leadership`, so work started under
/// the old generation can never continue under a new token.
pub struct Leadership {
    store: Box<dyn LeaseStore>,
    holder: String,
    ttl: Duration,
    state: Mutex<Generation>,
}

#[derive(Default)]
struct Generation {
    /// The token this leadership leads under; 0 before it first leads.
    token: u64,
    /// When the lease was last renewed, while held.
    renewed: Option<Instant>,
    lost: bool,
}

impl Leadership {
    /// `label` names the coordinator (a host name, say). The holder recorded
    /// in the lease adds this incarnation's process id and start time, so two
    /// processes with one label, or a restarted one, never share a lease.
    pub fn new(store: Box<dyn LeaseStore>, label: impl Into<String>, ttl: Duration) -> Self {
        let started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        Self {
            store,
            holder: format!(
                "{}#{}-{started}-{}",
                label.into(),
                std::process::id(),
                unique()
            ),
            ttl,
            state: Mutex::new(Generation::default()),
        }
    }

    /// The holder recorded in the lease.
    pub fn holder(&self) -> &str {
        &self.holder
    }

    /// Takes or renews the lease. `None` while another coordinator leads, or
    /// once this leadership has lost its generation.
    pub fn try_lead(&self) -> Result<Option<u64>> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lost {
            return Ok(None);
        }
        let granted = match self.store.acquire(&self.holder, self.ttl, now_ms()) {
            Ok(granted) => granted,
            Err(error) => {
                // Unrenewed, the lease may lapse; stop reporting it as held.
                state.renewed = None;
                return Err(error);
            }
        };
        match granted {
            Some(lease) if state.token == 0 || lease.token == state.token => {
                state.token = lease.token;
                state.renewed = Some(Instant::now());
                Ok(Some(lease.token))
            }
            Some(lease) => {
                // The lease lapsed and came back with a new token: that is a
                // new generation, not ours. Hand it straight back.
                state.lost = true;
                state.renewed = None;
                self.store.release(&self.holder, lease.token)?;
                Ok(None)
            }
            None => {
                if state.token != 0 {
                    state.lost = true;
                }
                state.renewed = None;
                Ok(None)
            }
        }
    }

    /// Waits as a standby until the lease is ours, checking every `poll`.
    pub fn lead(&self, poll: Duration) -> Result<u64> {
        loop {
            if let Some(token) = self.try_lead()? {
                return Ok(token);
            }
            std::thread::sleep(poll);
        }
    }

    /// Renews the lease before an assignment and returns the token to send.
    /// A coordinator that lost the lease, or never led, is fenced.
    pub fn assignment_token(&self) -> Result<u64> {
        self.try_lead()?.ok_or_else(|| {
            Error::Fenced(format!(
                "{} does not hold the coordinator lease",
                self.holder
            ))
        })
    }

    /// Renews the lease every `every` on a background thread, so a live but
    /// idle leader keeps it. Stops when the guard drops or the lease is lost.
    /// A paused or partitioned process stops renewing, and the fencing token
    /// keeps its late work out after a standby takes over.
    pub fn keep_alive(self: &std::sync::Arc<Self>, every: Duration) -> KeepAlive {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handle = {
            let leadership = self.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                    std::thread::sleep(every);
                    if stop.load(std::sync::atomic::Ordering::SeqCst) {
                        break;
                    }
                    // A store error is retried; a lost lease ends renewal.
                    if let Ok(None) = leadership.try_lead() {
                        break;
                    }
                }
            })
        };
        KeepAlive {
            stop,
            handle: Some(handle),
        }
    }

    /// Gives up the lease so a standby takes over without waiting. This
    /// leadership is fenced afterwards.
    pub fn release(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let held = state.renewed.take().is_some();
        state.lost = state.token != 0;
        if held {
            self.store.release(&self.holder, state.token)?;
        }
        Ok(())
    }

    /// Leader only within the lease's TTL of the last successful renewal.
    pub fn status(&self) -> LeaderStatus {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        LeaderStatus {
            leader: !state.lost
                && state
                    .renewed
                    .is_some_and(|renewed| renewed.elapsed() < self.ttl),
            token: state.token,
            lease_age: state.renewed.map(|renewed| renewed.elapsed()),
        }
    }
}

/// Background lease renewal from [`Leadership::keep_alive`]; stops on drop.
pub struct KeepAlive {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for KeepAlive {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Worker side of the fence: the highest token seen so far, and optionally
/// the lease directory as the authority on the current token.
///
/// Admission and publication share one lock, so a successor's assignment
/// cannot raise the fence between an old assignment's check and its write.
#[derive(Debug, Default)]
pub struct Fence {
    highest: Mutex<u64>,
    authority: Option<DirLeaseStore>,
}

impl Fence {
    /// A fence that also refuses tokens below the lease directory's current
    /// token. A worker that mounts the lease directory cannot be fooled by a
    /// delayed assignment after a restart cleared its own high-water mark.
    pub fn with_authority(authority: DirLeaseStore) -> Self {
        Self {
            highest: Mutex::new(0),
            authority: Some(authority),
        }
    }

    /// Refuses `token` unless it is current. With a lease directory the
    /// current generation is exactly the directory's token, so a token from
    /// a superseded or a not-yet-existing generation is refused; without one
    /// it is any token at or above the high-water mark. A lease directory
    /// that cannot be read fails the check closed.
    fn check(&self, token: Option<u64>, highest: u64) -> Result<()> {
        let authoritative = match &self.authority {
            Some(store) => Some(
                store
                    .current_token()
                    .map_err(|e| Error::Fenced(format!("lease authority unreadable: {e}")))?,
            ),
            None => None,
        };
        let floor = authoritative.unwrap_or(0).max(highest);
        match token {
            Some(token) if token < floor => Err(Error::Fenced(format!(
                "stale fencing token {token}; current is {floor}"
            ))),
            Some(token) if authoritative.is_some_and(|current| token != current) => {
                Err(Error::Fenced(format!(
                    "fencing token {token} is not the current generation {floor}"
                )))
            }
            None if floor > 0 => Err(Error::Fenced(format!(
                "unfenced assignment; current token is {floor}"
            ))),
            _ => Ok(()),
        }
    }

    /// Admits an assignment carrying `token` (`None` for an unfenced
    /// coordinator) and raises the high-water mark. Once any fenced
    /// coordinator has been seen, unfenced assignments are refused too.
    pub fn admit(&self, token: Option<u64>) -> Result<()> {
        let mut highest = self.highest.lock().unwrap_or_else(|e| e.into_inner());
        self.check(token, *highest)?;
        if let Some(token) = token {
            *highest = (*highest).max(token);
        }
        Ok(())
    }

    /// Runs `publish` only while `token` is still current, holding the fence
    /// so it cannot advance until the write is done.
    pub fn publish<T>(&self, token: Option<u64>, publish: impl FnOnce() -> T) -> Result<T> {
        let highest = self.highest.lock().unwrap_or_else(|e| e.into_inner());
        self.check(token, *highest)?;
        Ok(publish())
    }

    /// Re-checks `token` at a commit point that writes nothing.
    pub fn still_current(&self, token: Option<u64>) -> Result<()> {
        self.publish(token, || ())
    }

    pub fn highest(&self) -> u64 {
        *self.highest.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_millis(1000);

    fn stores() -> Vec<(&'static str, Box<dyn LeaseStore>)> {
        let dir =
            std::env::temp_dir().join(format!("mizuchi-lease-{}-{}", std::process::id(), unique()));
        vec![
            ("memory", Box::new(MemoryLeaseStore::default())),
            ("dir", Box::new(DirLeaseStore::new(dir).unwrap())),
        ]
    }

    #[test]
    fn one_holder_at_a_time_and_every_takeover_raises_the_token() {
        for (name, store) in stores() {
            let a = store.acquire("a", TTL, 10_000).unwrap().unwrap();
            assert_eq!(a.token, 1, "{name}");
            assert_eq!(store.acquire("b", TTL, 10_500).unwrap(), None, "{name}");
            // Renewal keeps the token and moves the expiry.
            let renewed = store.acquire("a", TTL, 10_900).unwrap().unwrap();
            assert_eq!(
                (renewed.token, renewed.expires_at_ms),
                (1, 11_900),
                "{name}"
            );
            assert_eq!(store.acquire("b", TTL, 11_500).unwrap(), None, "{name}");
            // Expired: the standby takes over with a higher token.
            let b = store.acquire("b", TTL, 11_900).unwrap().unwrap();
            assert_eq!(b.holder, "b", "{name}");
            assert!(b.token > a.token, "{name}");
            // The old leader cannot renew its way back to token 1.
            assert_eq!(store.acquire("a", TTL, 12_000).unwrap(), None, "{name}");
            store.release("b", b.token).unwrap();
            let again = store.acquire("a", TTL, 12_100).unwrap().unwrap();
            assert!(again.token > b.token, "{name}");
            // Releasing a superseded token changes nothing.
            store.release("b", b.token).unwrap();
            assert_eq!(store.acquire("b", TTL, 12_200).unwrap(), None, "{name}");
        }
    }

    #[test]
    fn racing_coordinators_on_a_shared_directory_get_one_token_each() {
        let dir = std::env::temp_dir().join(format!(
            "mizuchi-lease-race-{}-{}",
            std::process::id(),
            unique()
        ));
        let winners: Vec<Lease> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|i| {
                    let dir = dir.clone();
                    scope.spawn(move || {
                        DirLeaseStore::new(dir)
                            .unwrap()
                            .acquire(&format!("c{i}"), TTL, 50_000)
                            .unwrap()
                    })
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|h| h.join().unwrap())
                .collect()
        });
        assert_eq!(winners.len(), 1, "{winners:?}");
        assert_eq!(winners[0].token, 1);
        let current = DirLeaseStore::new(&dir)
            .unwrap()
            .current()
            .unwrap()
            .unwrap();
        assert_eq!(current.lease, winners[0]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_renewal_and_a_takeover_on_one_version_cannot_both_win() {
        let dir = std::env::temp_dir().join(format!(
            "mizuchi-lease-cas-{}-{}",
            std::process::id(),
            unique()
        ));
        let store = DirLeaseStore::new(&dir).unwrap();
        store.acquire("a", TTL, 1_000).unwrap().unwrap();
        // Both read version 1; the renewal publishes version 2 first.
        let renewal = Lease {
            holder: "a".into(),
            token: 1,
            expires_at_ms: 2_500,
        };
        assert!(store.claim(2, &renewal).unwrap());
        // The standby's takeover, decided on its stale read, loses the link.
        let takeover = Lease {
            holder: "b".into(),
            token: 2,
            expires_at_ms: 3_000,
        };
        assert!(!store.claim(2, &takeover).unwrap());
        assert_eq!(store.current().unwrap().unwrap().lease, renewal);
        assert_eq!(store.acquire("b", TTL, 2_100).unwrap(), None);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_contender_that_reclaims_a_pruned_version_loses() {
        let dir = std::env::temp_dir().join(format!(
            "mizuchi-lease-pruned-{}-{}",
            std::process::id(),
            unique()
        ));
        let store = DirLeaseStore::new(&dir).unwrap();
        for at in 0..20 {
            store.acquire("a", TTL, 1_000 + at).unwrap().unwrap();
        }
        assert!(!store.path(2).exists(), "version 2 is pruned");
        let stale = Lease {
            holder: "b".into(),
            token: 2,
            expires_at_ms: 9_000,
        };
        assert!(!store.claim(2, &stale).unwrap());
        assert!(!store.path(2).exists());
        assert_eq!(store.current().unwrap().unwrap().lease.holder, "a");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_torn_claim_counts_as_taken() {
        let dir = std::env::temp_dir().join(format!(
            "mizuchi-lease-torn-{}-{}",
            std::process::id(),
            unique()
        ));
        let store = DirLeaseStore::new(&dir).unwrap();
        std::fs::write(store.path(4), "").unwrap();
        let now = now_ms();
        assert_eq!(store.acquire("a", TTL, now).unwrap(), None);
        // Past the grace, the damaged version is superseded, with a token
        // above any it may have held.
        let later = now + TORN_GRACE.as_millis() as u64 + 1000;
        assert_eq!(store.acquire("a", TTL, later).unwrap().unwrap().token, 5);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_coordinator_that_lost_the_lease_is_fenced() {
        let store = std::sync::Arc::new(MemoryLeaseStore::default());
        let a = Leadership::new(Box::new(store.clone()), "a", TTL);
        let b = Leadership::new(Box::new(store), "b", TTL);
        assert_eq!(a.assignment_token().unwrap(), 1);
        assert!(a.status().leader);
        a.release().unwrap();
        assert_eq!(b.try_lead().unwrap(), Some(2));
        assert!(matches!(a.assignment_token(), Err(Error::Fenced(_))));
        assert_eq!(b.status().token, 2);
    }

    #[test]
    fn two_incarnations_with_one_label_do_not_share_the_lease() {
        let store = std::sync::Arc::new(MemoryLeaseStore::default());
        let first = Leadership::new(Box::new(store.clone()), "host", TTL);
        let second = Leadership::new(Box::new(store), "host", TTL);
        assert_ne!(first.holder(), second.holder());
        assert_eq!(first.try_lead().unwrap(), Some(1));
        assert_eq!(second.try_lead().unwrap(), None);
    }

    #[test]
    fn a_kept_alive_leader_holds_the_lease_while_idle() {
        let store = std::sync::Arc::new(MemoryLeaseStore::default());
        let ttl = Duration::from_millis(60);
        let leader = std::sync::Arc::new(Leadership::new(Box::new(store.clone()), "a", ttl));
        let standby = Leadership::new(Box::new(store), "b", ttl);
        assert_eq!(leader.try_lead().unwrap(), Some(1));
        {
            let _alive = leader.keep_alive(ttl / 4);
            std::thread::sleep(ttl * 3);
            assert_eq!(standby.try_lead().unwrap(), None);
        }
        // Without renewal the lease lapses and the standby takes over.
        std::thread::sleep(ttl * 2);
        assert_eq!(standby.try_lead().unwrap(), Some(2));
    }

    #[test]
    fn a_lapsed_leader_does_not_lead_again_under_a_new_token() {
        let store = std::sync::Arc::new(MemoryLeaseStore::default());
        let ttl = Duration::from_millis(20);
        let leader = Leadership::new(Box::new(store.clone()), "a", ttl);
        assert_eq!(leader.assignment_token().unwrap(), 1);
        std::thread::sleep(ttl * 2);
        // Unrenewed past its TTL, the lease is no longer reported as held.
        assert!(!leader.status().leader);
        // The lease lapsed; renewing would grant token 2, a new generation.
        assert_eq!(leader.try_lead().unwrap(), None);
        assert!(matches!(leader.assignment_token(), Err(Error::Fenced(_))));
        assert!(!leader.status().leader);
        // The token it was handed is given straight back.
        let standby = Leadership::new(Box::new(store), "b", ttl);
        assert_eq!(standby.try_lead().unwrap(), Some(3));
    }

    #[test]
    fn a_fence_backed_by_the_lease_directory_survives_a_restart() {
        let dir = std::env::temp_dir().join(format!(
            "mizuchi-lease-fence-{}-{}",
            std::process::id(),
            unique()
        ));
        let store = DirLeaseStore::new(&dir).unwrap();
        store.acquire("a", TTL, 1).unwrap().unwrap();
        let b = store.acquire("b", TTL, 5_000).unwrap().unwrap();
        assert_eq!(b.token, 2);
        // A freshly started worker has no high-water mark of its own.
        let fence = Fence::with_authority(DirLeaseStore::new(&dir).unwrap());
        assert!(matches!(fence.admit(Some(1)), Err(Error::Fenced(_))));
        assert!(matches!(fence.admit(None), Err(Error::Fenced(_))));
        // A generation that does not exist yet cannot poison the fence.
        assert!(matches!(fence.admit(Some(u64::MAX)), Err(Error::Fenced(_))));
        fence.admit(Some(2)).unwrap();
        assert_eq!(fence.highest(), 2);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn workers_refuse_stale_and_unfenced_assignments() {
        let fence = Fence::default();
        fence.admit(None).unwrap();
        fence.admit(Some(1)).unwrap();
        fence.admit(Some(2)).unwrap();
        assert!(matches!(fence.admit(Some(1)), Err(Error::Fenced(_))));
        assert!(matches!(fence.admit(None), Err(Error::Fenced(_))));
        fence.admit(Some(2)).unwrap();
        assert!(matches!(
            fence.still_current(Some(1)),
            Err(Error::Fenced(_))
        ));
        fence.still_current(Some(2)).unwrap();
        assert_eq!(fence.highest(), 2);
    }
}
