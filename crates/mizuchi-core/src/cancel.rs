//! Cooperative cancellation flag.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::error::{Error, Result};

/// Cooperative cancellation flag. Operators check it between morsels.
#[derive(Clone, Debug)]
pub struct CancelToken {
    flag: Arc<AtomicBool>,
    extra: Arc<Mutex<Vec<Arc<AtomicBool>>>>,
}

impl Default for CancelToken {
    fn default() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            extra: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl CancelToken {
    /// A token that is not cancelled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request that the running query stop at the next morsel boundary.
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }

    /// Whether [`Self::cancel`] has been called.
    pub fn is_cancelled(&self) -> bool {
        if self.flag.load(Ordering::SeqCst) {
            return true;
        }
        match self.extra.lock() {
            Ok(extra) => extra.iter().any(|f| f.load(Ordering::SeqCst)),
            Err(_) => true,
        }
    }

    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }

    /// A token that is cancelled when either `self` or `other` is cancelled.
    /// The merged token has its own flag, so cancelling it leaves both inputs
    /// running and concurrent queries stay isolated.
    pub fn merge(&self, other: &CancelToken) -> CancelToken {
        let mut extra = vec![self.flag.clone()];
        extra.extend(self.extra_flags());
        extra.push(other.flag.clone());
        extra.extend(other.extra_flags());
        CancelToken {
            flag: Arc::default(),
            extra: Arc::new(Mutex::new(extra)),
        }
    }

    fn extra_flags(&self) -> Vec<Arc<AtomicBool>> {
        self.extra.lock().map(|g| g.clone()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merged_token_follows_its_inputs_without_cancelling_them() {
        let session = CancelToken::new();
        let query = CancelToken::new();
        let merged = session.merge(&query);
        merged.cancel();
        assert!(merged.is_cancelled());
        assert!(
            !session.is_cancelled(),
            "cancelling the merge cancelled the session"
        );
        assert!(
            !query.is_cancelled(),
            "cancelling the merge cancelled the query"
        );

        // Either input, at any merge depth, still cancels the merged token.
        let outer = session.merge(&query).merge(&CancelToken::new());
        session.cancel();
        assert!(outer.is_cancelled());
    }
}
