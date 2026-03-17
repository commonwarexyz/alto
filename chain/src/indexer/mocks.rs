use super::{Block, Client as IndexerClient, Finalized, Notarized, Seed};
use commonware_cryptography::{sha256::Digest, Digestible};
use commonware_utils::{channel::oneshot, sync::Mutex};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

struct InflightGuard(Arc<AtomicUsize>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A mock indexer client for testing.
#[derive(Clone)]
pub struct Client {
    pub seed_seen: Arc<AtomicBool>,
    pub notarization_seen: Arc<AtomicBool>,
    pub finalization_seen: Arc<AtomicBool>,
    pub block_upload_started: Arc<AtomicUsize>,
    pub block_upload_completed: Arc<AtomicUsize>,
    pub block_upload_max_inflight: Arc<AtomicUsize>,
    pub block_upload_started_digests: Arc<Mutex<Vec<Digest>>>,
    pub block_upload_completed_digests: Arc<Mutex<Vec<Digest>>>,
    cert_upload_inflight: Arc<AtomicUsize>,
    cert_upload_waiters: Arc<Mutex<Vec<oneshot::Receiver<()>>>>,
    block_upload_inflight: Arc<AtomicUsize>,
    block_upload_waiters: Arc<Mutex<Vec<oneshot::Receiver<()>>>>,
    pub fail_certs: bool,
}

impl Client {
    pub fn new() -> Self {
        Self {
            seed_seen: Arc::new(AtomicBool::new(false)),
            notarization_seen: Arc::new(AtomicBool::new(false)),
            finalization_seen: Arc::new(AtomicBool::new(false)),
            block_upload_started: Arc::new(AtomicUsize::new(0)),
            block_upload_completed: Arc::new(AtomicUsize::new(0)),
            block_upload_max_inflight: Arc::new(AtomicUsize::new(0)),
            block_upload_started_digests: Arc::new(Mutex::new(Vec::new())),
            block_upload_completed_digests: Arc::new(Mutex::new(Vec::new())),
            cert_upload_inflight: Arc::new(AtomicUsize::new(0)),
            cert_upload_waiters: Arc::new(Mutex::new(Vec::new())),
            block_upload_inflight: Arc::new(AtomicUsize::new(0)),
            block_upload_waiters: Arc::new(Mutex::new(Vec::new())),
            fail_certs: false,
        }
    }

    pub fn with_fail_certs(mut self) -> Self {
        self.fail_certs = true;
        self
    }

    pub fn with_block_upload_waiters(self, waiters: Vec<oneshot::Receiver<()>>) -> Self {
        *self.block_upload_waiters.lock() = waiters.into_iter().rev().collect();
        self
    }

    pub fn with_cert_upload_waiters(self, waiters: Vec<oneshot::Receiver<()>>) -> Self {
        *self.cert_upload_waiters.lock() = waiters.into_iter().rev().collect();
        self
    }

    pub fn current_cert_upload_inflight(&self) -> usize {
        self.cert_upload_inflight.load(Ordering::SeqCst)
    }

    pub fn current_block_upload_inflight(&self) -> usize {
        self.block_upload_inflight.load(Ordering::SeqCst)
    }

    async fn wait_for_cert_upload(&self) {
        self.cert_upload_inflight.fetch_add(1, Ordering::SeqCst);
        let _guard = InflightGuard(self.cert_upload_inflight.clone());

        let waiter = self.cert_upload_waiters.lock().pop();
        if let Some(waiter) = waiter {
            let _ = waiter.await;
        }
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexerClient for Client {
    type Error = std::io::Error;

    async fn seed_upload(&self, _: Seed) -> Result<(), Self::Error> {
        self.seed_seen.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn notarized_upload(&self, _: Notarized) -> Result<(), Self::Error> {
        if self.fail_certs {
            return Err(std::io::Error::other("cert upload disabled"));
        }
        self.wait_for_cert_upload().await;
        self.notarization_seen.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn finalized_upload(&self, _: Finalized) -> Result<(), Self::Error> {
        if self.fail_certs {
            return Err(std::io::Error::other("cert upload disabled"));
        }
        self.wait_for_cert_upload().await;
        self.finalization_seen.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn block_upload(&self, block: Block) -> Result<(), Self::Error> {
        let digest = block.digest();
        self.block_upload_started.fetch_add(1, Ordering::SeqCst);
        self.block_upload_started_digests.lock().push(digest);
        let inflight = self.block_upload_inflight.fetch_add(1, Ordering::SeqCst) + 1;
        self.block_upload_max_inflight
            .fetch_max(inflight, Ordering::SeqCst);
        let _guard = InflightGuard(self.block_upload_inflight.clone());

        let waiter = self.block_upload_waiters.lock().pop();
        if let Some(waiter) = waiter {
            let _ = waiter.await;
        }

        self.block_upload_completed.fetch_add(1, Ordering::SeqCst);
        self.block_upload_completed_digests.lock().push(digest);
        Ok(())
    }
}
