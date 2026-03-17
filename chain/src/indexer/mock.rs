use super::Indexer;
use alto_types::{Block, Finalized, Identity, Notarized, Seed};
use commonware_cryptography::{sha256::Digest, Digestible};
use commonware_utils::{channel::oneshot, sync::Mutex};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize},
    Arc,
};

/// A mock indexer implementation for testing.
#[derive(Clone)]
pub struct Mock {
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

impl Mock {
    pub fn new(_: &str, _: Identity) -> Self {
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
        self.cert_upload_inflight
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn current_block_upload_inflight(&self) -> usize {
        self.block_upload_inflight
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn wait_for_cert_upload(&self) {
        struct InflightGuard(Arc<AtomicUsize>);

        impl Drop for InflightGuard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        self.cert_upload_inflight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _guard = InflightGuard(self.cert_upload_inflight.clone());

        let waiter = self.cert_upload_waiters.lock().pop();
        if let Some(waiter) = waiter {
            let _ = waiter.await;
        }
    }
}

impl Indexer for Mock {
    type Error = std::io::Error;

    async fn seed_upload(&self, _: Seed) -> Result<(), Self::Error> {
        self.seed_seen
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn notarized_upload(&self, _: Notarized) -> Result<(), Self::Error> {
        if self.fail_certs {
            return Err(std::io::Error::other("cert upload disabled"));
        }
        self.wait_for_cert_upload().await;
        self.notarization_seen
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn finalized_upload(&self, _: Finalized) -> Result<(), Self::Error> {
        if self.fail_certs {
            return Err(std::io::Error::other("cert upload disabled"));
        }
        self.wait_for_cert_upload().await;
        self.finalization_seen
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn block_upload(&self, block: Block) -> Result<(), Self::Error> {
        struct InflightGuard(Arc<AtomicUsize>);

        impl Drop for InflightGuard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let digest = block.digest();
        self.block_upload_started
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.block_upload_started_digests.lock().push(digest);
        let inflight = self
            .block_upload_inflight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        self.block_upload_max_inflight
            .fetch_max(inflight, std::sync::atomic::Ordering::SeqCst);
        let _guard = InflightGuard(self.block_upload_inflight.clone());

        let waiter = self.block_upload_waiters.lock().pop();
        if let Some(waiter) = waiter {
            let _ = waiter.await;
        }

        self.block_upload_completed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.block_upload_completed_digests.lock().push(digest);
        Ok(())
    }
}
