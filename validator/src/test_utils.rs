use alto_chain::indexer::Indexer;
use alto_types::{Finalized, Identity, Notarized, Seed};
use std::{future::Future, sync::Arc};
use std::sync::atomic::AtomicBool;

#[derive(Clone)]
pub struct MockIndexer {
    pub seed_seen: Arc<AtomicBool>,
    pub notarization_seen: Arc<AtomicBool>,
    pub finalization_seen: Arc<AtomicBool>,
}

impl MockIndexer {
    pub fn new(_: &str, _: Identity) -> Self {
        Self {
            seed_seen: Arc::new(AtomicBool::new(false)),
            notarization_seen: Arc::new(AtomicBool::new(false)),
            finalization_seen: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Indexer for MockIndexer {
    type Error = std::io::Error;

    fn seed_upload(&self, _: Seed) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.seed_seen
            .store(true, std::sync::atomic::Ordering::Relaxed);
        async { Ok(()) }
    }

    fn notarized_upload(
        &self,
        _: Notarized,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.notarization_seen
            .store(true, std::sync::atomic::Ordering::Relaxed);
        async { Ok(()) }
    }

    fn finalized_upload(
        &self,
        _: Finalized,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.finalization_seen
            .store(true, std::sync::atomic::Ordering::Relaxed);
        async { Ok(()) }
    }
}
