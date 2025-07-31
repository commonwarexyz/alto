use alto_types::Activity;
use commonware_consensus::Reporter;
use commonware_runtime::{Handle, Spawner};

#[derive(Clone)]
pub struct Indexer<E: Spawner, I: crate::Indexer> {
    context: E,
    indexer: I,
}

impl<E: Spawner, I: crate::Indexer> Indexer<E, I> {
    pub fn new(context: E, indexer: I) -> Self {
        Self { context, indexer }
    }
}

impl<E: Spawner, I: crate::Indexer> Reporter for Indexer<E, I> {
    type Activity = Activity;

    async fn report(&mut self, activity: Self::Activity) {
        // TODO: wait for block associated with activity to upload (from marshal)
    }
}
