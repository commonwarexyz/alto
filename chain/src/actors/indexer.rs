use alto_types::Activity;
use commonware_consensus::Reporter;

#[derive(Clone)]
pub struct Indexer<I: crate::Indexer> {
    indexer: I,
}

impl<I: crate::Indexer> Reporter for Indexer<I> {
    type Activity = Activity;

    async fn report(&mut self, activity: Self::Activity) {
        // TODO: wait for block associated with activity to upload (from marshal)
    }
}
