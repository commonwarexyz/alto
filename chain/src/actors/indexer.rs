use alto_types::{Activity, Block};
use commonware_consensus::{marshal, Reporter};
use commonware_cryptography::bls12381::primitives::variant::MinSig;
use commonware_runtime::{Handle, Spawner};

#[derive(Clone)]
pub struct Indexer<E: Spawner, I: crate::Indexer> {
    context: E,
    indexer: I,
    marshal: marshal::ingress::mailbox::Mailbox<MinSig, Block>,
}

impl<E: Spawner, I: crate::Indexer> Indexer<E, I> {
    pub fn new(
        context: E,
        indexer: I,
        marshal: marshal::ingress::mailbox::Mailbox<MinSig, Block>,
    ) -> Self {
        Self {
            context,
            indexer,
            marshal,
        }
    }
}

impl<E: Spawner, I: crate::Indexer> Reporter for Indexer<E, I> {
    type Activity = Activity;

    async fn report(&mut self, activity: Self::Activity) {
        unimplemented!()
    }
}
