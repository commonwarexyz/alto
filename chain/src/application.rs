use std::marker::PhantomData;

use alto_types::Block;
use commonware_consensus::{
    threshold_simplex::types::Context, types::CodingCommitment, Application, Block as _, Epochable,
};
use commonware_cryptography::{Committable, Digestible, Hasher, Sha256};
use commonware_runtime::{Clock, Metrics, Spawner};
use commonware_utils::SystemTimeExt;
use prometheus_client::metrics::gauge::Gauge;
use rand::Rng;
use tracing::info;

/// Genesis message to use during initialization.
const GENESIS: &[u8] = b"commonware bit twiddling club";

#[derive(Clone)]
pub struct AltoApp<E: Rng + Clock + Metrics + Spawner> {
    genesis: Block,
    bandwidth: Gauge,

    _context: PhantomData<E>,
}

impl<E: Rng + Clock + Metrics + Spawner> AltoApp<E> {
    pub fn new(context: E) -> Self {
        // Setup genesis
        let genesis_parent = Sha256::hash(GENESIS);
        let coding_commitment = CodingCommitment::from((genesis_parent, Default::default()));

        // Setup bandwidth gauge
        let bandwidth = Gauge::default();
        context.register("bandwidth", "Finalized block bandwidth", bandwidth.clone());

        Self {
            genesis: Block::new(coding_commitment, 0, 0, Vec::new()),
            bandwidth,

            _context: PhantomData,
        }
    }
}

impl<E> Application<E> for AltoApp<E>
where
    E: Rng + Clock + Metrics + Spawner,
{
    type Context = Context<CodingCommitment>;
    type Block = Block;

    async fn genesis(&mut self, _: <Self::Context as Epochable>::Epoch) -> Self::Block {
        self.genesis.clone()
    }

    async fn build(
        &mut self,
        mut ctx: E,
        parent_commitment: <Self::Block as Committable>::Commitment,
        parent_block: Self::Block,
    ) -> Self::Block {
        let mut current = ctx.current().epoch_millis();
        if current <= parent_block.timestamp {
            current = parent_block.timestamp + 1;
        }

        // Add 1 MiB of junk data to the block.
        let mut junk = vec![0u8; 1024 * 1024];
        ctx.fill_bytes(&mut junk);

        Block::new(parent_commitment, parent_block.height + 1, current, junk)
    }

    async fn finalize(&mut self, block: Self::Block) {
        info!(
            height = block.height(),
            timestamp = block.timestamp,
            digest = %block.digest(),
            "New finalized head"
        );
        self.bandwidth.inc_by(block.junk.len() as i64);
    }
}
