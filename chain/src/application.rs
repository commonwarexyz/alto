use alto_types::Block;
use commonware_consensus::{
    threshold_simplex::types::Context, types::CodingCommitment, Application, Block as _, Epochable,
};
use commonware_cryptography::{Committable, Digestible, Hasher, Sha256};
use commonware_runtime::{Clock, Metrics, Spawner};
use commonware_utils::SystemTimeExt;
use rand::Rng;
use tracing::info;

/// Genesis message to use during initialization.
const GENESIS: &[u8] = b"commonware bit twiddling club";

#[derive(Clone)]
pub struct AltoApp {
    genesis: Block,
}

impl Default for AltoApp {
    fn default() -> Self {
        let genesis_parent = Sha256::hash(GENESIS);
        let coding_commitment = CodingCommitment::from((genesis_parent, Default::default()));
        Self {
            genesis: Block::new(coding_commitment, 0, 0),
        }
    }
}

impl<E> Application<E> for AltoApp
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
        ctx: E,
        parent_commitment: <Self::Block as Committable>::Commitment,
        parent_block: Self::Block,
    ) -> Self::Block {
        let mut current = ctx.current().epoch_millis();
        if current <= parent_block.timestamp {
            current = parent_block.timestamp + 1;
        }
        Block::new(parent_commitment, parent_block.height + 1, current)
    }

    async fn finalize(&mut self, block: Self::Block) {
        info!(
            height = block.height(),
            timestamp = block.timestamp,
            digest = %block.digest(),
            "New finalized head"
        );
    }
}
