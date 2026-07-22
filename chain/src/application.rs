use crate::indexer;
use alto_types::{Block, Context, Scheme, EPOCH};
use commonware_actor::Feedback;
use commonware_consensus::{
    marshal::{ancestry::Ancestry, Update},
    types::{Height, Round, View},
    Heightable, Reporter,
};
use commonware_cryptography::{ed25519, sha256, Digest as _, Digestible, Hasher, Sha256, Signer};
use commonware_runtime::{BufferPooler, Clock, Metrics, Spawner, Storage};
use commonware_utils::{Acknowledgement, SystemTimeExt};
use futures::StreamExt;
use rand::Rng;
use std::time::{Duration, SystemTime};
use tracing::info;

/// Genesis message to use during initialization.
const GENESIS: &[u8] = b"commonware is neat";

/// Fixed consensus cutoff for block timestamps: 2200-01-01T00:00:00Z.
///
/// Different platforms have different `SystemTime` limits, so we use a fixed
/// timestamp to ensure consistent application of block validity rules.
const MAX_BLOCK_TIMESTAMP_MS: u64 = 7_258_118_400_000;
const TARGET_BLOCK_INTERVAL_MS: u64 = 50;
const MAX_FUTURE_SKEW_MS: u64 = 1_000;

pub struct Application<E: Clock + Storage + Metrics + Spawner + BufferPooler> {
    backfiller: Option<indexer::Producer<E>>,
}

impl<E: Clock + Storage + Metrics + Spawner + BufferPooler> Clone for Application<E> {
    fn clone(&self) -> Self {
        Self {
            backfiller: self.backfiller.clone(),
        }
    }
}

impl<E: Clock + Storage + Metrics + Spawner + BufferPooler> Application<E> {
    pub fn new() -> Self {
        Self { backfiller: None }
    }

    pub(crate) fn genesis_block() -> Block {
        let genesis_context = Context {
            round: Round::new(EPOCH, View::zero()),
            leader: ed25519::PrivateKey::from_seed(0).public_key(),
            parent: (View::zero(), sha256::Digest::EMPTY),
        };
        Block::new(genesis_context, Sha256::hash(&[GENESIS]), Height::zero(), 0)
    }

    pub(crate) fn with_backfiller(mut self, backfiller: indexer::Producer<E>) -> Self {
        self.backfiller = Some(backfiller);
        self
    }
}

impl<E: Clock + Storage + Metrics + Spawner + BufferPooler> Default for Application<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Clock + Storage + Metrics> commonware_consensus::Application<E> for Application<E>
where
    E: Rng + Spawner + Metrics + Clock + Storage + BufferPooler,
{
    type SigningScheme = Scheme;
    type Context = Context;
    type Block = Block;
    type Input = ();

    async fn propose(
        &mut self,
        (runtime_context, context): (E, Self::Context),
        mut ancestry: impl Ancestry<Self::Block>,
        _input: Self::Input,
    ) -> Option<Self::Block> {
        let parent = ancestry.next().await?;

        // Create a new block.
        let min_timestamp = parent
            .timestamp
            .checked_add(TARGET_BLOCK_INTERVAL_MS)
            .expect("parent timestamp overflowed");
        let mut current = runtime_context.current().epoch_millis();
        if current < min_timestamp {
            let deadline = SystemTime::UNIX_EPOCH
                .checked_add(Duration::from_millis(min_timestamp))
                .expect("proposed timestamp exceeded maximum");
            runtime_context.sleep_until(deadline).await;
            current = runtime_context.current().epoch_millis();
        }
        current = current.max(min_timestamp);
        assert!(
            current <= MAX_BLOCK_TIMESTAMP_MS,
            "proposed timestamp exceeded maximum",
        );

        Some(Block::new(
            context,
            parent.digest(),
            parent.height.next(),
            current,
        ))
    }

    async fn verify(
        &mut self,
        (runtime_context, _): (E, Self::Context),
        mut ancestry: impl Ancestry<Self::Block>,
    ) -> bool {
        let Some(block) = ancestry.next().await else {
            return false;
        };
        let Some(parent) = ancestry.next().await else {
            return false;
        };

        // Verify the block (allowing a bounded amount of future clock skew).
        if block.timestamp <= parent.timestamp || block.timestamp > MAX_BLOCK_TIMESTAMP_MS {
            return false;
        }
        let now = runtime_context.current().epoch_millis();
        if block.timestamp > now.saturating_add(MAX_FUTURE_SKEW_MS) {
            return false;
        }

        // The height and digest invariants are enforced in `Marshaled`:
        // - The block height must be one greater than the parent's height.
        // - The block's parent digest must match the parent's digest.
        true
    }
}

impl<E: Clock + Storage + Metrics + Spawner + BufferPooler> Reporter for Application<E> {
    type Activity = Update<Block>;

    fn report(&mut self, activity: Self::Activity) -> Feedback {
        if let Update::Block(block, ack_rx) = activity {
            info!(height = %block.height(), "finalized block");
            if let Some(backfiller) = &self.backfiller {
                backfiller.record(Update::Block(block, ack_rx));
            } else {
                ack_rx.acknowledge();
            }
        }
        Feedback::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_consensus::marshal::ancestry;
    use commonware_runtime::{deterministic, Runner as _, Supervisor as _};
    use std::sync::Arc;

    fn test_context(view: u64, parent: (View, sha256::Digest)) -> Context {
        Context {
            round: Round::new(EPOCH, View::new(view)),
            leader: ed25519::PrivateKey::from_seed(view).public_key(),
            parent,
        }
    }

    async fn setup_application_test(
        _context: deterministic::Context,
    ) -> Application<deterministic::Context> {
        Application::new()
    }

    async fn verify_block(
        context: deterministic::Context,
        application: &mut Application<deterministic::Context>,
        block: &Block,
        parent: &Block,
    ) -> bool {
        let ancestry = ancestry::from_iter([Arc::new(block.clone()), Arc::new(parent.clone())]);
        commonware_consensus::Application::verify(
            application,
            (context, block.context.clone()),
            ancestry,
        )
        .await
    }

    async fn propose_child(
        context: deterministic::Context,
        application: &mut Application<deterministic::Context>,
        child_context: Context,
        parent: &Block,
    ) -> Block {
        let ancestry = ancestry::from_iter([Arc::new(parent.clone())]);
        commonware_consensus::Application::propose(
            application,
            (context, child_context),
            ancestry,
            (),
        )
        .await
        .expect("expected proposal")
    }

    #[test]
    fn verify_rejects_far_future_block_timestamp() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let mut application = setup_application_test(context.child("application")).await;

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                now,
            );
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                now + MAX_FUTURE_SKEW_MS + 100_000,
            );

            let start = context.current();
            assert!(
                !verify_block(context.child("verify"), &mut application, &block, &parent).await
            );
            let finished = context.current();
            assert!(finished.duration_since(start).unwrap() < Duration::from_millis(10));
        });
    }

    #[test]
    fn verify_rejects_equal_parent_timestamp() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let mut application = setup_application_test(context.child("application")).await;

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                now,
            );
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                now,
            );

            assert!(
                !verify_block(context.child("verify"), &mut application, &block, &parent).await
            );
        });
    }

    #[test]
    fn verify_returns_immediately_for_mature_block_timestamp() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let mut application = setup_application_test(context.child("application")).await;

            context.sleep(Duration::from_millis(10)).await;
            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                now - 1,
            );
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                now,
            );

            let start = context.current();
            assert!(verify_block(context.child("verify"), &mut application, &block, &parent).await);
            let finished = context.current();
            assert!(finished.duration_since(start).unwrap() < Duration::from_millis(10));
        });
    }

    #[test]
    fn propose_uses_parent_timestamp_plus_interval_when_clock_is_behind() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let mut application = setup_application_test(context.child("application")).await;

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                now + 5_000,
            );
            let proposal = propose_child(
                context.child("propose"),
                &mut application,
                test_context(2, (View::new(1), parent.digest())),
                &parent,
            )
            .await;

            assert_eq!(proposal.parent, parent.digest());
            assert_eq!(proposal.height, parent.height.next());
            assert_eq!(
                proposal.timestamp,
                parent.timestamp + TARGET_BLOCK_INTERVAL_MS
            );
        });
    }

    #[test]
    fn verify_rejects_timestamp_above_maximum() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let mut application = setup_application_test(context.child("application")).await;

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                now,
            );
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                // Verification should reject timestamps outside the fixed
                // protocol range before attempting to sleep.
                MAX_BLOCK_TIMESTAMP_MS + 1,
            );

            assert!(
                !verify_block(context.child("verify"), &mut application, &block, &parent).await
            );
        });
    }

    #[test]
    #[should_panic(expected = "proposed timestamp exceeded maximum")]
    fn propose_panics_when_parent_timestamp_is_maximum() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let mut application = setup_application_test(context.child("application")).await;

            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                // Proposing on top of a parent already at the maximum would
                // require `parent.timestamp + 1`, which must be rejected.
                MAX_BLOCK_TIMESTAMP_MS,
            );
            let _ = propose_child(
                context.child("propose"),
                &mut application,
                test_context(2, (View::new(1), parent.digest())),
                &parent,
            )
            .await;
        });
    }
}
