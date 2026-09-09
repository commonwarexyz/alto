use crate::indexer;
use alto_types::{Block, Context, Scheme};
use commonware_actor::Feedback;
use commonware_consensus::{
    marshal::{ancestry::Ancestry, Update},
    Application as ConsensusApplication, Heightable, Reporter,
};
use commonware_cryptography::Digestible;
use commonware_runtime::{Clock, Metrics, Spawner, Storage};
use commonware_utils::{Acknowledgement, SystemTimeExt};
use futures::StreamExt;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{
    marker::PhantomData,
    time::{Duration, SystemTime},
};
use tracing::info;

/// Fixed consensus cutoff for block timestamps: 2200-01-01T00:00:00Z.
///
/// Different platforms have different `SystemTime` limits, so we use a fixed
/// timestamp to ensure consistent application of block validity rules.
const MAX_BLOCK_TIMESTAMP_MS: u64 = 7_258_118_400_000;
const MAX_FUTURE_SKEW_MS: u64 = 1_000;

#[derive(Clone)]
pub struct Application<S: Scheme> {
    backfiller: Option<indexer::Producer>,
    delay_ms: u64,
    block_size: usize,
    _scheme: PhantomData<S>,
}

impl<S: Scheme> Application<S> {
    pub fn new(delay_ms: u64, block_size: u32) -> Self {
        Self {
            backfiller: None,
            delay_ms,
            block_size: usize::try_from(block_size)
                .expect("configured block size is unsupported on this platform"),
            _scheme: PhantomData,
        }
    }

    pub(crate) fn with_backfiller(mut self, backfiller: indexer::Producer) -> Self {
        self.backfiller = Some(backfiller);
        self
    }
}

impl<E, S> ConsensusApplication<E> for Application<S>
where
    E: Rng + Spawner + Metrics + Clock + Storage,
    S: Scheme,
{
    type SigningScheme = S;
    type Context = Context;
    type Block = Block;
    type Input = ();

    async fn propose(
        &mut self,
        (mut runtime_context, context): (E, Self::Context),
        mut ancestry: impl Ancestry<Self::Block>,
        _input: Self::Input,
    ) -> Option<Self::Block> {
        let parent = ancestry.next().await?;

        // Pace each proposal from its parent's timestamp.
        let min_timestamp = parent
            .timestamp
            .checked_add(self.delay_ms)
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

        // Seed a userspace generator once per proposal to produce an incompressible load payload
        // without drawing every byte from the runtime's random source.
        let mut data = vec![0; self.block_size];
        if self.block_size > 0 {
            StdRng::from_rng(&mut runtime_context).fill_bytes(&mut data);
        }

        Some(Block::new(
            context,
            parent.digest(),
            parent.height.next(),
            current,
            data.into(),
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

        // Block size is a consensus rule, so every proposal must match the local configuration.
        if block.data.len() != self.block_size {
            return false;
        }

        // Require nondecreasing timestamps within the protocol's fixed range.
        if block.timestamp < parent.timestamp || block.timestamp > MAX_BLOCK_TIMESTAMP_MS {
            return false;
        }

        // Never reject on the local clock: this verdict gates certification, which Simplex requires
        // to be deterministic across validators. Wait for the skew window instead. Nullification
        // keeps the pending certification alive, and finalization cancels it at or below the
        // finalized view.
        //
        // The timestamp is chosen by an untrusted proposer, but the check above caps it at
        // `MAX_BLOCK_TIMESTAMP_MS` (year 2200), which `SystemTime` represents on every platform,
        // so this addition cannot overflow.
        let deadline = SystemTime::UNIX_EPOCH
            + Duration::from_millis(block.timestamp.saturating_sub(MAX_FUTURE_SKEW_MS));
        runtime_context.sleep_until(deadline).await;

        // The height and digest invariants are enforced in `Marshaled`:
        // - The block height must be one greater than the parent's height.
        // - The block's parent digest must match the parent's digest.
        true
    }
}

impl<S: Scheme> Reporter for Application<S> {
    type Activity = Update<Block>;

    fn report(&mut self, activity: Self::Activity) -> Feedback {
        if let Update::Block(block, _) = &activity {
            info!(
                height = %block.height(),
                digest = ?block.digest(),
                timestamp = block.timestamp,
                "finalized block"
            );
        }

        if let Some(backfiller) = &mut self.backfiller {
            return backfiller.report(activity);
        }

        if let Update::Block(_, ack_rx) = activity {
            ack_rx.acknowledge();
        }
        Feedback::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alto_types::{VrfScheme, EPOCH};
    use bytes::Bytes;
    use commonware_consensus::{
        marshal::ancestry,
        types::{Height, Round, View},
    };
    use commonware_cryptography::{ed25519, sha256, Digest as _, Hasher, Sha256, Signer};
    use commonware_runtime::{deterministic, Runner as _, Supervisor as _};
    use std::sync::Arc;

    const DELAY_MS: u64 = 10;

    fn test_context(view: u64, parent: (View, sha256::Digest)) -> Context {
        Context {
            round: Round::new(EPOCH, View::new(view)),
            leader: ed25519::PrivateKey::from_seed(view).public_key(),
            parent,
        }
    }

    async fn verify_block(
        context: deterministic::Context,
        application: &mut Application<VrfScheme>,
        block: &Block,
        parent: &Block,
    ) -> bool {
        let ancestry = ancestry::from_iter([Arc::new(block.clone()), Arc::new(parent.clone())]);
        ConsensusApplication::verify(application, (context, block.context.clone()), ancestry).await
    }

    async fn propose_child(
        context: deterministic::Context,
        application: &mut Application<VrfScheme>,
        child_context: Context,
        parent: &Block,
    ) -> Block {
        let ancestry = ancestry::from_iter([Arc::new(parent.clone())]);
        ConsensusApplication::propose(application, (context, child_context), ancestry, ())
            .await
            .expect("expected proposal")
    }

    #[test]
    fn verify_waits_until_future_block_enters_skew_window() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let mut application = Application::new(DELAY_MS, 0);

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                now,
                Bytes::new(),
            );
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                now + MAX_FUTURE_SKEW_MS + 1,
                Bytes::new(),
            );

            let start = context.current();
            assert!(verify_block(context.child("verify"), &mut application, &block, &parent).await);
            let finished = context.current();
            assert_eq!(
                finished.duration_since(start).unwrap(),
                Duration::from_millis(1)
            );
            assert_eq!(
                finished.epoch_millis(),
                block.timestamp - MAX_FUTURE_SKEW_MS
            );
        });
    }

    #[test]
    fn verify_requires_nondecreasing_timestamps() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                now + 1,
                Bytes::new(),
            );

            // Timestamp validity does not depend on the local proposal delay.
            for delay_ms in [0, DELAY_MS] {
                let mut application = Application::new(delay_ms, 0);
                for (timestamp, valid) in [(now, false), (now + 1, true), (now + 2, true)] {
                    let block = Block::new(
                        test_context(2, (View::new(1), parent.digest())),
                        parent.digest(),
                        parent.height.next(),
                        timestamp,
                        Bytes::new(),
                    );
                    assert_eq!(
                        verify_block(context.child("verify"), &mut application, &block, &parent)
                            .await,
                        valid,
                        "timestamp {timestamp} with parent timestamp {} and delay {delay_ms}",
                        parent.timestamp,
                    );
                }
            }
        });
    }

    #[test]
    fn verify_requires_configured_block_size() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let block_size = 4;
            let mut application = Application::new(DELAY_MS, block_size);

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                now,
                Bytes::new(),
            );
            let child_context = test_context(2, (View::new(1), parent.digest()));

            for size in [0, 3, 5] {
                let block = Block::new(
                    child_context.clone(),
                    parent.digest(),
                    parent.height.next(),
                    now + 1,
                    Bytes::from(vec![0; size]),
                );
                assert!(
                    !verify_block(context.child("verify"), &mut application, &block, &parent).await
                );
            }

            let block = Block::new(
                child_context,
                parent.digest(),
                parent.height.next(),
                now + 1,
                Bytes::from(vec![0; usize::try_from(block_size).unwrap()]),
            );
            assert!(verify_block(context.child("verify"), &mut application, &block, &parent).await);
        });
    }

    #[test]
    fn verify_returns_immediately_for_mature_block_timestamp() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let mut application = Application::new(DELAY_MS, 0);

            context.sleep(Duration::from_millis(10)).await;
            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                now - 1,
                Bytes::new(),
            );
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                now,
                Bytes::new(),
            );

            let start = context.current();
            assert!(verify_block(context.child("verify"), &mut application, &block, &parent).await);
            let finished = context.current();
            assert!(finished.duration_since(start).unwrap() < Duration::from_millis(10));
        });
    }

    #[test]
    fn propose_uses_configured_delay_when_clock_is_behind() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let delay_ms = 37;
            let mut application = Application::new(delay_ms, 0);

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                now + 5_000,
                Bytes::new(),
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
            assert_eq!(proposal.timestamp, parent.timestamp + delay_ms);
            assert!(proposal.data.is_empty());
        });
    }

    #[test]
    fn propose_without_delay_uses_current_or_parent_timestamp() {
        for parent_timestamp in [0, 10, 15, MAX_BLOCK_TIMESTAMP_MS] {
            let runner = deterministic::Runner::default();
            runner.start(|context| async move {
                let mut application = Application::new(0, 0);

                // Cover past, equal, and future parents, including the protocol's maximum.
                context.sleep(Duration::from_millis(10)).await;
                let now = context.current().epoch_millis();
                let parent = Block::new(
                    test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                    Sha256::hash(&[b"genesis"]),
                    Height::new(1),
                    parent_timestamp,
                    Bytes::new(),
                );
                let proposal = propose_child(
                    context.child("propose"),
                    &mut application,
                    test_context(2, (View::new(1), parent.digest())),
                    &parent,
                )
                .await;

                // Zero delay adds no timestamp increment or wait beyond reaching the parent.
                let expected = now.max(parent_timestamp);
                assert_eq!(proposal.timestamp, expected);
                assert_eq!(context.current().epoch_millis(), expected);
                assert!(
                    verify_block(
                        context.child("verify"),
                        &mut application,
                        &proposal,
                        &parent
                    )
                    .await
                );
            });
        }
    }

    #[test]
    fn propose_appends_configured_random_data() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let block_size = 128;
            let mut application = Application::new(DELAY_MS, block_size);

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                now,
                Bytes::new(),
            );
            let proposal = propose_child(
                context.child("propose"),
                &mut application,
                test_context(2, (View::new(1), parent.digest())),
                &parent,
            )
            .await;

            assert_eq!(proposal.data.len(), usize::try_from(block_size).unwrap());
            assert!(proposal.data.iter().any(|byte| *byte != 0));
        });
    }

    #[test]
    fn verify_rejects_timestamp_above_maximum() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let mut application = Application::new(DELAY_MS, 0);

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                now,
                Bytes::new(),
            );

            // Verification rejects timestamps outside the fixed protocol range before sleeping.
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                MAX_BLOCK_TIMESTAMP_MS + 1,
                Bytes::new(),
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
            let mut application = Application::new(DELAY_MS, 0);

            // Adding the proposal delay to a parent at the timestamp limit exceeds that limit.
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                MAX_BLOCK_TIMESTAMP_MS,
                Bytes::new(),
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
