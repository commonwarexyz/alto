use crate::indexer;
use alto_types::{Block, Context, Scheme, EPOCH};
use commonware_consensus::{
    marshal::{
        ancestry::{AncestorStream, BlockProvider},
        Update,
    },
    types::{Height, Round, View},
    Heightable, Reporter,
};
use commonware_cryptography::{ed25519, sha256, Digest as _, Digestible, Hasher, Sha256, Signer};
use commonware_runtime::{Clock, Metrics, Spawner, Storage};
use commonware_utils::{Acknowledgement, SystemTimeExt};
use futures::StreamExt;
use rand::Rng;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing::info;

/// Genesis message to use during initialization.
const GENESIS: &[u8] = b"commonware is neat";

/// Fixed consensus cutoff for block timestamps: 2200-01-01T00:00:00Z.
///
/// Different platforms have different `SystemTime` limits, so we use a fixed
/// timestamp to ensure consistent application of block validity rules.
const MAX_BLOCK_TIMESTAMP_MS: u64 = 7_258_118_400_000;

#[derive(Clone)]
pub struct Application<E: Clock + Storage + Metrics> {
    genesis: Arc<Block>,
    backfiller: Option<indexer::Producer<E>>,
}

impl<E: Clock + Storage + Metrics> Application<E> {
    pub fn new() -> Self {
        let genesis_context = Context {
            round: Round::new(EPOCH, View::zero()),
            leader: ed25519::PrivateKey::from_seed(0).public_key(),
            parent: (View::zero(), sha256::Digest::EMPTY),
        };
        let genesis = Block::new(genesis_context, Sha256::hash(GENESIS), Height::zero(), 0);
        Self {
            genesis: Arc::new(genesis),
            backfiller: None,
        }
    }

    pub(crate) fn with_backfiller(mut self, backfiller: indexer::Producer<E>) -> Self {
        self.backfiller = Some(backfiller);
        self
    }
}

impl<E: Clock + Storage + Metrics> Default for Application<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Clock + Storage + Metrics> commonware_consensus::Application<E> for Application<E>
where
    E: Rng + Spawner + Metrics + Clock + Storage,
{
    type SigningScheme = Scheme;
    type Context = Context;
    type Block = Block;

    async fn genesis(&mut self) -> Self::Block {
        self.genesis.as_ref().clone()
    }

    async fn propose<A: BlockProvider<Block = Self::Block>>(
        &mut self,
        (runtime_context, context): (E, Self::Context),
        mut ancestry: AncestorStream<A, Self::Block>,
    ) -> Option<Self::Block> {
        let parent = ancestry.next().await?;

        // Create a new block.
        let mut current = runtime_context.current().epoch_millis();
        if current <= parent.timestamp {
            current = parent
                .timestamp
                .checked_add(1)
                .expect("parent timestamp overflowed");
        }
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
}

impl<E: Clock + Storage + Metrics> commonware_consensus::VerifyingApplication<E> for Application<E>
where
    E: Rng + Spawner + Metrics + Clock + Storage,
{
    async fn verify<A: BlockProvider<Block = Self::Block>>(
        &mut self,
        (runtime_context, _): (E, Context),
        mut ancestry: AncestorStream<A, Self::Block>,
    ) -> bool {
        let Some(block) = ancestry.next().await else {
            return false;
        };
        let Some(parent) = ancestry.next().await else {
            return false;
        };

        // Verify the block (waiting until the block timestamp has passed to vote in case of skew).
        if block.timestamp <= parent.timestamp || block.timestamp > MAX_BLOCK_TIMESTAMP_MS {
            return false;
        }
        let deadline = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(block.timestamp))
            .expect("block timestamp exceeded maximum");
        runtime_context.sleep_until(deadline).await;

        // The height and digest invariants are enforced in `Marshaled`:
        // - The block height must be one greater than the parent's height.
        // - The block's parent digest must match the parent's digest.
        true
    }
}

impl<E: Clock + Storage + Metrics> Reporter for Application<E> {
    type Activity = Update<Block>;

    async fn report(&mut self, activity: Self::Activity) {
        if let Update::Block(block, ack_rx) = activity {
            // Cache the finalized block in memory and enqueue its digest
            // before acking so the consumer can recover it across restarts.
            if let Some(backfiller) = &self.backfiller {
                backfiller.record(&block).await;
            }

            // Acknowledge the block.
            info!(height = %block.height(), "finalized block");
            ack_rx.acknowledge();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_consensus::{
        marshal::{
            core::{Actor as MarshalActor, Buffer, Mailbox},
            resolver::handler,
            standard::Standard,
            Config as MarshalConfig,
        },
        simplex::scheme::bls12381_threshold::vrf as bls12381_threshold,
    };
    use commonware_cryptography::{
        bls12381::primitives::variant::MinSig,
        certificate::{mocks::Fixture, ConstantProvider},
    };
    use commonware_parallel::Sequential;
    use commonware_resolver::Resolver;
    use commonware_runtime::{buffer::paged::CacheRef, deterministic, Runner as _};
    use commonware_storage::archive::immutable;
    use commonware_utils::{
        channel::{mpsc, oneshot},
        vec::NonEmptyVec,
        NZUsize, NZU16, NZU64,
    };

    const TEST_NAMESPACE: &[u8] = b"application-test";
    const TEST_PAGE_SIZE: u16 = 1024;
    const TEST_PAGE_CACHE_SIZE: usize = 10;

    fn test_context(view: u64, parent: (View, sha256::Digest)) -> Context {
        Context {
            round: Round::new(EPOCH, View::new(view)),
            leader: ed25519::PrivateKey::from_seed(view).public_key(),
            parent,
        }
    }

    #[derive(Clone)]
    struct NoopBuffer;

    impl Buffer<Standard<Block>> for NoopBuffer {
        type CachedBlock = Block;

        async fn find_by_digest(&self, _: sha256::Digest) -> Option<Self::CachedBlock> {
            None
        }

        async fn find_by_commitment(&self, _: sha256::Digest) -> Option<Self::CachedBlock> {
            None
        }

        async fn subscribe_by_digest(
            &self,
            _: sha256::Digest,
        ) -> oneshot::Receiver<Self::CachedBlock> {
            let (_tx, rx) = oneshot::channel();
            rx
        }

        async fn subscribe_by_commitment(
            &self,
            _: sha256::Digest,
        ) -> oneshot::Receiver<Self::CachedBlock> {
            let (_tx, rx) = oneshot::channel();
            rx
        }

        async fn finalized(&self, _: sha256::Digest) {}

        async fn proposed(&self, _: Round, _: Block) {}
    }

    #[derive(Clone)]
    struct NoopResolver;

    impl Resolver for NoopResolver {
        type Key = handler::Request<sha256::Digest>;
        type PublicKey = alto_types::PublicKey;

        async fn fetch(&mut self, _: Self::Key) {}

        async fn fetch_all(&mut self, _: Vec<Self::Key>) {}

        async fn fetch_targeted(&mut self, _: Self::Key, _: NonEmptyVec<Self::PublicKey>) {}

        async fn fetch_all_targeted(&mut self, _: Vec<(Self::Key, NonEmptyVec<Self::PublicKey>)>) {}

        async fn cancel(&mut self, _: Self::Key) {}

        async fn clear(&mut self) {}

        async fn retain(&mut self, _: impl Fn(&Self::Key) -> bool + Send + 'static) {}
    }

    fn test_archive_config(
        prefix: &str,
        label: &str,
        page_cache: CacheRef,
    ) -> immutable::Config<()> {
        immutable::Config {
            metadata_partition: format!("{prefix}-{label}-metadata"),
            freezer_table_partition: format!("{prefix}-{label}-freezer-table"),
            freezer_table_initial_size: 64,
            freezer_table_resize_frequency: 10,
            freezer_table_resize_chunk_size: 10,
            freezer_key_partition: format!("{prefix}-{label}-key"),
            freezer_key_page_cache: page_cache,
            freezer_value_partition: format!("{prefix}-{label}-value"),
            freezer_value_target_size: 1024,
            freezer_value_compression: None,
            ordinal_partition: format!("{prefix}-{label}-ordinal"),
            items_per_section: NZU64!(10),
            codec_config: (),
            replay_buffer: NZUsize!(1024),
            freezer_key_write_buffer: NZUsize!(1024),
            freezer_value_write_buffer: NZUsize!(1024),
            ordinal_write_buffer: NZUsize!(1024),
        }
    }

    async fn init_mailbox(
        context: deterministic::Context,
        scheme: Scheme,
    ) -> (
        Mailbox<Scheme, Standard<Block>>,
        mpsc::Sender<handler::Message<sha256::Digest>>,
    ) {
        let page_cache = CacheRef::from_pooler(
            &context,
            NZU16!(TEST_PAGE_SIZE),
            NZUsize!(TEST_PAGE_CACHE_SIZE),
        );
        let partition_prefix = "application-test";

        let finalizations_by_height = immutable::Archive::init(
            context.with_label("finalizations_by_height"),
            test_archive_config(
                partition_prefix,
                "finalizations-by-height",
                page_cache.clone(),
            ),
        )
        .await
        .expect("failed to initialize finalizations archive");
        let finalized_blocks = immutable::Archive::init(
            context.with_label("finalized_blocks"),
            test_archive_config(partition_prefix, "finalized-blocks", page_cache.clone()),
        )
        .await
        .expect("failed to initialize finalized blocks archive");

        let (actor, mailbox, _) = MarshalActor::init(
            context.clone(),
            finalizations_by_height,
            finalized_blocks,
            MarshalConfig {
                provider: ConstantProvider::new(scheme),
                epocher: commonware_consensus::types::FixedEpocher::new(alto_types::EPOCH_LENGTH),
                partition_prefix: partition_prefix.to_string(),
                mailbox_size: 16,
                view_retention_timeout: commonware_consensus::types::ViewDelta::new(4),
                prunable_items_per_section: NZU64!(10),
                replay_buffer: NZUsize!(1024),
                key_write_buffer: NZUsize!(1024),
                value_write_buffer: NZUsize!(1024),
                block_codec_config: (),
                max_repair: NZUsize!(4),
                max_pending_acks: NZUsize!(4),
                page_cache,
                strategy: Sequential,
            },
        )
        .await;
        let (resolver_tx, resolver_rx) = mpsc::channel::<handler::Message<sha256::Digest>>(1);
        actor.start(
            Application::<deterministic::Context>::default(),
            NoopBuffer,
            (resolver_rx, NoopResolver),
        );

        (mailbox, resolver_tx)
    }

    async fn setup_application_test(
        context: &mut deterministic::Context,
    ) -> (
        Application<deterministic::Context>,
        Mailbox<Scheme, Standard<Block>>,
        mpsc::Sender<handler::Message<sha256::Digest>>,
    ) {
        let Fixture { schemes, .. } =
            bls12381_threshold::fixture::<MinSig, _>(context, TEST_NAMESPACE, 1);
        let (mailbox, resolver_tx) = init_mailbox(context.clone(), schemes[0].clone()).await;
        (
            Application::<deterministic::Context>::default(),
            mailbox,
            resolver_tx,
        )
    }

    async fn cache_block(
        context: &deterministic::Context,
        mailbox: &Mailbox<Scheme, Standard<Block>>,
        block: &Block,
    ) {
        mailbox.verified(block.context.round, block.clone()).await;
        loop {
            if mailbox.get_block(&block.digest()).await.is_some() {
                break;
            }
            context.sleep(Duration::from_millis(1)).await;
        }
    }

    async fn verify_block(
        context: &deterministic::Context,
        application: &mut Application<deterministic::Context>,
        mailbox: &Mailbox<Scheme, Standard<Block>>,
        block: &Block,
    ) -> bool {
        let ancestry = mailbox
            .ancestry((Some(block.context.round), block.digest()))
            .await
            .expect("expected cached ancestry");
        commonware_consensus::VerifyingApplication::verify(
            application,
            (context.clone(), block.context.clone()),
            ancestry,
        )
        .await
    }

    async fn propose_child(
        context: &deterministic::Context,
        application: &mut Application<deterministic::Context>,
        mailbox: &Mailbox<Scheme, Standard<Block>>,
        child_context: Context,
        parent: &Block,
    ) -> Block {
        let ancestry = mailbox
            .ancestry((Some(parent.context.round), parent.digest()))
            .await
            .expect("expected cached ancestry");
        commonware_consensus::Application::propose(
            application,
            (context.clone(), child_context),
            ancestry,
        )
        .await
        .expect("expected proposal")
    }

    #[test]
    fn verify_waits_for_far_future_block_timestamp() {
        let runner = deterministic::Runner::default();
        runner.start(|mut context| async move {
            let (mut application, mailbox, _resolver_tx) =
                setup_application_test(&mut context).await;

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(b"genesis"),
                Height::new(1),
                now,
            );
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                now + 5_000,
            );

            cache_block(&context, &mailbox, &parent).await;
            cache_block(&context, &mailbox, &block).await;

            let start = context.current();
            assert!(verify_block(&context, &mut application, &mailbox, &block).await);
            let finished = context.current();
            assert!(finished.duration_since(start).unwrap() > Duration::ZERO);
            assert!(finished.epoch_millis() >= block.timestamp);
        });
    }

    #[test]
    fn verify_rejects_equal_parent_timestamp() {
        let runner = deterministic::Runner::default();
        runner.start(|mut context| async move {
            let (mut application, mailbox, _resolver_tx) =
                setup_application_test(&mut context).await;

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(b"genesis"),
                Height::new(1),
                now,
            );
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                now,
            );

            cache_block(&context, &mailbox, &parent).await;
            cache_block(&context, &mailbox, &block).await;

            assert!(!verify_block(&context, &mut application, &mailbox, &block).await);
        });
    }

    #[test]
    fn verify_returns_immediately_for_mature_block_timestamp() {
        let runner = deterministic::Runner::default();
        runner.start(|mut context| async move {
            let (mut application, mailbox, _resolver_tx) =
                setup_application_test(&mut context).await;

            context.sleep(Duration::from_millis(10)).await;
            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(b"genesis"),
                Height::new(1),
                now - 1,
            );
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                now,
            );

            cache_block(&context, &mailbox, &parent).await;
            cache_block(&context, &mailbox, &block).await;

            let start = context.current();
            assert!(verify_block(&context, &mut application, &mailbox, &block).await);
            let finished = context.current();
            assert!(finished.duration_since(start).unwrap() < Duration::from_millis(10));
        });
    }

    #[test]
    fn propose_uses_parent_timestamp_plus_one_when_clock_is_behind() {
        let runner = deterministic::Runner::default();
        runner.start(|mut context| async move {
            let (mut application, mailbox, _resolver_tx) =
                setup_application_test(&mut context).await;

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(b"genesis"),
                Height::new(1),
                now + 5_000,
            );
            cache_block(&context, &mailbox, &parent).await;

            let proposal = propose_child(
                &context,
                &mut application,
                &mailbox,
                test_context(2, (View::new(1), parent.digest())),
                &parent,
            )
            .await;

            assert_eq!(proposal.parent, parent.digest());
            assert_eq!(proposal.height, parent.height.next());
            assert_eq!(proposal.timestamp, parent.timestamp + 1);
        });
    }

    #[test]
    fn verify_rejects_timestamp_above_maximum() {
        let runner = deterministic::Runner::default();
        runner.start(|mut context| async move {
            let (mut application, mailbox, _resolver_tx) =
                setup_application_test(&mut context).await;

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(b"genesis"),
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

            cache_block(&context, &mailbox, &parent).await;
            cache_block(&context, &mailbox, &block).await;

            assert!(!verify_block(&context, &mut application, &mailbox, &block).await);
        });
    }

    #[test]
    #[should_panic(expected = "proposed timestamp exceeded maximum")]
    fn propose_panics_when_parent_timestamp_is_maximum() {
        let runner = deterministic::Runner::default();
        runner.start(|mut context| async move {
            let (mut application, mailbox, _resolver_tx) =
                setup_application_test(&mut context).await;

            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(b"genesis"),
                Height::new(1),
                // Proposing on top of a parent already at the maximum would
                // require `parent.timestamp + 1`, which must be rejected.
                MAX_BLOCK_TIMESTAMP_MS,
            );
            cache_block(&context, &mailbox, &parent).await;

            let _ = propose_child(
                &context,
                &mut application,
                &mailbox,
                test_context(2, (View::new(1), parent.digest())),
                &parent,
            )
            .await;
        });
    }
}
