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
use commonware_glue::stateful::{
    db::{DatabaseSet, Merkleized as _, Unmerkleized as _},
    Application as StatefulApplication, Proposed,
};
use commonware_runtime::{Clock, Metrics, Spawner, Storage};
use commonware_storage::{
    mmr::Location,
    qmdb::{any::unordered::fixed, sync::Target},
    translator::FourCap,
};
use commonware_utils::{
    hex, non_empty_range,
    sequence::{U32, U64},
    sync::AsyncRwLock,
    Acknowledgement, SystemTimeExt,
};
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

/// Empty QMDB database root.
///
/// This must match the merkle root of a freshly initialized `Qmdb` with
/// U32 keys and U64 values. Changing the key/value types or hasher
/// requires recomputing this constant.
const EMPTY_DB_ROOT: [u8; 32] =
    hex!("9aca3ad4db9497dd1b00a5dd039574365aa0d4895dd64f071fd36c87552463ec");

/// The QMDB database type: unordered fixed-size key-value store with u32 keys and u64 values.
///
/// `FourCap` maps the full 4-byte U32 key into a u32 bucket — a perfect 1:1
/// mapping with zero collisions for our key space, and much cheaper HashMap
/// lookups than `FourCap`.
pub type Qmdb<E> = fixed::Db<E, U32, U64, Sha256, FourCap>;

/// A single QMDB database wrapped for use as a [`DatabaseSet`].
pub type SingleDatabaseSet<E> = Arc<AsyncRwLock<Qmdb<E>>>;

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
        let genesis = Block::new(
            genesis_context,
            Sha256::hash(GENESIS),
            Height::zero(),
            0,
            sha256::Digest::from(EMPTY_DB_ROOT),
            non_empty_range!(Location::new(0), Location::new(1)),
            vec![],
        );
        Self {
            genesis: Arc::new(genesis),
            backfiller: None,
        }
    }

    pub(crate) fn with_backfiller(mut self, backfiller: indexer::Producer<E>) -> Self {
        self.backfiller = Some(backfiller);
        self
    }

    /// Number of key slots selected per block.
    const SLOTS_PER_BLOCK: usize = 1 << 13; // 8192

    /// Total key space: `[0, 2^16)`.
    const KEY_SPACE: u16 = u16::MAX;

    /// Select `SLOTS_PER_BLOCK` unique random keys from `[0, KEY_SPACE]`.
    fn select_slots(rng: &mut impl Rng) -> Vec<u16> {
        let mut slots = Vec::with_capacity(Self::SLOTS_PER_BLOCK);
        let mut seen = std::collections::HashSet::with_capacity(Self::SLOTS_PER_BLOCK);
        while slots.len() < Self::SLOTS_PER_BLOCK {
            let s = rng.gen_range(0..=Self::KEY_SPACE);
            if seen.insert(s) {
                slots.push(s);
            }
        }
        slots
    }

    /// Execute a block: write the block's height to each slot in `slots`.
    async fn execute(
        height: Height,
        slots: &[u16],
        mut batches: <SingleDatabaseSet<E> as DatabaseSet<E>>::Unmerkleized,
    ) -> <SingleDatabaseSet<E> as DatabaseSet<E>>::Merkleized
    where
        E: Rng + Spawner,
    {
        let value = U64::new(height.get());
        for &s in slots {
            batches = batches.write(U32::new(s as u32), Some(value.clone()));
        }
        batches.merkleize().await.expect("merkleization failed")
    }
}

impl<E: Clock + Storage + Metrics> Default for Application<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Clock + Storage + Metrics> StatefulApplication<E> for Application<E>
where
    E: Rng + Spawner + Metrics + Clock + Storage,
{
    type SigningScheme = Scheme;
    type Context = Context;
    type Block = Block;
    type Databases = SingleDatabaseSet<E>;
    type InputProvider = ();

    async fn genesis(&mut self) -> Self::Block {
        self.genesis.as_ref().clone()
    }

    async fn propose<A: BlockProvider<Block = Self::Block>>(
        &mut self,
        (mut runtime_context, context): (E, Self::Context),
        ancestry: AncestorStream<A, Self::Block>,
        batches: <Self::Databases as DatabaseSet<E>>::Unmerkleized,
        _input: &mut Self::InputProvider,
    ) -> Option<Proposed<Self, E>> {
        let parent = ancestry.peek()?;
        let parent_digest = parent.digest();
        let parent_timestamp = parent.timestamp;
        let height = parent.height().next();

        // Compute the timestamp, ensuring it is strictly greater than the parent's.
        let mut current = runtime_context.current().epoch_millis();
        if current <= parent_timestamp {
            current = parent_timestamp
                .checked_add(1)
                .expect("parent timestamp overflowed");
        }
        assert!(
            current <= MAX_BLOCK_TIMESTAMP_MS,
            "proposed timestamp exceeded maximum",
        );

        // Select random key slots for this block.
        let slots = Self::select_slots(&mut runtime_context);

        let merkleized = Self::execute(height, &slots, batches).await;
        let block = Block::new(
            context,
            parent_digest,
            height,
            current,
            merkleized.root(),
            non_empty_range!(merkleized.inactivity_floor(), merkleized.size()),
            slots,
        );
        Some(Proposed { block, merkleized })
    }

    async fn verify<A: BlockProvider<Block = Self::Block>>(
        &mut self,
        (runtime_context, _): (E, Self::Context),
        ancestry: AncestorStream<A, Self::Block>,
        batches: <Self::Databases as DatabaseSet<E>>::Unmerkleized,
    ) -> Option<<Self::Databases as DatabaseSet<E>>::Merkleized> {
        let block = ancestry.peek()?;

        // Reject timestamps outside the protocol range.
        if block.timestamp > MAX_BLOCK_TIMESTAMP_MS {
            return None;
        }

        // Wait until the block timestamp has passed to vote in case of skew.
        let deadline = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(block.timestamp))
            .expect("block timestamp exceeded maximum");
        runtime_context.sleep_until(deadline).await;

        // Execute using the slots embedded in the block.
        let merkleized = Self::execute(block.height(), &block.slots, batches).await;
        if merkleized.root() != block.state_root
            || non_empty_range!(merkleized.inactivity_floor(), merkleized.size()) != block.range
        {
            return None;
        }
        Some(merkleized)
    }

    async fn apply(
        &mut self,
        _context: (E, Self::Context),
        block: &Self::Block,
        batches: <Self::Databases as DatabaseSet<E>>::Unmerkleized,
    ) -> <Self::Databases as DatabaseSet<E>>::Merkleized {
        Self::execute(block.height(), &block.slots, batches).await
    }

    fn sync_targets(block: &Self::Block) -> <Self::Databases as DatabaseSet<E>>::SyncTargets {
        Target {
            root: block.state_root,
            range: block.range.clone(),
        }
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
    use commonware_glue::stateful::db::DatabaseSet;
    use commonware_parallel::Sequential;
    use commonware_resolver::Resolver;
    use commonware_runtime::{buffer::paged::CacheRef, deterministic, Runner as _};
    use commonware_storage::{
        archive::immutable, journal::contiguous::fixed::Config as FixedLogConfig,
        mmr::journaled::Config as MmrJournalConfig, qmdb::any::FixedConfig,
    };
    use commonware_utils::{
        channel::{mpsc, oneshot},
        range::NonEmptyRange,
        vec::NonEmptyVec,
        NZUsize, NZU16, NZU64,
    };

    const TEST_NAMESPACE: &[u8] = b"application-test";
    const TEST_PAGE_SIZE: u16 = 1024;
    const TEST_PAGE_CACHE_SIZE: usize = 10;

    fn default_range() -> NonEmptyRange<Location> {
        non_empty_range!(Location::new(0), Location::new(1))
    }

    fn test_context(view: u64, parent: (View, sha256::Digest)) -> Context {
        Context {
            round: Round::new(EPOCH, View::new(view)),
            leader: ed25519::PrivateKey::from_seed(view).public_key(),
            parent,
        }
    }

    /// Dummy consensus application so marshal can start and serve ancestry
    /// streams. Does not execute any state transitions.
    #[derive(Clone)]
    struct DummyConsensusApp {
        genesis: Block,
    }

    impl<E> commonware_consensus::Application<E> for DummyConsensusApp
    where
        E: Rng + Spawner + Metrics + Clock + Storage,
    {
        type SigningScheme = Scheme;
        type Context = Context;
        type Block = Block;

        async fn genesis(&mut self) -> Block {
            self.genesis.clone()
        }

        async fn propose<A: BlockProvider<Block = Block>>(
            &mut self,
            _: (E, Context),
            _: AncestorStream<A, Block>,
        ) -> Option<Block> {
            None
        }
    }

    impl<E> commonware_consensus::VerifyingApplication<E> for DummyConsensusApp
    where
        E: Rng + Spawner + Metrics + Clock + Storage,
    {
        async fn verify<A: BlockProvider<Block = Block>>(
            &mut self,
            _: (E, Context),
            _: AncestorStream<A, Block>,
        ) -> bool {
            true
        }
    }

    impl Reporter for DummyConsensusApp {
        type Activity = Update<Block>;
        async fn report(&mut self, _: Self::Activity) {}
    }

    fn test_qmdb_config(prefix: &str, page_cache: CacheRef) -> FixedConfig<FourCap> {
        FixedConfig {
            mmr_config: MmrJournalConfig {
                journal_partition: format!("{prefix}-qmdb-mmr-journal"),
                metadata_partition: format!("{prefix}-qmdb-mmr-metadata"),
                items_per_blob: NZU64!(11),
                write_buffer: NZUsize!(2048),
                thread_pool: None,
                page_cache: page_cache.clone(),
            },
            journal_config: FixedLogConfig {
                partition: format!("{prefix}-qmdb-log-journal"),
                items_per_blob: NZU64!(7),
                page_cache,
                write_buffer: NZUsize!(2048),
            },
            translator: FourCap,
        }
    }

    /// Compute the state root and range for a block at the given height
    /// by executing against a fresh QMDB database.
    async fn compute_state(
        db: &SingleDatabaseSet<deterministic::Context>,
        height: Height,
        slots: &[u16],
    ) -> (sha256::Digest, NonEmptyRange<Location>) {
        let batches = db.new_batches().await;
        let merkleized =
            Application::<deterministic::Context>::execute(height, slots, batches).await;
        let root = merkleized.root();
        let range = non_empty_range!(merkleized.inactivity_floor(), merkleized.size());
        (root, range)
    }

    #[derive(Clone)]
    struct NoopBuffer;

    impl Buffer<Standard<Block>> for NoopBuffer {
        type CachedBlock = Block;
        type PublicKey = alto_types::PublicKey;

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

        async fn send(
            &self,
            _round: Round,
            _block: <Standard<Block> as commonware_consensus::marshal::core::Variant>::Block,
            _recipients: commonware_p2p::Recipients<Self::PublicKey>,
        ) {
        }
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
        genesis: Block,
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
            DummyConsensusApp { genesis },
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
        SingleDatabaseSet<deterministic::Context>,
        mpsc::Sender<handler::Message<sha256::Digest>>,
    ) {
        let app = Application::<deterministic::Context>::default();
        let genesis = app.genesis.as_ref().clone();

        let Fixture { schemes, .. } =
            bls12381_threshold::fixture::<MinSig, _>(context, TEST_NAMESPACE, 1);
        let (mailbox, resolver_tx) =
            init_mailbox(context.clone(), schemes[0].clone(), genesis).await;

        let page_cache = CacheRef::from_pooler(
            context,
            NZU16!(TEST_PAGE_SIZE),
            NZUsize!(TEST_PAGE_CACHE_SIZE),
        );
        let db = <SingleDatabaseSet<deterministic::Context> as DatabaseSet<
            deterministic::Context,
        >>::init(
            context.clone(),
            test_qmdb_config("application-test", page_cache),
        )
        .await;

        (app, mailbox, db, resolver_tx)
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
        db: &SingleDatabaseSet<deterministic::Context>,
        block: &Block,
    ) -> bool {
        let ancestry = mailbox
            .ancestry((Some(block.context.round), block.digest()))
            .await
            .expect("expected cached ancestry");
        let batches = db.new_batches().await;
        StatefulApplication::verify(
            application,
            (context.clone(), block.context.clone()),
            ancestry,
            batches,
        )
        .await
        .is_some()
    }

    async fn propose_child(
        context: &deterministic::Context,
        application: &mut Application<deterministic::Context>,
        mailbox: &Mailbox<Scheme, Standard<Block>>,
        db: &SingleDatabaseSet<deterministic::Context>,
        child_context: Context,
        parent: &Block,
    ) -> Block {
        let ancestry = mailbox
            .ancestry((Some(parent.context.round), parent.digest()))
            .await
            .expect("expected cached ancestry");
        let batches = db.new_batches().await;
        StatefulApplication::propose(
            application,
            (context.clone(), child_context),
            ancestry,
            batches,
            &mut (),
        )
        .await
        .expect("expected proposal")
        .block
    }

    #[test]
    fn empty_db_root_matches_fresh_qmdb() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let page_cache = CacheRef::from_pooler(
                &context,
                NZU16!(TEST_PAGE_SIZE),
                NZUsize!(TEST_PAGE_CACHE_SIZE),
            );
            let db = <SingleDatabaseSet<deterministic::Context> as DatabaseSet<
                deterministic::Context,
            >>::init(
                context.clone(),
                test_qmdb_config("empty-root-test", page_cache),
            )
            .await;
            // Merkleize an empty batch to get the actual root.
            let batches = db.new_batches().await;
            let merkleized = batches.merkleize().await.expect("merkleize failed");
            let actual_root: [u8; 32] = merkleized.root().as_ref().try_into().unwrap();
            assert_eq!(
                actual_root, EMPTY_DB_ROOT,
                "EMPTY_DB_ROOT constant does not match fresh QMDB root"
            );
        });
    }

    #[test]
    fn block_slots_roundtrip() {
        use commonware_codec::{DecodeExt, Encode};
        let slots: Vec<u16> = (0..1 << 14).collect();
        let block = Block::new(
            test_context(1, (View::zero(), sha256::Digest::EMPTY)),
            Sha256::hash(b"test"),
            Height::new(1),
            100,
            sha256::Digest::EMPTY,
            default_range(),
            slots.clone(),
        );
        let encoded = block.encode();
        let decoded = Block::decode(encoded);
        assert!(decoded.is_ok(), "decode failed: {:?}", decoded.err());
        let decoded = decoded.unwrap();
        assert_eq!(decoded.slots, slots);
        assert_eq!(decoded.digest(), block.digest());
    }

    #[test]
    fn execute_is_deterministic() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let page_cache = CacheRef::from_pooler(
                &context,
                NZU16!(TEST_PAGE_SIZE),
                NZUsize!(TEST_PAGE_CACHE_SIZE),
            );
            let db1 = <SingleDatabaseSet<deterministic::Context> as DatabaseSet<
                deterministic::Context,
            >>::init(
                context.with_label("db1"),
                test_qmdb_config("det-test-1", page_cache.clone()),
            )
            .await;
            let db2 = <SingleDatabaseSet<deterministic::Context> as DatabaseSet<
                deterministic::Context,
            >>::init(
                context.with_label("db2"),
                test_qmdb_config("det-test-2", page_cache),
            )
            .await;

            let slots: Vec<u16> = vec![0, 1, 2, 100, 1000, 65535];
            let b1 = db1.new_batches().await;
            let b2 = db2.new_batches().await;
            let m1 =
                Application::<deterministic::Context>::execute(Height::new(1), &slots, b1).await;
            let m2 =
                Application::<deterministic::Context>::execute(Height::new(1), &slots, b2).await;
            assert_eq!(m1.root(), m2.root(), "execute must be deterministic");
        });
    }

    #[test]
    fn verify_waits_for_far_future_block_timestamp() {
        let runner = deterministic::Runner::default();
        runner.start(|mut context| async move {
            let (mut application, mailbox, db, _resolver_tx) =
                setup_application_test(&mut context).await;

            let now = context.current().epoch_millis();
            let (state_root, range) = compute_state(&db, Height::new(2), &[]).await;
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(b"genesis"),
                Height::new(1),
                now,
                sha256::Digest::EMPTY,
                default_range(),
                vec![],
            );
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                now + 5_000,
                state_root,
                range,
                vec![],
            );

            cache_block(&context, &mailbox, &parent).await;
            cache_block(&context, &mailbox, &block).await;

            let start = context.current();
            assert!(verify_block(&context, &mut application, &mailbox, &db, &block).await);
            let finished = context.current();
            assert!(finished.duration_since(start).unwrap() > Duration::ZERO);
            assert!(finished.epoch_millis() >= block.timestamp);
        });
    }

    #[test]
    fn verify_rejects_equal_parent_timestamp() {
        let runner = deterministic::Runner::default();
        runner.start(|mut context| async move {
            let (mut application, mailbox, db, _resolver_tx) =
                setup_application_test(&mut context).await;

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(b"genesis"),
                Height::new(1),
                now,
                sha256::Digest::EMPTY,
                default_range(),
                vec![],
            );
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                now,
                sha256::Digest::EMPTY,
                default_range(),
                vec![],
            );

            cache_block(&context, &mailbox, &parent).await;
            cache_block(&context, &mailbox, &block).await;

            assert!(!verify_block(&context, &mut application, &mailbox, &db, &block).await);
        });
    }

    #[test]
    fn verify_returns_immediately_for_mature_block_timestamp() {
        let runner = deterministic::Runner::default();
        runner.start(|mut context| async move {
            let (mut application, mailbox, db, _resolver_tx) =
                setup_application_test(&mut context).await;

            context.sleep(Duration::from_millis(10)).await;
            let now = context.current().epoch_millis();
            let (state_root, range) = compute_state(&db, Height::new(2), &[]).await;
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(b"genesis"),
                Height::new(1),
                now - 1,
                sha256::Digest::EMPTY,
                default_range(),
                vec![],
            );
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                now,
                state_root,
                range,
                vec![],
            );

            cache_block(&context, &mailbox, &parent).await;
            cache_block(&context, &mailbox, &block).await;

            let start = context.current();
            assert!(verify_block(&context, &mut application, &mailbox, &db, &block).await);
            let finished = context.current();
            assert!(finished.duration_since(start).unwrap() < Duration::from_millis(10));
        });
    }

    #[test]
    fn propose_uses_parent_timestamp_plus_one_when_clock_is_behind() {
        let runner = deterministic::Runner::default();
        runner.start(|mut context| async move {
            let (mut application, mailbox, db, _resolver_tx) =
                setup_application_test(&mut context).await;

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(b"genesis"),
                Height::new(1),
                now + 5_000,
                sha256::Digest::EMPTY,
                default_range(),
                vec![],
            );
            cache_block(&context, &mailbox, &parent).await;

            let proposal = propose_child(
                &context,
                &mut application,
                &mailbox,
                &db,
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
            let (mut application, mailbox, db, _resolver_tx) =
                setup_application_test(&mut context).await;

            let now = context.current().epoch_millis();
            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(b"genesis"),
                Height::new(1),
                now,
                sha256::Digest::EMPTY,
                default_range(),
                vec![],
            );
            let block = Block::new(
                test_context(2, (View::new(1), parent.digest())),
                parent.digest(),
                parent.height.next(),
                // Verification should reject timestamps outside the fixed
                // protocol range before attempting to sleep.
                MAX_BLOCK_TIMESTAMP_MS + 1,
                sha256::Digest::EMPTY,
                default_range(),
                vec![],
            );

            cache_block(&context, &mailbox, &parent).await;
            cache_block(&context, &mailbox, &block).await;

            assert!(!verify_block(&context, &mut application, &mailbox, &db, &block).await);
        });
    }

    #[test]
    #[should_panic(expected = "proposed timestamp exceeded maximum")]
    fn propose_panics_when_parent_timestamp_is_maximum() {
        let runner = deterministic::Runner::default();
        runner.start(|mut context| async move {
            let (mut application, mailbox, db, _resolver_tx) =
                setup_application_test(&mut context).await;

            let parent = Block::new(
                test_context(1, (View::zero(), sha256::Digest::EMPTY)),
                Sha256::hash(b"genesis"),
                Height::new(1),
                // Proposing on top of a parent already at the maximum would
                // require `parent.timestamp + 1`, which must be rejected.
                MAX_BLOCK_TIMESTAMP_MS,
                sha256::Digest::EMPTY,
                default_range(),
                vec![],
            );
            cache_block(&context, &mailbox, &parent).await;

            let _ = propose_child(
                &context,
                &mut application,
                &mailbox,
                &db,
                test_context(2, (View::new(1), parent.digest())),
                &parent,
            )
            .await;
        });
    }
}
