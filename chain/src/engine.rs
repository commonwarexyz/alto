use crate::{
    application::Application,
    indexer::{self, Indexer},
};
use alto_types::{
    ActivityOf, Block, FinalizationOf, SchemeOf, EPOCH, EPOCH_LENGTH, NAMESPACE,
};
use commonware_broadcast::buffered;
use commonware_consensus::{
    application::marshaled::Marshaled as ConsensusMarshaled,
    marshal::{self, ingress::handler},
    simplex::{self, elector::Random, Engine as Consensus},
    types::{Epoch, FixedEpocher, ViewDelta},
    Reporters,
};
use commonware_cryptography::{
    bls12381::primitives::{group, sharing::Sharing, variant::MinSig},
    certificate::{ConstantProvider, Scheme as CertificateScheme},
    ed25519::PublicKey,
    sha256::Digest,
};
use commonware_p2p::{Blocker, Receiver, Sender};
use commonware_parallel::Strategy;
use commonware_resolver::Resolver;
use commonware_runtime::{
    buffer::PoolRef, spawn_cell, Clock, ContextCell, Handle, Metrics, RayonPoolSpawner, Spawner,
    Storage,
};
use commonware_storage::archive::immutable;
use commonware_utils::{ordered::Set, NZU16};
use commonware_utils::{NZUsize, NZU64};
use futures::{channel::mpsc, future::try_join_all};
use governor::clock::Clock as GClock;
use governor::Quota;
use rand::{CryptoRng, Rng};
use std::{
    num::NonZero,
    time::{Duration, Instant},
};
use tracing::{error, info, warn};

/// Reporter type for [simplex::Engine].
type Reporter<E, I, S> =
    Reporters<ActivityOf<S>, marshal::Mailbox<SchemeOf<S>, Block>, Option<indexer::Pusher<E, I, S>>>;

/// To better support peers near tip during network instability, we multiply
/// the consensus activity timeout by this factor.
const SYNCER_ACTIVITY_TIMEOUT_MULTIPLIER: u64 = 10;
const PRUNABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(4_096);
const IMMUTABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(262_144);
const FREEZER_TABLE_RESIZE_FREQUENCY: u8 = 4;
const FREEZER_TABLE_RESIZE_CHUNK_SIZE: u32 = 2u32.pow(16); // 3MB
const FREEZER_JOURNAL_TARGET_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
const FREEZER_JOURNAL_COMPRESSION: Option<u8> = Some(3);
const REPLAY_BUFFER: NonZero<usize> = NZUsize!(8 * 1024 * 1024); // 8MB
const WRITE_BUFFER: NonZero<usize> = NZUsize!(1024 * 1024); // 1MB
const BUFFER_POOL_PAGE_SIZE: NonZero<u16> = NZU16!(4_096); // 4KB
const BUFFER_POOL_CAPACITY: NonZero<usize> = NZUsize!(8_192); // 32MB
const MAX_REPAIR: NonZero<usize> = NZUsize!(20);

/// Configuration for the [Engine].
pub struct Config<B: Blocker<PublicKey = PublicKey>, I: Indexer, S: Strategy> {
    pub blocker: B,
    pub partition_prefix: String,
    pub blocks_freezer_table_initial_size: u32,
    pub finalized_freezer_table_initial_size: u32,
    pub me: PublicKey,
    pub polynomial: Sharing<MinSig>,
    pub share: group::Share,
    pub participants: Set<PublicKey>,
    pub mailbox_size: usize,
    pub deque_size: usize,

    pub leader_timeout: Duration,
    pub notarization_timeout: Duration,
    pub nullify_retry: Duration,
    pub fetch_timeout: Duration,
    pub activity_timeout: ViewDelta,
    pub skip_timeout: ViewDelta,
    pub max_fetch_count: usize,
    pub max_fetch_size: usize,
    pub fetch_concurrent: usize,
    pub fetch_rate_per_peer: Quota,

    pub indexer: Option<I>,

    /// Execution strategy for parallel cryptographic operations.
    pub strategy: S,
}

type Marshaled<E, S> = ConsensusMarshaled<E, SchemeOf<S>, Application<S>, Block, FixedEpocher>;

/// The engine that drives the [Application].
#[allow(clippy::type_complexity)]
pub struct Engine<
    E: Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
    B: Blocker<PublicKey = PublicKey>,
    I: Indexer,
    S: Strategy,
> {
    context: ContextCell<E>,

    buffer: buffered::Engine<E, PublicKey, Block>,
    buffer_mailbox: buffered::Mailbox<PublicKey, Block>,
    marshal: marshal::Actor<
        E,
        Block,
        ConstantProvider<SchemeOf<S>, Epoch>,
        immutable::Archive<E, Digest, FinalizationOf<S>>,
        immutable::Archive<E, Digest, Block>,
        FixedEpocher,
    >,
    marshaled: Marshaled<E, S>,

    consensus: Consensus<E, SchemeOf<S>, Random, B, Digest, Marshaled<E, S>, Marshaled<E, S>, Reporter<E, I, S>>,
}

impl<
        E: Clock + GClock + Rng + CryptoRng + Spawner + RayonPoolSpawner + Storage + Metrics,
        B: Blocker<PublicKey = PublicKey>,
        I: Indexer,
        S: Strategy,
    > Engine<E, B, I, S>
{
    /// Create a new [Engine].
    pub async fn new(context: E, cfg: Config<B, I, S>) -> Self {
        // Create the buffer
        let (buffer, buffer_mailbox) = buffered::Engine::new(
            context.with_label("buffer"),
            buffered::Config {
                public_key: cfg.me,
                mailbox_size: cfg.mailbox_size,
                deque_size: cfg.deque_size,
                priority: true,
                codec_config: (),
            },
        );

        // Create the buffer pool
        let buffer_pool = PoolRef::new(BUFFER_POOL_PAGE_SIZE, BUFFER_POOL_CAPACITY);

        // Initialize finalizations by height
        let start = Instant::now();
        let finalizations_by_height = immutable::Archive::init(
            context.with_label("finalizations_by_height"),
            immutable::Config {
                metadata_partition: format!(
                    "{}-finalizations-by-height-metadata",
                    cfg.partition_prefix
                ),
                freezer_table_partition: format!(
                    "{}-finalizations-by-height-freezer-table",
                    cfg.partition_prefix
                ),
                freezer_table_initial_size: cfg.finalized_freezer_table_initial_size,
                freezer_table_resize_frequency: FREEZER_TABLE_RESIZE_FREQUENCY,
                freezer_table_resize_chunk_size: FREEZER_TABLE_RESIZE_CHUNK_SIZE,
                freezer_key_partition: format!(
                    "{}-finalizations-by-height-freezer-key-journal",
                    cfg.partition_prefix
                ),
                freezer_key_buffer_pool: buffer_pool.clone(),
                freezer_key_write_buffer: WRITE_BUFFER,
                freezer_value_partition: format!(
                    "{}-finalizations-by-height-freezer-value-journal",
                    cfg.partition_prefix
                ),
                freezer_value_write_buffer: WRITE_BUFFER,
                freezer_value_target_size: FREEZER_JOURNAL_TARGET_SIZE,
                freezer_value_compression: FREEZER_JOURNAL_COMPRESSION,
                ordinal_partition: format!(
                    "{}-finalizations-by-height-ordinal",
                    cfg.partition_prefix
                ),
                ordinal_write_buffer: WRITE_BUFFER,
                items_per_section: IMMUTABLE_ITEMS_PER_SECTION,
                codec_config: SchemeOf::<S>::certificate_codec_config_unbounded(),
                replay_buffer: REPLAY_BUFFER,
            },
        )
        .await
        .expect("failed to initialize finalizations by height archive");
        info!(elapsed = ?start.elapsed(), "restored finalizations by height archive");

        // Initialize finalized blocks
        let start = Instant::now();
        let finalized_blocks = immutable::Archive::init(
            context.with_label("finalized_blocks"),
            immutable::Config {
                metadata_partition: format!("{}-finalized_blocks-metadata", cfg.partition_prefix),
                freezer_table_partition: format!(
                    "{}-finalized_blocks-freezer-table",
                    cfg.partition_prefix
                ),
                freezer_table_initial_size: cfg.blocks_freezer_table_initial_size,
                freezer_table_resize_frequency: FREEZER_TABLE_RESIZE_FREQUENCY,
                freezer_table_resize_chunk_size: FREEZER_TABLE_RESIZE_CHUNK_SIZE,
                freezer_key_partition: format!(
                    "{}-finalized-blocks-freezer-key-journal",
                    cfg.partition_prefix
                ),
                freezer_key_buffer_pool: buffer_pool.clone(),
                freezer_key_write_buffer: WRITE_BUFFER,
                freezer_value_partition: format!(
                    "{}-finalized-blocks-freezer-value-journal",
                    cfg.partition_prefix
                ),
                freezer_value_write_buffer: WRITE_BUFFER,
                freezer_value_target_size: FREEZER_JOURNAL_TARGET_SIZE,
                freezer_value_compression: FREEZER_JOURNAL_COMPRESSION,
                ordinal_partition: format!("{}-finalized-blocks-ordinal", cfg.partition_prefix),
                ordinal_write_buffer: WRITE_BUFFER,
                items_per_section: IMMUTABLE_ITEMS_PER_SECTION,
                codec_config: (),
                replay_buffer: REPLAY_BUFFER,
            },
        )
        .await
        .expect("failed to initialize finalized blocks archive");
        info!(elapsed = ?start.elapsed(), "restored finalized blocks archive");

        // Create marshal
        let scheme = SchemeOf::<S>::signer(
            NAMESPACE,
            cfg.participants,
            cfg.polynomial,
            cfg.share,
            cfg.strategy,
        )
        .expect("failed to create scheme");
        let provider = ConstantProvider::new(scheme.clone());
        let epocher = FixedEpocher::new(EPOCH_LENGTH);
        let (marshal, marshal_mailbox, _) = marshal::Actor::init(
            context.with_label("marshal"),
            finalizations_by_height,
            finalized_blocks,
            marshal::Config {
                provider,
                epocher: epocher.clone(),
                partition_prefix: cfg.partition_prefix.clone(),
                mailbox_size: cfg.mailbox_size,
                view_retention_timeout: ViewDelta::new(
                    cfg.activity_timeout
                        .get()
                        .saturating_mul(SYNCER_ACTIVITY_TIMEOUT_MULTIPLIER),
                ),
                prunable_items_per_section: PRUNABLE_ITEMS_PER_SECTION,
                replay_buffer: REPLAY_BUFFER,
                key_write_buffer: WRITE_BUFFER,
                value_write_buffer: WRITE_BUFFER,
                block_codec_config: (),
                max_repair: MAX_REPAIR,
                buffer_pool: buffer_pool.clone(),
            },
        )
        .await;

        // Create the application
        let app = Application::new();
        let marshaled = Marshaled::new(
            context.with_label("marshaled"),
            app,
            marshal_mailbox.clone(),
            epocher,
        );

        // Create the reporter
        let reporter = (
            marshal_mailbox.clone(),
            cfg.indexer.map(|indexer| {
                indexer::Pusher::new(
                    context.with_label("indexer"),
                    indexer,
                    marshal_mailbox.clone(),
                )
            }),
        )
            .into();

        // Create the consensus engine
        let consensus = Consensus::new(
            context.with_label("consensus"),
            simplex::Config {
                epoch: EPOCH,
                scheme,
                automaton: marshaled.clone(),
                relay: marshaled.clone(),
                reporter,
                partition: format!("{}-consensus", cfg.partition_prefix),
                mailbox_size: cfg.mailbox_size,
                leader_timeout: cfg.leader_timeout,
                notarization_timeout: cfg.notarization_timeout,
                nullify_retry: cfg.nullify_retry,
                fetch_timeout: cfg.fetch_timeout,
                activity_timeout: cfg.activity_timeout,
                skip_timeout: cfg.skip_timeout,
                fetch_concurrent: cfg.fetch_concurrent,
                replay_buffer: REPLAY_BUFFER,
                write_buffer: WRITE_BUFFER,
                blocker: cfg.blocker,
                buffer_pool,
                elector: Random,
            },
        );

        // Return the engine
        Self {
            context: ContextCell::new(context),

            buffer,
            buffer_mailbox,
            marshal,
            marshaled,
            consensus,
        }
    }

    /// Start the [simplex::Engine].
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        mut self,
        pending: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        recovered: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        resolver: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        broadcast: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        marshal: (
            mpsc::Receiver<handler::Message<Block>>,
            impl Resolver<Key = handler::Request<Block>, PublicKey = PublicKey>,
        ),
    ) -> Handle<()> {
        spawn_cell!(
            self.context,
            self.run(pending, recovered, resolver, broadcast, marshal)
                .await
        )
    }

    #[allow(clippy::too_many_arguments)]
    async fn run(
        self,
        pending: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        recovered: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        resolver: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        broadcast: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        marshal: (
            mpsc::Receiver<handler::Message<Block>>,
            impl Resolver<Key = handler::Request<Block>, PublicKey = PublicKey>,
        ),
    ) {
        // Start the buffer
        let buffer_handle = self.buffer.start(broadcast);

        // Start marshal
        let marshal_handle = self
            .marshal
            .start(self.marshaled, self.buffer_mailbox, marshal);

        // Start consensus
        //
        // We start the application prior to consensus to ensure we can handle enqueued events from consensus (otherwise
        // restart could block).
        let consensus_handle = self.consensus.start(pending, recovered, resolver);

        // Wait for any actor to finish
        if let Err(e) = try_join_all(vec![buffer_handle, marshal_handle, consensus_handle]).await {
            error!(?e, "engine failed");
        } else {
            warn!("engine stopped");
        }
    }
}
