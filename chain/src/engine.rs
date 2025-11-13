use crate::{
    application::AltoApp,
    indexer::{self, Indexer},
    StaticSchemeProvider,
};
use alto_types::{Activity, Block, Evaluation, PublicKey, Scheme, EPOCH, EPOCH_LENGTH, NAMESPACE};
use commonware_broadcast::buffered;
use commonware_coding::ReedSolomon;
use commonware_consensus::{
    marshal::{
        self,
        coding::{
            self,
            ingress::handler,
            shards,
            types::{CodingCommitment, Shard},
            Marshaled,
        },
    },
    simplex::{self, Engine as Consensus},
    Reporters,
};
use commonware_cryptography::{
    bls12381::primitives::{group, poly::Poly},
    Sha256,
};
use commonware_p2p::{Blocker, Receiver, Sender};
use commonware_resolver::Resolver;
use commonware_runtime::{
    buffer::PoolRef, spawn_cell, Clock, ContextCell, Handle, Metrics, Spawner, Storage,
};
use commonware_utils::set::Ordered;
use commonware_utils::{NZUsize, NZU64};
use futures::{channel::mpsc, future::try_join_all};
use governor::clock::Clock as GClock;
use governor::Quota;
use rand::{CryptoRng, Rng};
use std::marker::PhantomData;
use std::{num::NonZero, time::Duration};
use tracing::{error, warn};

/// Reporter type for [simplex::Engine].
type Reporter<E, I> = Reporters<
    Activity,
    Reporters<
        Activity,
        coding::Mailbox<Scheme, Block, ReedSolomon<Sha256>>,
        shards::Mailbox<Block, Scheme, ReedSolomon<Sha256>, PublicKey>,
    >,
    Option<indexer::Pusher<E, I>>,
>;

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
const BUFFER_POOL_PAGE_SIZE: NonZero<usize> = NZUsize!(4_096); // 4KB
const BUFFER_POOL_CAPACITY: NonZero<usize> = NZUsize!(8_192); // 32MB
const MAX_REPAIR: NonZero<u64> = NZU64!(20);

/// Configuration for the [Engine].
pub struct Config<B: Blocker<PublicKey = PublicKey>, I: Indexer> {
    pub blocker: B,
    pub partition_prefix: String,
    pub blocks_freezer_table_initial_size: u32,
    pub finalized_freezer_table_initial_size: u32,
    pub me: PublicKey,
    pub polynomial: Poly<Evaluation>,
    pub share: group::Share,
    pub participants: Ordered<PublicKey>,
    pub mailbox_size: usize,
    pub deque_size: usize,

    pub leader_timeout: Duration,
    pub notarization_timeout: Duration,
    pub nullify_retry: Duration,
    pub fetch_timeout: Duration,
    pub activity_timeout: u64,
    pub skip_timeout: u64,
    pub max_fetch_count: usize,
    pub max_fetch_size: usize,
    pub fetch_concurrent: usize,
    pub fetch_rate_per_peer: Quota,

    pub indexer: Option<I>,
}

/// The engine that drives the [application].
pub struct Engine<
    E: Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
    B: Blocker<PublicKey = PublicKey>,
    I: Indexer,
> {
    context: ContextCell<E>,

    buffer: buffered::Engine<E, PublicKey, Shard<ReedSolomon<Sha256>, Sha256>>,
    shards: shards::Engine<E, Scheme, ReedSolomon<Sha256>, Sha256, Block, PublicKey>,
    shard_mailbox: shards::Mailbox<Block, Scheme, ReedSolomon<Sha256>, PublicKey>,
    marshal: coding::Actor<E, Block, ReedSolomon<Sha256>, StaticSchemeProvider, Scheme>,

    marshaled:
        Marshaled<E, Scheme, AltoApp, Block, ReedSolomon<Sha256>, PublicKey, StaticSchemeProvider>,
    #[allow(clippy::type_complexity)]
    consensus: Consensus<
        E,
        PublicKey,
        Scheme,
        B,
        CodingCommitment,
        Marshaled<E, Scheme, AltoApp, Block, ReedSolomon<Sha256>, PublicKey, StaticSchemeProvider>,
        Marshaled<E, Scheme, AltoApp, Block, ReedSolomon<Sha256>, PublicKey, StaticSchemeProvider>,
        Reporter<E, I>,
    >,
}

impl<
        E: Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
        B: Blocker<PublicKey = PublicKey>,
        I: Indexer,
    > Engine<E, B, I>
{
    /// Create a new [Engine].
    pub async fn new(context: E, cfg: Config<B, I>) -> Self {
        // Create the buffer
        let (buffer, buffer_mailbox) = buffered::Engine::new(
            context.with_label("buffer"),
            buffered::Config {
                public_key: cfg.me,
                mailbox_size: cfg.mailbox_size,
                deque_size: cfg.deque_size,
                priority: true,
                codec_config: commonware_coding::CodecConfig {
                    maximum_shard_size: usize::MAX,
                },
            },
        );

        // Create the shard engine
        let (shards, shard_mailbox) = shards::Engine::new(
            context.with_label("shards"),
            buffer_mailbox,
            (),
            cfg.mailbox_size,
        );

        // Create the buffer pool
        let buffer_pool = PoolRef::new(BUFFER_POOL_PAGE_SIZE, BUFFER_POOL_CAPACITY);

        // Create the signing scheme
        let scheme = Scheme::new(cfg.participants, &cfg.polynomial, cfg.share);

        // Create marshal
        let (marshal, marshal_mailbox) = coding::Actor::init(
            context.with_label("marshal"),
            marshal::Config {
                scheme_provider: scheme.clone().into(),
                epoch_length: EPOCH_LENGTH,
                partition_prefix: cfg.partition_prefix.clone(),
                mailbox_size: cfg.mailbox_size,
                view_retention_timeout: cfg
                    .activity_timeout
                    .saturating_mul(SYNCER_ACTIVITY_TIMEOUT_MULTIPLIER),
                namespace: NAMESPACE.to_vec(),
                prunable_items_per_section: PRUNABLE_ITEMS_PER_SECTION,
                immutable_items_per_section: IMMUTABLE_ITEMS_PER_SECTION,
                freezer_table_initial_size: cfg.blocks_freezer_table_initial_size,
                freezer_table_resize_frequency: FREEZER_TABLE_RESIZE_FREQUENCY,
                freezer_table_resize_chunk_size: FREEZER_TABLE_RESIZE_CHUNK_SIZE,
                freezer_journal_target_size: FREEZER_JOURNAL_TARGET_SIZE,
                freezer_journal_compression: FREEZER_JOURNAL_COMPRESSION,
                freezer_journal_buffer_pool: buffer_pool.clone(),
                replay_buffer: REPLAY_BUFFER,
                write_buffer: WRITE_BUFFER,
                block_codec_config: (),
                max_repair: MAX_REPAIR,
                _marker: PhantomData,
            },
        )
        .await;

        // Create the application
        let app = AltoApp::new();
        let marshaled = Marshaled::new(
            context.with_label("marshaled"),
            app,
            marshal_mailbox.clone(),
            shard_mailbox.clone(),
            scheme.clone().into(),
            EPOCH_LENGTH,
        );

        // Create the reporter
        let reporter = (
            Reporters::from((marshal_mailbox.clone(), shard_mailbox.clone())),
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
                namespace: NAMESPACE.to_vec(),
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
                max_fetch_count: cfg.max_fetch_count,
                fetch_concurrent: cfg.fetch_concurrent,
                fetch_rate_per_peer: cfg.fetch_rate_per_peer,
                replay_buffer: REPLAY_BUFFER,
                write_buffer: WRITE_BUFFER,
                blocker: cfg.blocker,
                buffer_pool,
            },
        );

        // Return the engine
        Self {
            context: ContextCell::new(context),

            buffer,
            shards,
            shard_mailbox,
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
            impl Resolver<Key = handler::Request<Block>>,
        ),
    ) -> Handle<()> {
        spawn_cell!(
            self.context,
            self.run(pending, recovered, resolver, broadcast, marshal,)
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
            impl Resolver<Key = handler::Request<Block>>,
        ),
    ) {
        // Start the buffer
        let buffer_handle = self.buffer.start(broadcast);

        // Start the shard engine
        let shard_handle = self.shards.start();

        // Start marshal
        let marshal_handle = self
            .marshal
            .start(self.marshaled, self.shard_mailbox, marshal);

        // Start consensus
        //
        // We start the application prior to consensus to ensure we can handle enqueued events from consensus (otherwise
        // restart could block).
        let consensus_handle = self.consensus.start(pending, recovered, resolver);

        // Wait for any actor to finish
        if let Err(e) = try_join_all(vec![
            buffer_handle,
            shard_handle,
            marshal_handle,
            consensus_handle,
        ])
        .await
        {
            error!(?e, "engine failed");
        } else {
            warn!("engine stopped");
        }
    }
}
