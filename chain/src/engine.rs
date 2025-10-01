use crate::{
    application::AltoApp,
    indexer::{self, Indexer},
    supervisor::Supervisor,
};
use alto_types::{Activity, Block, Evaluation, NAMESPACE};
use commonware_broadcast::buffered;
use commonware_coding::ReedSolomon;
use commonware_consensus::{
    marshal::{
        self,
        coding::{self, CodedBlock, CodingAdapter, Shard},
        ingress::handler,
    },
    threshold_simplex::{self, Engine as Consensus},
    types::CodingCommitment,
    Reporters,
};
use commonware_cryptography::{
    bls12381::primitives::{
        group,
        poly::{public, Poly},
        variant::MinSig,
    },
    ed25519::{PrivateKey, PublicKey},
    Sha256, Signer,
};
use commonware_p2p::{Blocker, Receiver, Sender};
use commonware_runtime::{buffer::PoolRef, Clock, Handle, Metrics, Spawner, Storage};
use commonware_utils::{NZUsize, NZU64};
use futures::{channel::mpsc, future::try_join_all};
use governor::clock::Clock as GClock;
use governor::Quota;
use rand::{CryptoRng, Rng};
use std::{num::NonZero, time::Duration};
use tracing::{error, warn};

/// Reporter type for [threshold_simplex::Engine].
type Reporter<E, I> = Reporters<
    Activity,
    Reporters<
        Activity,
        marshal::Mailbox<MinSig, Block>,
        coding::Mailbox<MinSig, Block, ReedSolomon<Sha256>, PublicKey>,
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
const MAX_REPAIR: u64 = 20;

/// Configuration for the [Engine].
pub struct Config<B: Blocker<PublicKey = PublicKey>, I: Indexer> {
    pub blocker: B,
    pub partition_prefix: String,
    pub blocks_freezer_table_initial_size: u32,
    pub finalized_freezer_table_initial_size: u32,
    pub signer: PrivateKey,
    pub polynomial: Poly<Evaluation>,
    pub share: group::Share,
    pub participants: Vec<PublicKey>,
    pub mailbox_size: usize,
    pub backfill_quota: Quota,
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

type Application<E> =
    CodingAdapter<E, AltoApp, MinSig, Block, ReedSolomon<Sha256>, PublicKey, Supervisor>;

/// The engine that drives the application.
pub struct Engine<
    E: Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
    B: Blocker<PublicKey = PublicKey>,
    I: Indexer,
> {
    context: E,

    application: Application<E>,
    buffer: buffered::Engine<E, PublicKey, Shard<ReedSolomon<Sha256>, Sha256>>,
    shard_mailbox: coding::Mailbox<MinSig, Block, ReedSolomon<Sha256>, PublicKey>,
    coding: coding::Actor<E, MinSig, ReedSolomon<Sha256>, Sha256, Block, PublicKey>,
    marshal: marshal::Actor<Block, E, MinSig, ReedSolomon<Sha256>>,

    pub supervisor: Supervisor,

    #[allow(clippy::type_complexity)]
    consensus: Consensus<
        E,
        PrivateKey,
        B,
        MinSig,
        CodingCommitment,
        Application<E>,
        Application<E>,
        Reporter<E, I>,
        Supervisor,
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
        let identity = *public::<MinSig>(&cfg.polynomial);
        let buffer_pool = PoolRef::new(BUFFER_POOL_PAGE_SIZE, BUFFER_POOL_CAPACITY);

        // Create marshal
        let (marshal, marshal_mailbox): (_, marshal::Mailbox<MinSig, Block>) =
            marshal::Actor::init(
                context.with_label("marshal"),
                marshal::Config {
                    identity,
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
                    codec_config: (),
                    max_repair: MAX_REPAIR,
                },
            )
            .await;

        // Create the buffer
        let (buffer, buffer_mailbox) = buffered::Engine::new(
            context.with_label("buffer"),
            buffered::Config {
                public_key: cfg.signer.public_key(),
                mailbox_size: cfg.mailbox_size,
                deque_size: cfg.deque_size,
                priority: true,
                codec_config: (usize::MAX, usize::MAX),
            },
        );

        let (coding, shard_mailbox) =
            coding::Actor::new(context.with_label("coding"), buffer_mailbox, ());

        let supervisor = Supervisor::new(cfg.polynomial, cfg.participants.clone(), cfg.share);
        let application = CodingAdapter::new(
            context.with_label("app"),
            AltoApp::default(),
            shard_mailbox.clone(),
            cfg.signer.public_key(),
            supervisor.clone(),
        );

        // Create the reporter
        let reporter = (
            Reporters::from((marshal_mailbox.clone(), shard_mailbox.clone())),
            cfg.indexer.map(|indexer| {
                indexer::Pusher::new(context.with_label("indexer"), indexer, marshal_mailbox)
            }),
        )
            .into();

        // Create the consensus engine
        let consensus = Consensus::new(
            context.with_label("consensus"),
            threshold_simplex::Config {
                epoch: 0,
                namespace: NAMESPACE.to_vec(),
                crypto: cfg.signer,
                automaton: application.clone(),
                relay: application.clone(),
                reporter,
                supervisor: supervisor.clone(),
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
            context,

            application,
            buffer,
            shard_mailbox,
            coding,
            marshal,

            supervisor,
            consensus,
        }
    }

    /// Start the [threshold_simplex::Engine].
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    pub fn start(
        self,
        pending_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        recovered_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        resolver_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        broadcast_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        backfill_network: (
            mpsc::Receiver<handler::Message<CodedBlock<Block, ReedSolomon<Sha256>>>>,
            commonware_resolver::p2p::Mailbox<
                handler::Request<CodedBlock<Block, ReedSolomon<Sha256>>>,
            >,
        ),
    ) -> Handle<()> {
        self.context.clone().spawn(|_| {
            self.run(
                pending_network,
                recovered_network,
                resolver_network,
                broadcast_network,
                backfill_network,
            )
        })
    }

    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    async fn run(
        self,
        pending_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        recovered_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        resolver_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        broadcast_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        backfill_network: (
            mpsc::Receiver<handler::Message<CodedBlock<Block, ReedSolomon<Sha256>>>>,
            commonware_resolver::p2p::Mailbox<
                handler::Request<CodedBlock<Block, ReedSolomon<Sha256>>>,
            >,
        ),
    ) {
        // Start the buffer
        let buffer_handle = self.buffer.start(broadcast_network);

        // Start coding
        let coding_handle = self.coding.start();

        // Start marshal
        let marshal_handle =
            self.marshal
                .start(self.application, self.shard_mailbox, backfill_network);

        // Start consensus
        //
        // We start the application prior to consensus to ensure we can handle enqueued events from consensus (otherwise
        // restart could block).
        let consensus_handle =
            self.consensus
                .start(pending_network, recovered_network, resolver_network);

        // Wait for any actor to finish
        if let Err(e) = try_join_all(vec![
            buffer_handle,
            coding_handle,
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
