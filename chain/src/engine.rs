use crate::{
    actors::{application, coordinator::Coordinator, indexer, syncer},
    Indexer,
};
use alto_types::{Activity, Block, Evaluation, NAMESPACE};
use commonware_broadcast::buffered;
use commonware_consensus::{
    marshal,
    threshold_simplex::{self, Engine as Consensus},
};
use commonware_cryptography::{
    bls12381::primitives::{
        group,
        poly::{public, Poly},
        variant::MinSig,
    },
    ed25519::{PrivateKey, PublicKey},
    sha256::Digest,
    Signer,
};
use commonware_p2p::{Blocker, Receiver, Sender};
use commonware_runtime::{Clock, Handle, Metrics, Spawner, Storage};
use futures::{future::try_join_all, join};
use governor::clock::Clock as GClock;
use governor::Quota;
use rand::{CryptoRng, Rng};
use std::time::Duration;
use tracing::{error, warn};

#[derive(Clone)]
struct Reporter<E: Spawner, I: Indexer> {
    marshal: marshal::ingress::mailbox::Mailbox<MinSig, Block>,
    indexer: Option<indexer::Indexer<E, I>>,
}

impl<E: Spawner, I: Indexer> Reporter<E, I> {
    pub fn new(
        marshal: marshal::ingress::mailbox::Mailbox<MinSig, Block>,
        indexer: Option<indexer::Indexer<E, I>>,
    ) -> Self {
        Self { marshal, indexer }
    }
}

impl<E: Spawner, I: Indexer> commonware_consensus::Reporter for Reporter<E, I> {
    type Activity = Activity;

    async fn report(&mut self, activity: Self::Activity) {
        if let Some(indexer) = self.indexer.as_mut() {
            join!(
                self.marshal.report(activity.clone()),
                indexer.report(activity)
            );
        } else {
            self.marshal.report(activity).await;
        }
    }
}

/// To better support peers near tip during network instability, we multiply
/// the consensus activity timeout by this factor.
const SYNCER_ACTIVITY_TIMEOUT_MULTIPLIER: u64 = 10;
const PRUNABLE_ITEMS_PER_SECTION: u64 = 4_096;
const IMMUTABLE_ITEMS_PER_SECTION: u64 = 262_144;
const FREEZER_TABLE_RESIZE_FREQUENCY: u8 = 4;
const FREEZER_TABLE_RESIZE_CHUNK_SIZE: u32 = 2u32.pow(16); // 3MB
const FREEZER_JOURNAL_TARGET_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
const FREEZER_JOURNAL_COMPRESSION: Option<u8> = Some(3);
const REPLAY_BUFFER: usize = 8 * 1024 * 1024; // 8MB
const WRITE_BUFFER: usize = 1024 * 1024; // 1MB
const MAX_REPAIR: u64 = 20;

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

pub struct Engine<
    E: Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
    B: Blocker<PublicKey = PublicKey>,
    I: Indexer,
> {
    context: E,

    application: application::Actor<E>,
    application_mailbox: application::Mailbox,
    buffer: buffered::Engine<E, PublicKey, Block>,
    buffer_mailbox: buffered::Mailbox<PublicKey, Block>,
    marshal: marshal::actor::Actor<Block, E, MinSig, PublicKey, Coordinator<PublicKey>>,
    marshal_mailbox: marshal::ingress::mailbox::Mailbox<MinSig, Block>,
    consensus: Consensus<
        E,
        PrivateKey,
        B,
        MinSig,
        Digest,
        application::Mailbox,
        application::Mailbox,
        Reporter<E, I>,
        application::Supervisor,
    >,
}

impl<
        E: Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
        B: Blocker<PublicKey = PublicKey>,
        I: Indexer,
    > Engine<E, B, I>
{
    pub async fn new(context: E, cfg: Config<B, I>) -> Self {
        // Create the application
        let identity = *public::<MinSig>(&cfg.polynomial);
        let (application, supervisor, application_mailbox) = application::Actor::new(
            context.with_label("application"),
            application::Config {
                participants: cfg.participants.clone(),
                polynomial: cfg.polynomial,
                share: cfg.share,
                mailbox_size: cfg.mailbox_size,
            },
        );

        // Create the buffer
        let (buffer, buffer_mailbox) = buffered::Engine::new(
            context.with_label("buffer"),
            buffered::Config {
                public_key: cfg.signer.public_key(),
                mailbox_size: cfg.mailbox_size,
                deque_size: cfg.deque_size,
                priority: true,
                codec_config: (),
            },
        );

        // Create the coordinator
        let coordinator = Coordinator::new(cfg.participants);

        // Create marshal
        let (marshal, marshal_mailbox): (_, marshal::ingress::mailbox::Mailbox<MinSig, Block>) =
            marshal::actor::Actor::init(
                context.with_label("marshal"),
                marshal::config::Config {
                    public_key: cfg.signer.public_key(),
                    identity,
                    coordinator,
                    partition_prefix: cfg.partition_prefix.clone(),
                    mailbox_size: cfg.mailbox_size,
                    backfill_quota: cfg.backfill_quota,
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
                    replay_buffer: REPLAY_BUFFER,
                    write_buffer: WRITE_BUFFER,
                    codec_config: (),
                    max_repair: MAX_REPAIR,
                },
            )
            .await;

        // Create the reporter
        let reporter = Reporter::new(
            marshal_mailbox.clone(),
            cfg.indexer.map(|indexer| {
                indexer::Indexer::new(
                    context.with_label("indexer"),
                    indexer,
                    marshal_mailbox.clone(),
                )
            }),
        );

        // Create the consensus engine
        let consensus = Consensus::new(
            context.with_label("consensus"),
            threshold_simplex::Config {
                namespace: NAMESPACE.to_vec(),
                crypto: cfg.signer,
                automaton: application_mailbox.clone(),
                relay: application_mailbox.clone(),
                reporter,
                supervisor,
                partition: format!("{}-consensus", cfg.partition_prefix),
                compression: None,
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
            },
        );

        // Return the engine
        Self {
            context,

            application,
            application_mailbox,
            buffer,
            buffer_mailbox,
            marshal,
            marshal_mailbox,
            consensus,
        }
    }

    /// Start the `simplex` consensus engine.
    ///
    /// This will also rebuild the state of the engine from provided `Journal`.
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
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
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
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
    ) {
        // Start the application
        let application_handle = self.application.start(self.marshal_mailbox);

        // Start the buffer
        let buffer_handle = self.buffer.start(broadcast_network);

        // Start marshal
        let marshal_handle = self.marshal.start(
            self.application_mailbox,
            self.buffer_mailbox,
            backfill_network,
        );

        // Start consensus
        //
        // We start the application prior to consensus to ensure we can handle enqueued events from consensus (otherwise
        // restart could block).
        let consensus_handle =
            self.consensus
                .start(pending_network, recovered_network, resolver_network);

        // Wait for any actor to finish
        if let Err(e) = try_join_all(vec![
            application_handle,
            buffer_handle,
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
