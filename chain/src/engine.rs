use crate::{
    actors::{application, syncer},
    Indexer,
};
use alto_types::{Block, NAMESPACE};
use commonware_broadcast::buffered;
use commonware_consensus::threshold_simplex::{self, Engine as Consensus};
use commonware_cryptography::{
    bls12381::primitives::{
        group,
        poly::{public, Poly},
    },
    ed25519::{self, PublicKey},
    sha256::Digest,
    Ed25519, Signer,
};
use commonware_p2p::{Receiver, Sender};
use commonware_runtime::{Clock, Handle, Metrics, Spawner, Storage};
use futures::future::try_join_all;
use governor::clock::Clock as GClock;
use governor::Quota;
use rand::{CryptoRng, Rng};
use std::time::Duration;
use tracing::{error, warn};

/// To better support peers near tip during network instability, we multiply
/// the consensus activity timeout by this factor.
const SYNCER_ACTIVITY_TIMEOUT_MULTIPLIER: u64 = 10;

pub struct Config<I: Indexer> {
    pub partition_prefix: String,
    pub signer: Ed25519,
    pub identity: Poly<group::Public>,
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

pub struct Engine<E: Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics, I: Indexer> {
    context: E,

    application: application::Actor<E>,
    buffer: buffered::Engine<E, ed25519::PublicKey, Digest, Digest, Block>,
    buffer_mailbox: buffered::Mailbox<ed25519::PublicKey, Digest, Digest, Block>,
    syncer: syncer::Actor<E, I>,
    syncer_mailbox: syncer::Mailbox,
    consensus: Consensus<
        E,
        Ed25519,
        Digest,
        application::Mailbox,
        application::Mailbox,
        syncer::Mailbox,
        application::Supervisor,
    >,
}

impl<E: Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics, I: Indexer> Engine<E, I> {
    pub async fn new(context: E, cfg: Config<I>) -> Self {
        // Create the application
        let public = public(&cfg.identity);
        let (application, supervisor, application_mailbox) = application::Actor::new(
            context.with_label("application"),
            application::Config {
                participants: cfg.participants.clone(),
                identity: cfg.identity.clone(),
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

        // Create the syncer
        let (syncer, syncer_mailbox) = syncer::Actor::init(
            context.with_label("syncer"),
            syncer::Config {
                partition_prefix: cfg.partition_prefix.clone(),
                public_key: cfg.signer.public_key(),
                identity: *public,
                participants: cfg.participants,
                mailbox_size: cfg.mailbox_size,
                backfill_quota: cfg.backfill_quota,
                activity_timeout: cfg
                    .activity_timeout
                    .saturating_mul(SYNCER_ACTIVITY_TIMEOUT_MULTIPLIER),
                indexer: cfg.indexer,
            },
        )
        .await;

        // Create the consensus engine
        let consensus = Consensus::new(
            context.with_label("consensus"),
            threshold_simplex::Config {
                namespace: NAMESPACE.to_vec(),
                crypto: cfg.signer,
                automaton: application_mailbox.clone(),
                relay: application_mailbox.clone(),
                reporter: syncer_mailbox.clone(),
                supervisor,
                partition: format!("{}-consensus", cfg.partition_prefix),
                compression: Some(3),
                mailbox_size: cfg.mailbox_size,
                replay_concurrency: 1,
                leader_timeout: cfg.leader_timeout,
                notarization_timeout: cfg.notarization_timeout,
                nullify_retry: cfg.nullify_retry,
                fetch_timeout: cfg.fetch_timeout,
                activity_timeout: cfg.activity_timeout,
                skip_timeout: cfg.skip_timeout,
                max_fetch_count: cfg.max_fetch_count,
                fetch_concurrent: cfg.fetch_concurrent,
                fetch_rate_per_peer: cfg.fetch_rate_per_peer,
            },
        );

        // Return the engine
        Self {
            context,

            application,
            buffer,
            buffer_mailbox,
            syncer,
            syncer_mailbox,
            consensus,
        }
    }

    /// Start the `simplex` consensus engine.
    ///
    /// This will also rebuild the state of the engine from provided `Journal`.
    pub fn start(
        self,
        voter_network: (
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
                voter_network,
                resolver_network,
                broadcast_network,
                backfill_network,
            )
        })
    }

    async fn run(
        self,
        voter_network: (
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
        let application_handle = self.application.start(self.syncer_mailbox);

        // Start the buffer
        let buffer_handle = self.buffer.start(broadcast_network);

        // Start the syncer
        let syncer_handle = self.syncer.start(self.buffer_mailbox, backfill_network);

        // Start consensus
        //
        // We start the application prior to consensus to ensure we can handle enqueued events from consensus (otherwise
        // restart could block).
        let consensus_handle = self.consensus.start(voter_network, resolver_network);

        // Wait for any actor to finish
        if let Err(e) = try_join_all(vec![
            application_handle,
            buffer_handle,
            syncer_handle,
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
