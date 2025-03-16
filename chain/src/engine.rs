use crate::{
    actors::{application, syncer},
    Indexer,
};
use alto_types::NAMESPACE;
use commonware_consensus::threshold_simplex::{self, Engine as Consensus, Prover};
use commonware_cryptography::{
    bls12381::primitives::{group, poly::public, poly::Poly},
    ed25519::PublicKey,
    sha256::Digest,
    Ed25519, Scheme,
};
use commonware_p2p::{Receiver, Sender};
use commonware_runtime::{Blob, Clock, Handle, Metrics, Spawner, Storage};
use commonware_storage::journal::variable::{self, Journal};
use futures::future::try_join_all;
use governor::clock::Clock as GClock;
use governor::Quota;
use rand::{CryptoRng, Rng};
use std::time::Duration;
use tracing::{error, warn};

pub struct Config<I: Indexer> {
    pub partition_prefix: String,
    pub signer: Ed25519,
    pub identity: Poly<group::Public>,
    pub share: group::Share,
    pub participants: Vec<PublicKey>,
    pub mailbox_size: usize,
    pub backfill_quota: Quota,

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
    B: Blob,
    E: Clock + GClock + Rng + CryptoRng + Spawner + Storage<B> + Metrics,
    I: Indexer,
> {
    context: E,

    application: application::Actor<E>,
    syncer: syncer::Actor<B, E, I>,
    syncer_mailbox: syncer::Mailbox,
    consensus: Consensus<
        B,
        E,
        Ed25519,
        Digest,
        application::Mailbox,
        application::Mailbox,
        application::Mailbox,
        application::Supervisor,
    >,
}

impl<B: Blob, E: Clock + GClock + Rng + CryptoRng + Spawner + Storage<B> + Metrics, I: Indexer>
    Engine<B, E, I>
{
    pub async fn new(context: E, cfg: Config<I>) -> Self {
        // Create the application
        let public = public(&cfg.identity);
        let (application, supervisor, application_mailbox) = application::Actor::new(
            context.with_label("application"),
            application::Config {
                prover: Prover::new(*public, NAMESPACE),
                participants: cfg.participants.clone(),
                identity: cfg.identity.clone(),
                share: cfg.share,
                mailbox_size: cfg.mailbox_size,
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
                activity_timeout: cfg.activity_timeout,
                indexer: cfg.indexer,
            },
        )
        .await;

        // Create the consensus engine
        let journal = Journal::init(
            context.with_label("consensus_journal"),
            variable::Config {
                partition: format!("{}-consensus-journal", cfg.partition_prefix),
            },
        )
        .await
        .expect("failed to create journal");
        let consensus = Consensus::new(
            context.with_label("consensus"),
            journal,
            threshold_simplex::Config {
                namespace: NAMESPACE.to_vec(),
                crypto: cfg.signer,
                automaton: application_mailbox.clone(),
                relay: application_mailbox.clone(),
                committer: application_mailbox,
                supervisor,
                mailbox_size: cfg.mailbox_size,
                replay_concurrency: 1,
                leader_timeout: cfg.leader_timeout,
                notarization_timeout: cfg.notarization_timeout,
                nullify_retry: cfg.nullify_retry,
                fetch_timeout: cfg.fetch_timeout,
                activity_timeout: cfg.activity_timeout,
                skip_timeout: cfg.skip_timeout,
                max_fetch_count: cfg.max_fetch_count,
                max_fetch_size: cfg.max_fetch_size,
                fetch_concurrent: cfg.fetch_concurrent,
                fetch_rate_per_peer: cfg.fetch_rate_per_peer,
            },
        );

        // Return the engine
        Self {
            context,

            application,
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

        // Start the syncer
        let syncer_handle = self.syncer.start(broadcast_network, backfill_network);

        // Start consensus
        let consensus_handle = self.consensus.start(voter_network, resolver_network);

        // Wait for any actor to finish
        if let Err(e) =
            try_join_all(vec![application_handle, syncer_handle, consensus_handle]).await
        {
            error!(?e, "engine failed");
        } else {
            warn!("engine stopped");
        }
    }
}
