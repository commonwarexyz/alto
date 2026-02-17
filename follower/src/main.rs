use alto_client::consensus::{Message, Payload};
use alto_client::{ClientBuilder, IndexQuery, Query};
use alto_types::{Finalized, Identity, Notarized, Scheme, NAMESPACE};
use clap::{Arg, Command};
use commonware_codec::DecodeExt;
use commonware_consensus::types::Height;
use commonware_cryptography::ed25519::PublicKey;
use commonware_macros::select;
use commonware_p2p::Recipients;
use commonware_parallel::Sequential;
use commonware_runtime::{tokio, Clock, IoBufs, Metrics, Runner, ThreadPooler};
use commonware_utils::{channel::mpsc, from_hex_formatted, time::SystemTimeExt};
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZero,
    path::PathBuf,
    str::FromStr,
    time::{Duration, SystemTime},
};
use tracing::{error, info, warn, Level};

mod engine;
mod feeder;
mod resolver;
mod throughput;

#[cfg(test)]
mod test_utils;

/// Configuration for the follower binary.
#[derive(Deserialize, Serialize)]
pub struct Config {
    pub source: String,
    pub identity: String,
    pub directory: String,
    pub worker_threads: usize,
    pub log_level: String,
    pub metrics_port: u16,
    pub mailbox_size: usize,
    pub max_repair: usize,
    pub tip: bool,
    pub pruning_depth: Option<u64>,
}

/// Abstraction over the certificate source (HTTP client) used by the
/// [feeder::Feeder] and [resolver::Actor].
#[allow(dead_code)]
pub(crate) trait Source: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Check if the source is reachable.
    fn health(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Fetch a block by digest or index.
    fn block(&self, query: Query) -> impl Future<Output = Result<Payload, Self::Error>> + Send;

    /// Fetch a notarized block by view or latest.
    fn notarized(
        &self,
        query: IndexQuery,
    ) -> impl Future<Output = Result<Notarized, Self::Error>> + Send;

    /// Fetch a finalized block by height or latest.
    fn finalized(
        &self,
        query: IndexQuery,
    ) -> impl Future<Output = Result<Finalized, Self::Error>> + Send;

    /// Open a WebSocket stream of certificate messages.
    fn listen(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<Message, Self::Error>> + Send + Unpin,
            Self::Error,
        >,
    > + Send;
}

impl<S: commonware_parallel::Strategy> Source for alto_client::Client<S> {
    type Error = alto_client::Error;

    fn health(&self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.health()
    }

    fn block(&self, query: Query) -> impl Future<Output = Result<Payload, Self::Error>> + Send {
        self.block_get(query)
    }

    fn notarized(
        &self,
        query: IndexQuery,
    ) -> impl Future<Output = Result<Notarized, Self::Error>> + Send {
        self.notarized_get(query)
    }

    fn finalized(
        &self,
        query: IndexQuery,
    ) -> impl Future<Output = Result<Finalized, Self::Error>> + Send {
        self.finalized_get(query)
    }

    fn listen(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<Message, Self::Error>> + Send + Unpin,
            Self::Error,
        >,
    > + Send {
        self.listen()
    }
}

/// Noop p2p sender used by the follower's buffer engine.
///
/// The follower does not participate in p2p broadcast, so all send
/// operations are dropped.
#[derive(Clone)]
pub(crate) struct NoopSender;

pub(crate) struct NoopCheckedSender;

impl commonware_p2p::CheckedSender for NoopCheckedSender {
    type PublicKey = PublicKey;
    type Error = std::io::Error;

    async fn send(
        self,
        _message: impl Into<IoBufs> + Send,
        _priority: bool,
    ) -> Result<Vec<Self::PublicKey>, Self::Error> {
        Ok(Vec::new())
    }
}

impl commonware_p2p::LimitedSender for NoopSender {
    type PublicKey = PublicKey;
    type Checked<'a> = NoopCheckedSender;

    async fn check(
        &mut self,
        _recipients: Recipients<Self::PublicKey>,
    ) -> Result<Self::Checked<'_>, SystemTime> {
        // Return the maximum possible time to ensure buffered never
        // retries sending -- the follower has no p2p peers.
        Err(SystemTime::limit())
    }
}

/// Noop p2p receiver used by the follower's buffer engine.
///
/// The follower does not participate in p2p broadcast, so recv blocks
/// forever (via [std::future::pending]).
#[derive(Debug)]
pub(crate) struct NoopReceiver;

impl commonware_p2p::Receiver for NoopReceiver {
    type Error = std::io::Error;
    type PublicKey = PublicKey;

    async fn recv(&mut self) -> Result<commonware_p2p::Message<Self::PublicKey>, Self::Error> {
        std::future::pending().await
    }
}

fn main() {
    // Parse arguments
    let matches = Command::new("follower")
        .about("Follower node for an alto chain (non-validator)")
        .arg(Arg::new("config").long("config").required(true))
        .get_matches();

    // Load config
    let config: Config = {
        let config_path = matches.get_one::<String>("config").unwrap();
        let config_file = std::fs::read_to_string(config_path).expect("Could not read config file");
        serde_yaml::from_str(&config_file).expect("Could not parse config file")
    };

    // Parse identity
    let identity_bytes =
        from_hex_formatted(&config.identity).expect("Could not parse identity hex");
    let identity =
        Identity::decode(identity_bytes.as_ref()).expect("Could not decode identity public key");

    // Initialize runtime
    let cfg = tokio::Config::default()
        .with_tcp_nodelay(Some(true))
        .with_worker_threads(config.worker_threads)
        .with_storage_directory(PathBuf::from(config.directory))
        .with_catch_panics(false);
    let executor = tokio::Runner::new(cfg);

    // Start runtime
    executor.start(|context| async move {
        // Configure telemetry
        let log_level = Level::from_str(&config.log_level).expect("Invalid log level");
        tokio::telemetry::init(
            context.with_label("telemetry"),
            tokio::telemetry::Logging {
                level: log_level,
                json: false,
            },
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                config.metrics_port,
            )),
            None,
        );
        info!(
            source = %config.source,
            pruning_depth = config.pruning_depth,
            "starting follower node"
        );

        // Create scheme and client
        //
        // The client is created without verification because signatures are
        // checked downstream at each ingestion point:
        //
        //   WebSocket path:  Feeder::handle_message             (feeder.rs)
        //   Resolver path:   marshal::Actor Deliver handler    (commonware-consensus)
        //   Tip check below: explicit finalized.verify call
        //
        // Any certificate verification failure is treated as fatal and
        // intentionally crashes the follower (fail-fast).
        let scheme = Scheme::certificate_verifier(NAMESPACE, identity);
        let client = ClientBuilder::new(&config.source, identity, Sequential)
            .with_verification_disabled()
            .build();

        // Wait for certificate source to be available
        while let Err(e) = client.health().await {
            warn!(error = ?e, "waiting for certificate source to be available...");
            context.sleep(Duration::from_secs(1)).await;
        }
        info!("connected to certificate source");

        // Create engine
        let strategy = context
            .create_strategy(NonZero::new(config.worker_threads).expect("worker_threads must be non-zero"))
            .unwrap();
        let (engine, mut mailbox, last_processed_height) = engine::Engine::new(
            context.with_label("engine"),
            scheme.clone(),
            config.mailbox_size,
            NonZero::new(config.max_repair).expect("max_repair must be non-zero"),
            strategy,
            config.pruning_depth,
        )
        .await;

        // On the first run (no previously synced data), optionally skip to the
        // latest finalized height so the follower starts near tip instead of
        // backfilling from genesis.
        if config.tip && last_processed_height == Height::zero() {
            match client.finalized_get(IndexQuery::Latest).await {
                Ok(finalized) => {
                    assert!(
                        finalized.verify(&scheme, &Sequential),
                        "failed to verify finalization signature for checkpoint"
                    );
                    let height = finalized.block.height;
                    info!(height = height.get(), "setting checkpoint floor from latest finalized block");
                    mailbox.set_floor(height).await;
                }
                Err(e) => {
                    warn!(error = ?e, "failed to fetch latest finalized block for checkpoint, will backfill from genesis");
                }
            }
        }

        // Create resolver
        let (ingress_tx, ingress_rx) = mpsc::channel(config.mailbox_size);
        let (resolver_actor, resolver) = resolver::Actor::new(
            context.with_label("resolver"),
            client.clone(),
            ingress_tx,
            config.mailbox_size,
        );
        let resolver_handle = resolver_actor.start();

        // Start engine
        let engine_handle = engine.start((ingress_rx, resolver));

        // Start certificate feeder
        let feeder = feeder::Feeder::new(
            context.with_label("feeder"),
            client,
            scheme,
            mailbox,
        );
        let feeder_handle = feeder.start();

        // Wait for any task to finish
        select! {
            _ = engine_handle => {},
            _ = feeder_handle => {},
            _ = resolver_handle => {},
        };
        error!("follower stopped unexpectedly");
    });
}
