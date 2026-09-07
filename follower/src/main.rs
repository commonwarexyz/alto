use alto_client::consensus::{Message, Payload};
use alto_client::{ClientBuilder, IndexQuery, Query};
use alto_types::{
    CertificateMode, Identity, Notarized, Scheme, StandardScheme, VrfScheme, NAMESPACE,
};
use clap::{Arg, Command};
use commonware_codec::DecodeExt;
use commonware_consensus::types::Height;
use commonware_formatting::from_hex;
use commonware_macros::select;
use commonware_parallel::{Rayon, Sequential};
use commonware_runtime::{tokio, Clock, Runner, Supervisor as _};
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZero,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};
use tracing::{error, info, warn, Level};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod application;
mod archive;
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
    pub certificate_mode: CertificateMode,
    pub directory: String,
    pub worker_threads: NonZero<usize>,
    pub signature_threads: NonZero<usize>,
    pub log_level: String,
    pub metrics_port: u16,
    pub mailbox_size: NonZero<usize>,
    pub max_repair: NonZero<usize>,
    pub fetch_retry_timeout_ms: u64,
    pub tip: bool,
    pub pruning_depth: Option<u64>,
}

/// Abstraction over the certificate source (HTTP client) used by the
/// [feeder::Feeder] and [resolver::Resolver].
pub(crate) trait Source: Clone + Send + Sync + 'static {
    type Scheme: Scheme;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Fetch a block by digest or index.
    fn block(
        &self,
        query: Query,
    ) -> impl Future<Output = Result<Payload<Self::Scheme>, Self::Error>> + Send;

    /// Fetch a notarized block by view or latest.
    fn notarized(
        &self,
        query: IndexQuery,
    ) -> impl Future<Output = Result<Notarized<Self::Scheme>, Self::Error>> + Send;

    /// Open a WebSocket stream of certificate messages.
    #[allow(clippy::type_complexity)]
    fn listen(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<Message<Self::Scheme>, Self::Error>> + Send + Unpin,
            Self::Error,
        >,
    > + Send;
}

impl<S: commonware_parallel::Strategy, C: Scheme> Source for alto_client::Client<S, C> {
    type Scheme = C;
    type Error = alto_client::Error;

    fn block(&self, query: Query) -> impl Future<Output = Result<Payload<C>, Self::Error>> + Send {
        self.block_get(query)
    }

    fn notarized(
        &self,
        query: IndexQuery,
    ) -> impl Future<Output = Result<Notarized<C>, Self::Error>> + Send {
        self.notarized_get(query)
    }

    fn listen(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<Message<C>, Self::Error>> + Send + Unpin,
            Self::Error,
        >,
    > + Send {
        self.listen()
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
    let identity_bytes = from_hex(&config.identity).expect("Could not parse identity hex");
    let identity =
        Identity::decode(identity_bytes.as_ref()).expect("Could not decode identity public key");

    // Initialize runtime
    let cfg = tokio::Config::default()
        .with_tcp_nodelay(Some(true))
        .with_worker_threads(config.worker_threads.get())
        .with_storage_directory(PathBuf::from(&config.directory))
        .with_catch_panics(false);
    let executor = tokio::Runner::new(cfg);

    // Start runtime
    executor.start(|context| async move {
        // Configure telemetry
        let log_level = Level::from_str(&config.log_level).expect("Invalid log level");
        tokio::telemetry::init(
            context.child("telemetry"),
            tokio::telemetry::Logs {
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
            fetch_retry_timeout_ms = config.fetch_retry_timeout_ms,
            pruning_depth = config.pruning_depth,
            "starting follower node"
        );

        match config.certificate_mode {
            CertificateMode::Standard => run::<StandardScheme>(context, config, identity).await,
            CertificateMode::Vrf => run::<VrfScheme>(context, config, identity).await,
        }
    });
}

async fn run<C: Scheme>(context: tokio::Context, config: Config, identity: Identity) {
    // The client decodes this concrete format but leaves signature checks to the feeder,
    // resolver-backed marshal, and checkpoint path so certificates are not verified twice.
    let scheme = C::certificate_verifier(NAMESPACE, identity);
    let client = ClientBuilder::new(&config.source, scheme.clone(), Sequential)
        .with_verification_disabled()
        .build();

    while let Err(e) = client.health().await {
        warn!(error = ?e, "waiting for certificate source to be available...");
        context.sleep(Duration::from_secs(1)).await;
    }
    info!("connected to certificate source");

    let strategy = Rayon::new(config.signature_threads).unwrap();
    let (engine, mailbox, last_processed_height) = engine::Engine::new(
        context.child("engine"),
        scheme.clone(),
        config.mailbox_size,
        config.max_repair,
        strategy,
        config.pruning_depth,
    )
    .await;

    if config.tip && last_processed_height.is_none_or(|height| height == Height::zero()) {
        match client.finalized_get(IndexQuery::Latest).await {
            Ok(finalized) => {
                assert!(
                    finalized.verify(&scheme, &Sequential),
                    "failed to verify finalization signature for checkpoint"
                );
                let height = finalized.block.height;
                info!(
                    height = height.get(),
                    "setting checkpoint floor from latest finalized block"
                );
                mailbox.set_floor(finalized.proof.clone());
            }
            Err(e) => {
                warn!(error = ?e, "failed to fetch latest finalized block for checkpoint, will backfill from genesis");
            }
        }
    }

    let marshal_resolver = resolver::init(
        context.child("resolver"),
        client.clone(),
        config.mailbox_size,
        Duration::from_millis(config.fetch_retry_timeout_ms),
    );
    let engine_handle = engine.start(marshal_resolver);

    let feeder = feeder::Feeder::new(context.child("feeder"), client, scheme, mailbox);
    let feeder_handle = feeder.start();

    select! {
        _ = engine_handle => {},
        _ = feeder_handle => {},
    };
    error!("follower stopped unexpectedly");
}
