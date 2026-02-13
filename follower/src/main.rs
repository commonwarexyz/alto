use alto_client::Client;
use alto_follower::{
    backfill::Backfiller,
    engine::{self, Engine},
    feeder::CertificateFeeder,
    resolver::HttpResolverActor,
    Config, IndexQuery,
};
use alto_types::{Identity, Scheme, NAMESPACE};
use clap::{Arg, Command};
use commonware_codec::DecodeExt;
use commonware_macros::select;
use commonware_parallel::Sequential;
use commonware_runtime::{tokio, Clock, Metrics, Runner, Spawner};
use commonware_utils::{channel::mpsc, from_hex_formatted};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZero,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};
use tracing::{error, info, warn, Level};

fn main() {
    // Parse arguments
    let matches = Command::new("follower")
        .about("Follower node for an alto chain (non-validator)")
        .arg(Arg::new("config").long("config").required(true))
        .get_matches();

    // Load config
    let config: Config = {
        let config_path = matches.get_one::<String>("config").unwrap();
        let config_file =
            std::fs::read_to_string(config_path).expect("Could not read config file");
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

        info!(source = %config.source, "starting follower node");

        // Create scheme and client
        let scheme = Scheme::certificate_verifier(NAMESPACE, identity);
        let client = Client::new(&config.source, identity, Sequential);

        // Wait for certificate source to be available
        while let Err(e) = client.health().await {
            warn!(error = ?e, "waiting for certificate source to be available...");
            context.sleep(Duration::from_secs(1)).await;
        }
        info!("connected to certificate source");

        // Create archives
        let (mut finalizations_by_height, mut finalized_blocks, page_cache) =
            engine::create_archives(&context, &scheme).await;

        // Backfill or skip to tip
        if config.tip {
            // Skip to tip: set marshal's floor to the latest finalized block
        } else {
            // Backfill: fetch all finalized blocks from the source
            match client.finalized_get(IndexQuery::Latest).await {
                Ok(finalized) => {
                    let tip = finalized.block.height;
                    info!(tip = tip.get(), "backfilling to tip");
                    let backfiller = Backfiller::new(
                        client.clone(),
                        scheme.clone(),
                        finalizations_by_height,
                        finalized_blocks,
                        tip,
                        config.backfill_concurrency,
                    );
                    (finalizations_by_height, finalized_blocks) = backfiller.run().await;
                }
                Err(e) => {
                    warn!(error = ?e, "failed to fetch tip for backfill, starting from genesis");
                }
            }
        }

        // Create engine with pre-filled archives
        let max_repair = NonZero::new(config.max_repair).expect("max_repair must be non-zero");
        let engine = Engine::new(
            context.clone(),
            scheme.clone(),
            finalizations_by_height,
            finalized_blocks,
            page_cache,
            config.mailbox_size,
            max_repair,
        )
        .await;
        let mut marshal_mailbox = engine.mailbox();

        // If tip mode, set marshal's floor to the latest finalized block
        if config.tip {
            match client.finalized_get(IndexQuery::Latest).await {
                Ok(finalized) => {
                    if !finalized.verify(&scheme, &Sequential) {
                        warn!("failed to verify finalization signature for checkpoint, will backfill from genesis");
                    } else {
                        let height = finalized.block.height;
                        info!(height = height.get(), "setting checkpoint floor from latest finalized block");
                        marshal_mailbox.set_floor(height).await;
                    }
                }
                Err(e) => {
                    warn!(error = ?e, "failed to fetch latest finalized block for checkpoint, will backfill from genesis");
                }
            }
        }

        // Create resolver
        let (ingress_tx, ingress_rx) = mpsc::channel(config.mailbox_size);
        let (resolver_actor, resolver) =
            HttpResolverActor::new(client.clone(), ingress_tx, config.mailbox_size);
        let resolver_handle = context.clone().spawn(|_| resolver_actor.run());

        // Start engine
        let (engine_handle, buffer_handle) = engine.start(ingress_rx, resolver);

        // Start certificate feeder
        let feeder = CertificateFeeder::new(context, client, scheme, marshal_mailbox);
        let feeder_handle = feeder.start();

        // Wait for any task to finish
        select! {
            _ = engine_handle => {},
            _ = buffer_handle => {},
            _ = feeder_handle => {},
            _ = resolver_handle => {},
        };
        error!("follower stopped unexpectedly");
    });
}
