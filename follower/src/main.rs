use alto_client::ClientBuilder;
use alto_follower::{
    engine::Engine, feeder::Feeder, resolver::Actor, Config, IndexQuery,
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

        info!(source = %config.source, "starting follower node");

        // Create scheme and client
        //
        // The client is created without verification because signatures are
        // checked downstream at each ingestion point:
        //
        //   WebSocket path:  Feeder::handle_message             (feeder.rs)
        //   Resolver path:   marshal::Actor Deliver handler    (commonware-consensus)
        //   Tip check below: explicit finalized.verify call
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
        let engine =
            Engine::new(context.clone(), scheme.clone(), config.mailbox_size, NonZero::new(config.max_repair).expect("max_repair must be non-zero")).await;
        let mut marshal_mailbox = engine.mailbox();

        // Optionally set checkpoint floor from the latest finalized block
        if config.tip {
            match client.finalized_get(IndexQuery::Latest).await {
                Ok(finalized) => {
                    assert!(
                        finalized.verify(&scheme, &Sequential),
                        "failed to verify finalization signature for checkpoint"
                    );
                    let height = finalized.block.height;
                    info!(height = height.get(), "setting checkpoint floor from latest finalized block");
                    marshal_mailbox.set_floor(height).await;
                }
                Err(e) => {
                    warn!(error = ?e, "failed to fetch latest finalized block for checkpoint, will backfill from genesis");
                }
            }
        }

        // Create resolver
        let (ingress_tx, ingress_rx) = mpsc::channel(config.mailbox_size);
        let (resolver_actor, resolver) =
            Actor::new(client.clone(), ingress_tx, config.mailbox_size);
        let resolver_handle = context.clone().spawn(|_| resolver_actor.run());

        // Start engine
        let (engine_handle, buffer_handle) = engine.start(ingress_rx, resolver);

        // Start certificate feeder
        let feeder = Feeder::new(
            context,
            client,
            scheme,
            marshal_mailbox,
        );
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
