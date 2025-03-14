use alto_chain::{engine, Config};
use alto_client::Client;
use alto_types::P2P_NAMESPACE;
use axum::{routing::get, serve, Extension, Router};
use clap::{Arg, Command};
use commonware_cryptography::{
    bls12381::primitives::{
        group::{self, Element},
        poly,
    },
    ed25519::{PrivateKey, PublicKey},
    Ed25519, Scheme,
};
use commonware_deployer::ec2::Peers;
use commonware_p2p::authenticated;
use commonware_runtime::{tokio, Clock, Metrics, Network, Runner, Spawner};
use commonware_utils::{from_hex_formatted, hex, quorum};
use futures::future::try_join_all;
use governor::Quota;
use prometheus_client::metrics::gauge::Gauge;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroU32,
    path::PathBuf,
    str::FromStr,
    sync::atomic::{AtomicI64, AtomicU64},
    time::Duration,
};
use sysinfo::{Disks, System};
use tracing::{error, info, Level};

const SYSTEM_METRICS_REFRESH: Duration = Duration::from_secs(5);
const METRICS_PORT: u16 = 9090;

const VOTER_CHANNEL: u32 = 0;
const RESOLVER_CHANNEL: u32 = 1;
const BROADCASTER_CHANNEL: u32 = 2;
const BACKFILLER_CHANNEL: u32 = 3;

const LEADER_TIMEOUT: Duration = Duration::from_secs(1);
const NOTARIZATION_TIMEOUT: Duration = Duration::from_secs(2);
const NULLIFY_RETRY: Duration = Duration::from_secs(10);
const ACTIVITY_TIMEOUT: u64 = 256;
const SKIP_TIMEOUT: u64 = 32;
const FETCH_TIMEOUT: Duration = Duration::from_secs(2);
const FETCH_CONCURRENT: usize = 4;
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;
const MAX_FETCH_COUNT: usize = 16;
const MAX_FETCH_SIZE: usize = 512 * 1024;

/// Parse the log level.
fn parse_log_level(level: &str) -> Option<Level> {
    match level {
        "trace" => Some(Level::TRACE),
        "debug" => Some(Level::DEBUG),
        "info" => Some(Level::INFO),
        "warn" => Some(Level::WARN),
        "error" => Some(Level::ERROR),
        _ => None,
    }
}

fn main() {
    // Parse arguments
    let matches = Command::new("validator")
        .about("Validator for an alto chain.")
        .arg(Arg::new("peers").long("peers").required(true))
        .arg(Arg::new("config").long("config").required(true))
        .get_matches();

    // Load peers
    let peer_file = matches.get_one::<String>("peers").unwrap();
    let peers_file = std::fs::read_to_string(peer_file).expect("Could not read peers file");
    let peers: Peers = serde_yaml::from_str(&peers_file).expect("Could not parse peers file");
    let peers: HashMap<PublicKey, IpAddr> = peers
        .peers
        .into_iter()
        .map(|peer| {
            let key = from_hex_formatted(&peer.name).expect("Could not parse peer key");
            let key = PublicKey::try_from(key).expect("Peer key is invalid");
            (key, peer.ip)
        })
        .collect();
    info!(peers = peers.len(), "loaded peers");
    let peers_u32 = peers.len() as u32;

    // Load config
    let config_file = matches.get_one::<String>("config").unwrap();
    let config_file = std::fs::read_to_string(config_file).expect("Could not read config file");
    let config: Config = serde_yaml::from_str(&config_file).expect("Could not parse config file");
    let key = from_hex_formatted(&config.private_key).expect("Could not parse private key");
    let key = PrivateKey::try_from(key).expect("Private key is invalid");
    let signer = <Ed25519 as Scheme>::from(key).expect("Could not create signer");
    let share = from_hex_formatted(&config.share).expect("Could not parse share");
    let share = group::Share::deserialize(&share).expect("Share is invalid");
    let threshold = quorum(peers_u32).expect("unable to derive quorum");
    let identity = from_hex_formatted(&config.identity).expect("Could not parse identity");
    let identity = poly::Public::deserialize(&identity, threshold).expect("Identity is invalid");
    let identity_public = poly::public(&identity);
    let public_key = signer.public_key();
    let ip = peers.get(&public_key).expect("Could not find self in IPs");
    info!(
        ?public_key,
        identity = hex(&identity_public.serialize()),
        ?ip,
        port = config.port,
        "loaded config"
    );

    // Create logger
    let log_level = parse_log_level(&config.log_level).expect("Invalid log level");
    tracing_subscriber::fmt()
        .json()
        .with_max_level(log_level)
        .with_line_number(true)
        .with_file(true)
        .init();

    // Configure peers and bootstrappers
    let peer_keys = peers.keys().cloned().collect::<Vec<_>>();
    let mut bootstrappers = Vec::new();
    for bootstrapper in &config.bootstrappers {
        let key = from_hex_formatted(bootstrapper).expect("Could not parse bootstrapper key");
        let key = PublicKey::try_from(key).expect("Bootstrapper key is invalid");
        let ip = peers.get(&key).expect("Could not find bootstrapper in IPs");
        let bootstrapper_socket = format!("{}:{}", ip, config.port);
        let bootstrapper_socket = SocketAddr::from_str(&bootstrapper_socket)
            .expect("Could not parse bootstrapper socket");
        bootstrappers.push((key, bootstrapper_socket));
    }

    // Initialize runtime
    let cfg = tokio::Config {
        tcp_nodelay: Some(true),
        threads: config.worker_threads,
        storage_directory: PathBuf::from(config.directory),
        ..Default::default()
    };
    let (executor, context) = tokio::Executor::init(cfg);

    // Configure network
    let mut p2p_cfg = authenticated::Config::aggressive(
        signer.clone(),
        P2P_NAMESPACE,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.port),
        SocketAddr::new(*ip, config.port),
        bootstrappers,
        MAX_MESSAGE_SIZE,
    );
    p2p_cfg.mailbox_size = config.mailbox_size;

    // Start runtime
    executor.start(async move {
        // Start p2p
        let (mut network, mut oracle) =
            authenticated::Network::new(context.with_label("network"), p2p_cfg);

        // Provide authorized peers
        oracle.register(0, peer_keys.clone()).await;

        // Register voter channel
        let voter_limit = Quota::per_second(NonZeroU32::new(128).unwrap());
        let voter = network.register(VOTER_CHANNEL, voter_limit, config.message_backlog, None);

        // Register resolver channel
        let resolver_limit = Quota::per_second(NonZeroU32::new(128).unwrap());
        let resolver = network.register(
            RESOLVER_CHANNEL,
            resolver_limit,
            config.message_backlog,
            None,
        );

        // Register broadcast channel
        let broadcaster_limit = Quota::per_second(NonZeroU32::new(8).unwrap());
        let broadcaster = network.register(
            BROADCASTER_CHANNEL,
            broadcaster_limit,
            config.message_backlog,
            Some(3),
        );

        // Register backfill channel
        let backfiller_limit = Quota::per_second(NonZeroU32::new(8).unwrap());
        let backfiller = network.register(
            BACKFILLER_CHANNEL,
            backfiller_limit,
            config.message_backlog,
            Some(3),
        );

        // Create network
        let p2p = network.start();

        // Create indexer
        let mut indexer = None;
        if let Some(uri) = config.indexer {
            indexer = Some(Client::new(&uri, identity_public.into()));
        }

        // Create engine
        let config = engine::Config {
            partition_prefix: "engine".to_string(),
            signer,
            identity,
            share,
            participants: peer_keys,
            mailbox_size: config.mailbox_size,
            backfill_quota: backfiller_limit,
            leader_timeout: LEADER_TIMEOUT,
            notarization_timeout: NOTARIZATION_TIMEOUT,
            nullify_retry: NULLIFY_RETRY,
            activity_timeout: ACTIVITY_TIMEOUT,
            skip_timeout: SKIP_TIMEOUT,
            fetch_timeout: FETCH_TIMEOUT,
            max_fetch_count: MAX_FETCH_COUNT,
            max_fetch_size: MAX_FETCH_SIZE,
            fetch_concurrent: FETCH_CONCURRENT,
            fetch_rate_per_peer: resolver_limit,
            indexer,
        };
        let engine = engine::Engine::new(context.with_label("engine"), config).await;

        // Start engine
        let engine = engine.start(voter, resolver, broadcaster, backfiller);

        // Start system metrics collector
        let system = context.with_label("system").spawn(|context| async move {
            // Register metrics
            let cpu_usage: Gauge<f64, AtomicU64> = Gauge::default();
            context.register("cpu_usage", "CPU usage", cpu_usage.clone());
            let memory_used: Gauge<i64, AtomicI64> = Gauge::default();
            context.register("memory_used", "Memory used", memory_used.clone());
            let memory_free: Gauge<i64, AtomicI64> = Gauge::default();
            context.register("memory_free", "Memory free", memory_free.clone());
            let swap_used: Gauge<i64, AtomicI64> = Gauge::default();
            context.register("swap_used", "Swap used", swap_used.clone());
            let swap_free: Gauge<i64, AtomicI64> = Gauge::default();
            context.register("swap_free", "Swap free", swap_free.clone());
            let disk_used: Gauge<i64, AtomicI64> = Gauge::default();
            context.register("disk_used", "Disk used", disk_used.clone());
            let disk_free: Gauge<i64, AtomicI64> = Gauge::default();
            context.register("disk_free", "Disk free", disk_free.clone());

            // Initialize system info
            let mut sys = System::new_all();
            let mut disks = Disks::new_with_refreshed_list();

            // Check metrics every
            loop {
                // Refresh system info
                sys.refresh_all();
                disks.refresh(true);

                // Update metrics
                cpu_usage.set(sys.global_cpu_usage() as f64);
                memory_used.set(sys.used_memory() as i64);
                memory_free.set(sys.free_memory() as i64);
                swap_used.set(sys.used_swap() as i64);
                swap_free.set(sys.free_swap() as i64);

                // Update disk metrics for root disk
                for disk in disks.list() {
                    if disk.mount_point() == std::path::Path::new("/") {
                        let total = disk.total_space();
                        let available = disk.available_space();
                        let used = total.saturating_sub(available);
                        disk_used.set(used as i64);
                        disk_free.set(available as i64);
                        break;
                    }
                }

                // Wait to pull metrics again
                context.sleep(SYSTEM_METRICS_REFRESH).await;
            }
        });

        // Serve metrics
        let metrics = context.with_label("metrics").spawn(|context| async move {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), METRICS_PORT);
            let listener = context
                .bind(addr)
                .await
                .expect("Could not bind to metrics address");
            let app = Router::new()
                .route(
                    "/metrics",
                    get(|extension: Extension<tokio::Context>| async move { extension.0.encode() }),
                )
                .layer(Extension(context));
            serve(listener, app.into_make_service())
                .await
                .expect("Could not serve metrics");
        });

        // Wait for any task to error
        if let Err(e) = try_join_all(vec![p2p, engine, system, metrics]).await {
            error!(?e, "task failed");
        }
    });
}
