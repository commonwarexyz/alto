use alto_chain::{engine, Config, Peers};
use alto_client::Client;
use alto_types::{EPOCH, NAMESPACE};
use clap::{Arg, Command};
use commonware_codec::{Decode, DecodeExt};
use commonware_consensus::{marshal, types::ViewDelta};
use commonware_cryptography::{
    bls12381::primitives::{group, sharing::Sharing, variant::MinSig},
    ed25519::{PrivateKey, PublicKey},
    Signer,
};
use commonware_deployer::ec2::Hosts;
use commonware_p2p::{authenticated::discovery as authenticated, Ingress, Manager};
use commonware_runtime::{tokio, Metrics, RayonPoolSpawner, Runner};
use commonware_utils::{from_hex_formatted, ordered::Set, union_unique, NZUsize, NZU32};
use futures::future::try_join_all;
use governor::Quota;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroU32,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};
use tracing::{error, info, Level};

const PENDING_CHANNEL: u64 = 0;
const RECOVERED_CHANNEL: u64 = 1;
const RESOLVER_CHANNEL: u64 = 2;
const BROADCASTER_CHANNEL: u64 = 3;
const MARSHAL_CHANNEL: u64 = 4;

const LEADER_TIMEOUT: Duration = Duration::from_secs(1);
const NOTARIZATION_TIMEOUT: Duration = Duration::from_secs(2);
const NULLIFY_RETRY: Duration = Duration::from_secs(10);
const ACTIVITY_TIMEOUT: ViewDelta = ViewDelta::new(256);
const SKIP_TIMEOUT: ViewDelta = ViewDelta::new(32);
const FETCH_TIMEOUT: Duration = Duration::from_secs(2);
const FETCH_CONCURRENT: usize = 4;
const MAX_MESSAGE_SIZE: u32 = 1024 * 1024;
const MAX_FETCH_COUNT: usize = 16;
const MAX_FETCH_SIZE: usize = 512 * 1024;
const BLOCKS_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21); // 100MB
const FINALIZED_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21); // 100MB

fn main() {
    // Parse arguments
    let matches = Command::new("validator")
        .about("Validator for an alto chain.")
        .arg(Arg::new("hosts").long("hosts").required(false))
        .arg(Arg::new("peers").long("peers").required(false))
        .arg(Arg::new("config").long("config").required(true))
        .get_matches();

    // Load ip file
    let hosts_file = matches.get_one::<String>("hosts");
    let peers_file = matches.get_one::<String>("peers");
    assert!(
        hosts_file.is_some() || peers_file.is_some(),
        "Either --hosts or --peers must be provided"
    );

    // Load config
    let config_file = matches.get_one::<String>("config").unwrap();
    let config_file = std::fs::read_to_string(config_file).expect("Could not read config file");
    let config: Config = serde_yaml::from_str(&config_file).expect("Could not parse config file");
    let key = from_hex_formatted(&config.private_key).expect("Could not parse private key");
    let signer = PrivateKey::decode(key.as_ref()).expect("Private key is invalid");
    let public_key = signer.public_key();

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
                // If we are using `commonware-deployer`, we should use structured logging.
                json: hosts_file.is_some(),
            },
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                config.metrics_port,
            )),
            None,
        );

        // Load peers
        let (ip, peers, bootstrappers) = if let Some(hosts_file) = hosts_file {
            let hosts_file = std::fs::read_to_string(hosts_file).unwrap();
            let hosts: Hosts =
                serde_yaml::from_str(&hosts_file).expect("Could not parse peers file");
            let peers: HashMap<PublicKey, IpAddr> = hosts
                .hosts
                .into_iter()
                .map(|peer| {
                    let key = from_hex_formatted(&peer.name).expect("Could not parse peer key");
                    let key = PublicKey::decode(key.as_ref()).expect("Peer key is invalid");
                    (key, peer.ip)
                })
                .collect();

            let peer_keys = peers.keys().cloned().collect::<Vec<_>>();
            let mut bootstrappers = Vec::new();
            for bootstrapper in &config.bootstrappers {
                let key =
                    from_hex_formatted(bootstrapper).expect("Could not parse bootstrapper key");
                let key = PublicKey::decode(key.as_ref()).expect("Bootstrapper key is invalid");
                let ip = peers.get(&key).expect("Could not find bootstrapper in IPs");
                let bootstrapper_socket = format!("{}:{}", ip, config.port);
                let bootstrapper_socket = SocketAddr::from_str(&bootstrapper_socket)
                    .expect("Could not parse bootstrapper socket");
                bootstrappers.push((key, Ingress::Socket(bootstrapper_socket)));
            }
            let ip = peers.get(&public_key).expect("Could not find self in IPs");
            (*ip, peer_keys, bootstrappers)
        } else {
            let peers_file = std::fs::read_to_string(peers_file.unwrap()).unwrap();
            let peers: Peers =
                serde_yaml::from_str(&peers_file).expect("Could not parse peers file");
            let peers: HashMap<PublicKey, SocketAddr> = peers
                .addresses
                .into_iter()
                .map(|peer| {
                    let key = from_hex_formatted(&peer.0).expect("Could not parse peer key");
                    let key = PublicKey::decode(key.as_ref()).expect("Peer key is invalid");
                    (key, peer.1)
                })
                .collect();

            let peer_keys = peers.keys().cloned().collect::<Vec<_>>();
            let mut bootstrappers = Vec::new();
            for bootstrapper in &config.bootstrappers {
                let key =
                    from_hex_formatted(bootstrapper).expect("Could not parse bootstrapper key");
                let key = PublicKey::decode(key.as_ref()).expect("Bootstrapper key is invalid");
                let socket = peers.get(&key).expect("Could not find bootstrapper in IPs");
                bootstrappers.push((key, Ingress::Socket(*socket)));
            }
            let ip = peers
                .get(&public_key)
                .expect("Could not find self in IPs")
                .ip();
            (ip, peer_keys, bootstrappers)
        };
        info!(peers = peers.len(), "loaded peers");
        let peers_u32 = peers.len() as u32;

        // Parse config
        let share = from_hex_formatted(&config.share).expect("Could not parse share");
        let share = group::Share::decode(share.as_ref()).expect("Share is invalid");
        let polynomial =
            from_hex_formatted(&config.polynomial).expect("Could not parse polynomial");
        let polynomial = Sharing::<MinSig>::decode_cfg(polynomial.as_ref(), &NZU32!(peers_u32))
            .expect("polynomial is invalid");
        let identity = polynomial.public();
        info!(
            ?public_key,
            ?identity,
            ?ip,
            port = config.port,
            "loaded config"
        );

        // Configure network
        let p2p_namespace = union_unique(NAMESPACE, b"_P2P");
        let mut p2p_cfg = if config.local {
            authenticated::Config::local(
                signer.clone(),
                &p2p_namespace,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.port),
                SocketAddr::new(ip, config.port),
                bootstrappers,
                MAX_MESSAGE_SIZE,
            )
        } else {
            authenticated::Config::recommended(
                signer.clone(),
                &p2p_namespace,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.port),
                SocketAddr::new(ip, config.port),
                bootstrappers,
                MAX_MESSAGE_SIZE,
            )
        };
        p2p_cfg.mailbox_size = config.mailbox_size;

        // Start p2p
        let (mut network, mut oracle) =
            authenticated::Network::new(context.with_label("network"), p2p_cfg);

        // Provide authorized peers
        let participants: Set<PublicKey> = Set::from_iter_dedup(peers.clone());
        oracle.update(EPOCH.get(), participants.clone()).await;

        // Register pending channel
        let pending_limit = Quota::per_second(NonZeroU32::new(128).unwrap());
        let pending = network.register(PENDING_CHANNEL, pending_limit, config.message_backlog);

        // Register recovered channel
        let recovered_limit = Quota::per_second(NonZeroU32::new(128).unwrap());
        let recovered =
            network.register(RECOVERED_CHANNEL, recovered_limit, config.message_backlog);

        // Register resolver channel
        let resolver_limit = Quota::per_second(NonZeroU32::new(128).unwrap());
        let resolver = network.register(RESOLVER_CHANNEL, resolver_limit, config.message_backlog);

        // Register broadcast channel
        let broadcaster_limit = Quota::per_second(NonZeroU32::new(8).unwrap());
        let broadcaster = network.register(
            BROADCASTER_CHANNEL,
            broadcaster_limit,
            config.message_backlog,
        );

        // Register marshal channel
        let marshal_quota = Quota::per_second(NonZeroU32::new(8).unwrap());
        let marshal = network.register(MARSHAL_CHANNEL, marshal_quota, config.message_backlog);

        // Create network
        let p2p = network.start();

        let strategy = context
            .create_strategy(NZUsize!(config.signature_threads))
            .unwrap();

        // Create indexer
        let mut indexer = None;
        if let Some(uri) = config.indexer {
            indexer = Some(Client::new(&uri, *identity, strategy.clone()));
        }

        // Create engine
        let engine_cfg = engine::Config {
            blocker: oracle.clone(),
            partition_prefix: "engine".to_string(),
            blocks_freezer_table_initial_size: BLOCKS_FREEZER_TABLE_INITIAL_SIZE,
            finalized_freezer_table_initial_size: FINALIZED_FREEZER_TABLE_INITIAL_SIZE,
            me: public_key.clone(),
            participants,
            mailbox_size: config.mailbox_size,
            deque_size: config.deque_size,
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
            polynomial,
            share,
            strategy,
        };
        let engine = engine::Engine::new(context.with_label("engine"), engine_cfg).await;

        let marshal_resolver_cfg = marshal::resolver::p2p::Config {
            public_key: public_key.clone(),
            manager: oracle.clone(),
            blocker: oracle,
            mailbox_size: config.mailbox_size,
            initial: Duration::from_secs(1),
            timeout: Duration::from_secs(2),
            fetch_retry_timeout: Duration::from_millis(100),
            priority_requests: false,
            priority_responses: false,
        };
        let marshal_resolver =
            marshal::resolver::p2p::init(&context, marshal_resolver_cfg, marshal);

        // Start engine
        let engine = engine.start(pending, recovered, resolver, broadcaster, marshal_resolver);

        // Wait for any task to error
        if let Err(e) = try_join_all(vec![p2p, engine]).await {
            error!(?e, "task failed");
        }
    });
}
