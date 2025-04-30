use alto_chain::{engine, Config};
use alto_client::Client;
use alto_types::NAMESPACE;
use clap::{Arg, Command};
use commonware_codec::{Decode, DecodeExt};
use commonware_cryptography::{
    bls12381::primitives::{group, poly},
    ed25519::{PrivateKey, PublicKey},
    Ed25519, Signer,
};
use commonware_deployer::ec2::Hosts;
use commonware_p2p::authenticated;
use commonware_runtime::{tokio, Metrics, Runner};
use commonware_utils::{from_hex_formatted, quorum, union_unique};
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

    // Initialize runtime
    let cfg = tokio::Config {
        tcp_nodelay: Some(true),
        worker_threads: config.worker_threads,
        storage_directory: PathBuf::from(config.directory),
        catch_panics: false,
        ..Default::default()
    };
    let executor = tokio::Runner::new(cfg);

    // Start runtime
    executor.start(|context| async move {
        // Configure telemetry
        let log_level = Level::from_str(&config.log_level).expect("Invalid log level");
        let metrics_socket = config
            .metrics_port
            .map(|port| SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port));
        tokio::telemetry::init(
            context.with_label("telemetry"),
            log_level,
            metrics_socket,
            None,
        );

        // Load peers
        let hosts_file = matches.get_one::<String>("hosts").unwrap();
        let hosts_file = std::fs::read_to_string(hosts_file).expect("Could not read hosts file");
        let hosts: Hosts = serde_yaml::from_str(&hosts_file).expect("Could not parse peers file");
        let peers: HashMap<PublicKey, IpAddr> = hosts
            .hosts
            .into_iter()
            .map(|peer| {
                let key = from_hex_formatted(&peer.name).expect("Could not parse peer key");
                let key = PublicKey::decode(key.as_ref()).expect("Peer key is invalid");
                (key, peer.ip)
            })
            .collect();
        info!(peers = peers.len(), "loaded peers");
        let peers_u32 = peers.len() as u32;

        // Parse config
        let key = from_hex_formatted(&config.private_key).expect("Could not parse private key");
        let key = PrivateKey::decode(key.as_ref()).expect("Private key is invalid");
        let signer = <Ed25519 as Signer>::from(key).expect("Could not create signer");
        let share = from_hex_formatted(&config.share).expect("Could not parse share");
        let share = group::Share::decode(share.as_ref()).expect("Share is invalid");
        let threshold = quorum(peers_u32);
        let identity = from_hex_formatted(&config.identity).expect("Could not parse identity");
        let identity = poly::Public::decode_cfg(identity.as_ref(), &(threshold as usize))
            .expect("Identity is invalid");
        let identity_public = *poly::public(&identity);
        let public_key = signer.public_key();
        let ip = peers.get(&public_key).expect("Could not find self in IPs");
        info!(
            ?public_key,
            ?identity_public,
            ?ip,
            port = config.port,
            "loaded config"
        );

        // Configure peers and bootstrappers
        let peer_keys = peers.keys().cloned().collect::<Vec<_>>();
        let mut bootstrappers = Vec::new();
        for bootstrapper in &config.bootstrappers {
            let key = from_hex_formatted(bootstrapper).expect("Could not parse bootstrapper key");
            let key = PublicKey::decode(key.as_ref()).expect("Bootstrapper key is invalid");
            let ip = peers.get(&key).expect("Could not find bootstrapper in IPs");
            let bootstrapper_socket = format!("{}:{}", ip, config.port);
            let bootstrapper_socket = SocketAddr::from_str(&bootstrapper_socket)
                .expect("Could not parse bootstrapper socket");
            bootstrappers.push((key, bootstrapper_socket));
        }

        // Configure network
        let p2p_namespace = union_unique(NAMESPACE, b"_P2P");
        let mut p2p_cfg = authenticated::Config::aggressive(
            signer.clone(),
            &p2p_namespace,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.port),
            SocketAddr::new(*ip, config.port),
            bootstrappers,
            MAX_MESSAGE_SIZE,
        );
        p2p_cfg.mailbox_size = config.mailbox_size;

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
            deque_size: config.deque_size,
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

        // Wait for any task to error
        if let Err(e) = try_join_all(vec![p2p, engine]).await {
            error!(?e, "task failed");
        }
    });
}
