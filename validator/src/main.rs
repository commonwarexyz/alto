use alto_chain::{engine, Config, Leader, Peers, LEADER_TIMEOUT};
use alto_types::{Scheme, StandardScheme, VrfScheme, EPOCH, NAMESPACE, ROTATING_ELECTOR};
use clap::{Arg, Command};
use commonware_codec::{varint::UInt, Decode, DecodeExt, EncodeSize};
use commonware_consensus::{marshal, types::ViewDelta};
use commonware_cryptography::{
    bls12381::primitives::{
        group,
        sharing::{ModeVersion, Sharing},
        variant::MinSig,
    },
    ed25519::{PrivateKey, PublicKey},
    Signer,
};
use commonware_deployer::aws::Hosts;
use commonware_formatting::from_hex;
use commonware_p2p::{
    authenticated::{discovery as authenticated, peer_set_limit},
    Ingress, Manager,
};
use commonware_parallel::Rayon;
use commonware_runtime::{tokio, BufferPoolConfig, Runner, Supervisor as _};
use commonware_utils::{ordered::Set, union_unique, NZUsize, NZU32};
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

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const PENDING_CHANNEL: u64 = 0;
const RECOVERED_CHANNEL: u64 = 1;
const RESOLVER_CHANNEL: u64 = 2;
const BROADCASTER_CHANNEL: u64 = 3;
const MARSHAL_CHANNEL: u64 = 4;

// Per-peer message quotas: votes, certificates, and resolver traffic use the base quota, while
// the channels that carry blocks (broadcast and marshal backfill) get a higher one.
const BASE_CHANNEL_QUOTA_PER_SECOND: u32 = 1_500;
const BLOCK_CHANNEL_QUOTA_PER_SECOND: u32 = 3_000;

const CERTIFICATION_TIMEOUT: Duration = Duration::from_secs(2);
const NULLIFY_RETRY: Duration = Duration::from_secs(10);
const ACTIVITY_TIMEOUT: ViewDelta = ViewDelta::new(256);
const SKIP_TIMEOUT: Duration = Duration::from_secs(11);
const FETCH_TIMEOUT: Duration = Duration::from_secs(2);
const MARSHAL_RESOLVER_TIMEOUT: Duration = Duration::from_secs(10);
const BASE_MAX_MESSAGE_SIZE: u32 = 1024 * 1024;
const BLOCKS_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21); // 100MB
const FINALIZED_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21); // 100MB

fn configured_max_message_size(block_size: u32) -> u32 {
    // Block data contributes its bytes and the codec's variable-length prefix to each message.
    // The total must remain within the authenticated transport payload limit.
    let size = u64::from(BASE_MAX_MESSAGE_SIZE)
        + u64::from(block_size)
        + UInt(block_size).encode_size() as u64;
    u32::try_from(size)
        .ok()
        .filter(|size| *size <= authenticated::MAX_SIZE)
        .expect("block size exceeds authenticated transport maximum")
}

fn resolve_named_http_url(url: &str, hosts: &HashMap<String, IpAddr>) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    if !matches!(scheme, "http" | "https") {
        return url.to_string();
    }

    let (authority, suffix) = match rest.split_once('/') {
        Some((authority, suffix)) => (authority, format!("/{suffix}")),
        None => (rest, String::new()),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (authority, None),
    };
    let Some(ip) = hosts.get(host) else {
        return url.to_string();
    };

    match port {
        Some(port) => format!("{scheme}://{ip}:{port}{suffix}"),
        None => format!("{scheme}://{ip}{suffix}"),
    }
}

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
    let mut config: Config =
        serde_yaml::from_str(&config_file).expect("Could not parse config file");
    let max_message_size = configured_max_message_size(config.block_size);
    let key = from_hex(&config.private_key).expect("Could not parse private key");
    let signer = PrivateKey::decode(key.as_ref()).expect("Private key is invalid");
    let public_key = signer.public_key();

    // Initialize runtime
    let network_buffer_pool_parallelism = config
        .worker_threads
        .checked_add(config.signature_threads)
        .expect("network buffer pool parallelism overflowed");

    // Storage I/O runs on Tokio's blocking pool. Include those threads in the
    // pool parallelism calculation so buffers cannot be stranded in too few
    // thread-local caches and surface as exhaustion under restart pressure.
    let storage_buffer_pool_parallelism = network_buffer_pool_parallelism
        .checked_add(config.blocking_threads)
        .expect("storage buffer pool parallelism overflowed");
    let mut storage_buffer_pool_cfg = BufferPoolConfig::for_storage().with_parallelism(
        config
            .storage_buffer_pool_parallelism
            .unwrap_or(NZUsize!(storage_buffer_pool_parallelism)),
    );
    if let Some(max_per_class) = config.storage_buffer_pool_max_per_class {
        storage_buffer_pool_cfg = storage_buffer_pool_cfg.with_max_per_class(max_per_class);
    }
    let mut network_buffer_pool_cfg = BufferPoolConfig::for_network().with_parallelism(
        config
            .network_buffer_pool_parallelism
            .unwrap_or(NZUsize!(network_buffer_pool_parallelism)),
    );
    if let Some(max_per_class) = config.network_buffer_pool_max_per_class {
        network_buffer_pool_cfg = network_buffer_pool_cfg.with_max_per_class(max_per_class);
    }
    let cfg = tokio::Config::default()
        .with_tcp_nodelay(Some(true))
        .with_worker_threads(config.worker_threads)
        .with_max_blocking_threads(config.blocking_threads)
        .with_storage_directory(PathBuf::from(&config.directory))
        .with_storage_buffer_pool_config(storage_buffer_pool_cfg)
        .with_network_buffer_pool_config(network_buffer_pool_cfg)
        .with_catch_panics(false);
    let executor = tokio::Runner::new(cfg);

    // Start runtime
    executor.start(|context| async move {
        let log_level = Level::from_str(&config.log_level).expect("Invalid log level");

        // Resolve deployed host names for peer discovery and indexer uploads.
        let hosts = hosts_file.map(|hosts_file| {
            let hosts_file =
                std::fs::read_to_string(hosts_file).expect("Could not read hosts file");
            serde_yaml::from_str::<Hosts>(&hosts_file).expect("Could not parse hosts file")
        });
        let hosts_by_name: Option<HashMap<String, IpAddr>> = hosts.as_ref().map(|hosts| {
            hosts
                .hosts
                .iter()
                .map(|host| (host.name.clone(), host.ip))
                .collect()
        });
        if let (Some(hosts_by_name), Some(indexer_url)) =
            (hosts_by_name.as_ref(), config.indexer.as_deref())
        {
            config.indexer = Some(resolve_named_http_url(indexer_url, hosts_by_name));
        }

        // Export enabled traces to the deployer's monitoring collector on port 4318.
        // The deployer allows OTLP traffic from binary hosts to the collector.
        let traces_sample_rate = config.traces_sample_probability();
        let traces = hosts
            .as_ref()
            .filter(|_| !traces_sample_rate.is_zero())
            .map(|hosts| tokio::tracing::Config {
                endpoint: format!("http://{}:4318/v1/traces", hosts.monitoring.private),
                name: public_key.to_string(),
                rate: traces_sample_rate,
            });
        tokio::telemetry::init(
            context.child("telemetry"),
            tokio::telemetry::Logs {
                level: log_level,
                // If we are using `commonware-deployer`, we should use structured logging.
                json: hosts_file.is_some(),
            },
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                config.metrics_port,
            )),
            traces,
        );

        // Load peers
        let (ip, peers, bootstrappers) = if let Some(hosts_by_name) = hosts_by_name {
            let peers: HashMap<PublicKey, IpAddr> = config
                .allowed_peers
                .iter()
                .map(|peer| {
                    let ip = hosts_by_name
                        .get(peer)
                        .expect("Could not find peer in hosts file");
                    let key = from_hex(peer).expect("Could not parse peer key");
                    let key = PublicKey::decode(key.as_ref()).expect("Peer key is invalid");
                    (key, *ip)
                })
                .collect();

            let peer_keys = peers.keys().cloned().collect::<Vec<_>>();
            let mut bootstrappers = Vec::new();
            for bootstrapper in &config.bootstrappers {
                let key = from_hex(bootstrapper).expect("Could not parse bootstrapper key");
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
                    let key = from_hex(&peer.0).expect("Could not parse peer key");
                    let key = PublicKey::decode(key.as_ref()).expect("Peer key is invalid");
                    (key, peer.1)
                })
                .collect();

            let peer_keys = peers.keys().cloned().collect::<Vec<_>>();
            let mut bootstrappers = Vec::new();
            for bootstrapper in &config.bootstrappers {
                let key = from_hex(bootstrapper).expect("Could not parse bootstrapper key");
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
        let share = from_hex(&config.share).expect("Could not parse share");
        let share = group::Share::decode(share.as_ref()).expect("Share is invalid");
        let polynomial = from_hex(&config.polynomial).expect("Could not parse polynomial");
        let polynomial = Sharing::<MinSig>::decode_cfg(
            polynomial.as_ref(),
            &(NZU32!(peers_u32), ModeVersion::v0()),
        )
        .expect("polynomial is invalid");
        let identity = *polynomial.public();
        info!(
            ?public_key,
            ?identity,
            ?ip,
            port = config.port,
            "loaded config"
        );

        // Configure network
        let p2p_namespace = union_unique(NAMESPACE, b"_P2P");
        let max_peers_per_set = peer_set_limit(&peers, &public_key);
        let mut p2p_cfg = if config.local {
            authenticated::Config::local(
                signer.clone(),
                &p2p_namespace,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.port),
                SocketAddr::new(ip, config.port),
                bootstrappers,
                max_peers_per_set,
                max_message_size,
            )
        } else {
            authenticated::Config::recommended(
                signer.clone(),
                &p2p_namespace,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.port),
                SocketAddr::new(ip, config.port),
                bootstrappers,
                max_peers_per_set,
                max_message_size,
            )
        };
        p2p_cfg.mailbox_size = NZUsize!(config.mailbox_size);
        p2p_cfg.tracked_peer_sets = NZUsize!(1);

        // Start p2p
        let (mut network, mut oracle) =
            authenticated::Network::new(context.child("network"), p2p_cfg);

        // Provide authorized peers
        let participants: Set<PublicKey> = Set::from_iter_dedup(peers.clone());
        oracle.track(EPOCH.get(), participants.clone());

        // Register pending channel
        let pending_limit =
            Quota::per_second(NonZeroU32::new(BASE_CHANNEL_QUOTA_PER_SECOND).unwrap());
        let pending = network.register(PENDING_CHANNEL, pending_limit);

        // Register recovered channel
        let recovered_limit =
            Quota::per_second(NonZeroU32::new(BASE_CHANNEL_QUOTA_PER_SECOND).unwrap());
        let recovered = network.register(RECOVERED_CHANNEL, recovered_limit);

        // Register resolver channel
        let resolver_limit =
            Quota::per_second(NonZeroU32::new(BASE_CHANNEL_QUOTA_PER_SECOND).unwrap());
        let resolver = network.register(RESOLVER_CHANNEL, resolver_limit);

        // Register broadcast channel
        let broadcaster_limit =
            Quota::per_second(NonZeroU32::new(BLOCK_CHANNEL_QUOTA_PER_SECOND).unwrap());
        let broadcaster = network.register(BROADCASTER_CHANNEL, broadcaster_limit);

        // Register marshal channel
        let marshal_quota =
            Quota::per_second(NonZeroU32::new(BLOCK_CHANNEL_QUOTA_PER_SECOND).unwrap());
        let marshal = network.register(MARSHAL_CHANNEL, marshal_quota);

        // Create network
        let p2p = network.start();

        let strategy = Rayon::new(NZUsize!(config.signature_threads)).unwrap();

        let marshal_resolver_cfg = marshal::resolver::p2p::Config {
            public_key: public_key.clone(),
            peer_provider: oracle.clone(),
            blocker: oracle.clone(),
            mailbox_size: NZUsize!(config.mailbox_size),
            timeout: MARSHAL_RESOLVER_TIMEOUT,
            fetch_retry_timeout: Duration::from_millis(100),
            priority_requests: false,
            priority_responses: false,
        };
        let marshal_resolver = marshal::resolver::p2p::init(
            context.child("marshal_resolver"),
            marshal_resolver_cfg,
            marshal,
        );

        macro_rules! start_consensus {
            ($scheme:ty, $elector:expr, $delay_ms:expr) => {{
                let scheme = <$scheme>::signer(NAMESPACE, participants, polynomial, share)
                    .expect("failed to create consensus scheme");
                let indexer = config.indexer.as_deref().map(|indexer_url| {
                    alto_client::ClientBuilder::new(
                        indexer_url,
                        <$scheme as Scheme>::certificate_verifier(NAMESPACE, identity),
                        strategy.clone(),
                    )
                    .build()
                });
                let engine_cfg = engine::Config {
                    blocker: oracle.clone(),
                    provider: oracle.clone(),
                    partition_prefix: "engine".to_string(),
                    blocks_freezer_table_initial_size: BLOCKS_FREEZER_TABLE_INITIAL_SIZE,
                    finalized_freezer_table_initial_size: FINALIZED_FREEZER_TABLE_INITIAL_SIZE,
                    me: public_key.clone(),
                    scheme,
                    elector: $elector,
                    mailbox_size: config.mailbox_size,
                    deque_size: config.deque_size,
                    block_size: config.block_size,
                    proposal_delay_ms: $delay_ms,
                    leader_timeout: LEADER_TIMEOUT,
                    certification_timeout: CERTIFICATION_TIMEOUT,
                    nullify_retry: NULLIFY_RETRY,
                    activity_timeout: ACTIVITY_TIMEOUT,
                    skip_timeout: SKIP_TIMEOUT,
                    fetch_timeout: FETCH_TIMEOUT,
                    backfiller_max_active: config.backfiller_max_active,
                    backfiller_retry: Duration::from_millis(config.backfiller_retry_ms),
                    indexer,
                    strategy,
                };
                engine::Engine::new(context.child("engine"), engine_cfg)
                    .await
                    .start(pending, recovered, resolver, broadcaster, marshal_resolver)
            }};
        }

        // Consensus, certificate storage, and indexer clients share the selected certificate
        // scheme for the process lifetime.
        let engine = match config.leader {
            Leader::Stable {
                delay_ms,
                term_length,
                optimistic_views,
            } => start_consensus!(
                StandardScheme,
                engine::stable_elector(term_length, optimistic_views),
                delay_ms
            ),
            Leader::Rotating { delay_ms } => {
                start_consensus!(VrfScheme, ROTATING_ELECTOR, delay_ms)
            }
        };

        // Wait for any task to error
        if let Err(e) = try_join_all(vec![p2p, engine]).await {
            error!(?e, "task failed");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_message_size_includes_block_data() {
        assert_eq!(configured_max_message_size(0), BASE_MAX_MESSAGE_SIZE + 1);
        assert_eq!(
            configured_max_message_size(2 * 1024 * 1024),
            BASE_MAX_MESSAGE_SIZE + 2 * 1024 * 1024 + 4
        );
    }

    #[test]
    #[should_panic(expected = "block size exceeds authenticated transport maximum")]
    fn max_message_size_rejects_unsupported_block_size() {
        configured_max_message_size(u32::MAX);
    }

    #[test]
    fn named_http_url_resolves_deployed_indexer() {
        let hosts = HashMap::from([(
            "indexer".to_string(),
            "203.0.113.7".parse::<IpAddr>().unwrap(),
        )]);

        assert_eq!(
            resolve_named_http_url("http://indexer:8080/consensus", &hosts),
            "http://203.0.113.7:8080/consensus"
        );
        assert_eq!(
            resolve_named_http_url("https://external.example.com", &hosts),
            "https://external.example.com"
        );
    }
}
