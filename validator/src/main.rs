use alto_chain::{engine, Config, Election, Leader, Peers, LEADER_TIMEOUT};
use alto_types::{
    Block, CertificateMode, Scheme, StandardScheme, VrfScheme, EPOCH, NAMESPACE, ROTATING_ELECTOR,
};
use clap::{Arg, Command};
use commonware_allocator::mimalloc;
use commonware_codec::{Decode, DecodeExt};
use commonware_consensus::{
    marshal::{self, standard::Standard},
    simplex,
    types::ViewDelta,
    BoundedBlock,
};
use commonware_cryptography::{
    bls12381::primitives::{
        group,
        sharing::{ModeVersion, Sharing},
        variant::MinSig,
    },
    ed25519::{PrivateKey, PublicKey},
    sha256::Digest,
    ChaCha20Poly1305, Signer,
};
use commonware_deployer::aws::Hosts;
use commonware_formatting::from_hex;
use commonware_p2p::{
    authenticated::{discovery as authenticated, peer_set_limit},
    Ingress, Manager,
};
use commonware_parallel::Rayon;
use commonware_runtime::{tokio, Runner, Supervisor as _};
use commonware_stream::{
    cups::{self, Cups},
    sake::{self, Sake},
    SakeCups,
};
use commonware_utils::{ordered::Set, union_unique, NZDuration, NZUsize, NZU32};
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

/// Serves allocations from a 1 GiB pinned reservation, which mimalloc never returns to the
/// operating system, so pages emptied by recurring work such as metrics scrapes are reused
/// instead of being purged and faulted in again. The reservation is address space until touched:
/// resident memory grows to the peak working set and stays there. Memory beyond the reservation
/// is kept for a minute before mimalloc returns it.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc::new(mimalloc::Config {
    purge: mimalloc::Purge::After(Duration::from_secs(60)),
    reserve: Some(mimalloc::Reserve::pinned(1 << 30)),
    ..mimalloc::Config::DEFAULT
});

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
const FETCH_RETRY_DELAY: Duration = Duration::from_millis(100);
const MARSHAL_RESOLVER_TIMEOUT: Duration = Duration::from_secs(10);
const BLOCKS_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21); // 100MB
const FINALIZED_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21); // 100MB

/// Authenticated transport handshake shared by every validator.
type Handshake = SakeCups<PrivateKey, ChaCha20Poly1305>;

/// Returns the P2P `max_message_size` that admits every payload a validator sends in a network of
/// `participants` validators whose blocks carry at most `block_size` payload bytes.
///
/// # Panics
///
/// Panics if a block of `block_size` payload bytes would not fit the authenticated transport.
fn configured_max_message_size<S: Scheme>(participants: u32, block_size: u32) -> u32 {
    let codec_config = Block::codec_config(block_size);
    assert!(
        u32::try_from(Block::max_encode_size(&codec_config))
            .is_ok_and(|block| block <= authenticated::max_size::<Handshake>()),
        "block size exceeds authenticated transport maximum"
    );

    // Votes, certificates, and their fetches share the network with block broadcasts and
    // marshal's backfill.
    commonware_p2p::max_message_size(&[
        simplex::max_payload_size::<S, Digest>(participants),
        marshal::max_payload_size::<Standard<Block>, S>(participants, &codec_config),
    ])
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
    let key = from_hex(&config.private_key).expect("Could not parse private key");
    let signer = PrivateKey::decode(key).expect("Private key is invalid");
    let public_key = signer.public_key();

    // Initialize runtime
    let cfg = tokio::Config::default()
        .with_tcp_nodelay(Some(true))
        .with_worker_threads(config.worker_threads)
        .with_max_blocking_threads(config.blocking_threads)
        .with_storage_directory(PathBuf::from(&config.directory))
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
                    let key = PublicKey::decode(key).expect("Peer key is invalid");
                    (key, *ip)
                })
                .collect();

            let peer_keys = peers.keys().cloned().collect::<Vec<_>>();
            let mut bootstrappers = Vec::new();
            for bootstrapper in &config.bootstrappers {
                let key = from_hex(bootstrapper).expect("Could not parse bootstrapper key");
                let key = PublicKey::decode(key).expect("Bootstrapper key is invalid");
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
                    let key = PublicKey::decode(key).expect("Peer key is invalid");
                    (key, peer.1)
                })
                .collect();

            let peer_keys = peers.keys().cloned().collect::<Vec<_>>();
            let mut bootstrappers = Vec::new();
            for bootstrapper in &config.bootstrappers {
                let key = from_hex(bootstrapper).expect("Could not parse bootstrapper key");
                let key = PublicKey::decode(key).expect("Bootstrapper key is invalid");
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
        let share = group::Share::decode(share).expect("Share is invalid");
        let polynomial = from_hex(&config.polynomial).expect("Could not parse polynomial");
        let polynomial =
            Sharing::<MinSig>::decode_cfg(polynomial, &(NZU32!(peers_u32), ModeVersion::v0()))
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
        let max_message_size = match config.leader.certificate_mode() {
            CertificateMode::Standard => {
                configured_max_message_size::<StandardScheme>(peers_u32, config.block_size)
            }
            CertificateMode::Vrf => {
                configured_max_message_size::<VrfScheme>(peers_u32, config.block_size)
            }
        };
        let handshake: Handshake = Cups::new(
            Sake {
                signer: signer.clone(),
                synchrony_bound: Duration::from_secs(5),
                max_handshake_age: Duration::from_secs(10),
                version: sake::Version::V1,
            },
            cups::Version::V1,
        );
        let listen = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.port);
        let dialable = SocketAddr::new(ip, config.port);
        let mailbox_size = NZUsize!(config.mailbox_size);
        let mut p2p_cfg = if config.local {
            authenticated::Config::local(
                handshake,
                &p2p_namespace,
                listen,
                dialable,
                bootstrappers,
                max_peers_per_set,
                max_message_size,
                mailbox_size,
            )
        } else {
            authenticated::Config::recommended(
                handshake,
                &p2p_namespace,
                listen,
                dialable,
                bootstrappers,
                max_peers_per_set,
                max_message_size,
                mailbox_size,
            )
        };
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
            fetch_timeout: NZDuration!(MARSHAL_RESOLVER_TIMEOUT),
            fetch_retry_delay: FETCH_RETRY_DELAY,
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
                    fetch_timeout: NZDuration!(FETCH_TIMEOUT),
                    fetch_retry_delay: FETCH_RETRY_DELAY,
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
            Leader::Rotating {
                delay_ms,
                election: Election::RoundRobin,
            } => start_consensus!(
                StandardScheme,
                engine::RoundRobinElector::default(),
                delay_ms
            ),
            Leader::Rotating {
                delay_ms,
                election: Election::Vrf,
            } => start_consensus!(VrfScheme, ROTATING_ELECTOR, delay_ms),
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
    fn max_message_size_carries_whole_blocks() {
        // The broadcast channel sends each block whole, so the limit must admit the largest one.
        for block_size in [0, 2 * 1024 * 1024] {
            let block = Block::max_encode_size(&Block::codec_config(block_size));
            for limit in [
                configured_max_message_size::<StandardScheme>(50, block_size),
                configured_max_message_size::<VrfScheme>(50, block_size),
            ] {
                assert!(usize::try_from(limit).unwrap() >= block);
            }
        }
    }

    #[test]
    #[should_panic(expected = "block size exceeds authenticated transport maximum")]
    fn max_message_size_rejects_unsupported_block_size() {
        configured_max_message_size::<StandardScheme>(50, u32::MAX);
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
