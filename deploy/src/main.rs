use alto_chain::{
    Config, Leader, Peers, DEFAULT_BACKFILLER_MAX_ACTIVE, DEFAULT_BACKFILLER_RETRY_MS,
    DEFAULT_BLOCKING_THREADS, DEFAULT_NETWORK_BUFFER_POOL_MAX_PER_CLASS,
    DEFAULT_STORAGE_BUFFER_POOL_MAX_PER_CLASS, LEADER_TIMEOUT,
};
use alto_types::{CertificateMode, NAMESPACE};
use clap::{value_parser, Arg, ArgAction, ArgMatches, Command};
use commonware_codec::{Decode, DecodeExt, Encode};
use commonware_consensus::simplex::scheme::bls12381_threshold::vrf as bls12381_threshold;
use commonware_cryptography::{
    bls12381::primitives::{
        sharing::{ModeVersion, Sharing},
        variant::MinSig,
    },
    certificate::mocks::Fixture,
    ed25519::{PrivateKey, PublicKey},
    Signer,
};
use commonware_deployer::aws::{self, METRICS_PORT};
use commonware_formatting::{from_hex, hex};
use commonware_math::algebra::Random;
use commonware_utils::{sys_rng, NZU32};
use rand::seq::IteratorRandom;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
};
use tracing::{error, info};
use uuid::Uuid;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const BINARY_NAME: &str = "validator";
const INDEXER_BINARY_NAME: &str = "indexer";
const INDEXER_HOST: &str = "indexer";
const INDEXER_CONFIG_FILE: &str = "indexer.yaml";
const INDEXER_PORT: u16 = 8080;
const PORT: u16 = 4545;
const STORAGE_CLASS: &str = "gp3";
const DASHBOARD_FILE: &str = "dashboard.json";

fn leader_args() -> [Arg; 4] {
    [
        Arg::new("leader_mode")
            .long("leader-mode")
            .required(true)
            .value_parser(["rotating", "stable"]),
        // Validators refuse to start with a proposal delay at or above their leader timeout.
        Arg::new("leader_delay_ms")
            .long("leader-delay-ms")
            .required(true)
            .value_parser(value_parser!(u64).range(1..LEADER_TIMEOUT.as_millis() as u64)),
        Arg::new("leader_term_length")
            .long("leader-term-length")
            .required_if_eq("leader_mode", "stable")
            .value_parser(value_parser!(u32).range(2..)),
        Arg::new("leader_optimistic_views")
            .long("leader-optimistic-views")
            .required_if_eq("leader_mode", "stable")
            .value_parser(value_parser!(u64)),
    ]
}

fn parse_traces_sample_rate(value: &str) -> Result<f64, String> {
    let rate = value
        .parse::<f64>()
        .map_err(|_| "traces sample rate must be a number between 0 and 1".to_string())?;
    if rate.is_finite() && (0.0..=1.0).contains(&rate) {
        return Ok(rate);
    }
    Err("traces sample rate must be between 0 and 1".to_string())
}

fn traces_sample_rate_arg() -> Arg {
    Arg::new("traces_sample_rate")
        .long("traces-sample-rate")
        .default_value("0")
        .value_parser(parse_traces_sample_rate)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ConfiguredIndexer {
    url: String,
    count: usize,
}

#[derive(serde::Serialize)]
struct IndexerConfig {
    port: u16,
    identity: String,
    certificate_mode: CertificateMode,
    block_size: u32,
    explorer: ExplorerConfig,
}

#[derive(serde::Serialize)]
struct ExplorerConfig {
    name: String,
    description: String,
    participants: Vec<String>,
    /// One location per participant in public-key order, with None for unmapped regions.
    locations: Vec<Option<([f64; 2], String)>>,
}

fn local_indexer_port(url: &str) -> Option<u16> {
    let rest = url
        .strip_prefix("http://localhost:")
        .or_else(|| url.strip_prefix("http://127.0.0.1:"))?;
    let port = rest.split('/').next()?;
    port.parse().ok()
}

fn parse_indexers(specs: Option<&String>) -> Vec<ConfiguredIndexer> {
    let Some(specs) = specs else {
        return Vec::new();
    };

    specs
        .split(';')
        .map(str::trim)
        .filter(|spec| !spec.is_empty())
        .map(|spec| {
            let mut parts = spec.rsplitn(2, ':');
            let Some(count) = parts.next() else {
                error!("invalid indexer spec '{spec}', expected <url>:<count>");
                std::process::exit(1);
            };
            let Some(url) = parts.next() else {
                error!("invalid indexer spec '{spec}', expected <url>:<count>");
                std::process::exit(1);
            };
            let url = url.trim();
            let count = count.trim().parse::<usize>().unwrap_or_else(|_| {
                error!("invalid indexer count in '{spec}', expected <url>:<count>");
                std::process::exit(1);
            });
            if count == 0 {
                error!("indexer count must be greater than zero: '{spec}'");
                std::process::exit(1);
            }
            if url.is_empty() {
                error!("indexer url must be non-empty: '{spec}'");
                std::process::exit(1);
            }
            ConfiguredIndexer {
                url: url.to_string(),
                count,
            }
        })
        .collect()
}

fn select_regional_peers(
    regions: &[String],
    count: usize,
) -> (Vec<usize>, BTreeMap<String, usize>) {
    assert!(
        count <= regions.len(),
        "indexer count exceeds number of peers"
    );

    let mut region_to_peers: BTreeMap<String, VecDeque<usize>> = BTreeMap::new();
    for (index, region) in regions.iter().enumerate() {
        region_to_peers
            .entry(region.clone())
            .or_default()
            .push_back(index);
    }

    let mut selected = Vec::with_capacity(count);
    let mut assigned = BTreeMap::new();
    while selected.len() < count {
        for (region, peers) in &mut region_to_peers {
            let Some(peer) = peers.pop_front() else {
                continue;
            };
            selected.push(peer);
            *assigned.entry(region.clone()).or_insert(0) += 1;
            if selected.len() == count {
                break;
            }
        }
    }

    (selected, assigned)
}

/// Parses the explorer backend URL, which the explorer stores without a scheme and prefixes with
/// `http(s)://` or `ws(s)://` itself.
fn parse_backend_url(value: &str) -> Result<String, String> {
    if let Some((_, rest)) = value.split_once("://") {
        return Err(format!(
            "backend URL must be a host[:port] without a scheme (for example, {rest})"
        ));
    }
    let trimmed = value.trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("backend URL must not be empty".to_string());
    }
    Ok(trimmed.to_string())
}

fn parse_leader(matches: &ArgMatches) -> Result<Leader, &'static str> {
    let delay_ms = NonZeroU64::new(*matches.get_one::<u64>("leader_delay_ms").unwrap())
        .expect("clap bounds the leader delay");
    match matches.get_one::<String>("leader_mode").unwrap().as_str() {
        "rotating" => {
            if matches.get_one::<u32>("leader_term_length").is_some() {
                return Err("rotating leader mode does not accept --leader-term-length");
            }
            if matches.get_one::<u64>("leader_optimistic_views").is_some() {
                return Err("rotating leader mode does not accept --leader-optimistic-views");
            }
            Ok(Leader::rotating(delay_ms))
        }
        "stable" => Ok(Leader::stable(
            delay_ms,
            NonZeroU32::new(*matches.get_one::<u32>("leader_term_length").unwrap()).unwrap(),
            *matches.get_one::<u64>("leader_optimistic_views").unwrap(),
        )),
        _ => unreachable!("clap validates leader mode"),
    }
}

fn main() {
    // Initialize logger
    tracing_subscriber::fmt().init();

    // Define the main command with subcommands
    let app = Command::new("deploy")
        .about("Manage configuration files for an alto chain.")
        .subcommand(
            Command::new("generate")
                .about("Generate configuration files for an alto chain deploy")
                .arg(
                    Arg::new("peers")
                        .long("peers")
                        .required(true)
                        .value_parser(value_parser!(usize)),
                )
                .arg(
                    Arg::new("bootstrappers")
                        .long("bootstrappers")
                        .required(true)
                        .value_parser(value_parser!(usize)),
                )
                .arg(
                    Arg::new("worker_threads")
                        .long("worker-threads")
                        .required(true)
                        .value_parser(value_parser!(usize)),
                )
                .arg(
                    Arg::new("blocking_threads")
                        .long("blocking-threads")
                        .required(false)
                        .value_parser(value_parser!(usize)),
                )
                .arg(
                    Arg::new("storage_buffer_pool_max_per_class")
                        .long("storage-buffer-pool-max-per-class")
                        .required(false)
                        .value_parser(value_parser!(NonZeroU32)),
                )
                .arg(
                    Arg::new("network_buffer_pool_max_per_class")
                        .long("network-buffer-pool-max-per-class")
                        .required(false)
                        .value_parser(value_parser!(NonZeroU32)),
                )
                .arg(
                    Arg::new("storage_buffer_pool_parallelism")
                        .long("storage-buffer-pool-parallelism")
                        .required(false)
                        .value_parser(value_parser!(NonZeroUsize)),
                )
                .arg(
                    Arg::new("network_buffer_pool_parallelism")
                        .long("network-buffer-pool-parallelism")
                        .required(false)
                        .value_parser(value_parser!(NonZeroUsize)),
                )
                .arg(
                    Arg::new("log_level")
                        .long("log-level")
                        .required(true)
                        .value_parser(value_parser!(String)),
                )
                .arg(traces_sample_rate_arg())
                .arg(
                    Arg::new("mailbox_size")
                        .long("mailbox-size")
                        .required(true)
                        .value_parser(value_parser!(usize)),
                )
                .arg(
                    Arg::new("deque_size")
                        .long("deque-size")
                        .required(true)
                        .value_parser(value_parser!(usize)),
                )
                .arg(
                    Arg::new("block_size")
                        .long("block-size")
                        .default_value("0")
                        .value_parser(value_parser!(u32)),
                )
                .arg(
                    Arg::new("signature_threads")
                        .long("signature-threads")
                        .required(true)
                        .value_parser(value_parser!(usize)),
                )
                .args(leader_args())
                .arg(
                    Arg::new("output")
                        .long("output")
                        .required(true)
                        .value_parser(value_parser!(String)),
                )
                .subcommand(Command::new("local").about("Generate configuration files for local deployment")
                    .arg(
                        Arg::new("start_port")
                            .long("start-port")
                            .required(true)
                            .value_parser(value_parser!(u16)),
                    )
                    .arg(
                        Arg::new("indexers")
                            .long("indexers")
                            .required(false)
                            .value_parser(value_parser!(String)),
                    )
                )
                .subcommand(
                    Command::new("remote")
                        .about("Generate configuration files for `commonware-deployer`-managed deployment")
                        .arg(
                            Arg::new("regions")
                                .long("regions")
                                .required(true)
                                .value_delimiter(',')
                                .value_parser(value_parser!(String)),
                        )
                        .arg(
                            Arg::new("instance_type")
                                .long("instance-type")
                                .required(true)
                                .value_parser(value_parser!(String)),
                        )
                        .arg(
                            Arg::new("storage_size")
                                .long("storage-size")
                                .required(true)
                                .value_parser(value_parser!(i32)),
                        )
                        .arg(
                            Arg::new("monitoring_instance_type")
                                .long("monitoring-instance-type")
                                .required(true)
                                .value_parser(value_parser!(String)),
                        )
                        .arg(
                            Arg::new("monitoring_storage_size")
                                .long("monitoring-storage-size")
                                .required(true)
                                .value_parser(value_parser!(i32)),
                        )
                        .arg(
                            Arg::new("dashboard")
                                .long("dashboard")
                                .required(true)
                                .value_parser(value_parser!(String)),
                        )
                        .arg(
                            Arg::new("indexer")
                                .long("indexer")
                                .action(ArgAction::SetTrue)
                                .conflicts_with("indexers")
                                .help(
                                    "Deploy an indexer and configure one validator per region to upload to it",
                                ),
                        )
                        .arg(
                            Arg::new("indexers")
                                .long("indexers")
                                .required(false)
                                .value_parser(value_parser!(String)),
                        ),
                ),
        )
        .subcommand(
            Command::new("explorer")
                .about("Generate a config.ts for the explorer.")
                .arg(
                    Arg::new("dir")
                        .long("dir")
                        .required(true)
                        .value_parser(value_parser!(String)),
                )
                .arg(
                    Arg::new("backend-url")
                        .long("backend-url")
                        .required(true)
                        .help("Indexer host[:port] without a scheme (the explorer picks http/ws or https/wss by mode)")
                        .value_parser(parse_backend_url),
                )
                .subcommand(Command::new("local").about("Generate explorer config for local deployment"))
                .subcommand(Command::new("remote").about("Generate explorer config for remote deployment")),
        );

    // Parse arguments
    let matches = app.get_matches();

    // Handle subcommands
    match matches.subcommand() {
        Some(("generate", sub_matches)) => {
            let peers = *sub_matches.get_one::<usize>("peers").unwrap();
            let bootstrappers = *sub_matches.get_one::<usize>("bootstrappers").unwrap();
            let worker_threads = *sub_matches.get_one::<usize>("worker_threads").unwrap();
            let blocking_threads = sub_matches
                .get_one::<usize>("blocking_threads")
                .copied()
                .unwrap_or(DEFAULT_BLOCKING_THREADS);
            let storage_buffer_pool_max_per_class = sub_matches
                .get_one::<NonZeroU32>("storage_buffer_pool_max_per_class")
                .copied()
                .or(Some(DEFAULT_STORAGE_BUFFER_POOL_MAX_PER_CLASS));
            let network_buffer_pool_max_per_class = sub_matches
                .get_one::<NonZeroU32>("network_buffer_pool_max_per_class")
                .copied()
                .or(Some(DEFAULT_NETWORK_BUFFER_POOL_MAX_PER_CLASS));
            let storage_buffer_pool_parallelism = sub_matches
                .get_one::<NonZeroUsize>("storage_buffer_pool_parallelism")
                .copied();
            let network_buffer_pool_parallelism = sub_matches
                .get_one::<NonZeroUsize>("network_buffer_pool_parallelism")
                .copied();
            let log_level = sub_matches.get_one::<String>("log_level").unwrap().clone();
            let traces_sample_rate = *sub_matches.get_one::<f64>("traces_sample_rate").unwrap();
            let mailbox_size = *sub_matches.get_one::<usize>("mailbox_size").unwrap();
            let deque_size = *sub_matches.get_one::<usize>("deque_size").unwrap();
            let block_size = *sub_matches.get_one::<u32>("block_size").unwrap();
            let signature_threads = *sub_matches.get_one::<usize>("signature_threads").unwrap();
            let leader = parse_leader(sub_matches).unwrap_or_else(|message| {
                error!("{message}");
                std::process::exit(2);
            });
            let output = sub_matches.get_one::<String>("output").unwrap().clone();
            match sub_matches.subcommand() {
                Some(("local", sub_matches)) => generate_local(
                    sub_matches,
                    peers,
                    bootstrappers,
                    worker_threads,
                    blocking_threads,
                    storage_buffer_pool_max_per_class,
                    network_buffer_pool_max_per_class,
                    storage_buffer_pool_parallelism,
                    network_buffer_pool_parallelism,
                    log_level,
                    traces_sample_rate,
                    mailbox_size,
                    deque_size,
                    block_size,
                    signature_threads,
                    leader,
                    output,
                ),
                Some(("remote", sub_matches)) => generate_remote(
                    sub_matches,
                    peers,
                    bootstrappers,
                    worker_threads,
                    blocking_threads,
                    storage_buffer_pool_max_per_class,
                    network_buffer_pool_max_per_class,
                    storage_buffer_pool_parallelism,
                    network_buffer_pool_parallelism,
                    log_level,
                    traces_sample_rate,
                    mailbox_size,
                    deque_size,
                    block_size,
                    signature_threads,
                    leader,
                    output,
                ),
                _ => {
                    eprintln!("Invalid subcommand. Use 'local' or 'remote'.");
                    std::process::exit(1);
                }
            }
        }
        Some(("explorer", sub_matches)) => {
            let dir = sub_matches.get_one::<String>("dir").unwrap().clone();
            let backend_url = sub_matches
                .get_one::<String>("backend-url")
                .unwrap()
                .clone();
            match sub_matches.subcommand() {
                Some(("local", _)) => explorer_local(dir, backend_url),
                Some(("remote", _)) => explorer_remote(dir, backend_url),
                _ => {
                    eprintln!("Invalid subcommand. Use 'local' or 'remote'.");
                    std::process::exit(1);
                }
            }
        }
        _ => {
            eprintln!("Invalid subcommand. Use 'generate' or 'explorer'.");
            std::process::exit(1);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn generate_local(
    sub_matches: &ArgMatches,
    peers: usize,
    bootstrappers: usize,
    worker_threads: usize,
    blocking_threads: usize,
    storage_buffer_pool_max_per_class: Option<NonZeroU32>,
    network_buffer_pool_max_per_class: Option<NonZeroU32>,
    storage_buffer_pool_parallelism: Option<NonZeroUsize>,
    network_buffer_pool_parallelism: Option<NonZeroUsize>,
    log_level: String,
    traces_sample_rate: f64,
    mailbox_size: usize,
    deque_size: usize,
    block_size: u32,
    signature_threads: usize,
    leader: Leader,
    output: String,
) {
    // Extract arguments
    let start_port = *sub_matches.get_one::<u16>("start_port").unwrap();
    let configured_indexers = parse_indexers(sub_matches.get_one::<String>("indexers"));

    // Resolve relative output paths from the working directory.
    let current_dir = std::env::current_dir().unwrap();
    let output = current_dir.join(output).to_str().unwrap().to_string();
    let storage_output = format!("{output}/storage");

    // Check if output directory exists
    if fs::metadata(&output).is_ok() {
        error!("output directory already exists: {}", output);
        std::process::exit(1);
    }

    // Generate peers
    assert!(
        bootstrappers <= peers,
        "bootstrappers must be less than or equal to peers"
    );
    let mut peer_signers = (0..peers)
        .map(|_| PrivateKey::random(sys_rng()))
        .collect::<Vec<_>>();
    peer_signers.sort_by_key(|signer| signer.public_key());
    let allowed_peers: Vec<String> = peer_signers
        .iter()
        .map(|signer| signer.public_key().to_string())
        .collect();
    let bootstrappers = allowed_peers
        .iter()
        .sample(&mut sys_rng(), bootstrappers)
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();

    // Generate consensus key
    let peers_u32 = peers as u32;
    let Fixture { schemes, .. } =
        bls12381_threshold::fixture::<MinSig, _>(&mut sys_rng(), NAMESPACE, peers_u32);

    let identity = schemes[0].polynomial().public();
    info!(%identity, "generated network key");

    // Generate instance configurations
    let mut port = start_port;
    let mut addresses = HashMap::new();
    let mut configurations = Vec::new();
    for (signer, scheme) in peer_signers.iter().zip(schemes.iter()) {
        // Create peer config
        let name = signer.public_key().to_string();
        addresses.insert(
            name.clone(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        );
        let peer_config_file = format!("{name}.yaml");
        let directory = format!("{storage_output}/{name}");
        let peer_config = Config {
            private_key: hex(&signer.encode()),
            share: hex(&scheme.share().unwrap().encode()),
            polynomial: hex(&scheme.polynomial().encode()),

            port,
            metrics_port: port + 1,
            directory,
            worker_threads,
            blocking_threads,
            storage_buffer_pool_max_per_class,
            network_buffer_pool_max_per_class,
            storage_buffer_pool_parallelism,
            network_buffer_pool_parallelism,
            log_level: log_level.clone(),
            traces_sample_rate,

            local: true,
            allowed_peers: allowed_peers.clone(),
            bootstrappers: bootstrappers.clone(),

            mailbox_size,
            deque_size,
            block_size,

            signature_threads,
            leader,
            backfiller_max_active: DEFAULT_BACKFILLER_MAX_ACTIVE,
            backfiller_retry_ms: DEFAULT_BACKFILLER_RETRY_MS,

            indexer: None,
        };
        configurations.push((name, peer_config_file.clone(), peer_config));
        port += 2;
    }

    let total_indexer_count: usize = configured_indexers
        .iter()
        .map(|indexer| indexer.count)
        .sum();
    assert!(
        total_indexer_count <= configurations.len(),
        "indexer count exceeds number of peers"
    );
    // Assign each configured indexer URL to the requested number of validators
    // in order. Validators without an assignment simply run without an indexer.
    let indexer_assignments = configured_indexers
        .iter()
        .flat_map(|indexer| std::iter::repeat_n(indexer.url.clone(), indexer.count));
    for ((_, _, peer_config), uri) in configurations.iter_mut().zip(indexer_assignments) {
        peer_config.indexer = Some(uri);
    }

    // Create required output directories
    fs::create_dir_all(&output).unwrap();
    fs::create_dir_all(&storage_output).unwrap();

    // Write peers file
    let peers_path = format!("{output}/peers.yaml");
    let file = fs::File::create(&peers_path).unwrap();
    serde_yaml::to_writer(file, &Peers { addresses }).unwrap();

    // Write configuration files
    for (_, peer_config_file, peer_config) in &configurations {
        let path = format!("{output}/{peer_config_file}");
        let file = fs::File::create(&path).unwrap();
        serde_yaml::to_writer(file, peer_config).unwrap();
        info!(path = peer_config_file, "wrote peer configuration file");
    }

    // Emit start commands
    info!(?bootstrappers, "setup complete");
    let mut configured_local_indexers = configured_indexers
        .iter()
        .map(|indexer| indexer.url.clone())
        .collect::<Vec<_>>();
    configured_local_indexers.sort();
    configured_local_indexers.dedup();
    if !configured_local_indexers.is_empty() {
        println!("To start local indexers, run:");
        for url in &configured_local_indexers {
            if let Some(port) = local_indexer_port(url) {
                let certificate_mode = leader.certificate_mode().as_str();
                let command = format!(
                    "cargo run --bin indexer -- --port {port} --identity {identity} --certificate-mode {certificate_mode} --block-size {block_size}"
                );
                println!("{url}: {command}");
            }
        }
    }
    println!("To start validators, run:");
    for (name, peer_config_file, _) in &configurations {
        let path = format!("{output}/{peer_config_file}");
        let command =
            format!("cargo run --bin {BINARY_NAME} -- --peers={peers_path} --config={path}");
        println!("{name}: {command}");
    }
    if !configured_indexers.is_empty() {
        println!("Configured indexers:");
        for (name, _, peer_config) in &configurations {
            if let Some(indexer) = &peer_config.indexer {
                println!("{name}: {indexer}");
            }
        }
    }
    println!("To view metrics, run:");
    for (name, _, peer_config) in configurations {
        println!(
            "{}: curl http://localhost:{}/metrics",
            name, peer_config.metrics_port
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn generate_remote(
    sub_matches: &ArgMatches,
    peers: usize,
    bootstrappers: usize,
    worker_threads: usize,
    blocking_threads: usize,
    storage_buffer_pool_max_per_class: Option<NonZeroU32>,
    network_buffer_pool_max_per_class: Option<NonZeroU32>,
    storage_buffer_pool_parallelism: Option<NonZeroUsize>,
    network_buffer_pool_parallelism: Option<NonZeroUsize>,
    log_level: String,
    traces_sample_rate: f64,
    mailbox_size: usize,
    deque_size: usize,
    block_size: u32,
    signature_threads: usize,
    leader: Leader,
    output: String,
) {
    // Extract arguments
    let regions = sub_matches
        .get_many::<String>("regions")
        .unwrap()
        .cloned()
        .collect::<Vec<_>>();
    let instance_type = sub_matches
        .get_one::<String>("instance_type")
        .unwrap()
        .clone();
    let storage_size = *sub_matches.get_one::<i32>("storage_size").unwrap();
    let monitoring_instance_type = sub_matches
        .get_one::<String>("monitoring_instance_type")
        .unwrap()
        .clone();
    let monitoring_storage_size = *sub_matches
        .get_one::<i32>("monitoring_storage_size")
        .unwrap();
    let dashboard = sub_matches.get_one::<String>("dashboard").unwrap().clone();
    let deploy_indexer = sub_matches.get_flag("indexer");
    let mut configured_indexers = parse_indexers(sub_matches.get_one::<String>("indexers"));
    let unique_regions = regions.iter().fold(Vec::new(), |mut unique, region| {
        if !unique.contains(region) {
            unique.push(region.clone());
        }
        unique
    });
    if deploy_indexer {
        configured_indexers.push(ConfiguredIndexer {
            url: format!("http://{INDEXER_HOST}:{INDEXER_PORT}"),
            count: unique_regions.len(),
        });
    }

    // Resolve relative output paths from the working directory.
    let current_dir = std::env::current_dir().unwrap();
    let output = current_dir.join(output).to_str().unwrap().to_string();

    // Check if output directory exists
    if fs::metadata(&output).is_ok() {
        error!("output directory already exists: {}", output);
        std::process::exit(1);
    }

    // Generate UUID
    let tag = Uuid::new_v4().to_string();
    info!(tag, "generated deployment tag");

    // Generate peers
    assert!(
        bootstrappers <= peers,
        "bootstrappers must be less than or equal to peers"
    );
    let mut peer_signers = (0..peers)
        .map(|_| PrivateKey::random(sys_rng()))
        .collect::<Vec<_>>();
    peer_signers.sort_by_key(|signer| signer.public_key());
    let allowed_peers: Vec<String> = peer_signers
        .iter()
        .map(|signer| signer.public_key().to_string())
        .collect();
    let bootstrappers = allowed_peers
        .iter()
        .sample(&mut sys_rng(), bootstrappers)
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();

    // Generate consensus key
    let peers_u32 = peers as u32;
    let Fixture { schemes, .. } =
        bls12381_threshold::fixture::<MinSig, _>(&mut sys_rng(), NAMESPACE, peers_u32);

    let identity = schemes[0].polynomial().public();
    info!(%identity, "generated network key");

    // Generate instance configurations
    assert!(
        regions.len() <= peers,
        "must be at least one peer per specified region"
    );
    let mut instance_configs = Vec::new();
    let mut peer_configs = Vec::new();
    for (index, (signer, scheme)) in peer_signers.iter().zip(schemes.iter()).enumerate() {
        // Create peer config
        let name = signer.public_key().to_string();
        let peer_config_file = format!("{name}.yaml");
        let peer_config = Config {
            private_key: hex(&signer.encode()),
            share: hex(&scheme.share().unwrap().encode()),
            polynomial: hex(&scheme.polynomial().encode()),

            port: PORT,
            metrics_port: METRICS_PORT,
            directory: "/home/ubuntu/data".to_string(),
            worker_threads,
            blocking_threads,
            storage_buffer_pool_max_per_class,
            network_buffer_pool_max_per_class,
            storage_buffer_pool_parallelism,
            network_buffer_pool_parallelism,
            log_level: log_level.clone(),
            traces_sample_rate,

            local: false,
            allowed_peers: allowed_peers.clone(),
            bootstrappers: bootstrappers.clone(),

            mailbox_size,
            deque_size,
            block_size,

            signature_threads,
            leader,
            backfiller_max_active: DEFAULT_BACKFILLER_MAX_ACTIVE,
            backfiller_retry_ms: DEFAULT_BACKFILLER_RETRY_MS,

            indexer: None,
        };
        peer_configs.push((peer_config_file.clone(), peer_config));

        // Create instance config
        let region_index = index % regions.len();
        let region = regions[region_index].clone();
        let instance = aws::InstanceConfig {
            name: name.clone(),
            region,
            availability_zone_group: None,
            instance_type: instance_type.clone(),
            storage_size,
            storage_class: STORAGE_CLASS.to_string(),
            storage_iops: None,
            storage_throughput: None,
            binary: BINARY_NAME.to_string(),
            config: peer_config_file,
            profiling: false,
        };
        instance_configs.push(instance);
    }

    // Configure indexers if specified
    if !configured_indexers.is_empty() {
        let total_indexer_count: usize = configured_indexers
            .iter()
            .map(|indexer| indexer.count)
            .sum();
        let peer_regions = instance_configs
            .iter()
            .map(|instance| instance.region.clone())
            .collect::<Vec<_>>();
        let (selected_indices, assigned_regions) =
            select_regional_peers(&peer_regions, total_indexer_count);

        // Update selected peer configs
        let indexer_assignments = configured_indexers
            .iter()
            .flat_map(|indexer| std::iter::repeat_n(indexer.url.clone(), indexer.count));
        for (idx, url) in selected_indices.iter().zip(indexer_assignments) {
            peer_configs[*idx].1.indexer = Some(url);
        }

        info!(assignments = ?assigned_regions, "configured indexers");
    }

    let indexer_config = deploy_indexer.then(|| {
        let (participants, locations) = instance_configs
            .iter()
            .map(|instance| {
                (
                    instance.name.clone(),
                    get_aws_location(&instance.region),
                )
            })
            .unzip();

        IndexerConfig {
            port: INDEXER_PORT,
            identity: hex(&identity.encode()),
            certificate_mode: leader.certificate_mode(),
            block_size,
            explorer: ExplorerConfig {
                name: "Global Cluster".to_string(),
                description: format!(
                    "A live cluster of <strong>{peers} validators</strong> running {instance_type} nodes on AWS in <strong>{} regions</strong> ({}).",
                    unique_regions.len(),
                    unique_regions.join(", ")
                ),
                participants,
                locations,
            },
        }
    });

    if deploy_indexer {
        instance_configs.push(aws::InstanceConfig {
            name: INDEXER_HOST.to_string(),
            region: regions[0].clone(),
            availability_zone_group: None,
            instance_type: instance_type.clone(),
            storage_size,
            storage_class: STORAGE_CLASS.to_string(),
            storage_iops: None,
            storage_throughput: None,
            binary: INDEXER_BINARY_NAME.to_string(),
            config: INDEXER_CONFIG_FILE.to_string(),
            profiling: false,
        });
    }

    // Generate root config file
    let mut ports = vec![aws::PortConfig {
        protocol: "tcp".to_string(),
        port: PORT,
        cidr: "0.0.0.0/0".to_string(),
    }];
    if deploy_indexer {
        ports.push(aws::PortConfig {
            protocol: "tcp".to_string(),
            port: INDEXER_PORT,
            cidr: "0.0.0.0/0".to_string(),
        });
    }
    let config = aws::Config {
        tag,
        instances: instance_configs,
        monitoring: aws::MonitoringConfig {
            instance_type: monitoring_instance_type,
            storage_size: monitoring_storage_size,
            storage_class: STORAGE_CLASS.to_string(),
            storage_iops: None,
            storage_throughput: None,
            dashboard: DASHBOARD_FILE.to_string(),
        },
        ports,
    };

    // Write configuration files
    fs::create_dir_all(&output).unwrap();
    fs::copy(
        current_dir.join(&dashboard),
        format!("{output}/{DASHBOARD_FILE}"),
    )
    .unwrap();
    if let Some(indexer_config) = indexer_config {
        let path = format!("{output}/{INDEXER_CONFIG_FILE}");
        let file = fs::File::create(&path).unwrap();
        serde_yaml::to_writer(file, &indexer_config).unwrap();
        info!(
            path = INDEXER_CONFIG_FILE,
            "wrote indexer configuration file"
        );
    }
    for (peer_config_file, peer_config) in peer_configs {
        let path = format!("{output}/{peer_config_file}");
        let file = fs::File::create(&path).unwrap();
        serde_yaml::to_writer(file, &peer_config).unwrap();
        info!(path = peer_config_file, "wrote peer configuration file");
    }
    let path = format!("{output}/config.yaml");
    let file = fs::File::create(&path).unwrap();
    serde_yaml::to_writer(file, &config).unwrap();
    info!(path = "config.yaml", "wrote configuration file");
}

// Region-to-location mapping
fn get_aws_location(region: &str) -> Option<([f64; 2], String)> {
    match region {
        "us-west-1" => Some(([37.7749, -122.4194], "San Francisco".to_string())),
        "us-west-2" => Some(([45.9175, -119.2684], "Boardman".to_string())),
        "us-east-1" => Some(([38.8339, -77.3074], "Ashburn".to_string())),
        "us-east-2" => Some(([40.0946, -82.7541], "Columbus".to_string())),
        "eu-west-1" => Some(([53.3498, -6.2603], "Dublin".to_string())),
        "ap-northeast-1" => Some(([35.6895, 139.6917], "Tokyo".to_string())),
        "eu-north-1" => Some(([59.3293, 18.0686], "Stockholm".to_string())),
        "ap-south-1" => Some(([19.0760, 72.8777], "Mumbai".to_string())),
        "sa-east-1" => Some(([-23.5505, -46.6333], "Sao Paulo".to_string())),
        "eu-central-1" => Some(([50.1109, 8.6821], "Frankfurt".to_string())),
        "ap-northeast-2" => Some(([37.5665, 126.9780], "Seoul".to_string())),
        "ap-southeast-2" => Some(([-33.8688, 151.2093], "Sydney".to_string())),
        _ => None,
    }
}

fn explorer_local(dir: String, backend_url: String) {
    // Read peers.yaml to get participant count
    let peers_path = format!("{dir}/peers.yaml");
    let peers_content = fs::read_to_string(&peers_path).expect("failed to read peers.yaml");
    let peers: Peers = serde_yaml::from_str(&peers_content).expect("failed to parse peers.yaml");
    let num_peers = peers.addresses.len();

    // Read polynomial from first peer config
    let first_peer = peers.addresses.keys().next().expect("no peers found");
    let peer_config_path = format!("{dir}/{first_peer}.yaml");
    let peer_config_content =
        fs::read_to_string(&peer_config_path).expect("failed to read peer config");
    let peer_config: Config =
        serde_yaml::from_str(&peer_config_content).expect("failed to parse peer config");
    let certificate_mode = peer_config.leader.certificate_mode().as_str();
    let polynomial_hex = peer_config.polynomial;
    let polynomial = from_hex(&polynomial_hex).expect("invalid polynomial");
    let polynomial = Sharing::<MinSig>::decode_cfg(
        polynomial.as_ref(),
        &(NZU32!(num_peers as u32), ModeVersion::v0()),
    )
    .expect("polynomial is invalid");
    let identity = polynomial.public();

    // Generate config.ts with empty locations (explorer will hide map)
    let config_ts = format!(
        "export const BACKEND_URL = \"{}\";\n\
        export const PUBLIC_KEY_HEX = \"{}\";\n\
        export const CERTIFICATE_MODE = \"{}\" as const;\n\
        export const LOCATIONS: [[number, number], string][] = [];",
        backend_url,
        hex(&identity.encode()),
        certificate_mode,
    );

    // Write config.ts
    let config_ts_path = format!("{dir}/config.ts");
    fs::write(&config_ts_path, config_ts).expect("failed to write config.ts");
    info!(path = "config.ts", "wrote explorer configuration file");
}

fn explorer_remote(dir: String, backend_url: String) {
    // Collect all locations
    let config_path = format!("{dir}/config.yaml");
    let config_content = fs::read_to_string(&config_path).expect("failed to read config.yaml");
    let config: aws::Config =
        serde_yaml::from_str(&config_content).expect("failed to parse config.yaml");
    let validators = config
        .instances
        .iter()
        .filter(|instance| instance.binary == BINARY_NAME)
        .collect::<Vec<_>>();
    let mut participants = BTreeMap::new();
    for instance in &validators {
        let region = &instance.region;
        let public_key = from_hex(&instance.name).expect("invalid public key");
        let public_key = PublicKey::decode(public_key.as_ref()).expect("invalid public key");
        let location = match get_aws_location(region) {
            Some((coords, city)) => format!("    [[{}, {}], \"{}\"]", coords[0], coords[1], city),
            None => "    null".to_string(),
        };
        participants.insert(public_key, (format!("    \"{}\"", instance.name), location));
    }

    // Keep one location slot per participant in public-key order so missing coordinates never
    // shift another leader's location.
    let mut keys = Vec::new();
    let mut locations = Vec::new();
    for (_, (key, location)) in participants {
        keys.push(key);
        locations.push(location);
    }

    // Generate config.ts
    let participants_str = keys.join(",\n");
    let locations_str = locations.join(",\n");
    let first_instance = validators.first().expect("no validators found");
    let peer_config_path = format!("{}/{}", dir, first_instance.config);
    let peer_config_content =
        fs::read_to_string(&peer_config_path).expect("failed to read peer config");
    let peer_config: Config =
        serde_yaml::from_str(&peer_config_content).expect("failed to parse peer config");
    let certificate_mode = peer_config.leader.certificate_mode().as_str();
    let polynomial_hex = peer_config.polynomial;
    let polynomial = from_hex(&polynomial_hex).expect("invalid polynomial");
    let polynomial = Sharing::<MinSig>::decode_cfg(
        polynomial.as_ref(),
        &(NZU32!(locations.len() as u32), ModeVersion::v0()),
    )
    .expect("polynomial is invalid");
    let identity = polynomial.public();
    let config_ts = format!(
        "export const BACKEND_URL = \"{}\";\n\
        export const PUBLIC_KEY_HEX = \"{}\";\n\
        export const CERTIFICATE_MODE = \"{}\" as const;\n\
        export const PARTICIPANTS: string[] = [\n{}\n];\n\
        export const LOCATIONS: ([[number, number], string] | null)[] = [\n{}\n];",
        backend_url,
        hex(&identity.encode()),
        certificate_mode,
        participants_str,
        locations_str
    );

    // Write config.ts
    let config_ts_path = format!("{dir}/config.ts");
    fs::write(&config_ts_path, config_ts).expect("failed to write config.ts");
    info!(path = "config.ts", "wrote explorer configuration file");
}

#[cfg(test)]
mod tests {
    use super::{
        leader_args, parse_indexers, parse_leader, select_regional_peers, traces_sample_rate_arg,
        ConfiguredIndexer,
    };
    use alto_chain::Leader;
    use alto_types::CertificateMode;
    use clap::Command;
    use commonware_utils::{NZU32, NZU64};

    #[test]
    fn traces_sample_rate_accepts_only_fractions() {
        let matches = Command::new("test")
            .arg(traces_sample_rate_arg())
            .try_get_matches_from(["test"])
            .unwrap();
        assert_eq!(*matches.get_one::<f64>("traces_sample_rate").unwrap(), 0.0);

        let matches = Command::new("test")
            .arg(traces_sample_rate_arg())
            .try_get_matches_from(["test", "--traces-sample-rate", "0.0001"])
            .unwrap();
        assert_eq!(
            *matches.get_one::<f64>("traces_sample_rate").unwrap(),
            0.0001
        );

        for invalid in ["-0.1", "1.1", "NaN"] {
            assert!(Command::new("test")
                .arg(traces_sample_rate_arg())
                .try_get_matches_from(["test", "--traces-sample-rate", invalid])
                .is_err());
        }
    }

    #[test]
    fn validator_config_defaults_optional_settings() {
        let yaml = r#"
private_key: key
share: share
polynomial: polynomial
port: 1
metrics_port: 2
directory: data
worker_threads: 1
log_level: info
local: true
allowed_peers: []
bootstrappers: []
mailbox_size: 1
deque_size: 1
signature_threads: 1
"#;

        // The leader policy selects the certificate construction, so it is never defaulted.
        assert!(serde_yaml::from_str::<alto_chain::Config>(yaml).is_err());
        let yaml = &format!(
            "{yaml}\nleader:\n  mode: stable\n  delay_ms: 10\n  term_length: 1000\n  optimistic_views: 48\n"
        );

        let config: alto_chain::Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.block_size, 0);
        assert_eq!(config.traces_sample_rate, 0.0);

        let config: alto_chain::Config = serde_yaml::from_str(&format!(
            "{yaml}\nblock_size: 4096\ntraces_sample_rate: 0.0001\n"
        ))
        .unwrap();
        assert_eq!(config.block_size, 4096);
        assert_eq!(config.traces_sample_rate, 0.0001);

        assert!(serde_yaml::from_str::<alto_chain::Config>(&format!(
            "{yaml}\ntraces_sample_rate: 1.1\n"
        ))
        .is_err());
    }

    #[test]
    fn parse_rotating_leader() {
        let result = Command::new("test")
            .args(leader_args())
            .try_get_matches_from(["test", "--leader-mode", "rotating"]);
        assert!(result.is_err());

        let matches = Command::new("test")
            .args(leader_args())
            .try_get_matches_from([
                "test",
                "--leader-mode",
                "rotating",
                "--leader-delay-ms",
                "7",
            ])
            .unwrap();
        assert_eq!(parse_leader(&matches).unwrap(), Leader::rotating(NZU64!(7)));
    }

    #[test]
    fn leader_delay_must_stay_below_leader_timeout() {
        for delay in ["0", "1000", "5000"] {
            assert!(Command::new("test")
                .args(leader_args())
                .try_get_matches_from([
                    "test",
                    "--leader-mode",
                    "rotating",
                    "--leader-delay-ms",
                    delay
                ])
                .is_err());
        }
        assert!(Command::new("test")
            .args(leader_args())
            .try_get_matches_from([
                "test",
                "--leader-mode",
                "rotating",
                "--leader-delay-ms",
                "999"
            ])
            .is_ok());
    }

    #[test]
    fn backend_url_rejects_schemes() {
        assert_eq!(
            super::parse_backend_url("localhost:8080").unwrap(),
            "localhost:8080"
        );
        assert_eq!(
            super::parse_backend_url("global.alto.example.com/").unwrap(),
            "global.alto.example.com"
        );
        assert!(super::parse_backend_url("http://localhost:8080").is_err());
        assert!(super::parse_backend_url("").is_err());
    }

    #[test]
    fn indexer_config_and_explorer_spell_certificate_mode_alike() {
        // indexer.yaml serializes the mode with serde while config.ts is written with `as_str`.
        for mode in CertificateMode::ALL {
            assert_eq!(serde_yaml::to_string(&mode).unwrap().trim(), mode.as_str());
        }
    }

    #[test]
    fn parse_stable_leader() {
        let matches = Command::new("test")
            .args(leader_args())
            .try_get_matches_from([
                "test",
                "--leader-mode",
                "stable",
                "--leader-delay-ms",
                "10",
                "--leader-term-length",
                "1000",
                "--leader-optimistic-views",
                "48",
            ])
            .unwrap();
        assert_eq!(
            parse_leader(&matches).unwrap(),
            Leader::stable(NZU64!(10), NZU32!(1_000), 48)
        );
    }

    #[test]
    fn stable_leader_requires_all_settings() {
        let result = Command::new("test")
            .args(leader_args())
            .try_get_matches_from(["test", "--leader-mode", "stable"]);
        assert!(result.is_err());

        let result = Command::new("test")
            .args(leader_args())
            .try_get_matches_from([
                "test",
                "--leader-mode",
                "stable",
                "--leader-delay-ms",
                "10",
                "--leader-term-length",
                "1000",
            ]);
        assert!(result.is_err());

        let result = Command::new("test")
            .args(leader_args())
            .try_get_matches_from([
                "test",
                "--leader-mode",
                "stable",
                "--leader-delay-ms",
                "10",
                "--leader-term-length",
                "1",
                "--leader-optimistic-views",
                "48",
            ]);
        assert!(result.is_err());
    }

    #[test]
    fn rotating_leader_rejects_term_length() {
        let matches = Command::new("test")
            .args(leader_args())
            .try_get_matches_from([
                "test",
                "--leader-mode",
                "rotating",
                "--leader-delay-ms",
                "10",
                "--leader-term-length",
                "1000",
            ])
            .unwrap();
        assert!(parse_leader(&matches).is_err());

        let matches = Command::new("test")
            .args(leader_args())
            .try_get_matches_from([
                "test",
                "--leader-mode",
                "rotating",
                "--leader-delay-ms",
                "10",
                "--leader-optimistic-views",
                "48",
            ])
            .unwrap();
        assert!(parse_leader(&matches).is_err());
    }

    #[test]
    fn leader_config_round_trips() {
        for leader in [
            Leader::rotating(NZU64!(7)),
            Leader::stable(NZU64!(10), NZU32!(1_000), 48),
        ] {
            let encoded = serde_yaml::to_string(&leader).unwrap();
            assert_eq!(serde_yaml::from_str::<Leader>(&encoded).unwrap(), leader);
        }
    }

    #[test]
    fn leader_config_rejects_invalid_fields() {
        assert!(
            serde_yaml::from_str::<Leader>("mode: stable\ndelay_ms: 10\nterm_length: 1000\n")
                .is_err()
        );
        assert!(serde_yaml::from_str::<Leader>(
            "mode: stable\ndelay_ms: 10\nterm_length: 1\noptimistic_views: 48\n"
        )
        .is_err());
        assert!(serde_yaml::from_str::<Leader>("mode: rotating\n").is_err());
        assert!(serde_yaml::from_str::<Leader>(
            "mode: rotating\ndelay_ms: 10\nterm_length: 1000\n"
        )
        .is_err());
    }

    #[test]
    fn parse_indexers_supports_multiple_specs() {
        let specs = parse_indexers(Some(
            &"https://idx-a.example.com:2;https://idx-b.example.com:1".to_string(),
        ));
        assert_eq!(
            specs,
            vec![
                ConfiguredIndexer {
                    url: "https://idx-a.example.com".to_string(),
                    count: 2,
                },
                ConfiguredIndexer {
                    url: "https://idx-b.example.com".to_string(),
                    count: 1,
                },
            ]
        );
    }

    #[test]
    fn parse_indexers_allows_empty_configuration() {
        assert!(parse_indexers(None).is_empty());
    }

    #[test]
    fn regional_selection_uses_each_region_before_repeating() {
        let regions = [
            "us-west-1",
            "us-east-1",
            "eu-west-1",
            "us-west-1",
            "us-east-1",
            "eu-west-1",
        ]
        .map(str::to_string);

        let (selected, assigned) = select_regional_peers(&regions, 3);
        assert_eq!(selected, vec![2, 1, 0]);
        assert_eq!(
            assigned.values().copied().collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
    }
}
