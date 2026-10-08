use alto_chain::{
    Config, Leader, Peers, DEFAULT_BACKFILLER_MAX_ACTIVE, DEFAULT_BACKFILLER_RETRY_MS,
    DEFAULT_BLOCKING_THREADS, DEFAULT_NETWORK_BUFFER_POOL_MAX_PER_CLASS,
    DEFAULT_STORAGE_BUFFER_POOL_MAX_PER_CLASS, LEADER_TIMEOUT,
};
use alto_types::{host_name, PrivateKey, PublicKey};
use clap::{value_parser, Arg, ArgAction, ArgMatches, Command};
use commonware_codec::{DecodeExt, Encode};
use commonware_cryptography::{Hasher, Sha256, Signer};
use commonware_deployer::aws::{self, METRICS_PORT};
use commonware_formatting::{from_hex, hex};
use commonware_math::algebra::Random;
use commonware_utils::{ordered::Set, sys_rng};
use rand::seq::IteratorRandom;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU32, NonZeroUsize},
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

fn leader_args() -> [Arg; 3] {
    [
        // Validators refuse to start with a proposal delay at or above their leader timeout.
        Arg::new("leader_delay_ms")
            .long("leader-delay-ms")
            .required(true)
            .value_parser(value_parser!(u64).range(0..LEADER_TIMEOUT.as_millis() as u64)),
        Arg::new("leader_term_length")
            .long("leader-term-length")
            .required(true)
            .value_parser(value_parser!(u32).range(2..)),
        Arg::new("leader_optimistic_views")
            .long("leader-optimistic-views")
            .required(true)
            .value_parser(value_parser!(u64)),
    ]
}

fn parse_traces_sample_rate(value: &str) -> Result<f64, String> {
    let rate = value
        .parse::<f64>()
        .map_err(|_| "traces sample rate must be a number between 0 and 1".to_string())?;
    if (0.0..=1.0).contains(&rate) {
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

fn parse_leader(matches: &ArgMatches) -> Leader {
    Leader::new(
        *matches.get_one::<u64>("leader_delay_ms").unwrap(),
        NonZeroU32::new(*matches.get_one::<u32>("leader_term_length").unwrap()).unwrap(),
        *matches.get_one::<u64>("leader_optimistic_views").unwrap(),
    )
}

/// Returns the hex-encoded network identity of `signers`: their encoded participant set.
fn participant_identity(signers: &[PrivateKey]) -> String {
    let participants = Set::from_iter_dedup(signers.iter().map(|signer| signer.public_key()));
    let identity = participants.encode();
    let digest = Sha256::hash(&[identity.as_ref()]);
    info!(%digest, participants = participants.len(), "generated participant set");
    hex(&identity)
}

fn main() {
    // Initialize logger
    tracing_subscriber::fmt().init();

    // Parse arguments and run the selected subcommand.
    run(command().get_matches());
}

/// Define the main command with subcommands.
fn command() -> Command {
    Command::new("deploy")
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
                .about("Generate a config.ts for running the explorer from source against a local network.")
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
                        .help("Indexer host[:port] without a scheme")
                        .value_parser(parse_backend_url),
                ),
        )
}

/// Handle subcommands.
fn run(matches: ArgMatches) {
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
            let leader = parse_leader(sub_matches);
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
            explorer(dir, backend_url);
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

    // Generate the network identity
    let identity = participant_identity(&peer_signers);

    // Generate instance configurations
    let mut port = start_port;
    let mut addresses = HashMap::new();
    let mut configurations = Vec::new();
    for signer in &peer_signers {
        // Create peer config
        let public_key = signer.public_key();
        let name = host_name(&public_key);
        addresses.insert(
            public_key.to_string(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        );
        let peer_config_file = format!("{name}.yaml");
        let directory = format!("{storage_output}/{name}");
        let peer_config = Config {
            private_key: hex(&signer.encode()),

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
                let command = format!(
                    "cargo run --bin {INDEXER_BINARY_NAME} -- --port {port} --identity {identity} --block-size {block_size}"
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

    // Generate the network identity
    let identity = participant_identity(&peer_signers);

    // Generate instance configurations
    assert!(
        regions.len() <= peers,
        "must be at least one peer per specified region"
    );
    let mut instance_configs = Vec::new();
    let mut peer_configs = Vec::new();
    for (index, signer) in peer_signers.iter().enumerate() {
        // Create peer config
        let name = host_name(&signer.public_key());
        let peer_config_file = format!("{name}.yaml");
        let peer_config = Config {
            private_key: hex(&signer.encode()),

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
        // Validator instances follow the sorted participant order of `allowed_peers`.
        let participants = allowed_peers.clone();
        let locations = instance_configs
            .iter()
            .map(|instance| get_aws_location(&instance.region))
            .collect();

        IndexerConfig {
            port: INDEXER_PORT,
            identity: identity.clone(),
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

/// Decodes a hex-encoded validator public key from a generated configuration.
fn parse_public_key(public_key: &str) -> PublicKey {
    let public_key = from_hex(public_key).expect("invalid public key");
    PublicKey::decode(public_key).expect("invalid public key")
}

/// Returns the hex-encoded network identity of the validators configured by `config`: their
/// encoded participant set.
fn network_identity(config: &Config) -> String {
    let participants = Set::from_iter_dedup(
        config
            .allowed_peers
            .iter()
            .map(|peer| parse_public_key(peer)),
    );
    hex(&participants.encode())
}

/// Reads a generated validator configuration.
fn read_peer_config(path: &str) -> Config {
    let content = fs::read_to_string(path).expect("failed to read peer config");
    serde_yaml::from_str(&content).expect("failed to parse peer config")
}

fn explorer(dir: String, backend_url: String) {
    // Read any validator's configuration, named by the host of its key.
    let peers_path = format!("{dir}/peers.yaml");
    let peers_content = fs::read_to_string(&peers_path).expect("failed to read peers.yaml");
    let peers: Peers = serde_yaml::from_str(&peers_content).expect("failed to parse peers.yaml");
    let first_peer = peers.addresses.keys().next().expect("no peers found");
    let peer_config = read_peer_config(&format!(
        "{dir}/{}.yaml",
        host_name(&parse_public_key(first_peer))
    ));
    let identity = network_identity(&peer_config);

    // Generate config.ts with empty locations (explorer will hide map)
    let config_ts = format!(
        "export const BACKEND_URL = \"{}\";\n\
        export const PUBLIC_KEY_HEX = \"{}\";\n\
        export const LOCATIONS: [[number, number], string][] = [];",
        backend_url, identity,
    );

    // Write config.ts
    let config_ts_path = format!("{dir}/config.ts");
    fs::write(&config_ts_path, config_ts).expect("failed to write config.ts");
    info!(path = "config.ts", "wrote explorer configuration file");
}

#[cfg(test)]
mod tests {
    use super::{
        command, leader_args, parse_indexers, parse_leader, run, select_regional_peers,
        traces_sample_rate_arg, ConfiguredIndexer,
    };
    use alto_chain::Leader;
    use alto_types::{decode_identity, host_name, PrivateKey, PublicKey};
    use clap::Command;
    use commonware_codec::{DecodeExt, Encode};
    use commonware_cryptography::Signer;
    use commonware_formatting::{from_hex, hex};
    use commonware_utils::NZU32;
    use serde_yaml::Value;
    use std::{fs, path::Path};
    use uuid::Uuid;

    /// Generates a four-validator network into `output` with the given deployment target
    /// arguments.
    fn generate(output: &Path, target: &[&str]) {
        let mut args = vec![
            "deploy",
            "generate",
            "--peers",
            "4",
            "--bootstrappers",
            "1",
            "--worker-threads",
            "1",
            "--log-level",
            "info",
            "--mailbox-size",
            "16384",
            "--deque-size",
            "256",
            "--signature-threads",
            "1",
            "--leader-delay-ms",
            "5",
            "--leader-term-length",
            "1000",
            "--leader-optimistic-views",
            "48",
            "--output",
            output.to_str().unwrap(),
        ];
        args.extend(target);
        run(command().try_get_matches_from(args).unwrap());
    }

    /// Remote deployment arguments for validators in `regions`.
    fn remote_args(regions: &str) -> Vec<&str> {
        vec![
            "remote",
            "--regions",
            regions,
            "--monitoring-instance-type",
            "c7gd.4xlarge",
            "--monitoring-storage-size",
            "100",
            "--instance-type",
            "c7gd.4xlarge",
            "--storage-size",
            "25",
            "--dashboard",
            concat!(env!("CARGO_MANIFEST_DIR"), "/dashboard.json"),
        ]
    }

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

        // The leader settings are never defaulted.
        assert!(serde_yaml::from_str::<alto_chain::Config>(yaml).is_err());
        let yaml = &format!(
            "{yaml}\nleader:\n  delay_ms: 10\n  term_length: 1000\n  optimistic_views: 48\n"
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
    fn leader_delay_must_stay_below_leader_timeout() {
        let parse = |delay: &str| {
            Command::new("test")
                .args(leader_args())
                .try_get_matches_from([
                    "test",
                    "--leader-delay-ms",
                    delay,
                    "--leader-term-length",
                    "1000",
                    "--leader-optimistic-views",
                    "48",
                ])
        };
        for delay in ["1000", "5000"] {
            assert!(parse(delay).is_err());
        }
        assert!(parse("999").is_ok());
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
    fn parse_leader_settings() {
        let matches = Command::new("test")
            .args(leader_args())
            .try_get_matches_from([
                "test",
                "--leader-delay-ms",
                "10",
                "--leader-term-length",
                "1000",
                "--leader-optimistic-views",
                "48",
            ])
            .unwrap();
        assert_eq!(parse_leader(&matches), Leader::new(10, NZU32!(1_000), 48));
    }

    #[test]
    fn generation_names_hosts_by_key_digest() {
        use commonware_deployer::aws;

        let output = std::env::temp_dir().join(format!("alto-deploy-hosts-{}", Uuid::new_v4()));
        generate(&output, &remote_args("us-east-1,eu-west-1"));

        let deployment: aws::Config =
            serde_yaml::from_str(&fs::read_to_string(output.join("config.yaml")).unwrap()).unwrap();
        assert_eq!(deployment.instances.len(), 4);
        assert!(!output.join("indexer.yaml").exists());
        for instance in &deployment.instances {
            // Hosts and their configuration files are named by a short digest of the key.
            assert_eq!(instance.name.len(), 32);
            assert_eq!(instance.config, format!("{}.yaml", instance.name));
            let raw = fs::read_to_string(output.join(&instance.config)).unwrap();
            let config: alto_chain::Config = serde_yaml::from_str(&raw).unwrap();
            let signer = PrivateKey::decode(from_hex(&config.private_key).unwrap()).unwrap();
            assert_eq!(instance.name, host_name(&signer.public_key()));
            assert_eq!(config.leader, Leader::new(5, NZU32!(1_000), 48));

            // Peer lists keep full keys, and every peer resolves to a deployed host.
            assert!(config
                .allowed_peers
                .contains(&signer.public_key().to_string()));
            for peer in &config.allowed_peers {
                let key = PublicKey::decode(from_hex(peer).unwrap()).unwrap();
                assert!(deployment
                    .instances
                    .iter()
                    .any(|instance| instance.name == host_name(&key)));
            }
        }
        fs::remove_dir_all(output).unwrap();
    }

    #[test]
    fn leader_requires_all_settings() {
        for args in [
            &["test"][..],
            &[
                "test",
                "--leader-delay-ms",
                "10",
                "--leader-term-length",
                "1000",
            ],
            &[
                "test",
                "--leader-delay-ms",
                "10",
                "--leader-optimistic-views",
                "48",
            ],
            &[
                "test",
                "--leader-term-length",
                "1000",
                "--leader-optimistic-views",
                "48",
            ],
            &[
                "test",
                "--leader-delay-ms",
                "10",
                "--leader-term-length",
                "1",
                "--leader-optimistic-views",
                "48",
            ],
        ] {
            assert!(Command::new("test")
                .args(leader_args())
                .try_get_matches_from(args)
                .is_err());
        }
    }

    #[test]
    fn leader_config_round_trips() {
        for leader in [
            Leader::new(0, NZU32!(1_000), 48),
            Leader::new(10, NZU32!(1_000), 48),
        ] {
            let encoded = serde_yaml::to_string(&leader).unwrap();
            assert_eq!(serde_yaml::from_str::<Leader>(&encoded).unwrap(), leader);
        }
    }

    #[test]
    fn leader_config_rejects_invalid_fields() {
        assert!(serde_yaml::from_str::<Leader>("delay_ms: 10\nterm_length: 1000\n").is_err());
        assert!(serde_yaml::from_str::<Leader>(
            "delay_ms: 10\nterm_length: 1\noptimistic_views: 48\n"
        )
        .is_err());
        assert!(serde_yaml::from_str::<Leader>(
            "mode: stable\ndelay_ms: 10\nterm_length: 1000\noptimistic_views: 48\n"
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

    #[test]
    fn indexer_generation_preserves_unmapped_participants() {
        let output = std::env::temp_dir().join(format!("alto-deploy-region-{}", Uuid::new_v4()));
        let mut target = remote_args("us-east-1,eu-west-2,us-west-1");
        target.push("--indexer");
        generate(&output, &target);

        let deployment: Value =
            serde_yaml::from_str(&fs::read_to_string(output.join("config.yaml")).unwrap()).unwrap();
        let indexer: Value =
            serde_yaml::from_str(&fs::read_to_string(output.join("indexer.yaml")).unwrap())
                .unwrap();
        let validators = deployment["instances"]
            .as_sequence()
            .unwrap()
            .iter()
            .filter(|instance| instance["binary"] == "validator")
            .collect::<Vec<_>>();
        let keys = indexer
            .as_mapping()
            .unwrap()
            .keys()
            .map(|key| key.as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(keys, ["port", "identity", "block_size", "explorer"]);
        let participants = indexer["explorer"]["participants"].as_sequence().unwrap();
        let locations = indexer["explorer"]["locations"].as_sequence().unwrap();
        assert_eq!(validators.len(), 4);
        assert_eq!(participants.len(), 4);
        assert_eq!(locations.len(), 4);
        for (participant, validator) in participants.iter().zip(&validators) {
            // Participants are full keys in validator order, and each names its host.
            let config: alto_chain::Config = serde_yaml::from_str(
                &fs::read_to_string(output.join(validator["config"].as_str().unwrap())).unwrap(),
            )
            .unwrap();
            let public_key = PrivateKey::decode(from_hex(&config.private_key).unwrap())
                .unwrap()
                .public_key();
            assert_eq!(participant.as_str().unwrap(), public_key.to_string());
            assert_eq!(validator["name"].as_str().unwrap(), host_name(&public_key));
            assert_eq!(participants.len(), config.allowed_peers.len());
            assert_eq!(config.leader, Leader::new(5, NZU32!(1_000), 48));
        }

        // The identity is the encoded participant set.
        let identity =
            decode_identity(from_hex(indexer["identity"].as_str().unwrap()).unwrap()).unwrap();
        let keys = identity
            .iter()
            .map(|key| key.to_string())
            .collect::<Vec<_>>();
        let participant_keys = participants
            .iter()
            .map(|participant| participant.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(keys, participant_keys);
        assert_eq!(
            hex(&identity.encode()),
            indexer["identity"].as_str().unwrap()
        );

        // Missing coordinates keep their slot between known participants.
        assert_eq!(locations[0][1], "Ashburn");
        assert!(locations[1].is_null());
        assert_eq!(locations[2][1], "San Francisco");
        assert_eq!(locations[3][1], "Ashburn");

        fs::remove_dir_all(output).unwrap();
    }

    #[test]
    fn local_generation_configures_indexers_and_explorer_identity() {
        let output = std::env::temp_dir().join(format!("alto-deploy-local-{}", Uuid::new_v4()));
        generate(
            &output,
            &[
                "local",
                "--start-port",
                "3000",
                "--indexers",
                "http://localhost:8080:1",
            ],
        );

        // Exactly one validator uploads to the indexer.
        let configs = fs::read_dir(&output)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.file_name().unwrap() != "peers.yaml")
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "yaml")
            })
            .map(|path| {
                serde_yaml::from_str::<alto_chain::Config>(&fs::read_to_string(path).unwrap())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(configs.len(), 4);
        assert_eq!(
            configs
                .iter()
                .filter(|config| config.indexer.as_deref() == Some("http://localhost:8080"))
                .count(),
            1
        );

        run(command()
            .try_get_matches_from([
                "deploy",
                "explorer",
                "--dir",
                output.to_str().unwrap(),
                "--backend-url",
                "localhost:8080",
            ])
            .unwrap());
        let config = fs::read_to_string(output.join("config.ts")).unwrap();
        let (_, value) = config
            .split_once("export const PUBLIC_KEY_HEX = \"")
            .unwrap();
        let encoded = value.split('"').next().unwrap();
        let identity = decode_identity(from_hex(encoded).unwrap()).unwrap();
        assert_eq!(hex(&identity.encode()), encoded);

        // The identity is the sorted participant set.
        assert_eq!(
            identity
                .iter()
                .map(|key| key.to_string())
                .collect::<Vec<_>>(),
            configs[0].allowed_peers
        );
        fs::remove_dir_all(output).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn scripted_deployment_uses_leader_settings_and_current_frontend() {
        use std::{os::unix::fs::PermissionsExt, process::Command};

        let tag = format!("alto-build-test-{}", Uuid::new_v4());
        let output = std::env::temp_dir().join(&tag);
        for directory in ["bin", "deploy", "explorer/build"] {
            fs::create_dir_all(output.join(directory)).unwrap();
        }
        fs::write(output.join("deploy.sh"), include_str!("../../deploy.sh")).unwrap();
        fs::write(output.join("deploy/dashboard.json"), "{}").unwrap();
        fs::write(output.join("explorer/build/index.html"), "old frontend").unwrap();

        // Run the real script with isolated tools that expose its producer/consumer handoff.
        let stub = r#"#!/bin/sh
set -eu
case "${0##*/}" in
    uname) echo Linux ;;
    cargo)
        mkdir -p assets
        printf 'tag: %s\n' "$ALTO_BUILD_TEST_TAG" > assets/config.yaml
        cargo_args=
        while [ "$1" != -- ]; do
            case "$1" in run|--locked|--bin|deploy) ;; *) cargo_args="$cargo_args $1" ;; esac
            shift
        done
        printf '%s\n' "$cargo_args" > assets/cargo-args
        shift
        printf '%s\n' deploy "$@" > assets/generator-args
        ;;
    npm)
        if [ "$3" = run ]; then
            printf '%s\n' "$4" > explorer/npm-script
            mkdir -p "explorer/${BUILD_PATH:-build}"
            frontend_base="${PUBLIC_URL:-}"
            printf '<script src="%s/runtime-config.js"></script>current frontend' "${frontend_base%/}" > "explorer/${BUILD_PATH:-build}/index.html"
        fi
        ;;
    just)
        printf '%s\n' "$1" > assets/just-recipe
        cp explorer/build/index.html assets/embedded.html
        ;;
    *) ;;
esac
"#;
        for tool in [
            "cargo",
            "just",
            "docker",
            "deployer",
            "npm",
            "wasm-pack",
            "uname",
        ] {
            let path = output.join("bin").join(tool);
            fs::write(&path, stub).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        let script = |args: &[&str]| {
            Command::new("bash")
                .arg(output.join("deploy.sh"))
                .args(args)
                .env(
                    "PATH",
                    format!("{}:/usr/bin:/bin", output.join("bin").display()),
                )
                .env("BUILD_PATH", "alternate")
                .env("PUBLIC_URL", "/alternate")
                .env("ALTO_BUILD_TEST_TAG", &tag)
                .output()
                .unwrap()
        };

        // The script takes no arguments.
        assert!(!script(&["stable"]).status.success());
        assert!(!output.join("assets").exists());

        let result = script(&[]);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let args = fs::read_to_string(output.join("assets/generator-args")).unwrap();
        let matches = command().try_get_matches_from(args.lines()).unwrap();
        let generate = matches.subcommand_matches("generate").unwrap();
        assert_eq!(
            super::parse_leader(generate),
            Leader::new(5, NZU32!(100_000), 48)
        );
        let remote = generate.subcommand_matches("remote").unwrap();
        assert!(remote.get_flag("indexer"));
        assert_eq!(
            fs::read_to_string(output.join("assets/cargo-args")).unwrap(),
            "\n"
        );
        assert_eq!(
            fs::read_to_string(output.join("assets/just-recipe")).unwrap(),
            "graviton-binaries\n"
        );
        assert_eq!(
            fs::read_to_string(output.join("explorer/npm-script")).unwrap(),
            "build\n"
        );

        // The binaries embed the freshly built frontend.
        let embedded = fs::read_to_string(output.join("assets/embedded.html")).unwrap();
        assert!(embedded.ends_with("current frontend"), "{embedded}");
        assert!(
            embedded.contains(r#"src="/runtime-config.js""#),
            "{embedded}"
        );
        fs::remove_dir_all(output).unwrap();
    }
}
