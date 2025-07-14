use alto_chain::{Config, Peers};
use clap::{value_parser, Arg, ArgMatches, Command};
use commonware_codec::{Decode, DecodeExt, Encode};
use commonware_cryptography::{
    bls12381::{
        dkg::ops,
        primitives::{poly, variant::MinSig},
    },
    ed25519::{PrivateKey, PublicKey},
    PrivateKeyExt, Signer,
};
use commonware_deployer::ec2::{self, METRICS_PORT};
use commonware_utils::{from_hex_formatted, hex, quorum};
use rand::{rngs::OsRng, seq::IteratorRandom};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::AddAssign,
};
use tracing::{error, info};
use uuid::Uuid;

const BINARY_NAME: &str = "validator";
const PORT: u16 = 4545;
const STORAGE_CLASS: &str = "gp3";
const DASHBOARD_FILE: &str = "dashboard.json";

fn main() {
    // Initialize logger
    tracing_subscriber::fmt().init();

    // Define the main command with subcommands
    let app = Command::new("setup")
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
                    Arg::new("log_level")
                        .long("log-level")
                        .required(true)
                        .value_parser(value_parser!(String)),
                )
                .arg(
                    Arg::new("message_backlog")
                        .long("message-backlog")
                        .required(true)
                        .value_parser(value_parser!(usize)),
                )
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
                        ),
                ),
        )
        .subcommand(
            Command::new("indexer")
                .about("Add indexer support for an alto chain.")
                .arg(
                    Arg::new("count")
                        .long("count")
                        .required(true)
                        .value_parser(value_parser!(usize)),
                )
                .arg(
                    Arg::new("dir")
                        .long("dir")
                        .required(true)
                        .value_parser(value_parser!(String)),
                )
                .arg(
                    Arg::new("url")
                        .long("url")
                        .required(true)
                        .value_parser(value_parser!(String)),
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
                        .value_parser(value_parser!(String)),
                ),
        );

    // Parse arguments
    let matches = app.get_matches();

    // Handle subcommands
    match matches.subcommand() {
        Some(("generate", sub_matches)) => {
            let peers = *sub_matches.get_one::<usize>("peers").unwrap();
            let bootstrappers = *sub_matches.get_one::<usize>("bootstrappers").unwrap();
            let worker_threads = *sub_matches.get_one::<usize>("worker_threads").unwrap();
            let log_level = sub_matches.get_one::<String>("log_level").unwrap().clone();
            let message_backlog = *sub_matches.get_one::<usize>("message_backlog").unwrap();
            let mailbox_size = *sub_matches.get_one::<usize>("mailbox_size").unwrap();
            let deque_size = *sub_matches.get_one::<usize>("deque_size").unwrap();
            let output = sub_matches.get_one::<String>("output").unwrap().clone();
            match sub_matches.subcommand() {
                Some(("local", sub_matches)) => generate_local(
                    sub_matches,
                    peers,
                    bootstrappers,
                    worker_threads,
                    log_level,
                    message_backlog,
                    mailbox_size,
                    deque_size,
                    output,
                ),
                Some(("remote", sub_matches)) => generate_remote(
                    sub_matches,
                    peers,
                    bootstrappers,
                    worker_threads,
                    log_level,
                    message_backlog,
                    mailbox_size,
                    deque_size,
                    output,
                ),
                _ => {
                    eprintln!("Invalid subcommand. Use 'local' or 'remote'.");
                    std::process::exit(1);
                }
            }
        }
        Some(("indexer", sub_matches)) => indexer(sub_matches),
        Some(("explorer", sub_matches)) => explorer(sub_matches),
        _ => {
            eprintln!("Invalid subcommand. Use 'generate' or 'indexer'.");
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
    log_level: String,
    message_backlog: usize,
    mailbox_size: usize,
    deque_size: usize,
    output: String,
) {
    // Extract arguments
    let start_port = *sub_matches.get_one::<u16>("start_port").unwrap();

    // Construct output path
    let raw_current_dir = std::env::current_dir().unwrap();
    let current_dir = raw_current_dir.to_str().unwrap();
    let output = format!("{current_dir}/{output}");
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
        .map(|_| PrivateKey::from_rng(&mut OsRng))
        .collect::<Vec<_>>();
    peer_signers.sort_by_key(|signer| signer.public_key());
    let allowed_peers: Vec<String> = peer_signers
        .iter()
        .map(|signer| signer.public_key().to_string())
        .collect();
    let bootstrappers = allowed_peers
        .iter()
        .choose_multiple(&mut OsRng, bootstrappers)
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();

    // Generate consensus key
    let peers_u32 = peers as u32;
    let threshold = quorum(peers_u32);
    let (polynomial, shares) =
        ops::generate_shares::<_, MinSig>(&mut OsRng, None, peers_u32, threshold);
    info!(identity = ?poly::public::<MinSig>(&polynomial), "generated network key");

    // Generate instance configurations
    let mut port = start_port;
    let mut addresses = HashMap::new();
    let mut configurations = Vec::new();
    for (signer, share) in peer_signers.iter().zip(shares.iter()) {
        // Create peer config
        let name = signer.public_key().to_string();
        addresses.insert(
            name.clone(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        );
        let peer_config_file = format!("{name}.yaml");
        let directory = format!("{storage_output}/{name}");
        let peer_config = Config {
            private_key: signer.to_string(),
            share: hex(&share.encode()),
            polynomial: hex(&polynomial.encode()),

            port,
            metrics_port: port + 1,
            directory,
            worker_threads,
            log_level: log_level.clone(),

            allowed_peers: allowed_peers.clone(),
            bootstrappers: bootstrappers.clone(),

            message_backlog,
            mailbox_size,
            deque_size,

            indexer: None,
        };
        configurations.push((name, peer_config_file.clone(), peer_config));
        port += 2;
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
    println!("To start validators, run:");
    for (name, peer_config_file, _) in &configurations {
        let path = format!("{output}/{peer_config_file}");
        let command =
            format!("cargo run --bin {BINARY_NAME} -- --peers={peers_path} --config={path}");
        println!("{name}: {command}");
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
    log_level: String,
    message_backlog: usize,
    mailbox_size: usize,
    deque_size: usize,
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

    // Construct output path
    let raw_current_dir = std::env::current_dir().unwrap();
    let current_dir = raw_current_dir.to_str().unwrap();
    let output = format!("{current_dir}/{output}");

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
        .map(|_| PrivateKey::from_rng(&mut OsRng))
        .collect::<Vec<_>>();
    peer_signers.sort_by_key(|signer| signer.public_key());
    let allowed_peers: Vec<String> = peer_signers
        .iter()
        .map(|signer| signer.public_key().to_string())
        .collect();
    let bootstrappers = allowed_peers
        .iter()
        .choose_multiple(&mut OsRng, bootstrappers)
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();

    // Generate consensus key
    let peers_u32 = peers as u32;
    let threshold = quorum(peers_u32);
    let (polynomial, shares) =
        ops::generate_shares::<_, MinSig>(&mut OsRng, None, peers_u32, threshold);
    info!(identity = ?poly::public::<MinSig>(&polynomial), "generated network key");

    // Generate instance configurations
    assert!(
        regions.len() <= peers,
        "must be at least one peer per specified region"
    );
    let mut instance_configs = Vec::new();
    let mut peer_configs = Vec::new();
    for (index, signer) in peer_signers.iter().enumerate() {
        // Create peer config
        let name = signer.public_key().to_string();
        let peer_config_file = format!("{name}.yaml");
        let peer_config = Config {
            private_key: signer.to_string(),
            share: hex(&shares[index].encode()),
            polynomial: hex(&polynomial.encode()),

            port: PORT,
            metrics_port: METRICS_PORT,
            directory: "/home/ubuntu/data".to_string(),
            worker_threads,
            log_level: log_level.clone(),

            allowed_peers: allowed_peers.clone(),
            bootstrappers: bootstrappers.clone(),

            message_backlog,
            mailbox_size,
            deque_size,

            indexer: None,
        };
        peer_configs.push((peer_config_file.clone(), peer_config));

        // Create instance config
        let region_index = index % regions.len();
        let region = regions[region_index].clone();
        let instance = ec2::InstanceConfig {
            name: name.clone(),
            region,
            instance_type: instance_type.clone(),
            storage_size,
            storage_class: STORAGE_CLASS.to_string(),
            binary: BINARY_NAME.to_string(),
            config: peer_config_file,
            profiling: false,
        };
        instance_configs.push(instance);
    }

    // Generate root config file
    let config = ec2::Config {
        tag,
        instances: instance_configs,
        monitoring: ec2::MonitoringConfig {
            instance_type: monitoring_instance_type,
            storage_size: monitoring_storage_size,
            storage_class: STORAGE_CLASS.to_string(),
            dashboard: DASHBOARD_FILE.to_string(),
        },
        ports: vec![ec2::PortConfig {
            protocol: "tcp".to_string(),
            port: PORT,
            cidr: "0.0.0.0/0".to_string(),
        }],
    };

    // Write configuration files
    fs::create_dir_all(&output).unwrap();
    fs::copy(
        format!("{current_dir}/{dashboard}"),
        format!("{output}/{DASHBOARD_FILE}"),
    )
    .unwrap();
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

fn indexer(sub_matches: &ArgMatches) {
    // Extract arguments
    let count = *sub_matches.get_one::<usize>("count").unwrap();
    assert!(count > 0, "count must be greater than zero");
    let dir = sub_matches.get_one::<String>("dir").unwrap().clone();
    let url = sub_matches.get_one::<String>("url").unwrap().clone();

    // Construct directory path
    let raw_current_dir = std::env::current_dir().unwrap();
    let current_dir = raw_current_dir.to_str().unwrap();
    let dir = format!("{current_dir}/{dir}");

    // Check if directory exists
    if fs::metadata(&dir).is_err() {
        error!("directory does not exist: {}", dir);
        std::process::exit(1);
    }

    // Read config.yaml to get peer-to-region mappings
    let config_path = format!("{dir}/config.yaml");
    let config_content = fs::read_to_string(&config_path).expect("failed to read config.yaml");
    let config: ec2::Config =
        serde_yaml::from_str(&config_content).expect("failed to parse config.yaml");
    assert!(
        count <= config.instances.len(),
        "count exceeds number of peers"
    );

    // Group peers by region
    let mut region_to_peers: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for instance in &config.instances {
        let peer_name = instance.name.clone();
        let region = instance.region.clone();
        region_to_peers.entry(region).or_default().push(peer_name);
    }

    // Sort peers within each region for deterministic selection
    for peers in region_to_peers.values_mut() {
        peers.sort();
    }

    // Get sorted list of regions for consistent iteration
    let regions: Vec<String> = region_to_peers.keys().cloned().collect();

    // Select peers for indexers in a round-robin fashion across regions
    let mut selected = Vec::new();
    let mut region_index = 0;
    let mut assigned_regions = BTreeMap::new();
    while selected.len() < count && !region_to_peers.is_empty() {
        let region = &regions[region_index % regions.len()];
        if let Some(peers) = region_to_peers.get_mut(region) {
            if !peers.is_empty() {
                let peer = peers.remove(0); // Take the first available peer
                selected.push(peer);
                if peers.is_empty() {
                    region_to_peers.remove(region); // Remove region if no peers remain
                }
                assigned_regions.entry(region).or_insert(0).add_assign(1);
            }
        }
        region_index += 1;
    }

    // Update configuration files for selected peers
    for peer_name in &selected {
        let config_file = format!("{dir}/{peer_name}.yaml");
        let relative_path = format!("{peer_name}.yaml");
        match fs::read_to_string(&config_file) {
            Ok(content) => match serde_yaml::from_str::<Config>(&content) {
                Ok(mut config) => {
                    config.indexer = Some(url.clone());
                    match serde_yaml::to_string(&config) {
                        Ok(updated_content) => {
                            if let Err(e) = fs::write(&config_file, updated_content) {
                                error!(
                                    path = ?relative_path,
                                    error = ?e,
                                    "failed to write"
                                );
                            } else {
                                info!(path = ?relative_path, "updated");
                            }
                        }
                        Err(e) => {
                            error!(
                                path = ?relative_path,
                                error = ?e,
                                "failed to serialize config"
                            );
                        }
                    }
                }
                Err(e) => {
                    error!(
                        path = ?relative_path,
                        error = ?e,
                        "failed to parse"
                    );
                }
            },
            Err(e) => {
                error!(
                    path = ?relative_path,
                    error = ?e,
                    "failed to read"
                );
            }
        }
    }

    // Log assignment of indexers to regions
    info!(assignments = ?assigned_regions, "configured indexers");
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

// Explorer subcommand implementation
fn explorer(sub_matches: &ArgMatches) {
    // Parse arguments
    let dir = sub_matches.get_one::<String>("dir").unwrap().clone();
    let backend_url = sub_matches
        .get_one::<String>("backend-url")
        .unwrap()
        .clone();

    // Collect all locations
    let config_path = format!("{dir}/config.yaml");
    let config_content = std::fs::read_to_string(&config_path).expect("failed to read config.yaml");
    let config: ec2::Config =
        serde_yaml::from_str(&config_content).expect("failed to parse config.yaml");
    let mut participants = BTreeMap::new();
    for instance in &config.instances {
        let region = &instance.region;
        let public_key = from_hex_formatted(&instance.name).expect("invalid public key");
        let public_key = PublicKey::decode(public_key.as_ref()).expect("invalid public key");
        let (coords, city) = get_aws_location(region).expect("unknown region");
        participants.insert(
            public_key,
            format!("    [[{}, {}], \"{}\"]", coords[0], coords[1], city),
        );
    }

    // Order by public key
    let threshold = quorum(participants.len() as u32);
    let mut locations = Vec::new();
    for (_, location) in participants {
        locations.push(location);
    }

    // Generate config.ts
    let locations_str = locations.join(",\n");
    let first_instance = &config.instances[0];
    let peer_config_path = format!("{}/{}", dir, first_instance.config);
    let peer_config_content =
        std::fs::read_to_string(&peer_config_path).expect("failed to read peer config");
    let peer_config: Config =
        serde_yaml::from_str(&peer_config_content).expect("failed to parse peer config");
    let polynomial_hex = peer_config.polynomial;
    let polynomial = from_hex_formatted(&polynomial_hex).expect("invalid polynomial");
    let polynomial = poly::Public::<MinSig>::decode_cfg(polynomial.as_ref(), &(threshold as usize))
        .expect("polynomial is invalid");
    let identity = poly::public::<MinSig>(&polynomial);
    let config_ts = format!(
        "export const BACKEND_URL = \"{}\";\n\
        export const PUBLIC_KEY_HEX = \"{}\";\n\
        export const LOCATIONS: [[number, number], string][] = [\n{}\n];",
        backend_url,
        hex(&identity.encode()),
        locations_str
    );

    // Write config.ts
    let config_ts_path = format!("{dir}/config.ts");
    std::fs::write(&config_ts_path, config_ts).expect("failed to write config.ts");
    info!(path = "config.ts", "wrote explorer configuration file");
}
