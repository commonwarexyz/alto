use alto_chain::Config;
use clap::{value_parser, Arg, ArgMatches, Command};
use commonware_cryptography::{
    bls12381::{
        dkg::ops,
        primitives::{group::Element, poly},
    },
    ed25519::PublicKey,
    Ed25519, Scheme,
};
use commonware_deployer::ec2;
use commonware_utils::{from_hex_formatted, hex, quorum};
use rand::{rngs::OsRng, seq::IteratorRandom};
use std::{collections::BTreeMap, fs};
use tracing::{error, info};
use uuid::Uuid;

const BINARY_NAME: &str = "validator";
const PORT: u16 = 4545;

fn main() {
    // Initialize logger
    tracing_subscriber::fmt().init();

    // Define the main command with subcommands
    let app = Command::new("setup")
        .about("Manage configuration files for an alto chain.")
        .subcommand(
            Command::new("generate")
                .about("Generate configuration files for an alto chain")
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
                    Arg::new("storage_class")
                        .long("storage-class")
                        .required(true)
                        .value_parser(value_parser!(String)),
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
                    Arg::new("dashboard")
                        .long("dashboard")
                        .required(true)
                        .value_parser(value_parser!(String)),
                )
                .arg(
                    Arg::new("output")
                        .long("output")
                        .required(true)
                        .value_parser(value_parser!(String)),
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
        Some(("generate", sub_matches)) => generate(sub_matches),
        Some(("indexer", sub_matches)) => indexer(sub_matches),
        Some(("explorer", sub_matches)) => explorer(sub_matches),
        _ => {
            eprintln!("Invalid subcommand. Use 'generate' or 'indexer'.");
            std::process::exit(1);
        }
    }
}

fn generate(sub_matches: &ArgMatches) {
    // Extract arguments
    let peers = *sub_matches.get_one::<usize>("peers").unwrap();
    let bootstrappers = *sub_matches.get_one::<usize>("bootstrappers").unwrap();
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
    let storage_class = sub_matches
        .get_one::<String>("storage_class")
        .unwrap()
        .clone();
    let worker_threads = *sub_matches.get_one::<usize>("worker_threads").unwrap();
    let log_level = sub_matches.get_one::<String>("log_level").unwrap().clone();
    let message_backlog = *sub_matches.get_one::<usize>("message_backlog").unwrap();
    let mailbox_size = *sub_matches.get_one::<usize>("mailbox_size").unwrap();
    let dashboard = sub_matches.get_one::<String>("dashboard").unwrap().clone();
    let output = sub_matches.get_one::<String>("output").unwrap().clone();

    // Construct output path
    let raw_current_dir = std::env::current_dir().unwrap();
    let current_dir = raw_current_dir.to_str().unwrap();
    let output = format!("{}/{}", current_dir, output);

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
    let mut peer_schemes = (0..peers)
        .map(|_| Ed25519::new(&mut OsRng))
        .collect::<Vec<_>>();
    peer_schemes.sort_by_key(|scheme| scheme.public_key());
    let allowed_peers: Vec<String> = peer_schemes
        .iter()
        .map(|scheme| scheme.public_key().to_string())
        .collect();
    let bootstrappers = allowed_peers
        .iter()
        .choose_multiple(&mut OsRng, bootstrappers)
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();

    // Generate consensus key
    let peers_u32 = peers as u32;
    let threshold = quorum(peers_u32).expect("unable to derive quorum");
    let (identity, shares) = ops::generate_shares(&mut OsRng, None, peers_u32, threshold);
    info!(
        identity = hex(&poly::public(&identity).serialize()),
        "generated network key"
    );

    // Generate instance configurations
    assert!(
        regions.len() <= peers,
        "must be at least one peer per specified region"
    );
    let mut instance_configs = Vec::new();
    let mut peer_configs = Vec::new();
    for (index, scheme) in peer_schemes.iter().enumerate() {
        // Create peer config
        let name = scheme.public_key().to_string();
        let peer_config_file = format!("{}.yaml", name);
        let peer_config = Config {
            private_key: scheme.private_key().to_string(),
            share: hex(&shares[index].serialize()),
            identity: hex(&identity.serialize()),

            port: PORT,
            directory: "/home/ubuntu/data".to_string(),
            worker_threads,
            log_level: log_level.clone(),

            allowed_peers: allowed_peers.clone(),
            bootstrappers: bootstrappers.clone(),

            message_backlog,
            mailbox_size,

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
            storage_class: storage_class.clone(),
            binary: BINARY_NAME.to_string(),
            config: peer_config_file,
        };
        instance_configs.push(instance);
    }

    // Generate root config file
    let config = ec2::Config {
        tag,
        instances: instance_configs,
        monitoring: ec2::MonitoringConfig {
            instance_type: instance_type.clone(),
            storage_size,
            storage_class: storage_class.clone(),
            dashboard: "dashboard.json".to_string(),
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
        format!("{}/{}", current_dir, dashboard),
        format!("{}/dashboard.json", output),
    )
    .unwrap();
    for (peer_config_file, peer_config) in peer_configs {
        let path = format!("{}/{}", output, peer_config_file);
        let file = fs::File::create(&path).unwrap();
        serde_yaml::to_writer(file, &peer_config).unwrap();
        info!(path = peer_config_file, "wrote peer configuration file");
    }
    let path = format!("{}/config.yaml", output);
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
    let dir = format!("{}/{}", current_dir, dir);

    // Check if directory exists
    if fs::metadata(&dir).is_err() {
        error!("directory does not exist: {}", dir);
        std::process::exit(1);
    }

    // Collect and sort file paths
    let mut file_paths = Vec::new();
    for entry in fs::read_dir(&dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                if file_name.ends_with(".yaml") && file_name != "config.yaml" {
                    file_paths.push(path);
                }
            }
        }
    }
    file_paths.sort();

    // Iterate over sorted file paths and add indexer URL
    let mut applied = 0;
    for path in file_paths {
        if applied >= count {
            break;
        }
        let relative_path = path.strip_prefix(&dir).unwrap();
        match fs::read_to_string(&path) {
            Ok(content) => match serde_yaml::from_str::<Config>(&content) {
                Ok(mut config) => {
                    config.indexer = Some(url.clone());
                    match serde_yaml::to_string(&config) {
                        Ok(updated_content) => {
                            if let Err(e) = fs::write(&path, updated_content) {
                                error!(
                                    path = ?relative_path,
                                    error = ?e,
                                    "failed to write",
                                );
                            } else {
                                info!(path = ?relative_path, "updated");
                                applied += 1;
                            }
                        }
                        Err(e) => {
                            error!(
                                path = ?relative_path,
                                error = ?e,
                                "failed to serialize config",
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
                    "failed to read",
                );
            }
        }
    }
}

// Region-to-location mapping
fn get_aws_location(region: &str) -> Option<([f64; 2], String)> {
    match region {
        "us-west-1" => Some(([37.7749, -122.4194], "San Francisco".to_string())),
        "us-east-1" => Some(([38.8339, -77.3074], "Ashburn".to_string())),
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
    let config_path = format!("{}/config.yaml", dir);
    let config_content = std::fs::read_to_string(&config_path).expect("failed to read config.yaml");
    let config: ec2::Config =
        serde_yaml::from_str(&config_content).expect("failed to parse config.yaml");
    let mut participants = BTreeMap::new();
    for instance in &config.instances {
        let region = &instance.region;
        let public_key = from_hex_formatted(&instance.name).expect("invalid public key");
        let public_key = PublicKey::try_from(public_key).expect("invalid public key");
        let (coords, city) = get_aws_location(region).expect("unknown region");
        participants.insert(
            public_key,
            format!("    [[{}, {}], \"{}\"]", coords[0], coords[1], city),
        );
    }

    // Order by public key
    let threshold = quorum(participants.len() as u32).expect("invalid quorum");
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
    let identity_hex = peer_config.identity;
    let identity = from_hex_formatted(&identity_hex).expect("invalid identity");
    let identity = poly::Public::deserialize(&identity, threshold).expect("identity is invalid");
    let identity_public = poly::public(&identity);
    let config_ts = format!(
        "export const BACKEND_URL = \"{}/consensus/ws\";\n\
        export const PUBLIC_KEY_HEX = \"{}\";\n\
        export const LOCATIONS: [[number, number], string][] = [\n{}\n];",
        backend_url,
        hex(&identity_public.serialize()),
        locations_str
    );

    // Write config.ts
    let config_ts_path = format!("{}/config.ts", dir);
    std::fs::write(&config_ts_path, config_ts).expect("failed to write config.ts");
    info!(path = "config.ts", "wrote explorer configuration file");
}
