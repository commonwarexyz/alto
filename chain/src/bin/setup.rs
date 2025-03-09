use alto_chain::Config;
use clap::{value_parser, Arg, Command};
use commonware_cryptography::{
    bls12381::{
        dkg::ops,
        primitives::{group::Element, poly},
    },
    Ed25519, Scheme,
};
use commonware_deployer::ec2;
use commonware_utils::{hex, quorum};
use rand::{rngs::OsRng, seq::IteratorRandom};
use tracing::info;
use uuid::Uuid;

const BINARY_NAME: &str = "validator";
const PORT: u16 = 4545;

fn main() {
    // Parse arguments
    let matches = Command::new("setup")
        .about("Generate configuration files for an alto chain.")
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
            Arg::new("worker-threads")
                .long("worker-threads")
                .required(true)
                .value_parser(value_parser!(usize)),
        )
        .arg(
            Arg::new("message-backlog")
                .long("message-backlog")
                .required(true)
                .value_parser(value_parser!(usize)),
        )
        .arg(
            Arg::new("mailbox-size")
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
        )
        .get_matches();

    // Create logger
    tracing_subscriber::fmt().init();

    // Generate UUID
    let tag = Uuid::new_v4().to_string();
    info!(tag, "generated deployment tag");

    // Generate peers
    let peers = *matches.get_one::<usize>("peers").unwrap();
    let bootstrappers = *matches.get_one::<usize>("bootstrappers").unwrap();
    assert!(
        bootstrappers <= peers,
        "bootstrappers must be less than peers"
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
        "generated consensus key"
    );

    // Generate instance configurations
    let regions = matches
        .get_many::<String>("regions")
        .unwrap()
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        regions.len() <= peers,
        "must be at least one peer per specified region"
    );
    let instance_type = matches.get_one::<String>("instance_type").unwrap();
    let storage_size = *matches.get_one::<i32>("storage_size").unwrap();
    let storage_class = matches.get_one::<String>("storage_class").unwrap();
    let worker_threads = *matches.get_one::<usize>("worker-threads").unwrap();
    let message_backlog = *matches.get_one::<usize>("message-backlog").unwrap();
    let mailbox_size = *matches.get_one::<usize>("mailbox-size").unwrap();
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

            allowed_peers: allowed_peers.clone(),
            bootstrappers: bootstrappers.clone(),

            message_backlog,
            mailbox_size,
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
    let raw_current_dir = std::env::current_dir().unwrap();
    let current_dir = raw_current_dir.to_str().unwrap();
    let output = matches.get_one::<String>("output").unwrap();
    let output = format!("{}/{}", current_dir, output);
    assert!(
        !std::path::Path::new(&output).exists(),
        "output directory already exists"
    );
    std::fs::create_dir_all(output.clone()).unwrap();
    let dashboard = matches.get_one::<String>("dashboard").unwrap().clone();
    std::fs::copy(
        format!("{}/{}", current_dir, dashboard),
        format!("{}/dashboard.json", output),
    )
    .unwrap();
    for (peer_config_file, peer_config) in peer_configs {
        let path = format!("{}/{}", output, peer_config_file);
        let file = std::fs::File::create(path).unwrap();
        serde_yaml::to_writer(file, &peer_config).unwrap();
    }
    let path = format!("{}/config.yaml", output);
    let file = std::fs::File::create(path.clone()).unwrap();
    serde_yaml::to_writer(file, &config).unwrap();
    info!(path, "wrote configuration files");
}
