use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr};

pub mod application;
pub mod indexer;
pub mod utils;

/// Configuration for an Alto validator node.
#[derive(Deserialize, Serialize)]
pub struct Config {
    pub private_key: String,
    pub share: String,
    pub polynomial: String,

    pub port: u16,
    pub metrics_port: u16,
    pub directory: String,
    pub worker_threads: usize,
    pub log_level: String,

    pub local: bool,
    pub allowed_peers: Vec<String>,
    pub bootstrappers: Vec<String>,

    pub message_backlog: usize,
    pub mailbox_size: usize,
    pub deque_size: usize,

    pub signature_threads: usize,

    pub indexer: Option<String>,
}

/// A list of peers provided when a validator is run locally.
///
/// When run remotely, [`commonware_deployer::aws::Hosts`](https://docs.rs/commonware-deployer/latest/commonware_deployer/aws/struct.Hosts.html) is used instead.
#[derive(Deserialize, Serialize)]
pub struct Peers {
    pub addresses: HashMap<String, SocketAddr>,
}
