//! Inspect alto activity.
//!
//! # Status
//!
//! `alto-inspector` is **ALPHA** software and is not yet recommended for production use. Developers should expect breaking changes and occasional instability.
//!
//! # Installation
//!
//! ## Local
//!
//! ```bash
//! cargo install --path . --force
//! ```
//!
//! ## Crates.io
//!
//! ```bash
//! cargo install alto-inspector
//! ```
//!
//! # Usage
//!
//! _Use `-v` or `--verbose` to enable verbose logging (like request latency). Use `--prepare` to initialize the connection before making the request (for accurate latency measurement)._
//!
//! ## Get the latest seed
//!
//! ```bash
//! inspector get seed latest
//! ```
//!
//! ## Get the notarization for view 100
//!
//! ```bash
//! inspector get notarization 100
//! ```
//!
//! ## Get the notarizations between views 100 to 110
//!
//! ```bash
//! inspector get notarization 100..110
//! ```
//!
//! ## Get the finalization for view 50
//!
//! ```bash
//! inspector get finalization 50
//! ```
//!
//! ## Get the latest finalized block
//!
//! ```bash
//! inspector get block latest
//! ```
//!
//! ## Get the block at height 10
//!
//! ```bash
//! inspector get block 10
//! ```
//!
//! ## Get the blocks between heights 10 and 20
//!
//! ```bash
//! inspector get block 10..20
//! ```
//!
//! ## Get the block with a specific digest
//!
//! ```bash
//! inspector -- get block 0x65016ff40e824e21fffe903953c07b6d604dbcf39f681c62e7b3ed57ab1d1994
//! ```
//!
//! ## Listen for consensus events
//!
//! ```bash
//! inspector listen
//! ```

use alto_client::{
    consensus::{Message, Payload},
    Client, IndexQuery, Query,
};
use clap::{value_parser, Arg, Command};
use commonware_cryptography::bls12381::PublicKey;
use commonware_utils::from_hex_formatted;
use futures::StreamExt;
use tracing::{info, warn, Level};
use utils::{
    log_block, log_finalization, log_latency, log_notarization, log_seed, parse_index_query,
    parse_query, IndexQueryKind, QueryKind,
};

mod utils;

const DEFAULT_INDEXER: &str = "https://alto.exoware.xyz";
const DEFAULT_IDENTITY: &str = "89c52d8766fb88c83b177c382b3af6579a41d45dc3540cd7a1f40b9b38a136468d1d9f2a3d8e9b24c5626eabc2c8a65e";

#[tokio::main]
async fn main() {
    let matches = Command::new("inspector")
        .about("Inspect alto activity.")
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Enable debug logging")
                .global(true)
                .action(clap::ArgAction::SetTrue),
        )
        .subcommand(
            Command::new("listen")
                .about("Listen for consensus messages")
                .arg(
                    Arg::new("indexer")
                        .long("indexer")
                        .value_parser(value_parser!(String))
                        .default_value(DEFAULT_INDEXER)
                        .help("URL of the indexer to connect to"),
                )
                .arg(
                    Arg::new("identity")
                        .long("identity")
                        .value_parser(value_parser!(String))
                        .default_value(DEFAULT_IDENTITY)
                        .help("Hex-encoded public key of the identity"),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("Get specific consensus data")
                .arg(
                    Arg::new("type")
                        .required(true)
                        .value_parser(["seed", "notarization", "finalization", "block"])
                        .help("Type of data to retrieve"),
                )
                .arg(
                    Arg::new("query")
                        .required(true)
                        .value_parser(value_parser!(String))
                        .help("Query parameter (e.g., 'latest', number, range like '23..45', or hex digest for block)"),
                )
                .arg(
                    Arg::new("indexer")
                        .long("indexer")
                        .value_parser(value_parser!(String))
                        .default_value(DEFAULT_INDEXER)
                        .help("URL of the indexer to connect to"),
                )
                .arg(
                    Arg::new("identity")
                        .long("identity")
                        .value_parser(value_parser!(String))
                        .default_value(DEFAULT_IDENTITY)
                        .help("Hex-encoded public key of the identity"),
                )
                .arg(
                    Arg::new("prepare")
                        .long("prepare")
                        .help("Prepare the connection for some request to get a more accurate latency observation")
                        .required(false)
                        .action(clap::ArgAction::SetTrue),
                ),
        )
        .get_matches();

    let log_level = if matches.get_flag("verbose") {
        Level::DEBUG
    } else {
        Level::INFO
    };
    tracing_subscriber::fmt().with_max_level(log_level).init();

    if let Some(matches) = matches.subcommand_matches("listen") {
        let indexer = matches.get_one::<String>("indexer").unwrap();
        let identity = matches.get_one::<String>("identity").unwrap();
        let identity = from_hex_formatted(identity).expect("Failed to decode identity");
        let identity = PublicKey::try_from(identity).expect("Invalid identity");
        let client = Client::new(indexer, identity);

        let mut stream = client.listen().await.expect("Failed to connect to indexer");
        info!("listening for consensus messages...");
        while let Some(message) = stream.next().await {
            let message = message.expect("Failed to receive message");
            match message {
                Message::Seed(seed) => log_seed(seed),
                Message::Notarization(notarized) => log_notarization(notarized),
                Message::Finalization(finalized) => log_finalization(finalized),
            }
        }
    } else if let Some(matches) = matches.subcommand_matches("get") {
        let type_ = matches.get_one::<String>("type").unwrap();
        let query_str = matches.get_one::<String>("query").unwrap();
        let indexer = matches.get_one::<String>("indexer").unwrap();
        let identity = matches.get_one::<String>("identity").unwrap();
        let identity = from_hex_formatted(identity).expect("Failed to decode identity");
        let identity = PublicKey::try_from(identity).expect("Invalid identity");
        let client = Client::new(indexer, identity);
        let prepare_flag = matches.get_flag("prepare");

        if prepare_flag {
            client.health().await.expect("Failed to prepare connection");
            info!("connection prepared");
        }

        match type_.as_str() {
            "seed" => {
                let query_kind = parse_index_query(query_str).expect("Invalid query");
                match query_kind {
                    IndexQueryKind::Single(query) => {
                        let start = std::time::Instant::now();
                        let seed = client.seed_get(query).await.expect("Failed to get seed");
                        log_latency(start);
                        log_seed(seed);
                    }
                    IndexQueryKind::Range(start_view, end_view) => {
                        for view in start_view..end_view {
                            let start = std::time::Instant::now();
                            let query = IndexQuery::Index(view);
                            match client.seed_get(query).await {
                                Ok(seed) => {
                                    log_latency(start);
                                    log_seed(seed);
                                }
                                Err(e) => {
                                    warn!(view, error=?e, "failed to get seed");
                                }
                            }
                        }
                    }
                }
            }
            "notarization" => {
                let query_kind = parse_index_query(query_str).expect("Invalid query");
                match query_kind {
                    IndexQueryKind::Single(query) => {
                        let start = std::time::Instant::now();
                        let notarized = client
                            .notarization_get(query)
                            .await
                            .expect("Failed to get notarization");
                        log_latency(start);
                        log_notarization(notarized);
                    }
                    IndexQueryKind::Range(start_view, end_view) => {
                        for view in start_view..end_view {
                            let start = std::time::Instant::now();
                            let query = IndexQuery::Index(view);
                            match client.notarization_get(query).await {
                                Ok(notarized) => {
                                    log_latency(start);
                                    log_notarization(notarized);
                                }
                                Err(e) => {
                                    warn!(view, error=?e, "failed to get notarization");
                                }
                            }
                        }
                    }
                }
            }
            "finalization" => {
                let query_kind = parse_index_query(query_str).expect("Invalid query");
                match query_kind {
                    IndexQueryKind::Single(query) => {
                        let start = std::time::Instant::now();
                        let finalized = client
                            .finalization_get(query)
                            .await
                            .expect("Failed to get finalization");
                        log_latency(start);
                        log_finalization(finalized);
                    }
                    IndexQueryKind::Range(start_view, end_view) => {
                        for view in start_view..end_view {
                            let start = std::time::Instant::now();
                            let query = IndexQuery::Index(view);
                            match client.finalization_get(query).await {
                                Ok(finalized) => {
                                    log_latency(start);
                                    log_finalization(finalized);
                                }
                                Err(e) => {
                                    warn!(view, error=?e, "failed to get finalization");
                                }
                            }
                        }
                    }
                }
            }
            "block" => {
                let query_kind = parse_query(query_str).expect("Invalid query");
                match query_kind {
                    QueryKind::Single(query) => {
                        let start = std::time::Instant::now();
                        let payload = client.block_get(query).await.expect("Failed to get block");
                        log_latency(start);
                        match payload {
                            Payload::Finalized(finalized) => log_finalization(*finalized),
                            Payload::Block(block) => log_block(block),
                        }
                    }
                    QueryKind::Range(start_height, end_height) => {
                        for height in start_height..end_height {
                            let start = std::time::Instant::now();
                            let query = Query::Index(height);
                            match client.block_get(query).await {
                                Ok(payload) => {
                                    log_latency(start);
                                    match payload {
                                        Payload::Finalized(finalized) => {
                                            log_finalization(*finalized)
                                        }
                                        Payload::Block(block) => log_block(block),
                                    }
                                }
                                Err(e) => {
                                    warn!(height, error=?e, "failed to get block");
                                }
                            }
                        }
                    }
                }
            }
            _ => unreachable!(),
        }
    }
}
