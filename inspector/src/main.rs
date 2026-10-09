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
//! Every command takes `--indexer <URL>` and `--identity <HEX>`, the hex-encoded participant set
//! of the network (the ordered ellipsoidal Falcon-512 public keys of its validators). Certificates are verified
//! against that set; the examples below omit both flags.
//!
//! Use `--block-size` to set the network's block payload size for streaming. The receive limit adds
//! 1 MiB for encoding and one message-kind byte. The default payload allowance is 4 MiB.
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
//! inspector get block 0x65016ff40e824e21fffe903953c07b6d604dbcf39f681c62e7b3ed57ab1d1994
//! ```
//!
//! ## Listen for consensus events
//!
//! ```bash
//! inspector listen
//! ```

use alto_client::{
    consensus::{Message, Payload},
    Client, ClientBuilder, IndexQuery, Query,
};
use alto_types::{decode_identity, ConsensusScheme, Scheme, NAMESPACE};
use clap::{value_parser, Arg, ArgMatches, Command};
use commonware_formatting::from_hex;
use commonware_parallel::Sequential;
use futures::StreamExt;
use tracing::{info, warn, Level};
use utils::{
    log_block, log_finalization, log_latency, log_notarization, parse_index_query, parse_query,
    IndexQueryKind, QueryKind,
};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod utils;

/// The indexer URL argument.
fn indexer_arg() -> Arg {
    Arg::new("indexer")
        .long("indexer")
        .value_parser(value_parser!(String))
        .required(true)
        .help("URL of the indexer to connect to")
}

/// The network identity argument.
fn identity_arg() -> Arg {
    Arg::new("identity")
        .long("identity")
        .value_parser(value_parser!(String))
        .required(true)
        .help("Hex-encoded participant set of the network")
}

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
        .arg(
            Arg::new("block_size")
                .long("block-size")
                .value_parser(value_parser!(u32))
                .global(true)
                .help("Network block payload size in bytes for streaming (default allowance: 4 MiB)"),
        )
        .subcommand(
            Command::new("listen")
                .about("Listen for consensus messages")
                .arg(indexer_arg())
                .arg(identity_arg()),
        )
        .subcommand(
            Command::new("get")
                .about("Get specific consensus data")
                .arg(
                    Arg::new("type")
                        .required(true)
                        .value_parser(["notarization", "finalization", "block"])
                        .help("Type of data to retrieve"),
                )
                .arg(
                    Arg::new("query")
                        .required(true)
                        .value_parser(value_parser!(String))
                        .help("Query parameter (e.g., 'latest', number, range like '23..45', or hex digest for block)"),
                )
                .arg(indexer_arg())
                .arg(identity_arg())
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
    run(&matches).await;
}

fn client(matches: &ArgMatches) -> Client<Sequential, ConsensusScheme> {
    let indexer = matches.get_one::<String>("indexer").unwrap();
    let identity = matches.get_one::<String>("identity").unwrap();
    let identity = from_hex(identity).expect("Failed to decode identity");
    let identity = decode_identity(identity).expect("Invalid identity");
    let mut builder = ClientBuilder::new(
        indexer,
        ConsensusScheme::certificate_verifier(NAMESPACE, identity),
        Sequential,
    );
    if let Some(block_size) = matches.get_one::<u32>("block_size") {
        builder = builder.with_block_size(*block_size);
    }
    builder.build()
}

async fn run(matches: &ArgMatches) {
    if let Some(matches) = matches.subcommand_matches("listen") {
        let client = client(matches);

        let mut stream = client.listen().await.expect("Failed to connect to indexer");
        info!("listening for consensus messages...");
        while let Some(message) = stream.next().await {
            let message = message.expect("Failed to receive message");
            match message {
                Message::Notarization(notarized) => log_notarization(notarized),
                Message::Finalization(finalized) => log_finalization(finalized),
            }
        }
    } else if let Some(matches) = matches.subcommand_matches("get") {
        let type_ = matches.get_one::<String>("type").unwrap();
        let query_str = matches.get_one::<String>("query").unwrap();
        let client = client(matches);
        let prepare_flag = matches.get_flag("prepare");

        if prepare_flag {
            client.health().await.expect("Failed to prepare connection");
            info!("connection prepared");
        }

        match type_.as_str() {
            "notarization" => {
                let query_kind = parse_index_query(query_str).expect("Invalid query");
                match query_kind {
                    IndexQueryKind::Single(query) => {
                        let start = std::time::Instant::now();
                        let notarized = client
                            .notarized_get(query)
                            .await
                            .expect("Failed to get notarization");
                        log_latency(start);
                        log_notarization(notarized);
                    }
                    IndexQueryKind::Range(start_view, end_view) => {
                        for view in start_view..end_view {
                            let start = std::time::Instant::now();
                            let query = IndexQuery::Index(view);
                            match client.notarized_get(query).await {
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
                            .finalized_get(query)
                            .await
                            .expect("Failed to get finalization");
                        log_latency(start);
                        log_finalization(finalized);
                    }
                    IndexQueryKind::Range(start_view, end_view) => {
                        for view in start_view..end_view {
                            let start = std::time::Instant::now();
                            let query = IndexQuery::Index(view);
                            match client.finalized_get(query).await {
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
                            Payload::Block(block) => log_block(*block),
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
                                        Payload::Block(block) => log_block(*block),
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
