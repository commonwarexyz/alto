use alto_client::{
    consensus::{Message, Payload},
    Client, IndexQuery, Query,
};
use alto_types::{Finalized, Notarized, Seed};
use clap::{value_parser, Arg, Command};
use commonware_cryptography::{bls12381::PublicKey, sha256::Digest};
use commonware_utils::{from_hex_formatted, SizedSerialize, SystemTimeExt};
use futures::StreamExt;
use std::time;
use tracing::info;

// Parse IndexQuery for seed, notarization, and finalization
fn parse_index_query(query: &str) -> Option<IndexQuery> {
    if query == "latest" {
        Some(IndexQuery::Latest)
    } else if let Ok(index) = query.parse::<u64>() {
        Some(IndexQuery::Index(index))
    } else {
        None
    }
}

// Parse Query for block
fn parse_query(query: &str) -> Option<Query> {
    if query == "latest" {
        Some(Query::Latest)
    } else if let Ok(index) = query.parse::<u64>() {
        Some(Query::Index(index))
    } else {
        let bytes = commonware_utils::from_hex(query).expect("Failed to decode hex");
        let digest: [u8; Digest::SERIALIZED_LEN] = bytes.try_into().expect("Invalid digest length");
        Some(Query::Digest(digest.into()))
    }
}

fn log_seed(seed: Seed) {
    info!(view = seed.view, signature = ?seed.signature, "seed");
}

fn log_notarization(notarized: Notarized) {
    let now = time::SystemTime::now().epoch_millis();
    info!(
        view = notarized.proof.view,
        height = notarized.block.height,
        timestamp = notarized.block.timestamp,
        age = now - notarized.block.timestamp,
        digest = ?notarized.block.digest(),
        "notarized"
    );
}

fn log_finalization(finalized: Finalized) {
    let now = time::SystemTime::now().epoch_millis();
    info!(
        view = finalized.proof.view,
        height = finalized.block.height,
        timestamp = finalized.block.timestamp,
        age = now - finalized.block.timestamp,
        digest = ?finalized.block.digest(),
        "finalized"
    );
}

fn log_block(block: alto_types::Block) {
    let now = time::SystemTime::now().epoch_millis();
    info!(
        height = block.height,
        timestamp = block.timestamp,
        age = now - block.timestamp,
        digest = ?block.digest(),
        "block"
    );
}

#[tokio::main]
async fn main() {
    // Initialize logger
    tracing_subscriber::fmt().init();

    // Define CLI structure with subcommands
    let matches = Command::new("inspector")
        .about("Monitor alto activity.")
        .subcommand(
            Command::new("listen")
                .about("Listen for consensus messages")
                .arg(
                    Arg::new("indexer")
                        .long("indexer")
                        .required(true)
                        .value_parser(value_parser!(String))
                        .help("URL of the indexer to connect to"),
                )
                .arg(
                    Arg::new("identity")
                        .long("identity")
                        .required(true)
                        .value_parser(value_parser!(String))
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
                        .help("Query parameter (e.g., 'latest', number, or hex digest for block)"),
                )
                .arg(
                    Arg::new("indexer")
                        .long("indexer")
                        .required(true)
                        .value_parser(value_parser!(String))
                        .help("URL of the indexer to connect to"),
                )
                .arg(
                    Arg::new("identity")
                        .long("identity")
                        .required(true)
                        .value_parser(value_parser!(String))
                        .help("Hex-encoded public key of the identity"),
                ),
        )
        .get_matches();

    // Handle 'listen' subcommand
    if let Some(matches) = matches.subcommand_matches("listen") {
        let indexer = matches.get_one::<String>("indexer").unwrap();
        let identity = matches.get_one::<String>("identity").unwrap();
        let identity = from_hex_formatted(identity).expect("Failed to decode identity");
        let identity = PublicKey::try_from(identity).expect("Invalid identity");
        let client = Client::new(indexer, identity);

        // Stream consensus messages
        let mut stream = client.listen().await.expect("Failed to connect to indexer");
        info!("listening for consensus messages...");
        while let Some(message) = stream.next().await {
            let message = message.expect("Failed to receive message");
            match message {
                Message::Seed(seed) => {
                    log_seed(seed);
                }
                Message::Notarization(notarized) => {
                    log_notarization(notarized);
                }
                Message::Finalization(finalized) => {
                    log_finalization(finalized);
                }
            }
        }
    }
    // Handle 'get' subcommand
    else if let Some(matches) = matches.subcommand_matches("get") {
        let type_ = matches.get_one::<String>("type").unwrap();
        let query_str = matches.get_one::<String>("query").unwrap();
        let indexer = matches.get_one::<String>("indexer").unwrap();
        let identity = matches.get_one::<String>("identity").unwrap();
        let identity = from_hex_formatted(identity).expect("Failed to decode identity");
        let identity = PublicKey::try_from(identity).expect("Invalid identity");
        let client = Client::new(indexer, identity);

        match type_.as_str() {
            "seed" => {
                let query = parse_index_query(query_str).expect("Invalid query");
                let seed = client.seed_get(query).await.expect("Failed to get seed");
                log_seed(seed);
            }
            "notarization" => {
                let query = parse_index_query(query_str).expect("Invalid query");
                let notarized = client
                    .notarization_get(query)
                    .await
                    .expect("Failed to get notarization");
                log_notarization(notarized);
            }
            "finalization" => {
                let query = parse_index_query(query_str).expect("Invalid query");
                let finalized = client
                    .finalization_get(query)
                    .await
                    .expect("Failed to get finalization");
                log_finalization(finalized);
            }
            "block" => {
                let query = parse_query(query_str).expect("Invalid query");
                let payload = client.block_get(query).await.expect("Failed to get block");
                match payload {
                    Payload::Finalized(finalized) => log_finalization(*finalized),
                    Payload::Block(block) => log_block(block),
                }
            }
            _ => unreachable!(),
        }
    }
}
