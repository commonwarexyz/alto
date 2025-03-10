use alto_client::{consensus::Message, Client};
use clap::{value_parser, Arg, Command};
use commonware_cryptography::bls12381::PublicKey;
use commonware_utils::{from_hex_formatted, SystemTimeExt};
use futures::StreamExt;
use std::time;
use tracing::info;

#[tokio::main]
async fn main() {
    // Parse arguments
    let matches = Command::new("inspector")
        .about("Monitor alto activity.")
        .arg(
            Arg::new("indexer")
                .long("indexer")
                .required(true)
                .value_parser(value_parser!(String)),
        )
        .arg(
            Arg::new("identity")
                .long("identity")
                .required(true)
                .value_parser(value_parser!(String)),
        )
        .get_matches();

    // Create logger
    tracing_subscriber::fmt().init();

    // Parse the identity
    let identity = matches.get_one::<String>("identity").unwrap();
    let identity = from_hex_formatted(identity).unwrap();
    let identity = PublicKey::try_from(identity).expect("Invalid identity");

    // Connect to the indexer
    let indexer = matches.get_one::<String>("indexer").unwrap();
    let client = Client::new(indexer, identity);

    // Stream the chain
    let mut stream = client
        .register()
        .await
        .expect("Failed to connect to indexer");
    info!("connected to indexer");
    while let Some(message) = stream.next().await {
        let message = message.expect("Failed to receive message");
        match message {
            Message::Seed(seed) => {
                info!(view = seed.view, signature = ?seed.signature, "seed");
            }
            Message::Notarization(notarized) => {
                let now = time::SystemTime::now().epoch_millis();
                info!(
                    view = notarized.proof.view,
                    height = notarized.block.height,
                    timestamp = notarized.block.timestamp,
                    age = now - notarized.block.timestamp,
                    digest = ?notarized.block.digest(),
                    "notarized");
            }
            Message::Finalization(finalized) => {
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
        }
    }
}
