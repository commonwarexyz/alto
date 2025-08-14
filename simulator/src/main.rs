use alto_simulator::{Api, Simulator};
use alto_types::Identity;
use clap::Parser;
use commonware_codec::DecodeExt;
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value_t = 8080)]
    port: u16,

    #[clap(
        long,
        help = "Identity public key in hex format (BLS12-381 public key)"
    )]
    identity: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse args
    let args = Args::parse();

    // Create logger
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Parse identity
    let bytes = commonware_utils::from_hex(&args.identity).ok_or("Invalid identity hex format")?;
    let identity: Identity =
        Identity::decode(&mut bytes.as_slice()).map_err(|_| "Failed to decode identity")?;

    // Initialize simulator
    let simulator = Arc::new(Simulator::new(identity));
    let api = Api::new(simulator);
    let app = api.router();

    // Start server
    let addr = format!("0.0.0.0:{}", args.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(?identity, ?addr, "started simulator");
    axum::serve(listener, app).await?;

    Ok(())
}
