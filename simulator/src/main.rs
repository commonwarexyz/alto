use alto_simulator::{Api, Simulator};
use alto_types::Identity;
use clap::Parser;
use commonware_codec::DecodeExt;
use commonware_utils::hex;
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value_t = 8080)]
    port: u16,

    #[clap(long, help = "Identity public key in hex format (BLS12-381 public key)")]
    identity: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse args
    let args = Args::parse();

    // Create logger
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Parse identity if provided
    let identity: Identity = if let Some(id_hex) = args.identity {
        let bytes = commonware_utils::from_hex(&id_hex)
            .ok_or("Invalid identity hex format")?;
        Identity::decode(&mut bytes.as_slice())
            .map_err(|_| "Failed to decode identity")?
    } else {
        // Generate a default identity for testing
        use commonware_cryptography::bls12381::primitives::{ops, variant::MinSig};
        use rand::{rngs::StdRng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(0);
        let (_, public) = ops::keypair::<_, MinSig>(&mut rng);
        public
    };

    info!("Using identity: {}", hex(&commonware_codec::Encode::encode(&identity).to_vec()));

    // Initialize simulator
    let simulator = Arc::new(Simulator::new(identity));
    let api = Api::new(simulator);
    let app = api.router();

    // Start server
    let addr = format!("0.0.0.0:{}", args.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("Listening on {}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}