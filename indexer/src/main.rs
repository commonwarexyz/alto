use alto_indexer::{Api, Indexer};
use alto_types::{decode_identity, ConsensusScheme, Scheme, NAMESPACE};
use axum::{
    body::{Body, Bytes},
    extract::Extension,
    http::{header, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
};
use clap::Parser;
use commonware_formatting::from_hex;
use commonware_parallel::Sequential;
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc};
use tracing::info;

include!(concat!(env!("OUT_DIR"), "/explorer_assets.rs"));

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value_t = 8080)]
    port: u16,

    #[clap(
        long,
        required_unless_present = "config",
        conflicts_with = "config",
        help = "Network identity in hex format (the encoded participant set)"
    )]
    identity: Option<String>,

    /// Payload size of the network's blocks in bytes. Uploads carrying a larger payload are
    /// rejected. The request body limit is this size plus 1 MiB, or 5 MiB when omitted.
    #[clap(long, conflicts_with = "config")]
    block_size: Option<u32>,

    /// Accepted because the deployer starts every binary with `--hosts`. The indexer does not use it.
    #[clap(long, conflicts_with = "identity")]
    hosts: Option<PathBuf>,

    /// Path to the deployer-provided indexer config YAML.
    #[clap(long)]
    config: Option<PathBuf>,
}

#[derive(Deserialize)]
struct DeployerConfig {
    port: u16,
    identity: String,
    block_size: u32,
    explorer: ExplorerConfig,
}

#[derive(Deserialize)]
struct ExplorerConfig {
    name: String,
    description: String,
    participants: Vec<String>,
    /// One location per participant in public-key order, with None for unmapped regions.
    locations: Vec<Option<([f64; 2], String)>>,
}

struct Settings {
    port: u16,
    identity: String,
    block_size: Option<u32>,
    explorer: ExplorerConfig,
}

fn load_settings(args: Args) -> Result<Settings, Box<dyn std::error::Error>> {
    if let Some(config) = args.config {
        let config = std::fs::read_to_string(config)?;
        let config: DeployerConfig = serde_yaml::from_str(&config)?;
        return Ok(Settings {
            port: config.port,
            identity: config.identity,
            block_size: Some(config.block_size),
            explorer: config.explorer,
        });
    }

    Ok(Settings {
        port: args.port,
        identity: args
            .identity
            .expect("clap requires --identity when --config is absent"),
        block_size: args.block_size,
        explorer: ExplorerConfig {
            name: "Local Indexer".to_string(),
            description: "An Alto indexer running on this machine.".to_string(),
            participants: Vec::new(),
            locations: Vec::new(),
        },
    })
}

fn explorer_script(settings: &Settings) -> String {
    let config = serde_json::json!({
        "PUBLIC_KEY_HEX": settings.identity,
        "LOCATIONS": settings.explorer.locations,
        "PARTICIPANTS": settings.explorer.participants,
        "name": settings.explorer.name,
        "description": settings.explorer.description,
    });
    format!(
        "window.ALTO_DEPLOYMENT = {config};\nwindow.ALTO_DEPLOYMENT.BACKEND_URL = window.location.host;\n"
    )
}

fn response(body: Body, content_type: &'static str) -> Response {
    let mut response = Response::new(body);
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate"),
    );
    response
}

async fn runtime_explorer_config(Extension(script): Extension<String>) -> Response {
    response(Body::from(script), "application/javascript; charset=utf-8")
}

async fn explorer_asset(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    let asset = EXPLORER_ASSETS
        .iter()
        .find(|(asset_path, _, _)| *asset_path == path);
    let Some((_, contents, content_type)) = asset else {
        return StatusCode::NOT_FOUND.into_response();
    };

    response(Body::from(Bytes::from_static(contents)), content_type)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse args
    let settings = load_settings(Args::parse())?;
    let script = explorer_script(&settings);

    // Create logger
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    serve(settings, script).await
}

async fn serve(settings: Settings, script: String) -> Result<(), Box<dyn std::error::Error>> {
    // Parse identity
    let bytes = from_hex(&settings.identity).ok_or("Invalid identity hex format")?;
    let identity = decode_identity(bytes).map_err(|_| "Failed to decode identity")?;
    let participants = identity.len();
    let verifier = ConsensusScheme::certificate_verifier(NAMESPACE, identity);
    let mut indexer = Indexer::new(verifier, Sequential);
    if let Some(block_size) = settings.block_size {
        indexer = indexer.with_block_size(block_size);
    }
    let indexer = Arc::new(indexer);
    let app = Api::new(indexer)
        .router()
        .route(
            "/runtime-config.js",
            get(runtime_explorer_config).layer(Extension(script)),
        )
        .fallback(explorer_asset);

    // Start server
    let addr = format!("0.0.0.0:{}", settings.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(
        participants,
        ?addr,
        block_size = ?settings.block_size,
        explorer = !EXPLORER_ASSETS.is_empty(),
        "started indexer"
    );
    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{explorer_script, Args, ExplorerConfig, Settings};
    use clap::Parser;

    #[test]
    fn accepts_direct_and_deployer_modes() {
        assert!(Args::try_parse_from(["indexer", "--identity", "abcd"]).is_ok());
        assert!(Args::try_parse_from([
            "indexer",
            "--hosts",
            "hosts.yaml",
            "--config",
            "config.yaml",
        ])
        .is_ok());
        assert!(Args::try_parse_from(["indexer"]).is_err());
        assert!(
            Args::try_parse_from(["indexer", "--identity", "abcd", "--block-size", "4096",])
                .is_ok()
        );
        assert!(Args::try_parse_from([
            "indexer",
            "--hosts",
            "hosts.yaml",
            "--config",
            "config.yaml",
            "--block-size",
            "1",
        ])
        .is_err());
    }

    #[test]
    fn runtime_config_uses_deployed_identity_and_locations() {
        let settings = Settings {
            port: 8080,
            identity: "abcd".to_string(),
            block_size: Some(0),
            explorer: serde_yaml::from_str::<ExplorerConfig>(
                r#"
name: Live Cluster
description: description
participants: [first, unmapped, last]
locations: [[[1.0, 2.0], City], null, [[3.0, 4.0], Other]]
"#,
            )
            .unwrap(),
        };

        let script = explorer_script(&settings);
        assert!(script.contains(r#""PUBLIC_KEY_HEX":"abcd""#));
        assert!(script.contains(r#""PARTICIPANTS":["first","unmapped","last"]"#));
        assert!(script.contains(r#""LOCATIONS":[[[1.0,2.0],"City"],null,[[3.0,4.0],"Other"]]"#));
        assert!(script.contains("window.location.host"));
    }
}
