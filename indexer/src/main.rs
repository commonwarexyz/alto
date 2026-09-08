use alto_indexer::{Api, Indexer};
use alto_types::{CertificateMode, Identity, Scheme, StandardScheme, VrfScheme, NAMESPACE};
use axum::{
    body::{Body, Bytes},
    extract::Extension,
    http::{header, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
};
use clap::Parser;
use commonware_codec::DecodeExt;
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
        help = "Identity public key in hex format (BLS12-381 public key)"
    )]
    identity: Option<String>,

    /// Threshold certificate construction used by the network.
    #[clap(
        long,
        required_unless_present = "config",
        conflicts_with = "config",
        value_parser = CertificateMode::ALL.map(CertificateMode::as_str)
    )]
    certificate_mode: Option<String>,

    /// Payload size of the network's blocks in bytes. Uploads carrying a larger payload are
    /// rejected and the request body limit is derived from it. Without it, blocks of any size up
    /// to a fixed limit are accepted.
    #[clap(long, conflicts_with = "config")]
    block_size: Option<u32>,

    /// Accepted because the deployer starts every binary with `--hosts`. The indexer does not use it.
    #[clap(long, conflicts_with = "identity")]
    hosts: Option<PathBuf>,

    /// Path to the deployer-provided indexer config YAML.
    #[clap(long, conflicts_with = "identity")]
    config: Option<PathBuf>,
}

#[derive(Deserialize)]
struct DeployerConfig {
    port: u16,
    identity: String,
    certificate_mode: CertificateMode,
    block_size: u32,
    explorer: ExplorerConfig,
}

#[derive(Deserialize)]
struct ExplorerConfig {
    name: String,
    description: String,
    participants: Vec<String>,
    locations: Vec<([f64; 2], String)>,
}

struct Settings {
    port: u16,
    identity: String,
    certificate_mode: CertificateMode,
    block_size: Option<u32>,
    explorer: ExplorerConfig,
    explorer_mode: &'static str,
}

#[derive(Clone)]
struct ExplorerScript(String);

fn local_explorer_config() -> ExplorerConfig {
    ExplorerConfig {
        name: "Local Indexer".to_string(),
        description: "An Alto indexer running on this machine.".to_string(),
        participants: Vec::new(),
        locations: Vec::new(),
    }
}

fn load_settings(args: Args) -> Result<Settings, Box<dyn std::error::Error>> {
    if let Some(config) = args.config {
        let config = std::fs::read_to_string(config)?;
        let config: DeployerConfig = serde_yaml::from_str(&config)?;
        return Ok(Settings {
            port: config.port,
            identity: config.identity,
            certificate_mode: config.certificate_mode,
            block_size: Some(config.block_size),
            explorer: config.explorer,
            explorer_mode: "public",
        });
    }

    Ok(Settings {
        port: args.port,
        identity: args
            .identity
            .expect("clap requires --identity when --config is absent"),
        certificate_mode: args
            .certificate_mode
            .expect("clap requires --certificate-mode when --config is absent")
            .parse()?,
        block_size: args.block_size,
        explorer: local_explorer_config(),
        explorer_mode: "local",
    })
}

fn explorer_script(settings: &Settings) -> String {
    let config = serde_json::json!({
        "PUBLIC_KEY_HEX": settings.identity,
        "LOCATIONS": settings.explorer.locations,
        "PARTICIPANTS": settings.explorer.participants,
        "CERTIFICATE_MODE": settings.certificate_mode,
        "name": settings.explorer.name,
        "description": settings.explorer.description,
        "mode": settings.explorer_mode,
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

async fn runtime_explorer_config(Extension(script): Extension<ExplorerScript>) -> Response {
    response(
        Body::from(script.0),
        "application/javascript; charset=utf-8",
    )
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
    let script = ExplorerScript(explorer_script(&settings));

    // Create logger
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Parse identity
    let bytes = from_hex(&settings.identity).ok_or("Invalid identity hex format")?;
    let identity: Identity =
        Identity::decode(&mut bytes.as_slice()).map_err(|_| "Failed to decode identity")?;

    match settings.certificate_mode {
        CertificateMode::Standard => serve::<StandardScheme>(settings, identity, script).await,
        CertificateMode::Vrf => serve::<VrfScheme>(settings, identity, script).await,
    }
}

async fn serve<C: Scheme>(
    settings: Settings,
    identity: Identity,
    script: ExplorerScript,
) -> Result<(), Box<dyn std::error::Error>> {
    let verifier = C::certificate_verifier(NAMESPACE, identity);
    let mut indexer = Indexer::new(verifier, Sequential);
    if let Some(block_size) = settings.block_size {
        indexer = indexer.with_block_size(block_size);
    }
    let indexer = Arc::new(indexer);
    let app = Api::new(indexer)
        .router()
        .route("/runtime-config.js", get(runtime_explorer_config))
        .fallback(explorer_asset)
        .layer(Extension(script));

    // Start server
    let addr = format!("0.0.0.0:{}", settings.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(
        ?identity,
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
    use super::{explorer_script, Args, CertificateMode, ExplorerConfig, Settings};
    use clap::Parser;

    #[test]
    fn accepts_direct_and_deployer_modes() {
        assert!(Args::try_parse_from([
            "indexer",
            "--identity",
            "abcd",
            "--certificate-mode",
            "standard",
        ])
        .is_ok());
        assert!(Args::try_parse_from([
            "indexer",
            "--hosts",
            "hosts.yaml",
            "--config",
            "config.yaml",
        ])
        .is_ok());
        assert!(Args::try_parse_from(["indexer", "--identity", "abcd"]).is_err());
        assert!(Args::try_parse_from(["indexer"]).is_err());
        assert!(Args::try_parse_from([
            "indexer",
            "--identity",
            "abcd",
            "--certificate-mode",
            "vrf",
            "--block-size",
            "4096",
        ])
        .is_ok());
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
            certificate_mode: CertificateMode::Standard,
            block_size: Some(0),
            explorer: ExplorerConfig {
                name: "Live Cluster".to_string(),
                description: "description".to_string(),
                participants: vec!["participant".to_string()],
                locations: vec![([1.0, 2.0], "City".to_string())],
            },
            explorer_mode: "public",
        };

        let script = explorer_script(&settings);
        assert!(script.contains(r#""PUBLIC_KEY_HEX":"abcd""#));
        assert!(script.contains(r#""PARTICIPANTS":["participant"]"#));
        assert!(script.contains(r#""CERTIFICATE_MODE":"standard""#));
        assert!(script.contains(r#""LOCATIONS":[[[1.0,2.0],"City"]]"#));
        assert!(script.contains("window.location.host"));
    }
}
