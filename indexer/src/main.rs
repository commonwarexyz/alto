use alto_indexer::{Api, Indexer};
use alto_types::{CertificateMode, Identity, Scheme, NAMESPACE};
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
use serde::{Deserialize, Serialize};
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
        value_parser = ["standard", "vrf"]
    )]
    certificate_mode: Option<String>,

    /// Path to the deployer-generated hosts file.
    #[clap(long, requires = "config", conflicts_with = "identity")]
    hosts: Option<PathBuf>,

    /// Path to the deployer-provided indexer config YAML.
    #[clap(long, requires = "hosts", conflicts_with = "identity")]
    config: Option<PathBuf>,
}

#[derive(Deserialize)]
struct DeployerConfig {
    port: u16,
    identity: String,
    certificate_mode: CertificateMode,
    explorer: Option<ExplorerConfig>,
}

#[derive(Deserialize)]
struct ExplorerConfig {
    name: String,
    description: String,
    #[serde(default)]
    participants: Vec<String>,
    locations: Vec<([f64; 2], String)>,
}

struct Settings {
    port: u16,
    identity: String,
    certificate_mode: CertificateMode,
    explorer: ExplorerConfig,
    explorer_mode: &'static str,
}

#[derive(Serialize)]
struct RuntimeExplorerConfig<'a> {
    #[serde(rename = "PUBLIC_KEY_HEX")]
    public_key_hex: &'a str,
    #[serde(rename = "LOCATIONS")]
    locations: &'a [([f64; 2], String)],
    #[serde(rename = "PARTICIPANTS")]
    participants: &'a [String],
    #[serde(rename = "CERTIFICATE_MODE")]
    certificate_mode: CertificateMode,
    name: &'a str,
    description: &'a str,
    mode: &'a str,
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
        debug_assert!(args.hosts.is_some());
        let config = std::fs::read_to_string(config)?;
        let config: DeployerConfig = serde_yaml::from_str(&config)?;
        let explorer_mode = if config.explorer.is_some() {
            "public"
        } else {
            "local"
        };
        return Ok(Settings {
            port: config.port,
            identity: config.identity,
            certificate_mode: config.certificate_mode,
            explorer: config.explorer.unwrap_or_else(local_explorer_config),
            explorer_mode,
        });
    }

    Ok(Settings {
        port: args.port,
        identity: args
            .identity
            .expect("clap requires --identity when --config is absent"),
        certificate_mode: match args.certificate_mode.as_deref() {
            Some("standard") => CertificateMode::Standard,
            Some("vrf") => CertificateMode::Vrf,
            None => unreachable!("clap requires --certificate-mode in direct mode"),
            Some(_) => unreachable!("clap validates certificate mode"),
        },
        explorer: local_explorer_config(),
        explorer_mode: "local",
    })
}

fn explorer_script(settings: &Settings) -> Result<String, serde_json::Error> {
    let config = RuntimeExplorerConfig {
        public_key_hex: &settings.identity,
        locations: &settings.explorer.locations,
        participants: &settings.explorer.participants,
        certificate_mode: settings.certificate_mode,
        name: &settings.explorer.name,
        description: &settings.explorer.description,
        mode: settings.explorer_mode,
    };
    Ok(format!(
        "window.ALTO_DEPLOYMENT = {};\nwindow.ALTO_DEPLOYMENT.BACKEND_URL = window.location.host;\n",
        serde_json::to_string(&config)?
    ))
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
        .find(|(asset_path, _, _)| *asset_path == path)
        .or_else(|| {
            (!path.contains('.'))
                .then(|| {
                    EXPLORER_ASSETS
                        .iter()
                        .find(|(asset_path, _, _)| *asset_path == "index.html")
                })
                .flatten()
        });
    let Some((_, contents, content_type)) = asset else {
        return StatusCode::NOT_FOUND.into_response();
    };

    response(Body::from(Bytes::from_static(contents)), content_type)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse args
    let settings = load_settings(Args::parse())?;
    let explorer_script = ExplorerScript(explorer_script(&settings)?);

    // Create logger
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Parse identity
    let bytes = from_hex(&settings.identity).ok_or("Invalid identity hex format")?;
    let identity: Identity =
        Identity::decode(&mut bytes.as_slice()).map_err(|_| "Failed to decode identity")?;

    // Initialize indexer
    let certificate_verifier =
        Scheme::certificate_verifier(settings.certificate_mode, NAMESPACE, identity);
    let indexer = Arc::new(Indexer::new(certificate_verifier, Sequential));
    let app = Api::new(indexer)
        .router()
        .route("/runtime-config.js", get(runtime_explorer_config))
        .fallback(explorer_asset)
        .layer(Extension(explorer_script));

    // Start server
    let addr = format!("0.0.0.0:{}", settings.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(
        ?identity,
        ?addr,
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
    }

    #[test]
    fn runtime_config_uses_deployed_identity_and_locations() {
        let settings = Settings {
            port: 8080,
            identity: "abcd".to_string(),
            certificate_mode: CertificateMode::Standard,
            explorer: ExplorerConfig {
                name: "Live Cluster".to_string(),
                description: "description".to_string(),
                participants: vec!["participant".to_string()],
                locations: vec![([1.0, 2.0], "City".to_string())],
            },
            explorer_mode: "public",
        };

        let script = explorer_script(&settings).unwrap();
        assert!(script.contains(r#""PUBLIC_KEY_HEX":"abcd""#));
        assert!(script.contains(r#""PARTICIPANTS":["participant"]"#));
        assert!(script.contains(r#""CERTIFICATE_MODE":"standard""#));
        assert!(script.contains(r#""LOCATIONS":[[[1.0,2.0],"City"]]"#));
        assert!(script.contains("window.location.host"));
    }
}
