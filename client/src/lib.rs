//! Interact with an `alto` indexer.

use alto_types::{Identity, Scheme, StandardScheme, VrfScheme, NAMESPACE};
use commonware_cryptography::sha256::Digest;
use commonware_formatting::hex;
use commonware_parallel::Strategy;
use std::sync::Arc;
use thiserror::Error;

pub mod consensus;
pub mod utils;

pub const LATEST: &str = "latest";

pub enum Query {
    Latest,
    Index(u64),
    Digest(Digest),
}

impl Query {
    pub fn serialize(&self) -> String {
        match self {
            Query::Latest => LATEST.to_string(),
            Query::Index(index) => hex(&index.to_be_bytes()),
            Query::Digest(digest) => hex(digest),
        }
    }
}

pub enum IndexQuery {
    Latest,
    Index(u64),
}

impl IndexQuery {
    pub fn serialize(&self) -> String {
        match self {
            IndexQuery::Latest => LATEST.to_string(),
            IndexQuery::Index(index) => hex(&index.to_be_bytes()),
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("tungstenite error: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("failed: {0}")]
    Failed(reqwest::StatusCode),
    #[error("invalid data: {0}")]
    InvalidData(#[from] commonware_codec::Error),
    #[error("invalid signature")]
    InvalidSignature,
    #[error("unexpected response")]
    UnexpectedResponse,
}

/// TLS connector for WebSocket connections.
type WsConnector = tokio_tungstenite::Connector;

/// Builder for creating a [`Client`].
pub struct ClientBuilder<S: Strategy, C: Scheme = VrfScheme> {
    uri: String,
    ws_uri: String,
    certificate_verifier: C,
    tls_certs: Vec<Vec<u8>>,
    strategy: S,
    verify: bool,
}

impl<S: Strategy> ClientBuilder<S, VrfScheme> {
    /// Create a new builder for the given indexer URI.
    ///
    /// The default constructor targets a rotating-leader network using VRF certificates.
    pub fn new(uri: &str, identity: Identity, strategy: S) -> Self {
        Self::new_with_scheme(
            uri,
            VrfScheme::certificate_verifier(NAMESPACE, identity),
            strategy,
        )
    }
}

impl<S: Strategy> ClientBuilder<S, StandardScheme> {
    /// Create a builder for a stable-leader network using native standard certificates.
    pub fn new_standard(uri: &str, identity: Identity, strategy: S) -> Self {
        Self::new_with_scheme(
            uri,
            StandardScheme::certificate_verifier(NAMESPACE, identity),
            strategy,
        )
    }
}

impl<S: Strategy, C: Scheme> ClientBuilder<S, C> {
    /// Create a builder with an already initialized concrete certificate verifier.
    pub fn new_with_scheme(uri: &str, certificate_verifier: C, strategy: S) -> Self {
        let uri = uri.to_string();
        let ws_uri = if let Some(rest) = uri.strip_prefix("https://") {
            format!("wss://{rest}")
        } else if let Some(rest) = uri.strip_prefix("http://") {
            format!("ws://{rest}")
        } else {
            panic!("URI must start with http:// or https://");
        };
        Self {
            uri,
            ws_uri,
            certificate_verifier,
            tls_certs: Vec::new(),
            strategy,
            verify: true,
        }
    }

    /// Disable signature verification for all returned data.
    pub fn with_verification_disabled(mut self) -> Self {
        self.verify = false;
        self
    }

    /// Add a trusted TLS certificate (DER-encoded).
    ///
    /// Use this for self-signed certificates that should be trusted.
    pub fn with_tls_cert(mut self, cert_der: Vec<u8>) -> Self {
        self.tls_certs.push(cert_der);
        self
    }

    /// Build the client.
    pub fn build(self) -> Client<S, C> {
        // HTTP/2 multiplexes all requests over a single connection, so
        // DNS is only resolved once on the initial connect.
        let mut http_builder = reqwest::Client::builder()
            .tcp_nodelay(true)
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(10))
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(std::time::Duration::from_secs(10))
            .http2_keep_alive_timeout(std::time::Duration::from_secs(5))
            .http2_keep_alive_while_idle(true);
        for cert_der in &self.tls_certs {
            let cert = reqwest::Certificate::from_der(cert_der).expect("invalid DER certificate");
            http_builder = http_builder.add_root_certificate(cert);
        }
        let http_client = http_builder.build().expect("failed to build HTTP client");

        // Build WebSocket TLS connector with native root certificates
        let mut root_store = rustls::RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs().expect("failed to load native certs") {
            root_store
                .add(cert)
                .expect("failed to add native certificate");
        }
        for cert_der in &self.tls_certs {
            let cert = rustls::pki_types::CertificateDer::from(cert_der.clone());
            root_store.add(cert).expect("failed to add certificate");
        }
        let ws_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("failed to set protocol versions")
        .with_root_certificates(root_store)
        .with_no_client_auth();
        let ws_connector = WsConnector::Rustls(Arc::new(ws_config));

        Client {
            uri: self.uri,
            ws_uri: self.ws_uri,
            certificate_verifier: self.certificate_verifier,
            verify: self.verify,
            http_client,
            ws_connector,
            strategy: self.strategy,
        }
    }
}

#[derive(Clone)]
pub struct Client<S: Strategy, C: Scheme = VrfScheme> {
    uri: String,
    ws_uri: String,
    certificate_verifier: C,
    verify: bool,

    http_client: reqwest::Client,
    ws_connector: WsConnector,
    strategy: S,
}

impl<S: Strategy> Client<S, VrfScheme> {
    /// Create a new client for the given indexer URI.
    ///
    /// This constructor verifies VRF certificates.
    ///
    /// TLS is automatically configured using the system's root certificates.
    /// For HTTPS/WSS endpoints with certificates signed by trusted CAs,
    /// no additional configuration is needed.
    ///
    /// For custom TLS configuration (e.g., self-signed certificates),
    /// use [`ClientBuilder`] instead.
    pub fn new(uri: &str, identity: Identity, strategy: S) -> Self {
        ClientBuilder::new(uri, identity, strategy).build()
    }
}

impl<S: Strategy> Client<S, StandardScheme> {
    /// Create a client for a stable-leader network using native standard certificates.
    pub fn new_standard(uri: &str, identity: Identity, strategy: S) -> Self {
        ClientBuilder::new_standard(uri, identity, strategy).build()
    }
}
