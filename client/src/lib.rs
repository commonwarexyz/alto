//! Client for interacting with `alto`.

use alto_types::{Identity, Scheme};
use commonware_cryptography::sha256::Digest;
use commonware_utils::hex;
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
pub struct ClientBuilder {
    uri: String,
    ws_uri: String,
    identity: Identity,
    tls_certs: Vec<Vec<u8>>,
}

impl ClientBuilder {
    /// Create a new builder for the given indexer URI.
    pub fn new(uri: &str, identity: Identity) -> Self {
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
            identity,
            tls_certs: Vec::new(),
        }
    }

    /// Add a trusted TLS certificate (DER-encoded).
    ///
    /// Use this for self-signed certificates that should be trusted.
    pub fn with_tls_cert(mut self, cert_der: Vec<u8>) -> Self {
        self.tls_certs.push(cert_der);
        self
    }

    /// Build the client.
    pub fn build(self) -> Client {
        let certificate_verifier = Scheme::certificate_verifier(self.identity);

        // Build HTTP client
        let mut http_builder = reqwest::Client::builder();
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
            certificate_verifier,
            http_client,
            ws_connector,
        }
    }
}

#[derive(Clone)]
pub struct Client {
    uri: String,
    ws_uri: String,
    certificate_verifier: Scheme,

    http_client: reqwest::Client,
    ws_connector: WsConnector,
}

impl Client {
    /// Create a new client for the given indexer URI.
    ///
    /// TLS is automatically configured using the system's root certificates.
    /// For HTTPS/WSS endpoints with certificates signed by trusted CAs,
    /// no additional configuration is needed.
    ///
    /// For custom TLS configuration (e.g., self-signed certificates),
    /// use [`ClientBuilder`] instead.
    pub fn new(uri: &str, identity: Identity) -> Self {
        ClientBuilder::new(uri, identity).build()
    }
}
