//! Interact with an `alto` indexer.

use alto_types::{Block, Scheme};
use commonware_consensus::marshal;
use commonware_cryptography::sha256::Digest;
use commonware_formatting::hex;
use commonware_parallel::Strategy;
use commonware_resolver::p2p::MAX_RESPONSE_OVERHEAD;
use std::sync::Arc;
use thiserror::Error;

pub mod consensus;
pub mod utils;

pub const LATEST: &str = "latest";

/// Maximum upload encoding size for the configured block payload and certificate scheme.
///
/// Covers blocks, notarized and finalized blocks, and round seeds.
///
/// # Panics
///
/// Panics if Marshal cannot bound the recovery overhead or the size exceeds this platform's limit.
pub fn max_upload_size<C: Scheme>(block_size: u32, scheme: &C) -> usize {
    // Marshal's bound is the proposal and certificate maximum plus resolver framing.
    // An upload carries the same proof and block without the resolver envelope.
    let proof_overhead = marshal::max_recovery_overhead::<_, Digest>(scheme)
        .expect("could not bound marshal recovery overhead")
        - MAX_RESPONSE_OVERHEAD;
    usize::try_from(block_size)
        .expect("block size is unsupported on this platform")
        .checked_add(Block::max_overhead(block_size) as usize)
        .and_then(|size| size.checked_add(usize::try_from(proof_overhead).ok()?))
        .expect("upload size is unsupported on this platform")
}

/// Payload allowance used when the network's block size is not configured.
pub const DEFAULT_MAX_BLOCK_SIZE: usize = 4 * 1024 * 1024;

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
pub struct ClientBuilder<S: Strategy, C: Scheme> {
    uri: String,
    ws_uri: String,
    /// Largest WebSocket frame or message, including the kind byte.
    max_message_size: usize,
    verifier: C,
    tls_certs: Vec<Vec<u8>>,
    strategy: S,
    verify: bool,
}

impl<S: Strategy, C: Scheme> ClientBuilder<S, C> {
    /// Create a builder with an already initialized concrete certificate verifier.
    ///
    /// TLS uses the system's root certificates. Add private roots with [`Self::with_tls_cert`].
    /// Streaming defaults to a 4 MiB payload allowance. Set the network's size with
    /// [`Self::with_block_size`].
    pub fn new(uri: &str, verifier: C, strategy: S) -> Self {
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
            max_message_size: max_upload_size(
                u32::try_from(DEFAULT_MAX_BLOCK_SIZE).expect("default block size exceeds u32"),
                &verifier,
            )
            .checked_add(1)
            .expect("message size is unsupported on this platform"),
            verifier,
            tls_certs: Vec::new(),
            strategy,
            verify: true,
        }
    }

    /// Set the network's block payload size for WebSocket receiving.
    ///
    /// Frames and complete messages include the maximum block and certificate encoding overhead
    /// and one message-kind byte. HTTP retrieval is unaffected.
    ///
    /// Panics if the receive limit cannot be represented on this platform.
    pub fn with_block_size(mut self, block_size: u32) -> Self {
        self.max_message_size = max_upload_size(block_size, &self.verifier)
            .checked_add(1)
            .expect("message size is unsupported on this platform");
        self
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
            max_message_size: self.max_message_size,
            verifier: self.verifier,
            verify: self.verify,
            http_client,
            ws_connector,
            strategy: self.strategy,
        }
    }
}

#[derive(Clone)]
pub struct Client<S: Strategy, C: Scheme> {
    uri: String,
    ws_uri: String,
    /// Largest WebSocket frame or message, including the kind byte.
    max_message_size: usize,
    verifier: C,
    verify: bool,

    http_client: reqwest::Client,
    ws_connector: WsConnector,
    strategy: S,
}

#[cfg(test)]
mod tests {
    use super::{max_upload_size, ClientBuilder, Error, DEFAULT_MAX_BLOCK_SIZE};
    use alto_types::{Identity, StandardScheme, NAMESPACE};
    use commonware_math::algebra::CryptoGroup;
    use commonware_parallel::Sequential;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::{io::AsyncWriteExt, net::TcpListener};
    use tokio_tungstenite::tungstenite::{error::CapacityError, Error as WsError};

    /// Send raw frames through a real client and return its transport or artifact error.
    async fn receive_error(block_size: Option<u32>, frames: Vec<u8>) -> Error {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            socket.get_mut().write_all(&frames).await.unwrap();
            socket.get_mut().shutdown().await.unwrap();
        });
        let mut builder = ClientBuilder::new(
            &format!("http://{addr}"),
            StandardScheme::certificate_verifier(NAMESPACE, Identity::generator()),
            Sequential,
        );
        if let Some(block_size) = block_size {
            builder = builder.with_block_size(block_size);
        }
        let client = builder.build();
        let mut stream = client.listen().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("frame processing stalled")
            .expect("frame did not produce a result");
        server.await.unwrap();
        match result {
            Err(error) => error,
            Ok(_) => panic!("unexpected consensus artifact"),
        }
    }

    #[tokio::test]
    async fn oversized_frames_are_rejected_from_the_header() {
        let scheme = StandardScheme::certificate_verifier(NAMESPACE, Identity::generator());
        for block_size in [None, Some(0), Some(4096)] {
            let limit =
                max_upload_size(block_size.unwrap_or(DEFAULT_MAX_BLOCK_SIZE as u32), &scheme) + 1;
            for fragmented in [false, true] {
                // Announce one byte beyond the receive limit, without sending its payload.
                let mut frames = Vec::new();
                if fragmented {
                    frames.extend_from_slice(&[0x02, 0x01, 0x00]);
                }
                frames.extend_from_slice(&[if fragmented { 0x80 } else { 0x82 }, 0x7f]);
                frames.extend_from_slice(&((limit + 1) as u64).to_be_bytes());
                assert!(matches!(
                    receive_error(block_size, frames).await,
                    Error::Tungstenite(WsError::Capacity(CapacityError::MessageTooLong {
                        size,
                        max_size,
                    })) if size == limit + 1 && max_size == limit
                ));
            }
        }
    }

    #[tokio::test]
    async fn streaming_budget_includes_the_message_kind() {
        // An HTTP artifact can fill the entire encoding allowance. Its stream adds one kind byte.
        let scheme = StandardScheme::certificate_verifier(NAMESPACE, Identity::generator());
        let length = max_upload_size(0, &scheme) + 1;
        let mut frame = vec![0x82, 0x7f];
        frame.extend_from_slice(&(length as u64).to_be_bytes());
        frame.resize(frame.len() + length, 0xff);

        // Reaching kind dispatch proves that the complete message passed the transport limit.
        assert!(matches!(
            receive_error(Some(0), frame).await,
            Error::UnexpectedResponse
        ));
    }

    #[tokio::test]
    async fn fragmented_messages_share_the_receive_budget() {
        // Each frame fits by itself, but their combined payload exceeds the message budget.
        let scheme = StandardScheme::certificate_verifier(NAMESPACE, Identity::generator());
        let length = max_upload_size(0, &scheme);
        let mut frames = vec![0x02, 0x7f];
        frames.extend_from_slice(&(length as u64).to_be_bytes());
        frames.resize(frames.len() + length, 0xff);
        frames.extend_from_slice(&[0x80, 0x02, 0xff, 0xff]);
        assert!(matches!(
            receive_error(Some(0), frames).await,
            Error::Tungstenite(WsError::Capacity(CapacityError::MessageTooLong {
                size,
                max_size,
            })) if size == length + 2 && max_size == length + 1
        ));
    }

    #[test]
    fn upload_size_supports_the_full_payload_range() {
        let scheme = StandardScheme::certificate_verifier(NAMESPACE, Identity::generator());
        for (block_size, overhead) in [
            (0, 257),
            (127, 257),
            (128, 258),
            (16_383, 258),
            (16_384, 259),
            (2_097_151, 259),
            (2_097_152, 260),
            (268_435_455, 260),
            (268_435_456, 261),
            (u32::MAX, 261),
        ] {
            let expected = u64::from(block_size) + overhead;
            match usize::try_from(expected) {
                Ok(expected) => assert_eq!(max_upload_size(block_size, &scheme), expected),
                Err(_) => assert!(std::panic::catch_unwind(|| max_upload_size(
                    block_size, &scheme
                ))
                .is_err()),
            }
        }
    }
}
