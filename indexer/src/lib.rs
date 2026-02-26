use alto_client::LATEST;
use alto_types::{Block, Finalized, Kind, Notarized, Scheme};
use axum::{
    body::Bytes,
    extract::{ws::WebSocketUpgrade, Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use commonware_codec::{DecodeExt, Encode, EncodeSize, FixedSize, Write};
use commonware_consensus::{types::View, Viewable};
use commonware_cryptography::{sha256::Digest, Digestible};
use commonware_parallel::Strategy;
use commonware_utils::from_hex;
use futures::{SinkExt, StreamExt};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tracing::info;

#[derive(Default)]
pub struct State {
    notarizations: BTreeMap<View, Notarized>,
    finalizations: BTreeMap<View, Finalized>,
    finalized_height_to_view: BTreeMap<u64, View>,
    blocks_by_digest: BTreeMap<Digest, Block>,
}

#[derive(Clone)]
pub struct Indexer<S: Strategy> {
    scheme: Scheme,
    verify_notarizations: bool,
    state: Arc<RwLock<State>>,
    consensus_tx: broadcast::Sender<Vec<u8>>,
    strategy: S,
}

impl<S: Strategy> Indexer<S> {
    pub fn new(scheme: Scheme, strategy: S) -> Self {
        Self::new_with_options(scheme, strategy, true)
    }

    /// Construct an indexer backed by a certificate-only verifier.
    ///
    /// In this mode, notarization signatures are not verified because minimmit
    /// may produce aggregated m-notarization certificates that require the full
    /// participant set and polynomial to validate.
    pub fn new_certificate_verifier(scheme: Scheme, strategy: S) -> Self {
        Self::new_with_options(scheme, strategy, false)
    }

    fn new_with_options(scheme: Scheme, strategy: S, verify_notarizations: bool) -> Self {
        let (consensus_tx, _) = broadcast::channel(1024);
        let state = Arc::new(RwLock::new(State::default()));

        Self {
            scheme,
            verify_notarizations,
            state,
            consensus_tx,
            strategy,
        }
    }

    pub fn submit_notarization(&self, notarized: Notarized) -> Result<(), &'static str> {
        // In certificate-verifier mode we cannot validate all m-notarizations:
        // aggregated certificates require full committee metadata.
        if self.verify_notarizations && !notarized.verify(&self.scheme, &self.strategy) {
            info!("rejected notarization upload: invalid signature");
            return Err("Invalid notarization signature");
        }

        let mut state = self.state.write().unwrap();

        // Store block by digest
        state
            .blocks_by_digest
            .insert(notarized.block.digest(), notarized.block.clone());

        // Store notarization
        let view = notarized.proof.view();
        if state
            .notarizations
            .insert(view, notarized.clone())
            .is_some()
        {
            info!(view = view.get(), "duplicate notarization upload ignored");
            return Ok(()); // Already exists
        }
        info!(view = view.get(), "accepted notarization upload");

        // Broadcast notarization
        let mut data = vec![0u8; u8::SIZE + notarized.encode_size()];
        data[0] = Kind::Notarization as u8;
        notarized.write(&mut data[1..].as_mut());
        let _ = self.consensus_tx.send(data);
        Ok(())
    }

    pub fn get_notarization(&self, query: &str) -> Option<Notarized> {
        let state = self.state.read().unwrap();
        if query == LATEST {
            state.notarizations.last_key_value().map(|(_, n)| n.clone())
        } else {
            // Parse as hex-encoded index
            let raw = from_hex(query)?;
            let index = u64::decode(raw.as_slice()).ok()?;
            state.notarizations.get(&View::new(index)).cloned()
        }
    }

    pub fn submit_finalization(&self, finalized: Finalized) -> Result<(), &'static str> {
        // Verify signature with identity
        if !finalized.verify(&self.scheme, &self.strategy) {
            info!("rejected finalization upload: invalid signature");
            return Err("Invalid finalization signature");
        }

        let mut state = self.state.write().unwrap();

        // Store block by digest
        state
            .blocks_by_digest
            .insert(finalized.block.digest(), finalized.block.clone());

        // Store finalization
        let view = finalized.proof.view();
        if state
            .finalizations
            .insert(view, finalized.clone())
            .is_some()
        {
            info!(view = view.get(), "duplicate finalization upload ignored");
            return Ok(()); // Already exists
        }
        state
            .finalized_height_to_view
            .insert(finalized.block.height.get(), view);
        info!(
            view = view.get(),
            height = finalized.block.height.get(),
            "accepted finalization upload"
        );

        // Broadcast finalization
        let mut data = vec![0u8; u8::SIZE + finalized.encode_size()];
        data[0] = Kind::Finalization as u8;
        finalized.write(&mut data[1..].as_mut());
        let _ = self.consensus_tx.send(data);
        Ok(())
    }

    pub fn get_finalization(&self, query: &str) -> Option<Finalized> {
        let state = self.state.read().unwrap();
        if query == LATEST {
            state.finalizations.last_key_value().map(|(_, f)| f.clone())
        } else {
            // Parse as hex-encoded index
            let raw = from_hex(query)?;
            let index = u64::decode(raw.as_slice()).ok()?;
            state.finalizations.get(&View::new(index)).cloned()
        }
    }

    pub fn get_block(&self, query: &str) -> Option<BlockResult> {
        let state = self.state.read().unwrap();

        if query == LATEST {
            // Return latest finalized block
            state
                .finalizations
                .last_key_value()
                .map(|(_, f)| BlockResult::Finalized(f.clone()))
        } else if let Some(raw) = from_hex(query) {
            // Try to parse as index (8 bytes)
            if raw.len() == u64::SIZE {
                let index = u64::decode(raw.as_slice()).ok()?;
                state.finalized_height_to_view.get(&index).and_then(|view| {
                    state
                        .finalizations
                        .get(view)
                        .map(|f| BlockResult::Finalized(f.clone()))
                })
            } else if raw.len() == Digest::SIZE {
                let digest = Digest::decode(raw.as_slice()).ok()?;
                state
                    .blocks_by_digest
                    .get(&digest)
                    .map(|b| BlockResult::Block(b.clone()))
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn consensus_subscriber(&self) -> broadcast::Receiver<Vec<u8>> {
        self.consensus_tx.subscribe()
    }
}

#[allow(clippy::large_enum_variant)]
pub enum BlockResult {
    Block(Block),
    Finalized(Finalized),
}

pub struct Api<S: Strategy> {
    indexer: Arc<Indexer<S>>,
}

impl<S: Strategy> Api<S> {
    pub fn new(indexer: Arc<Indexer<S>>) -> Self {
        Self { indexer }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/health", get(health_check))
            .route("/notarization", post(notarization_upload))
            .route("/notarization/{query}", get(notarization_get))
            .route("/finalization", post(finalization_upload))
            .route("/finalization/{query}", get(finalization_get))
            .route("/block/{query}", get(block_get))
            .route("/consensus/ws", get(consensus_ws))
            .layer(CorsLayer::permissive())
            .with_state(self.indexer)
    }
}

async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn notarization_upload<S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<S>>>,
    body: Bytes,
) -> impl IntoResponse {
    match Notarized::decode(&mut body.as_ref()) {
        Ok(notarized) => match indexer.submit_notarization(notarized) {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::UNAUTHORIZED,
        },
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

async fn notarization_get<S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<S>>>,
    Path(query): Path<String>,
) -> impl IntoResponse {
    match indexer.get_notarization(&query) {
        Some(notarized) => (StatusCode::OK, notarized.encode().to_vec()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn finalization_upload<S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<S>>>,
    body: Bytes,
) -> impl IntoResponse {
    match Finalized::decode(&mut body.as_ref()) {
        Ok(finalized) => match indexer.submit_finalization(finalized) {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::UNAUTHORIZED,
        },
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

async fn finalization_get<S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<S>>>,
    Path(query): Path<String>,
) -> impl IntoResponse {
    match indexer.get_finalization(&query) {
        Some(finalized) => (StatusCode::OK, finalized.encode().to_vec()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn block_get<S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<S>>>,
    Path(query): Path<String>,
) -> impl IntoResponse {
    match indexer.get_block(&query) {
        Some(BlockResult::Block(block)) => {
            (StatusCode::OK, block.encode().to_vec()).into_response()
        }
        Some(BlockResult::Finalized(finalized)) => {
            (StatusCode::OK, finalized.encode().to_vec()).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn consensus_ws<S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<S>>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_consensus_ws(socket, indexer))
}

async fn handle_consensus_ws<S: Strategy>(
    socket: axum::extract::ws::WebSocket,
    indexer: Arc<Indexer<S>>,
) {
    let (mut sender, _receiver) = socket.split();
    let mut consensus = indexer.consensus_subscriber();

    while let Ok(data) = consensus.recv().await {
        if sender
            .send(axum::extract::ws::Message::Binary(data.into()))
            .await
            .is_err()
        {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alto_client::{consensus::Message, Client, ClientBuilder, IndexQuery, Query};
    use alto_types::{Context, Identity, EPOCH, NAMESPACE};
    use commonware_consensus::{
        minimmit::{scheme::bls12381_threshold, types::Notarize, types::Proposal},
        types::{Height, Round, View},
        Viewable,
    };
    use commonware_cryptography::{
        bls12381::primitives::variant::MinSig, certificate::mocks::Fixture, ed25519, sha256,
        Digest as _, Digestible, Hasher, Sha256, Signer,
    };
    use commonware_parallel::Sequential;
    use futures::StreamExt;
    use rand::{rngs::StdRng, SeedableRng};
    use rcgen::{generate_simple_self_signed, CertifiedKey, KeyPair};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use std::{net::SocketAddr, sync::Arc};
    use tokio::{net::TcpListener, time::Duration};
    use tokio_rustls::TlsAcceptor;
    use tower::ServiceExt;

    struct TestContext {
        schemes: Vec<Scheme>,
        client: Client<Sequential>,
    }

    impl TestContext {
        async fn new() -> Self {
            let (schemes, verifier, identity) = fixture(0);
            let (addr, _handle) = start_server(verifier).await;
            let client = ClientBuilder::new(&format!("http://{addr}"), identity, Sequential)
                .with_verification_disabled()
                .build();
            wait_for_ready(&client).await;
            Self { schemes, client }
        }

        fn test_block(&self, height: u64, view: u64) -> Block {
            let context = Context {
                round: Round::new(EPOCH, View::new(view)),
                leader: ed25519::PrivateKey::from_seed(0).public_key(),
                parent: (View::new(view.saturating_sub(1)), sha256::Digest::EMPTY),
            };
            Block::new(
                context,
                Sha256::hash(format!("parent-{height}-{view}").as_bytes()),
                Height::new(height),
                1000 + height,
            )
        }

        fn proposal(&self, block: &Block) -> Proposal<sha256::Digest> {
            Proposal::new(
                block.context.round,
                block.context.parent.0,
                block.context.parent.1,
                block.digest(),
            )
        }

        fn notarized(&self) -> Notarized {
            let block = self.test_block(1, 1);
            let proposal = self.proposal(&block);
            Notarized::new(create_notarization(&self.schemes, proposal), block)
        }

        fn finalized(&self) -> Finalized {
            let block = self.test_block(1, 1);
            let proposal = self.proposal(&block);
            Finalized::new(create_finalization(&self.schemes, proposal), block)
        }
    }

    fn fixture(seed: u64) -> (Vec<Scheme>, Scheme, Identity) {
        let mut rng = StdRng::seed_from_u64(seed);
        let Fixture {
            schemes, verifier, ..
        } = bls12381_threshold::fixture::<MinSig, _>(&mut rng, NAMESPACE, 4);
        let identity = *verifier.identity();
        (schemes, verifier, identity)
    }

    fn create_notarization(
        schemes: &[Scheme],
        proposal: Proposal<sha256::Digest>,
    ) -> alto_types::Notarization {
        let notarizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        alto_types::Notarization::from_notarizes(&schemes[0], &notarizes, &Sequential).unwrap()
    }

    fn create_finalization(
        schemes: &[Scheme],
        proposal: Proposal<sha256::Digest>,
    ) -> alto_types::Finalization {
        let notarizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        alto_types::Finalization::from_notarizes(&schemes[0], &notarizes, &Sequential).unwrap()
    }

    async fn start_server_with_mode(
        scheme: Scheme,
        verify_notarizations: bool,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let indexer = if verify_notarizations {
            Arc::new(Indexer::new(scheme, Sequential))
        } else {
            Arc::new(Indexer::new_certificate_verifier(scheme, Sequential))
        };
        let api = Api::new(indexer);
        let app = api.router();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, handle)
    }

    async fn start_server(scheme: Scheme) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        start_server_with_mode(scheme, true).await
    }

    async fn wait_for_ready(client: &Client<Sequential>) {
        loop {
            if client.health().await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn test_notarization_operations() {
        let ctx = TestContext::new().await;
        let notarized = ctx.notarized();
        ctx.client.notarized_upload(notarized).await.unwrap();

        let latest = ctx.client.notarized_get(IndexQuery::Latest).await.unwrap();
        assert_eq!(latest.proof.view().get(), 1);

        let by_view = ctx
            .client
            .notarized_get(IndexQuery::Index(1))
            .await
            .unwrap();
        assert_eq!(by_view.proof.view().get(), 1);
    }

    #[tokio::test]
    async fn test_finalization_operations() {
        let ctx = TestContext::new().await;
        let finalized = ctx.finalized();
        ctx.client.finalized_upload(finalized).await.unwrap();

        let latest = ctx.client.finalized_get(IndexQuery::Latest).await.unwrap();
        assert_eq!(latest.proof.view().get(), 1);

        let by_view = ctx
            .client
            .finalized_get(IndexQuery::Index(1))
            .await
            .unwrap();
        assert_eq!(by_view.proof.view().get(), 1);
    }

    #[tokio::test]
    async fn test_block_retrieval() {
        let ctx = TestContext::new().await;
        let finalized = ctx.finalized();
        let block_digest = finalized.block.digest();
        ctx.client.finalized_upload(finalized).await.unwrap();

        match ctx.client.block_get(Query::Latest).await.unwrap() {
            alto_client::consensus::Payload::Finalized(f) => {
                assert_eq!(f.block.height.get(), 1);
            }
            _ => panic!("expected finalized payload"),
        }

        match ctx.client.block_get(Query::Index(1)).await.unwrap() {
            alto_client::consensus::Payload::Finalized(f) => {
                assert_eq!(f.block.height.get(), 1);
            }
            _ => panic!("expected finalized payload"),
        }

        match ctx
            .client
            .block_get(Query::Digest(block_digest))
            .await
            .unwrap()
        {
            alto_client::consensus::Payload::Block(b) => {
                assert_eq!(b.digest(), block_digest);
            }
            _ => panic!("expected block payload"),
        }
    }

    #[tokio::test]
    async fn test_websocket_streaming() {
        let ctx = TestContext::new().await;
        let mut stream = ctx.client.listen().await.unwrap();

        let notarized = ctx.notarized();
        ctx.client
            .notarized_upload(notarized.clone())
            .await
            .unwrap();
        match tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Notarization(value) => {
                assert_eq!(value.proof.view(), notarized.proof.view());
            }
            _ => panic!("expected notarization"),
        }

        let finalized = ctx.finalized();
        ctx.client
            .finalized_upload(finalized.clone())
            .await
            .unwrap();
        match tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Finalization(value) => {
                assert_eq!(value.proof.view(), finalized.proof.view());
            }
            _ => panic!("expected finalization"),
        }
    }

    #[tokio::test]
    async fn test_identity_verification() {
        let (schemes_1, verifier_1, identity_1) = fixture(0);
        let (_schemes_2, _verifier_2, identity_2) = fixture(1);

        let (addr, _handle) = start_server(verifier_1).await;
        let upload_client = ClientBuilder::new(&format!("http://{addr}"), identity_1, Sequential)
            .with_verification_disabled()
            .build();
        let verify_client = Client::new(&format!("http://{addr}"), identity_2, Sequential);
        wait_for_ready(&verify_client).await;

        let block = {
            let context = Context {
                round: Round::new(EPOCH, View::new(1)),
                leader: ed25519::PrivateKey::from_seed(0).public_key(),
                parent: (View::zero(), sha256::Digest::EMPTY),
            };
            Block::new(context, Sha256::hash(b"genesis"), Height::new(1), 1000)
        };
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(1)),
            View::zero(),
            block.parent,
            block.digest(),
        );
        let finalized = Finalized::new(create_finalization(&schemes_1, proposal), block);
        upload_client.finalized_upload(finalized).await.unwrap();

        let result = verify_client.finalized_get(IndexQuery::Latest).await;
        assert!(matches!(result, Err(alto_client::Error::InvalidSignature)));
    }

    #[tokio::test]
    async fn test_invalid_signature_rejection() {
        let ctx = TestContext::new().await;
        let (wrong_schemes, _wrong_verifier, _wrong_identity) = fixture(1);
        let block = ctx.test_block(1, 1);
        let proposal = ctx.proposal(&block);

        let bad_notarized = Notarized::new(
            create_notarization(&wrong_schemes, proposal.clone()),
            block.clone(),
        );
        assert!(ctx.client.notarized_upload(bad_notarized).await.is_err());

        let bad_finalized = Finalized::new(create_finalization(&wrong_schemes, proposal), block);
        assert!(ctx.client.finalized_upload(bad_finalized).await.is_err());
    }

    #[tokio::test]
    async fn test_certificate_verifier_mode_accepts_notarization() {
        let (schemes, _verifier, identity) = fixture(0);
        let certificate_verifier = Scheme::certificate_verifier(NAMESPACE, identity);
        let (addr, _handle) = start_server_with_mode(certificate_verifier, false).await;
        let client = ClientBuilder::new(&format!("http://{addr}"), identity, Sequential)
            .with_verification_disabled()
            .build();
        wait_for_ready(&client).await;

        let context = Context {
            round: Round::new(EPOCH, View::new(1)),
            leader: ed25519::PrivateKey::from_seed(0).public_key(),
            parent: (View::zero(), sha256::Digest::EMPTY),
        };
        let block = Block::new(context, Sha256::hash(b"genesis"), Height::new(1), 1000);
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(1)),
            View::zero(),
            block.parent,
            block.digest(),
        );
        let notarized = Notarized::new(create_notarization(&schemes, proposal), block);
        client.notarized_upload(notarized.clone()).await.unwrap();

        let retrieved = client.notarized_get(IndexQuery::Latest).await.unwrap();
        assert_eq!(retrieved.proof.view(), notarized.proof.view());
    }

    fn generate_self_signed_cert() -> CertifiedKey<KeyPair> {
        generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap()
    }

    async fn start_tls_server(
        scheme: Scheme,
        cert_key: &CertifiedKey<KeyPair>,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let indexer = Arc::new(Indexer::new(scheme, Sequential));
        let api = Api::new(indexer);
        let app = api.router();

        let cert_der = CertificateDer::from(cert_key.cert.der().to_vec());
        let key_der = PrivateKeyDer::try_from(cert_key.signing_key.serialize_der()).unwrap();
        let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .unwrap();
        let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let tls_acceptor = tls_acceptor.clone();
                let app = app.clone();

                tokio::spawn(async move {
                    let Ok(tls_stream) = tls_acceptor.accept(stream).await else {
                        return;
                    };
                    let io = hyper_util::rt::TokioIo::new(tls_stream);
                    let service = hyper::service::service_fn(move |req| {
                        let app = app.clone();
                        async move { app.oneshot(req).await }
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection_with_upgrades(io, service)
                    .await;
                });
            }
        });

        (addr, handle)
    }

    fn create_tls_client(
        addr: SocketAddr,
        identity: Identity,
        cert_key: &CertifiedKey<KeyPair>,
    ) -> Client<Sequential> {
        ClientBuilder::new(&format!("https://{addr}"), identity, Sequential)
            .with_tls_cert(cert_key.cert.der().to_vec())
            .with_verification_disabled()
            .build()
    }

    #[tokio::test]
    async fn test_tls_https_connection() {
        let cert_key = generate_self_signed_cert();
        let (schemes, verifier, identity) = fixture(0);
        let (addr, handle) = start_tls_server(verifier, &cert_key).await;
        let client = create_tls_client(addr, identity, &cert_key);
        wait_for_ready(&client).await;

        let block = {
            let context = Context {
                round: Round::new(EPOCH, View::new(1)),
                leader: ed25519::PrivateKey::from_seed(0).public_key(),
                parent: (View::zero(), sha256::Digest::EMPTY),
            };
            Block::new(context, Sha256::hash(b"genesis"), Height::new(1), 1000)
        };
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(1)),
            View::zero(),
            block.parent,
            block.digest(),
        );
        let finalized = Finalized::new(create_finalization(&schemes, proposal), block);
        client.finalized_upload(finalized).await.unwrap();
        let retrieved = client.finalized_get(IndexQuery::Latest).await.unwrap();
        assert_eq!(retrieved.proof.view().get(), 1);

        handle.abort();
    }

    #[tokio::test]
    async fn test_tls_websocket_connection() {
        let cert_key = generate_self_signed_cert();
        let (schemes, verifier, identity) = fixture(0);
        let (addr, handle) = start_tls_server(verifier, &cert_key).await;
        let client = create_tls_client(addr, identity, &cert_key);
        wait_for_ready(&client).await;

        let mut stream = client.listen().await.unwrap();
        let block = {
            let context = Context {
                round: Round::new(EPOCH, View::new(1)),
                leader: ed25519::PrivateKey::from_seed(0).public_key(),
                parent: (View::zero(), sha256::Digest::EMPTY),
            };
            Block::new(context, Sha256::hash(b"genesis"), Height::new(1), 1000)
        };
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(1)),
            View::zero(),
            block.parent,
            block.digest(),
        );
        let finalized = Finalized::new(create_finalization(&schemes, proposal), block);
        client.finalized_upload(finalized.clone()).await.unwrap();

        match tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Finalization(value) => {
                assert_eq!(value.proof.view(), finalized.proof.view());
            }
            _ => panic!("expected finalization"),
        }

        handle.abort();
    }
}
