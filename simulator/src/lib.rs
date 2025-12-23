use alto_client::LATEST;
use alto_types::{Block, Finalized, Kind, Notarized, Scheme, Seed, NAMESPACE};
use axum::{
    body::Bytes,
    extract::{ws::WebSocketUpgrade, Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use commonware_codec::{DecodeExt, Encode};
use commonware_consensus::{types::View, Viewable};
use commonware_cryptography::Digestible;
use commonware_utils::from_hex;
use futures::{SinkExt, StreamExt};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;

#[derive(Default)]
pub struct State {
    seeds: BTreeMap<View, Seed>,
    notarizations: BTreeMap<View, Notarized>,
    finalizations: BTreeMap<View, Finalized>,
    blocks_by_digest: BTreeMap<commonware_cryptography::sha256::Digest, Block>,
}

#[derive(Clone)]
pub struct Simulator {
    scheme: Scheme,
    state: Arc<RwLock<State>>,
    consensus_tx: broadcast::Sender<Vec<u8>>,
}

impl Simulator {
    pub fn new(scheme: Scheme) -> Self {
        let (consensus_tx, _) = broadcast::channel(1024);
        let state = Arc::new(RwLock::new(State::default()));

        Self {
            scheme,
            state,
            consensus_tx,
        }
    }

    pub fn submit_seed(&self, seed: Seed) -> Result<(), &'static str> {
        // Verify signature with identity
        if !seed.verify(&self.scheme, NAMESPACE) {
            return Err("Invalid seed signature");
        }

        let mut state = self.state.write().unwrap();
        if state.seeds.insert(seed.view(), seed.clone()).is_some() {
            return Ok(()); // Already exists
        }

        // Broadcast seed
        let mut data = vec![Kind::Seed as u8];
        data.extend(seed.encode().to_vec());
        let _ = self.consensus_tx.send(data);
        Ok(())
    }

    pub fn get_seed(&self, query: &str) -> Option<Seed> {
        let state = self.state.read().unwrap();
        if query == LATEST {
            state.seeds.last_key_value().map(|(_, seed)| seed.clone())
        } else {
            // Parse as hex-encoded index
            let raw = from_hex(query)?;
            let index = u64::from_be_bytes(raw.try_into().ok()?);
            state.seeds.get(&View::new(index)).cloned()
        }
    }

    pub fn submit_notarization(&self, notarized: Notarized) -> Result<(), &'static str> {
        // Verify signature with identity
        if !notarized.verify(&self.scheme, NAMESPACE) {
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
            return Ok(()); // Already exists
        }

        // Broadcast notarization
        let mut data = vec![Kind::Notarization as u8];
        data.extend(notarized.encode().to_vec());
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
            let index = u64::from_be_bytes(raw.try_into().ok()?);
            state.notarizations.get(&View::new(index)).cloned()
        }
    }

    pub fn submit_finalization(&self, finalized: Finalized) -> Result<(), &'static str> {
        // Verify signature with identity
        if !finalized.verify(&self.scheme, NAMESPACE) {
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
            return Ok(()); // Already exists
        }

        // Broadcast finalization
        let mut data = vec![Kind::Finalization as u8];
        data.extend(finalized.encode().to_vec());
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
            let index = u64::from_be_bytes(raw.try_into().ok()?);
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
            if raw.len() == 8 {
                let index = u64::from_be_bytes(raw.try_into().ok()?);
                // Find finalized block with this height
                for (_, finalized) in state.finalizations.iter() {
                    if finalized.block.height == index {
                        return Some(BlockResult::Finalized(finalized.clone()));
                    }
                }
                None
            } else if raw.len() == 32 {
                // Try to parse as digest (32 bytes)
                let digest_bytes: [u8; 32] = raw.try_into().ok()?;
                let digest = commonware_cryptography::sha256::Digest::from(digest_bytes);
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

pub struct Api {
    simulator: Arc<Simulator>,
}

impl Api {
    pub fn new(simulator: Arc<Simulator>) -> Self {
        Self { simulator }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/health", get(health_check))
            .route("/seed", post(seed_upload))
            .route("/seed/{query}", get(seed_get))
            .route("/notarization", post(notarization_upload))
            .route("/notarization/{query}", get(notarization_get))
            .route("/finalization", post(finalization_upload))
            .route("/finalization/{query}", get(finalization_get))
            .route("/block/{query}", get(block_get))
            .route("/consensus/ws", get(consensus_ws))
            .layer(CorsLayer::permissive())
            .with_state(self.simulator.clone())
    }
}

async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn seed_upload(
    AxumState(simulator): AxumState<Arc<Simulator>>,
    body: Bytes,
) -> impl IntoResponse {
    match Seed::decode(&mut body.as_ref()) {
        Ok(seed) => match simulator.submit_seed(seed) {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::UNAUTHORIZED,
        },
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

async fn seed_get(
    AxumState(simulator): AxumState<Arc<Simulator>>,
    Path(query): Path<String>,
) -> impl IntoResponse {
    match simulator.get_seed(&query) {
        Some(seed) => (StatusCode::OK, seed.encode().to_vec()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn notarization_upload(
    AxumState(simulator): AxumState<Arc<Simulator>>,
    body: Bytes,
) -> impl IntoResponse {
    match Notarized::decode(&mut body.as_ref()) {
        Ok(notarized) => match simulator.submit_notarization(notarized) {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::UNAUTHORIZED,
        },
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

async fn notarization_get(
    AxumState(simulator): AxumState<Arc<Simulator>>,
    Path(query): Path<String>,
) -> impl IntoResponse {
    match simulator.get_notarization(&query) {
        Some(notarized) => (StatusCode::OK, notarized.encode().to_vec()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn finalization_upload(
    AxumState(simulator): AxumState<Arc<Simulator>>,
    body: Bytes,
) -> impl IntoResponse {
    match Finalized::decode(&mut body.as_ref()) {
        Ok(finalized) => match simulator.submit_finalization(finalized) {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::UNAUTHORIZED,
        },
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

async fn finalization_get(
    AxumState(simulator): AxumState<Arc<Simulator>>,
    Path(query): Path<String>,
) -> impl IntoResponse {
    match simulator.get_finalization(&query) {
        Some(finalized) => (StatusCode::OK, finalized.encode().to_vec()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn block_get(
    AxumState(simulator): AxumState<Arc<Simulator>>,
    Path(query): Path<String>,
) -> impl IntoResponse {
    match simulator.get_block(&query) {
        Some(BlockResult::Block(block)) => {
            (StatusCode::OK, block.encode().to_vec()).into_response()
        }
        Some(BlockResult::Finalized(finalized)) => {
            (StatusCode::OK, finalized.encode().to_vec()).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn consensus_ws(
    AxumState(simulator): AxumState<Arc<Simulator>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_consensus_ws(socket, simulator))
}

async fn handle_consensus_ws(socket: axum::extract::ws::WebSocket, simulator: Arc<Simulator>) {
    let (mut sender, _receiver) = socket.split();
    let mut consensus = simulator.consensus_subscriber();

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
    use alto_client::{Client, IndexQuery, Query};
    use alto_types::{Identity, Seedable, EPOCH};
    use commonware_consensus::{
        simplex::{
            scheme::bls12381_threshold,
            types::{Finalization, Finalize, Notarization, Notarize, Proposal},
        },
        types::{Round, View},
        Viewable,
    };
    use commonware_cryptography::{
        bls12381::primitives::variant::MinSig, certificate::mocks::Fixture, Digestible, Hasher,
        Sha256,
    };
    use rand::{rngs::StdRng, SeedableRng};
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    /// Test context containing common setup for simulator tests.
    struct TestContext {
        schemes: Vec<Scheme>,
        client: Client,
    }

    impl TestContext {
        /// Create a new test context with a running server and client.
        async fn new() -> Self {
            let mut rng = StdRng::seed_from_u64(0);
            let Fixture { schemes, .. } = bls12381_threshold::fixture::<MinSig, _>(&mut rng, 4);
            let identity = *schemes[0].polynomial().public();

            let (addr, _) = start_server(schemes[0].clone()).await;
            let client = Client::new(&format!("http://{addr}"), identity);

            Self { schemes, client }
        }

        /// Create a test block with standard parameters.
        fn test_block(&self) -> Block {
            Block::new(Sha256::hash(b"genesis"), 1, 1000)
        }

        /// Create a proposal for the given block at view 1.
        fn proposal(&self, block: &Block) -> Proposal<commonware_cryptography::sha256::Digest> {
            Proposal::new(
                Round::new(EPOCH, View::new(1)),
                View::new(0),
                block.digest(),
            )
        }

        /// Create a seed by first creating a notarization.
        fn seed(&self) -> Seed {
            let block = self.test_block();
            let proposal = self.proposal(&block);
            create_notarization(&self.schemes, proposal).seed()
        }

        /// Create a notarized block.
        fn notarized(&self) -> Notarized {
            let block = self.test_block();
            let proposal = self.proposal(&block);
            Notarized::new(create_notarization(&self.schemes, proposal), block)
        }

        /// Create a finalized block.
        fn finalized(&self) -> Finalized {
            let block = self.test_block();
            let proposal = self.proposal(&block);
            Finalized::new(create_finalization(&self.schemes, proposal), block)
        }
    }

    fn create_notarization(
        schemes: &[Scheme],
        proposal: Proposal<commonware_cryptography::sha256::Digest>,
    ) -> alto_types::Notarization {
        let notarizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, NAMESPACE, proposal.clone()).unwrap())
            .collect();
        Notarization::from_notarizes(&schemes[0], &notarizes).unwrap()
    }

    fn create_finalization(
        schemes: &[Scheme],
        proposal: Proposal<commonware_cryptography::sha256::Digest>,
    ) -> alto_types::Finalization {
        let finalizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Finalize::sign(scheme, NAMESPACE, proposal.clone()).unwrap())
            .collect();
        Finalization::from_finalizes(&schemes[0], &finalizes).unwrap()
    }

    async fn start_server(scheme: Scheme) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let simulator = Arc::new(Simulator::new(scheme));
        let api = Api::new(simulator);
        let app = api.router();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Give the server a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        (addr, handle)
    }

    fn fixture(seed: u64) -> (Vec<Scheme>, Identity) {
        let mut rng = StdRng::seed_from_u64(seed);
        let Fixture { schemes, .. } = bls12381_threshold::fixture::<MinSig, _>(&mut rng, 4);
        let identity = *schemes[0].polynomial().public();
        (schemes, identity)
    }

    #[tokio::test]
    async fn test_seed_operations() {
        let ctx = TestContext::new().await;
        let seed = ctx.seed();

        ctx.client.seed_upload(seed.clone()).await.unwrap();

        let retrieved = ctx.client.seed_get(IndexQuery::Latest).await.unwrap();
        assert_eq!(retrieved.view(), seed.view());

        let retrieved = ctx.client.seed_get(IndexQuery::Index(1)).await.unwrap();
        assert_eq!(retrieved.view().get(), 1);
    }

    #[tokio::test]
    async fn test_notarization_operations() {
        let ctx = TestContext::new().await;
        let notarized = ctx.notarized();

        ctx.client.notarized_upload(notarized).await.unwrap();

        let retrieved = ctx.client.notarized_get(IndexQuery::Latest).await.unwrap();
        assert_eq!(retrieved.proof.view().get(), 1);

        let retrieved = ctx
            .client
            .notarized_get(IndexQuery::Index(1))
            .await
            .unwrap();
        assert_eq!(retrieved.proof.view().get(), 1);
    }

    #[tokio::test]
    async fn test_finalization_operations() {
        let ctx = TestContext::new().await;
        let finalized = ctx.finalized();

        ctx.client.finalized_upload(finalized).await.unwrap();

        let retrieved = ctx.client.finalized_get(IndexQuery::Latest).await.unwrap();
        assert_eq!(retrieved.proof.view().get(), 1);

        let retrieved = ctx
            .client
            .finalized_get(IndexQuery::Index(1))
            .await
            .unwrap();
        assert_eq!(retrieved.proof.view().get(), 1);
    }

    #[tokio::test]
    async fn test_block_retrieval() {
        let ctx = TestContext::new().await;
        let block = ctx.test_block();
        let finalized = ctx.finalized();

        ctx.client.finalized_upload(finalized).await.unwrap();

        // Test retrieval by latest
        let payload = ctx.client.block_get(Query::Latest).await.unwrap();
        match payload {
            alto_client::consensus::Payload::Finalized(f) => {
                assert_eq!(f.block.height, 1);
            }
            _ => panic!("Expected finalized block"),
        }

        // Test retrieval by index
        let payload = ctx.client.block_get(Query::Index(1)).await.unwrap();
        match payload {
            alto_client::consensus::Payload::Finalized(f) => {
                assert_eq!(f.block.height, 1);
            }
            _ => panic!("Expected finalized block"),
        }

        // Test retrieval by digest
        let payload = ctx
            .client
            .block_get(Query::Digest(block.digest()))
            .await
            .unwrap();
        match payload {
            alto_client::consensus::Payload::Block(b) => {
                assert_eq!(b.digest(), block.digest());
            }
            _ => panic!("Expected block"),
        }
    }

    #[tokio::test]
    async fn test_websocket_streaming() {
        let ctx = TestContext::new().await;
        let seed = ctx.seed();

        let mut stream = ctx.client.listen().await.unwrap();

        // Submit the seed while listening
        let client = ctx.client.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            client.seed_upload(seed).await.unwrap();
        });

        // Wait for the seed message
        use futures::StreamExt;
        if let Some(Ok(msg)) = stream.next().await {
            match msg {
                alto_client::consensus::Message::Seed(s) => {
                    assert_eq!(s.view().get(), 1);
                }
                _ => panic!("Expected seed message"),
            }
        } else {
            panic!("Expected to receive a message");
        }
    }

    #[tokio::test]
    async fn test_identity_verification() {
        // Create two different fixtures
        let (schemes1, _) = fixture(0);
        let (_, identity2) = fixture(1);

        // Start server with schemes1, but create client expecting identity2
        let (addr, _handle) = start_server(schemes1[0].clone()).await;
        let client = Client::new(&format!("http://{addr}"), identity2);

        // Create a seed signed by schemes1
        let block = Block::new(Sha256::hash(b"genesis"), 1, 1000);
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(1)),
            View::new(0),
            block.digest(),
        );
        let seed = create_notarization(&schemes1, proposal).seed();

        // Server accepts it (signed by schemes1, which server uses)
        client.seed_upload(seed).await.unwrap();

        // Client fails to verify (expects identity2 but seed is signed by schemes1)
        let result = client.seed_get(IndexQuery::Latest).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_signature_rejection() {
        let ctx = TestContext::new().await;

        // Create different schemes (wrong ones)
        let (wrong_schemes, _) = fixture(1);

        // Create a seed with wrong schemes
        let block = ctx.test_block();
        let proposal = ctx.proposal(&block);
        let bad_seed = create_notarization(&wrong_schemes, proposal).seed();

        // Server rejects it (signature doesn't match server's identity)
        let result = ctx.client.seed_upload(bad_seed).await;
        assert!(result.is_err());
    }
}
