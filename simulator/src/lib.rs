use alto_types::{Block, Finalized, Identity, Kind, Notarized, Seed, NAMESPACE};
use axum::{
    body::Bytes,
    extract::{ws::WebSocketUpgrade, Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use commonware_codec::{DecodeExt, Encode};
use commonware_consensus::Viewable;
use commonware_cryptography::Digestible;
use commonware_utils::from_hex;
use futures::{SinkExt, StreamExt};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use tokio::sync::broadcast;

const LATEST: &str = "latest";

#[derive(Default)]
pub struct State {
    seeds: BTreeMap<u64, Seed>,
    notarizations: BTreeMap<u64, Notarized>,
    finalizations: BTreeMap<u64, Finalized>,
    blocks_by_digest: BTreeMap<commonware_cryptography::sha256::Digest, Block>,
}

#[derive(Clone)]
pub struct Simulator {
    identity: Identity,
    state: Arc<RwLock<State>>,
    consensus_tx: broadcast::Sender<Vec<u8>>,
}

impl Simulator {
    pub fn new(identity: Identity) -> Self {
        let (consensus_tx, _) = broadcast::channel(1024);
        let state = Arc::new(RwLock::new(State::default()));

        Self {
            identity,
            state,
            consensus_tx,
        }
    }

    pub fn submit_seed(&self, seed: Seed) -> Result<(), &'static str> {
        // Verify signature with identity
        if !seed.verify(NAMESPACE, &self.identity) {
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
            state.seeds.get(&index).cloned()
        }
    }

    pub fn submit_notarization(&self, notarized: Notarized) -> Result<(), &'static str> {
        // Verify signature with identity
        if !notarized.verify(NAMESPACE, &self.identity) {
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
            state.notarizations.get(&index).cloned()
        }
    }

    pub fn submit_finalization(&self, finalized: Finalized) -> Result<(), &'static str> {
        // Verify signature with identity
        if !finalized.verify(NAMESPACE, &self.identity) {
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
            state.finalizations.get(&index).cloned()
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
            .route("/seed", post(seed_upload))
            .route("/seed/:query", get(seed_get))
            .route("/notarization", post(notarization_upload))
            .route("/notarization/:query", get(notarization_get))
            .route("/finalization", post(finalization_upload))
            .route("/finalization/:query", get(finalization_get))
            .route("/block/:query", get(block_get))
            .route("/consensus/ws", get(consensus_ws))
            .with_state(self.simulator.clone())
    }
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
            .send(axum::extract::ws::Message::Binary(data))
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
    use commonware_consensus::{
        threshold_simplex::types::{
            seed_namespace, view_message, Finalization, Finalize, Notarization, Notarize, Proposal,
        },
        Viewable,
    };
    use commonware_cryptography::{
        bls12381::{
            dkg::ops as dkg_ops,
            primitives::{
                group::{Private, Share},
                ops,
                ops::threshold_signature_recover,
                poly,
                variant::MinSig,
            },
        },
        hash, Digestible,
    };
    use rand::{rngs::StdRng, SeedableRng};
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    fn create_test_seed(master_secret: &Private, view: u64) -> Seed {
        let seed_namespace = seed_namespace(NAMESPACE);
        let message = view_message(view);
        Seed::new(
            view,
            ops::sign_message::<MinSig>(master_secret, Some(&seed_namespace), &message),
        )
    }

    fn create_notarization(
        shares: &[Share],
        proposal: Proposal<commonware_cryptography::sha256::Digest>,
    ) -> Notarization<MinSig, commonware_cryptography::sha256::Digest> {
        let partials = shares
            .iter()
            .map(|share| Notarize::<MinSig, _>::sign(NAMESPACE, share, proposal.clone()))
            .collect::<Vec<_>>();

        let proposal_partials = partials
            .iter()
            .map(|partial| partial.proposal_signature.clone())
            .collect::<Vec<_>>();
        let proposal_recovered =
            threshold_signature_recover::<MinSig, _>(3, &proposal_partials).unwrap();

        let seed_partials = partials
            .into_iter()
            .map(|partial| partial.seed_signature)
            .collect::<Vec<_>>();
        let seed_recovered = threshold_signature_recover::<MinSig, _>(3, &seed_partials).unwrap();

        Notarization::new(proposal, proposal_recovered, seed_recovered)
    }

    fn create_finalization(
        shares: &[Share],
        proposal: Proposal<commonware_cryptography::sha256::Digest>,
    ) -> Finalization<MinSig, commonware_cryptography::sha256::Digest> {
        let partials = shares
            .iter()
            .map(|share| Notarize::<MinSig, _>::sign(NAMESPACE, share, proposal.clone()))
            .collect::<Vec<_>>();
        let seed_partials = partials
            .into_iter()
            .map(|partial| partial.seed_signature)
            .collect::<Vec<_>>();
        let seed_recovered = threshold_signature_recover::<MinSig, _>(3, &seed_partials).unwrap();

        let finalize_partials = shares
            .iter()
            .map(|share| Finalize::<MinSig, _>::sign(NAMESPACE, share, proposal.clone()))
            .collect::<Vec<_>>();
        let finalize_partials = finalize_partials
            .into_iter()
            .map(|partial| partial.proposal_signature)
            .collect::<Vec<_>>();
        let finalize_recovered =
            threshold_signature_recover::<MinSig, _>(3, &finalize_partials).unwrap();

        Finalization::new(proposal, finalize_recovered, seed_recovered)
    }

    async fn start_test_server(identity: Identity) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let simulator = Arc::new(Simulator::new(identity));
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

    #[tokio::test]
    async fn test_seed_operations() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let (master_secret, identity) = ops::keypair::<_, MinSig>(&mut rng);

        // Start server
        let (addr, _handle) = start_test_server(identity).await;
        let client = Client::new(&format!("http://{addr}"), identity);

        // Test seed upload and retrieval
        let seed = create_test_seed(&master_secret, 1);
        client.seed_upload(seed.clone()).await.unwrap();

        let retrieved = client.seed_get(IndexQuery::Latest).await.unwrap();
        assert_eq!(retrieved.view(), seed.view());

        let retrieved = client.seed_get(IndexQuery::Index(1)).await.unwrap();
        assert_eq!(retrieved.view(), 1);
    }

    #[tokio::test]
    async fn test_notarization_operations() {
        // Create network key for identity (what the simulator checks)
        let mut rng = StdRng::seed_from_u64(0);
        let (polynomial, shares) = dkg_ops::generate_shares::<_, MinSig>(&mut rng, None, 4, 3);
        let identity = *poly::public::<MinSig>(&polynomial);

        // Start server
        let (addr, _handle) = start_test_server(identity).await;
        let client = Client::new(&format!("http://{addr}"), identity);

        // Test notarization
        let block = Block::new(hash(b"genesis"), 1, 1000);
        let proposal = Proposal::new(1, 0, block.digest());
        let notarization = create_notarization(&shares, proposal);
        let notarized = Notarized::new(notarization, block);

        client.notarized_upload(notarized.clone()).await.unwrap();

        let retrieved = client.notarized_get(IndexQuery::Latest).await.unwrap();
        assert_eq!(retrieved.proof.view(), 1);

        let retrieved = client.notarized_get(IndexQuery::Index(1)).await.unwrap();
        assert_eq!(retrieved.proof.view(), 1);
    }

    #[tokio::test]
    async fn test_finalization_operations() {
        // Create network key for identity (what the simulator checks)
        let mut rng = StdRng::seed_from_u64(0);
        let (polynomial, shares) = dkg_ops::generate_shares::<_, MinSig>(&mut rng, None, 4, 3);
        let identity = *poly::public::<MinSig>(&polynomial);

        // Start server
        let (addr, _handle) = start_test_server(identity).await;
        let client = Client::new(&format!("http://{addr}"), identity);

        // Test finalization
        let block = Block::new(hash(b"genesis"), 1, 1000);
        let proposal = Proposal::new(1, 0, block.digest());
        let finalization = create_finalization(&shares, proposal);
        let finalized = Finalized::new(finalization, block);

        client.finalized_upload(finalized.clone()).await.unwrap();

        let retrieved = client.finalized_get(IndexQuery::Latest).await.unwrap();
        assert_eq!(retrieved.proof.view(), 1);

        let retrieved = client.finalized_get(IndexQuery::Index(1)).await.unwrap();
        assert_eq!(retrieved.proof.view(), 1);
    }

    #[tokio::test]
    async fn test_block_retrieval() {
        // Create network key for identity (what the simulator checks)
        let mut rng = StdRng::seed_from_u64(0);
        let (polynomial, shares) = dkg_ops::generate_shares::<_, MinSig>(&mut rng, None, 4, 3);
        let identity = *poly::public::<MinSig>(&polynomial);

        // Start server
        let (addr, _handle) = start_test_server(identity).await;
        let client = Client::new(&format!("http://{addr}"), identity);

        // Submit finalized block
        let block = Block::new(hash(b"genesis"), 1, 1000);
        let proposal = Proposal::new(1, 0, block.digest());
        let finalization = create_finalization(&shares, proposal);
        let finalized = Finalized::new(finalization, block.clone());

        client.finalized_upload(finalized).await.unwrap();

        // Test retrieval by latest
        let payload = client.block_get(Query::Latest).await.unwrap();
        match payload {
            alto_client::consensus::Payload::Finalized(f) => {
                assert_eq!(f.block.height, 1);
            }
            _ => panic!("Expected finalized block"),
        }

        // Test retrieval by index
        let payload = client.block_get(Query::Index(1)).await.unwrap();
        match payload {
            alto_client::consensus::Payload::Finalized(f) => {
                assert_eq!(f.block.height, 1);
            }
            _ => panic!("Expected finalized block"),
        }

        // Test retrieval by digest
        let payload = client
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
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let (master_secret, identity) = ops::keypair::<_, MinSig>(&mut rng);

        // Start server
        let (addr, _handle) = start_test_server(identity).await;
        let client = Client::new(&format!("http://{addr}"), identity);

        // Start listening
        let mut stream = client.listen().await.unwrap();

        // Submit a seed while listening
        let seed = create_test_seed(&master_secret, 1);
        let client_clone = client.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            client_clone.seed_upload(seed).await.unwrap();
        });

        // Wait for the seed message
        use futures::StreamExt;
        if let Some(Ok(msg)) = stream.next().await {
            match msg {
                alto_client::consensus::Message::Seed(s) => {
                    assert_eq!(s.view(), 1);
                }
                _ => panic!("Expected seed message"),
            }
        } else {
            panic!("Expected to receive a message");
        }
    }

    #[tokio::test]
    async fn test_identity_verification() {
        // Create two different identities
        let mut rng = StdRng::seed_from_u64(0);
        let (master_secret1, identity1) = ops::keypair::<_, MinSig>(&mut rng);
        let (_, identity2) = ops::keypair::<_, MinSig>(&mut rng);

        // Start server with identity1
        let (addr, _handle) = start_test_server(identity1).await;

        // Create client with identity2 (different from server)
        let client = Client::new(&format!("http://{addr}"), identity2);

        // Upload a seed signed by identity1 (server will accept it)
        let seed = create_test_seed(&master_secret1, 1);
        client.seed_upload(seed).await.unwrap(); // This succeeds - server accepts it

        // Try to retrieve the seed - client will fail to verify since it expects identity2
        let result = client.seed_get(IndexQuery::Latest).await;
        assert!(result.is_err()); // Fails because client expects identity2 but seed is signed by identity1
    }

    #[tokio::test]
    async fn test_invalid_signature_rejection() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let (_, identity) = ops::keypair::<_, MinSig>(&mut rng);

        // Create a different master secret (wrong one)
        let (wrong_master_secret, _) = ops::keypair::<_, MinSig>(&mut rng);

        // Start server
        let (addr, _handle) = start_test_server(identity).await;
        let client = Client::new(&format!("http://{addr}"), identity);

        // Create seed with wrong signature
        let bad_seed = create_test_seed(&wrong_master_secret, 1);

        // This should fail at the server because the signature doesn't match the identity
        let result = client.seed_upload(bad_seed).await;
        assert!(result.is_err());
    }
}
