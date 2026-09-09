use alto_client::{max_upload_size, DEFAULT_MAX_BLOCK_SIZE, LATEST};
use alto_types::{Block, Finalized, Kind, Notarized, Scheme, Seed};
use axum::{
    body::Bytes,
    extract::{ws::WebSocketUpgrade, DefaultBodyLimit, Path, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use commonware_codec::{Decode, DecodeExt, Encode, EncodeSize, FixedSize, RangeCfg, Write};
use commonware_consensus::{types::View, Viewable};
use commonware_cryptography::{sha256::Digest, Digestible};
use commonware_formatting::from_hex;
use commonware_parallel::Strategy;
use futures::{SinkExt, StreamExt};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;

/// Capacity of the consensus broadcast channel feeding WebSocket subscribers.
const CONSENSUS_CHANNEL_CAPACITY: usize = 1024;

pub struct State<C: Scheme> {
    seeds: BTreeMap<View, Seed>,
    notarizations: BTreeMap<View, Notarized<C>>,
    finalizations: BTreeMap<View, Finalized<C>>,
    finalized_height_to_view: BTreeMap<u64, View>,
    blocks_by_digest: BTreeMap<Digest, Block>,
}

impl<C: Scheme> Default for State<C> {
    fn default() -> Self {
        Self {
            seeds: BTreeMap::new(),
            notarizations: BTreeMap::new(),
            finalizations: BTreeMap::new(),
            finalized_height_to_view: BTreeMap::new(),
            blocks_by_digest: BTreeMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct Indexer<C: Scheme, S: Strategy> {
    scheme: C,
    state: Arc<RwLock<State<C>>>,
    consensus_tx: broadcast::Sender<Bytes>,
    strategy: S,
    block_codec_config: RangeCfg<usize>,
    max_upload_size: usize,
}

impl<C: Scheme, S: Strategy> Indexer<C, S> {
    pub fn new(scheme: C, strategy: S) -> Self {
        let (consensus_tx, _) = broadcast::channel(CONSENSUS_CHANNEL_CAPACITY);
        let state = Arc::new(RwLock::new(State::default()));
        let block_size =
            u32::try_from(DEFAULT_MAX_BLOCK_SIZE).expect("default block size exceeds u32");
        let max_upload_size = max_upload_size(block_size, &scheme);

        Self {
            scheme,
            state,
            consensus_tx,
            strategy,
            block_codec_config: Block::codec_config(block_size),
            max_upload_size,
        }
    }

    /// Configure HTTP upload decoding to reject payloads larger than `block_size` and size the
    /// request body limit accordingly.
    pub fn with_block_size(mut self, block_size: u32) -> Self {
        self.block_codec_config = Block::codec_config(block_size);
        self.max_upload_size = max_upload_size(block_size, &self.scheme);
        self
    }

    /// Codec configuration used to decode uploaded blocks.
    pub fn block_codec_config(&self) -> &RangeCfg<usize> {
        &self.block_codec_config
    }

    /// Largest request body accepted by the upload endpoints.
    pub fn max_upload_size(&self) -> usize {
        self.max_upload_size
    }

    pub fn submit_seed(&self, seed: Seed) -> Result<(), &'static str> {
        // Several validators can upload the same seed. Skip signature checks for known views.
        if self.state.read().unwrap().seeds.contains_key(&seed.view()) {
            return Ok(());
        }

        // Verify signature with identity
        if !self.scheme.verify_seed(&seed) {
            return Err("Invalid seed signature");
        }

        let mut state = self.state.write().unwrap();
        if state.seeds.insert(seed.view(), seed.clone()).is_some() {
            return Ok(()); // Already exists
        }

        // Broadcast seed
        let mut data = vec![0u8; u8::SIZE + seed.encode_size()];
        data[0] = Kind::Seed as u8;
        seed.write(&mut data[1..].as_mut());
        let _ = self.consensus_tx.send(data.into());
        Ok(())
    }

    pub fn get_seed(&self, query: &str) -> Option<Seed> {
        let state = self.state.read().unwrap();
        if query == LATEST {
            state.seeds.last_key_value().map(|(_, seed)| seed.clone())
        } else {
            // Parse as hex-encoded index
            let raw = from_hex(query)?;
            let index = u64::decode(raw.as_slice()).ok()?;
            state.seeds.get(&View::new(index)).cloned()
        }
    }

    pub fn submit_notarization(&self, notarized: Notarized<C>) -> Result<(), &'static str> {
        // Several validators can upload the same certificate. Skip signature checks for known views.
        let view = notarized.proof.view();
        if self.state.read().unwrap().notarizations.contains_key(&view) {
            return Ok(());
        }

        // Verify signature with identity
        if !notarized.verify(&self.scheme, &self.strategy) {
            return Err("Invalid notarization signature");
        }

        let mut state = self.state.write().unwrap();

        // Store notarization
        if state
            .notarizations
            .insert(view, notarized.clone())
            .is_some()
        {
            return Ok(()); // Already exists
        }
        state
            .blocks_by_digest
            .insert(notarized.block.digest(), notarized.block.clone());

        // Broadcast notarization
        let mut data = vec![0u8; u8::SIZE + notarized.encode_size()];
        data[0] = Kind::Notarization as u8;
        notarized.write(&mut data[1..].as_mut());
        let _ = self.consensus_tx.send(data.into());
        Ok(())
    }

    pub fn get_notarization(&self, query: &str) -> Option<Notarized<C>> {
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

    pub fn submit_finalization(&self, finalized: Finalized<C>) -> Result<(), &'static str> {
        // Several validators can upload the same certificate. Skip signature checks for known views.
        let view = finalized.proof.view();
        if self.state.read().unwrap().finalizations.contains_key(&view) {
            return Ok(());
        }

        // Verify signature with identity
        if !finalized.verify(&self.scheme, &self.strategy) {
            return Err("Invalid finalization signature");
        }

        let mut state = self.state.write().unwrap();

        // Store finalization
        if state
            .finalizations
            .insert(view, finalized.clone())
            .is_some()
        {
            return Ok(()); // Already exists
        }
        state
            .finalized_height_to_view
            .insert(finalized.block.height.get(), view);
        state
            .blocks_by_digest
            .insert(finalized.block.digest(), finalized.block.clone());

        // Broadcast finalization
        let mut data = vec![0u8; u8::SIZE + finalized.encode_size()];
        data[0] = Kind::Finalization as u8;
        finalized.write(&mut data[1..].as_mut());
        let _ = self.consensus_tx.send(data.into());
        Ok(())
    }

    pub fn get_finalization(&self, query: &str) -> Option<Finalized<C>> {
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

    pub fn get_block(&self, query: &str) -> Option<BlockResult<C>> {
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
                // Try to parse as digest
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

    pub fn submit_block(&self, block: Block) {
        // Store block by digest (no guarantee this is part of the canonical chain)
        let mut state = self.state.write().unwrap();
        state.blocks_by_digest.insert(block.digest(), block);
    }

    pub fn consensus_subscriber(&self) -> broadcast::Receiver<Bytes> {
        self.consensus_tx.subscribe()
    }
}

#[allow(clippy::large_enum_variant)]
pub enum BlockResult<C: Scheme> {
    Block(Block),
    Finalized(Finalized<C>),
}

pub struct Api<C: Scheme, S: Strategy> {
    indexer: Arc<Indexer<C, S>>,
}

impl<C: Scheme, S: Strategy> Api<C, S> {
    pub fn new(indexer: Arc<Indexer<C, S>>) -> Self {
        Self { indexer }
    }

    pub fn router(self) -> Router {
        let max_upload_size = self.indexer.max_upload_size();
        Router::new()
            .route("/health", get(health_check))
            .route("/seed", post(seed_upload))
            .route("/seed/{query}", get(seed_get))
            .route("/notarization", post(notarization_upload))
            .route("/notarization/{query}", get(notarization_get))
            .route("/finalization", post(finalization_upload))
            .route("/finalization/{query}", get(finalization_get))
            .route("/block", post(block_upload))
            .route("/block/{query}", get(block_get))
            .route("/consensus/ws", get(consensus_ws))
            .layer(DefaultBodyLimit::max(max_upload_size))
            .layer(CorsLayer::permissive())
            .with_state(self.indexer)
    }
}

async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn seed_upload<C: Scheme, S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<C, S>>>,
    body: Bytes,
) -> impl IntoResponse {
    match Seed::decode(&mut body.as_ref()) {
        Ok(seed) => match indexer.submit_seed(seed) {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::UNAUTHORIZED,
        },
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

async fn seed_get<C: Scheme, S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<C, S>>>,
    Path(query): Path<String>,
) -> impl IntoResponse {
    match indexer.get_seed(&query) {
        Some(seed) => (StatusCode::OK, seed.encode().to_vec()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn notarization_upload<C: Scheme, S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<C, S>>>,
    body: Bytes,
) -> impl IntoResponse {
    match Notarized::<C>::decode_cfg(body.as_ref(), indexer.block_codec_config()) {
        Ok(notarized) => match indexer.submit_notarization(notarized) {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::UNAUTHORIZED,
        },
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

async fn notarization_get<C: Scheme, S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<C, S>>>,
    Path(query): Path<String>,
) -> impl IntoResponse {
    match indexer.get_notarization(&query) {
        Some(notarized) => (StatusCode::OK, notarized.encode().to_vec()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn finalization_upload<C: Scheme, S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<C, S>>>,
    body: Bytes,
) -> impl IntoResponse {
    match Finalized::<C>::decode_cfg(body.as_ref(), indexer.block_codec_config()) {
        Ok(finalized) => match indexer.submit_finalization(finalized) {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::UNAUTHORIZED,
        },
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

async fn finalization_get<C: Scheme, S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<C, S>>>,
    Path(query): Path<String>,
) -> impl IntoResponse {
    match indexer.get_finalization(&query) {
        Some(finalized) => (StatusCode::OK, finalized.encode().to_vec()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn block_upload<C: Scheme, S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<C, S>>>,
    body: Bytes,
) -> impl IntoResponse {
    match Block::decode_cfg(body.as_ref(), indexer.block_codec_config()) {
        Ok(block) => {
            indexer.submit_block(block);
            StatusCode::OK
        }
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

async fn block_get<C: Scheme, S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<C, S>>>,
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

async fn consensus_ws<C: Scheme, S: Strategy>(
    AxumState(indexer): AxumState<Arc<Indexer<C, S>>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_consensus_ws(socket, indexer))
}

async fn handle_consensus_ws<C: Scheme, S: Strategy>(
    socket: axum::extract::ws::WebSocket,
    indexer: Arc<Indexer<C, S>>,
) {
    let (mut sender, _receiver) = socket.split();
    let mut consensus = indexer.consensus_subscriber();

    loop {
        let data = match consensus.recv().await {
            Ok(data) => data,
            // Keep streaming from the current position when a slow subscriber misses artifacts.
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::debug!(skipped, "consensus subscriber lagged");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        };
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
    use alto_client::{Client, ClientBuilder, IndexQuery, Query};
    use alto_types::{Context, Identity, Seedable, StandardScheme, VrfScheme, EPOCH, NAMESPACE};
    use commonware_consensus::{
        simplex::{
            scheme::bls12381_threshold::{standard, vrf as bls12381_threshold},
            types::{Finalization, Finalize, Notarization, Notarize, Proposal},
        },
        types::{Epoch, Height, Round, View},
        Viewable,
    };
    use commonware_cryptography::{
        bls12381::primitives::variant::MinSig, certificate::mocks::Fixture, ed25519, sha256,
        Digest, Digestible, Hasher, Sha256, Signer,
    };
    use commonware_parallel::Sequential;
    use commonware_utils::{non_empty, test_rng};
    use futures::StreamExt;
    use rand::{rngs::StdRng, SeedableRng};
    use rcgen::{generate_simple_self_signed, CertifiedKey, KeyPair};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;
    use tower::ServiceExt;

    /// Test context containing common setup for indexer tests.
    struct TestContext {
        schemes: Vec<VrfScheme>,
        client: Client<Sequential, VrfScheme>,
    }

    impl TestContext {
        /// Create a new test context with a running server and client.
        async fn new() -> Self {
            let (schemes, identity) = fixture(0);

            let (addr, _) = start_server(schemes[0].clone(), Sequential).await;
            let client = ClientBuilder::new(
                &format!("http://{addr}"),
                VrfScheme::certificate_verifier(NAMESPACE, identity),
                Sequential,
            )
            .build();
            wait_for_ready(&client).await;

            Self { schemes, client }
        }

        /// Create a test block with standard parameters.
        fn test_block(&self) -> Block {
            let context = Context {
                round: Round::new(EPOCH, View::new(1)),
                leader: ed25519::PrivateKey::from_seed(0).public_key(),
                parent: (View::new(0), sha256::Digest::EMPTY),
            };
            Block::new(
                context,
                Sha256::hash(&[b"genesis"]),
                Height::new(1),
                1000,
                Bytes::new(),
            )
        }

        /// Create a proposal for the given block at view 1.
        fn proposal(&self, block: &Block) -> Proposal<sha256::Digest> {
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
            create_notarization(&self.schemes, proposal).seed().unwrap()
        }

        /// Create a notarized block.
        fn notarized(&self) -> Notarized<VrfScheme> {
            let block = self.test_block();
            let proposal = self.proposal(&block);
            Notarized::new(create_notarization(&self.schemes, proposal), block)
        }

        /// Create a finalized block.
        fn finalized(&self) -> Finalized<VrfScheme> {
            let block = self.test_block();
            let proposal = self.proposal(&block);
            Finalized::new(create_finalization(&self.schemes, proposal), block)
        }
    }

    /// Build a finalized block at `view` (height == view) carrying `payload`.
    fn finalized_with_payload<C: Scheme>(schemes: &[C], view: u64, payload: Bytes) -> Finalized<C> {
        let context = Context {
            round: Round::new(EPOCH, View::new(view)),
            leader: ed25519::PrivateKey::from_seed(0).public_key(),
            parent: (View::new(view - 1), sha256::Digest::EMPTY),
        };
        let block = Block::new(
            context,
            Sha256::hash(&[format!("parent-{view}").as_bytes()]),
            Height::new(view),
            view * 1_000,
            payload,
        );
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(view)),
            View::new(view - 1),
            block.digest(),
        );
        Finalized::new(create_finalization(schemes, proposal), block)
    }

    #[test]
    fn default_block_size_bounds_payload_decoding() {
        let (schemes, _) = fixture(0);
        let indexer = Indexer::new(schemes[0].clone(), Sequential);
        for size in [DEFAULT_MAX_BLOCK_SIZE, DEFAULT_MAX_BLOCK_SIZE + 1] {
            let finalized = finalized_with_payload(&schemes, 1, vec![0; size].into());
            let encoded = finalized.encode();
            assert!(encoded.len() <= indexer.max_upload_size());
            assert_eq!(
                Finalized::<VrfScheme>::decode_cfg(encoded, indexer.block_codec_config()).is_ok(),
                size == DEFAULT_MAX_BLOCK_SIZE,
            );
        }
    }

    #[test]
    fn block_size_bounds_uploads() {
        let (schemes, _) = fixture(0);
        let finalized = finalized_with_payload(&schemes, 1, Bytes::new());
        let encoded = finalized.encode();

        let default = Indexer::new(schemes[0].clone(), Sequential);
        assert!(
            Finalized::<VrfScheme>::decode_cfg(encoded.clone(), default.block_codec_config())
                .is_ok()
        );
        assert_eq!(
            default.max_upload_size(),
            max_upload_size(DEFAULT_MAX_BLOCK_SIZE as u32, &schemes[0])
        );

        let exact = Indexer::new(schemes[0].clone(), Sequential).with_block_size(0);
        assert!(
            Finalized::<VrfScheme>::decode_cfg(encoded.clone(), exact.block_codec_config()).is_ok()
        );
        assert_eq!(exact.max_upload_size(), max_upload_size(0, &schemes[0]));

        // A block larger than the configured size fails to decode, before any verification.
        let oversized = finalized_with_payload(&schemes, 1, Bytes::from_static(&[1, 2]));
        let bounded = Indexer::new(schemes[0].clone(), Sequential).with_block_size(1);
        assert!(Finalized::<VrfScheme>::decode_cfg(
            oversized.encode(),
            bounded.block_codec_config()
        )
        .is_err());
        assert!(Finalized::<VrfScheme>::decode_cfg(
            oversized.encode(),
            Indexer::new(schemes[0].clone(), Sequential)
                .with_block_size(2)
                .block_codec_config()
        )
        .is_ok());
        assert_eq!(bounded.max_upload_size(), max_upload_size(1, &schemes[0]));
    }

    fn create_notarization<C: Scheme>(
        schemes: &[C],
        proposal: Proposal<sha256::Digest>,
    ) -> alto_types::Notarization<C> {
        let notarizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        Notarization::from_notarizes(&schemes[0], non_empty![@&notarizes], &Sequential).unwrap()
    }

    fn create_finalization<C: Scheme>(
        schemes: &[C],
        proposal: Proposal<sha256::Digest>,
    ) -> alto_types::Finalization<C> {
        let finalizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Finalize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        Finalization::from_finalizes(&schemes[0], non_empty![@&finalizes], &Sequential).unwrap()
    }

    async fn maximum_uploads_stream<C: Scheme>(schemes: &[C]) {
        for block_size in [0, 127, 128, 16_383, 16_384, 2_097_151, 2_097_152] {
            let mut context = Block::genesis().context;
            context.round = Round::new(Epoch::new(u64::MAX), View::new(u64::MAX));
            context.parent.0 = View::new(u64::MAX - 1);
            let block = Block::new(
                context,
                sha256::Digest::EMPTY,
                Height::new(u64::MAX),
                u64::MAX,
                vec![0; block_size as usize].into(),
            );
            let proposal =
                Proposal::new(block.context.round, block.context.parent.0, block.digest());
            let notarized = Notarized::new(
                create_notarization(schemes, proposal.clone()),
                block.clone(),
            );
            let finalized = Finalized::new(create_finalization(schemes, proposal), block.clone());
            let limit = max_upload_size(block_size, &schemes[0]);
            assert!(block.encode().len() <= limit);
            assert_eq!(notarized.encode().len(), limit);
            assert_eq!(finalized.encode().len(), limit);
            let seed = notarized.proof.seed();
            if let Some(seed) = &seed {
                assert_eq!(seed.encode().len(), 68);
                assert!(seed.encode().len() <= limit);
            }

            let indexer =
                Arc::new(Indexer::new(schemes[0].clone(), Sequential).with_block_size(block_size));
            assert_eq!(indexer.max_upload_size(), limit);
            let app = Api::new(indexer.clone()).router();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let client =
                ClientBuilder::new(&format!("http://{addr}"), schemes[0].clone(), Sequential)
                    .with_block_size(block_size)
                    .build();
            let mut stream = client.listen().await.unwrap();
            while indexer.consensus_tx.receiver_count() == 0 {
                tokio::task::yield_now().await;
            }
            client.block_upload(&block).await.unwrap();
            client
                .notarized_upload(&notarized.proof, &notarized.block)
                .await
                .unwrap();
            assert!(matches!(
                tokio::time::timeout(std::time::Duration::from_secs(5), stream.next()).await.unwrap().unwrap().unwrap(),
                alto_client::consensus::Message::Notarization(received) if received == notarized
            ));
            client
                .finalized_upload(&finalized.proof, &finalized.block)
                .await
                .unwrap();
            assert!(matches!(
                tokio::time::timeout(std::time::Duration::from_secs(5), stream.next()).await.unwrap().unwrap().unwrap(),
                alto_client::consensus::Message::Finalization(received) if received == finalized
            ));
            if let Some(seed) = seed {
                client.seed_upload(&seed).await.unwrap();
                assert!(matches!(
                    tokio::time::timeout(std::time::Duration::from_secs(5), stream.next()).await.unwrap().unwrap().unwrap(),
                    alto_client::consensus::Message::Seed(received) if received == seed
                ));
            }
            server.abort();
        }
    }

    #[tokio::test]
    async fn maximum_standard_uploads_stream() {
        let fixture = standard::fixture::<MinSig, _>(&mut test_rng(), NAMESPACE, 4);
        maximum_uploads_stream(&fixture.schemes).await;
    }

    #[tokio::test]
    async fn maximum_vrf_uploads_stream() {
        let fixture = bls12381_threshold::fixture::<MinSig, _>(&mut test_rng(), NAMESPACE, 4);
        maximum_uploads_stream(&fixture.schemes).await;
    }

    async fn start_server<C: Scheme>(
        scheme: C,
        strategy: impl Strategy,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let indexer = Arc::new(Indexer::new(scheme, strategy));
        let api = Api::new(indexer);
        let app = api.router();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (addr, handle)
    }

    async fn wait_for_ready<C: Scheme>(client: &Client<Sequential, C>) {
        loop {
            if client.health().await.is_ok() {
                return;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    fn fixture(seed: u64) -> (Vec<VrfScheme>, Identity) {
        let mut rng = StdRng::seed_from_u64(seed);
        let Fixture { schemes, .. } =
            bls12381_threshold::fixture::<MinSig, _>(&mut rng, NAMESPACE, 4);
        let identity = *schemes[0].identity();
        (schemes, identity)
    }

    #[tokio::test]
    async fn test_seed_operations() {
        let ctx = TestContext::new().await;
        let seed = ctx.seed();

        ctx.client.seed_upload(&seed).await.unwrap();

        let retrieved = ctx.client.seed_get(IndexQuery::Latest).await.unwrap();
        assert_eq!(retrieved.view(), seed.view());

        let retrieved = ctx.client.seed_get(IndexQuery::Index(1)).await.unwrap();
        assert_eq!(retrieved.view().get(), 1);
    }

    #[tokio::test]
    async fn test_notarization_operations() {
        let ctx = TestContext::new().await;
        let notarized = ctx.notarized();

        ctx.client
            .notarized_upload(&notarized.proof, &notarized.block)
            .await
            .unwrap();

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

        ctx.client
            .finalized_upload(&finalized.proof, &finalized.block)
            .await
            .unwrap();

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
    async fn test_standard_certificate_operations() {
        let mut rng = StdRng::seed_from_u64(2);
        let Fixture { schemes, .. } = standard::fixture::<MinSig, _>(&mut rng, NAMESPACE, 4);
        let identity = *schemes[0].identity();
        let (addr, _handle) = start_server(schemes[0].clone(), Sequential).await;
        let client = ClientBuilder::new(
            &format!("http://{addr}"),
            StandardScheme::certificate_verifier(NAMESPACE, identity),
            Sequential,
        )
        .build();
        wait_for_ready(&client).await;

        let finalized = finalized_with_payload(&schemes, 1, Bytes::new());
        let notarized = Notarized::new(
            create_notarization(&schemes, finalized.proof.proposal.clone()),
            finalized.block.clone(),
        );

        client
            .notarized_upload(&notarized.proof, &notarized.block)
            .await
            .unwrap();
        client
            .finalized_upload(&finalized.proof, &finalized.block)
            .await
            .unwrap();
        assert_eq!(
            client
                .notarized_get(IndexQuery::Latest)
                .await
                .unwrap()
                .proof
                .view()
                .get(),
            1
        );
        assert_eq!(
            client
                .finalized_get(IndexQuery::Latest)
                .await
                .unwrap()
                .proof
                .view()
                .get(),
            1
        );
    }

    #[tokio::test]
    async fn test_block_retrieval() {
        let ctx = TestContext::new().await;
        let block = ctx.test_block();
        let finalized = ctx.finalized();

        ctx.client
            .finalized_upload(&finalized.proof, &finalized.block)
            .await
            .unwrap();

        // Test retrieval by latest
        let payload = ctx.client.block_get(Query::Latest).await.unwrap();
        match payload {
            alto_client::consensus::Payload::Finalized(f) => {
                assert_eq!(f.block.height.get(), 1);
            }
            _ => panic!("Expected finalized block"),
        }

        // Test retrieval by index
        let payload = ctx.client.block_get(Query::Index(1)).await.unwrap();
        match payload {
            alto_client::consensus::Payload::Finalized(f) => {
                assert_eq!(f.block.height.get(), 1);
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
    async fn test_block_upload() {
        let ctx = TestContext::new().await;
        let block = ctx.test_block();
        let digest = block.digest();

        ctx.client.block_upload(&block).await.unwrap();

        let payload = ctx.client.block_get(Query::Digest(digest)).await.unwrap();
        match payload {
            alto_client::consensus::Payload::Block(b) => {
                assert_eq!(b.digest(), digest);
            }
            _ => panic!("Expected block"),
        }
    }

    #[tokio::test]
    async fn large_block_uploads_stream_to_rust_clients() {
        let (schemes, identity) = fixture(0);
        let indexer = Arc::new(
            Indexer::new(schemes[0].clone(), Sequential).with_block_size(64 * 1024 * 1024),
        );
        let app = Api::new(indexer.clone()).router();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = ClientBuilder::new(
            &format!("http://{addr}"),
            VrfScheme::certificate_verifier(NAMESPACE, identity),
            Sequential,
        )
        .with_block_size(64 * 1024 * 1024)
        .build();
        let mut stream = client.listen().await.unwrap();
        while indexer.consensus_tx.receiver_count() == 0 {
            tokio::task::yield_now().await;
        }

        // The message includes the certificate and block fields beyond the 64 MiB payload.
        let finalized = finalized_with_payload(&schemes, 1, Bytes::from(vec![7; 64 * 1024 * 1024]));
        client
            .finalized_upload(&finalized.proof, &finalized.block)
            .await
            .unwrap();
        match stream.next().await.unwrap().unwrap() {
            alto_client::consensus::Message::Finalization(received) => {
                assert_eq!(received, finalized);
            }
            _ => panic!("expected finalization"),
        }
        server.abort();
    }

    #[tokio::test]
    async fn test_websocket_streaming() {
        let ctx = TestContext::new().await;
        let seed = ctx.seed();

        let mut stream = ctx.client.listen().await.unwrap();

        // Signal that websocket is connected, then upload the seed
        let (tx, rx) = tokio::sync::oneshot::channel();
        let client = ctx.client.clone();
        tokio::spawn(async move {
            rx.await.unwrap();
            client.seed_upload(&seed).await.unwrap();
        });

        // Signal ready and wait for the seed message
        tx.send(()).unwrap();
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
        let (addr, _handle) = start_server(schemes1[0].clone(), Sequential).await;
        let client = ClientBuilder::new(
            &format!("http://{addr}"),
            VrfScheme::certificate_verifier(NAMESPACE, identity2),
            Sequential,
        )
        .build();
        wait_for_ready(&client).await;

        // Create a seed signed by schemes1
        let context = Context {
            round: Round::new(EPOCH, View::new(1)),
            leader: ed25519::PrivateKey::from_seed(0).public_key(),
            parent: (View::new(0), sha256::Digest::EMPTY),
        };
        let block = Block::new(
            context,
            Sha256::hash(&[b"genesis"]),
            Height::new(1),
            1000,
            Bytes::new(),
        );
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(1)),
            View::new(0),
            block.digest(),
        );
        let seed = create_notarization(&schemes1, proposal).seed().unwrap();

        // Server accepts it (signed by schemes1, which server uses)
        client.seed_upload(&seed).await.unwrap();

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
        let bad_seed = create_notarization(&wrong_schemes, proposal)
            .seed()
            .unwrap();

        // Server rejects it (signature doesn't match server's identity)
        let result = ctx.client.seed_upload(&bad_seed).await;
        assert!(result.is_err());
    }

    fn generate_self_signed_cert() -> CertifiedKey<KeyPair> {
        let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
        generate_simple_self_signed(subject_alt_names).unwrap()
    }

    async fn start_tls_server(
        scheme: VrfScheme,
        cert_key: &CertifiedKey<KeyPair>,
        strategy: impl Strategy,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let indexer = Arc::new(Indexer::new(scheme, strategy));
        let api = Api::new(indexer);
        let app = api.router();

        // Create rustls server config
        let cert_der = CertificateDer::from(cert_key.cert.der().to_vec());
        let key_der = PrivateKeyDer::try_from(cert_key.signing_key.serialize_der()).unwrap();

        let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("Failed to create server config");
        let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let tls_acceptor = tls_acceptor.clone();
                let app = app.clone();

                tokio::spawn(async move {
                    let tls_stream = match tls_acceptor.accept(stream).await {
                        Ok(s) => s,
                        Err(_) => return,
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
    ) -> Client<Sequential, VrfScheme> {
        ClientBuilder::new(
            &format!("https://{addr}"),
            VrfScheme::certificate_verifier(NAMESPACE, identity),
            Sequential,
        )
        .with_tls_cert(cert_key.cert.der().to_vec())
        .build()
    }

    #[tokio::test]
    async fn test_tls_https_connection() {
        let cert_key = generate_self_signed_cert();

        let (schemes, identity) = fixture(0);

        let (addr, handle) = start_tls_server(schemes[0].clone(), &cert_key, Sequential).await;
        let client = create_tls_client(addr, identity, &cert_key);
        wait_for_ready(&client).await;

        // Create and upload a seed
        let context = Context {
            round: Round::new(EPOCH, View::new(1)),
            leader: ed25519::PrivateKey::from_seed(0).public_key(),
            parent: (View::new(0), sha256::Digest::EMPTY),
        };
        let block = Block::new(
            context,
            Sha256::hash(&[b"genesis"]),
            Height::new(1),
            1000,
            Bytes::new(),
        );
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(1)),
            View::new(0),
            block.digest(),
        );
        let seed = create_notarization(&schemes, proposal).seed().unwrap();

        // Test HTTPS POST
        client.seed_upload(&seed).await.unwrap();

        // Test HTTPS GET
        let retrieved = client.seed_get(IndexQuery::Latest).await.unwrap();
        assert_eq!(retrieved.view(), seed.view());

        handle.abort();
    }

    #[tokio::test]
    async fn test_tls_websocket_connection() {
        let cert_key = generate_self_signed_cert();

        let (schemes, identity) = fixture(0);

        let (addr, handle) = start_tls_server(schemes[0].clone(), &cert_key, Sequential).await;
        let client = create_tls_client(addr, identity, &cert_key);
        wait_for_ready(&client).await;

        // Create a seed
        let context = Context {
            round: Round::new(EPOCH, View::new(1)),
            leader: ed25519::PrivateKey::from_seed(0).public_key(),
            parent: (View::new(0), sha256::Digest::EMPTY),
        };
        let block = Block::new(
            context,
            Sha256::hash(&[b"genesis"]),
            Height::new(1),
            1000,
            Bytes::new(),
        );
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(1)),
            View::new(0),
            block.digest(),
        );
        let seed = create_notarization(&schemes, proposal).seed().unwrap();

        // Connect to WebSocket over TLS
        let mut stream = client.listen().await.unwrap();

        // Signal that websocket is connected, then upload the seed
        let (tx, rx) = tokio::sync::oneshot::channel();
        let upload_client = client.clone();
        tokio::spawn(async move {
            rx.await.unwrap();
            upload_client.seed_upload(&seed).await.unwrap();
        });

        // Signal ready and wait for the seed message
        tx.send(()).unwrap();
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

        handle.abort();
    }
}
