//! Follower node for alto.
//!
//! This binary provides a template for creating "follower" nodes that track consensus
//! by consuming finalization certificates from an external source (like exoware or an indexer)
//! without participating in the validator P2P network.
//!
//! This is useful for:
//! - Exchange nodes that need to follow chain state
//! - RPC nodes serving queries
//! - Archive nodes indexing blockchain data
//!
//! # Architecture
//!
//! The follower:
//! 1. Connects to an exoware relay or indexer endpoint via WebSocket
//! 2. Receives and verifies finalization certificates
//! 3. Feeds blocks to marshal for storage and state management
//! 4. Uses an HTTP resolver to fetch any missing ancestor blocks
//!
//! # Usage
//!
//! ```bash
//! follower --config config.yaml
//! ```
//!
//! # Configuration
//!
//! The configuration file should contain:
//! - `source`: URL of the exoware relay or indexer (e.g., "https://global.alto.exoware.xyz")
//! - `identity`: Hex-encoded BLS12-381 public key of the network
//! - `directory`: Path to store data
//! - `worker_threads`: Number of worker threads
//! - `log_level`: Log level (e.g., "info", "debug")
//! - `metrics_port`: Port for Prometheus metrics
//! - `mailbox_size`: Size of internal mailboxes

use alto_client::{consensus::Message, Client, Query};
use alto_types::{Block, Finalization, Identity, Scheme, EPOCH_LENGTH, NAMESPACE};
use bytes::Bytes;
use clap::{Arg, Command};
use commonware_broadcast::buffered;
use commonware_codec::{DecodeExt, Encode};
use commonware_consensus::{
    Reporter, Viewable, marshal::{self, Update, ingress::handler}, simplex::types::Activity, types::{FixedEpocher, Height, ViewDelta}
};
use commonware_cryptography::{
    certificate::{ConstantProvider, Scheme as CertScheme},
    ed25519::PublicKey,
    sha256::Digest,
};
use commonware_parallel::Sequential;
use commonware_resolver::{Consumer, Resolver};
use commonware_runtime::{
    buffer::PoolRef, tokio, Clock, Handle, Metrics, Runner, Spawner, Storage,
};
use commonware_storage::archive::immutable;
use commonware_utils::{from_hex_formatted, futures::Pool, vec::NonEmptyVec, Acknowledgement, NZUsize, NZU16, NZU64};
use commonware_macros::select;
use futures::{
    channel::mpsc,
    SinkExt, StreamExt,
};
use governor::clock::Clock as GClock;
use rand::{CryptoRng, Rng};
use serde::{Deserialize, Serialize};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZero,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};
use tracing::{debug, error, info, warn, Level};

// Storage constants
const PRUNABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(4_096);
const IMMUTABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(262_144);
const FREEZER_TABLE_RESIZE_FREQUENCY: u8 = 4;
const FREEZER_TABLE_RESIZE_CHUNK_SIZE: u32 = 2u32.pow(16); // 3MB
const FREEZER_JOURNAL_TARGET_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
const FREEZER_JOURNAL_COMPRESSION: Option<u8> = Some(3);
const REPLAY_BUFFER: NonZero<usize> = NZUsize!(8 * 1024 * 1024); // 8MB
const WRITE_BUFFER: NonZero<usize> = NZUsize!(8 * 1024 * 1024); // 8MB - larger buffer reduces flush frequency
const BUFFER_POOL_PAGE_SIZE: NonZero<u16> = NZU16!(4_096); // 4KB
const BUFFER_POOL_CAPACITY: NonZero<usize> = NZUsize!(8_192); // 32MB
const MAX_REPAIR: NonZero<usize> = NZUsize!(100); // Smaller batches so marshal can keep up
const VIEW_RETENTION_TIMEOUT: ViewDelta = ViewDelta::new(2560); // 10x activity timeout
const DEQUE_SIZE: usize = 10;

// Default freezer table sizes
const BLOCKS_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21); // 100MB
const FINALIZED_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21); // 100MB

/// Configuration for the follower node.
#[derive(Deserialize, Serialize)]
pub struct Config {
    /// URL of the exoware relay or indexer endpoint
    pub source: String,

    /// Hex-encoded BLS12-381 public key of the network identity
    pub identity: String,

    /// Directory for storing data
    pub directory: String,

    /// Number of worker threads
    pub worker_threads: usize,

    /// Log level (e.g., "info", "debug", "warn", "error")
    pub log_level: String,

    /// Port for Prometheus metrics
    pub metrics_port: u16,

    /// Size of internal mailboxes
    #[serde(default = "default_mailbox_size")]
    pub mailbox_size: usize,

    /// Whether to auto-checkpoint from the latest finalized block on first run.
    /// When true (default), the follower will fetch the latest finalized block
    /// from the indexer and set that as the starting floor, avoiding the need
    /// to backfill all historical blocks.
    #[serde(default = "default_auto_checkpoint")]
    pub auto_checkpoint: bool,
}

fn default_mailbox_size() -> usize {
    // Large mailbox size to support backfill from genesis without blocking
    // on report calls when marshal is busy processing
    1024
}

fn default_auto_checkpoint() -> bool {
    true
}

/// Messages processed by the HttpResolver actor.
#[derive(Debug)]
enum ResolverMessage {
    /// Fetch a block or finalization by key
    Fetch(handler::Request<Block>),
}

/// HTTP-based resolver handle that sends fetch requests to the actor.
///
/// This is the handle that implements the `Resolver` trait. It sends messages
/// to the `HttpResolverActor` which processes them in parallel by making
/// REST requests to the indexer and forwarding responses to marshal.
#[derive(Clone)]
pub struct HttpResolver {
    /// Sender for messages to the actor
    mailbox_tx: mpsc::Sender<ResolverMessage>,
}

impl Resolver for HttpResolver {
    type Key = handler::Request<Block>;
    type PublicKey = PublicKey;

    async fn fetch(&mut self, key: Self::Key) {
        let msg = ResolverMessage::Fetch(key);
        if let Err(e) = self.mailbox_tx.clone().send(msg).await {
            warn!(error = ?e, "failed to send fetch request to resolver actor");
        }
    }

    async fn fetch_all(&mut self, keys: Vec<Self::Key>) {
        // Send all fetch requests to the actor - it will process them in parallel
        for key in keys {
            self.fetch(key).await;
        }
    }

    async fn fetch_targeted(&mut self, key: Self::Key, _targets: NonEmptyVec<Self::PublicKey>) {
        // Ignore targets - just use the indexer
        self.fetch(key).await;
    }

    async fn fetch_all_targeted(
        &mut self,
        requests: Vec<(Self::Key, NonEmptyVec<Self::PublicKey>)>,
    ) {
        // Ignore targets - just use the indexer
        for (key, _) in requests {
            self.fetch(key).await;
        }
    }

    async fn cancel(&mut self, _key: Self::Key) {
        // No-op: HTTP requests are fire-and-forget
    }

    async fn clear(&mut self) {
        // No-op: nothing to clear
    }

    async fn retain(&mut self, _f: impl Fn(&Self::Key) -> bool + Send + 'static) {
        // No-op: nothing to retain
    }
}

/// HTTP resolver actor that processes fetch requests in parallel.
///
/// The actor runs in a loop, receiving messages from its mailbox and managing
/// in-flight HTTP requests using a futures Pool. This allows multiple REST
/// requests to be in-flight simultaneously without blocking the message loop.
///
/// The actor makes REST requests to the indexer and forwards responses directly
/// to marshal - no caching is performed.
pub struct HttpResolverActor {
    /// Client for making REST requests to the indexer
    client: Client<Sequential>,
    /// Receiver for messages from the handle
    mailbox_rx: mpsc::Receiver<ResolverMessage>,
    /// Handler for delivering blocks to marshal's ingress
    handler: handler::Handler<Block>,
    /// Pool of in-flight HTTP request futures
    in_flight: Pool<()>,
}

impl HttpResolverActor {
    /// Create a new HttpResolver actor and its handle.
    pub fn new(
        client: Client<Sequential>,
        ingress_tx: mpsc::Sender<handler::Message<Block>>,
        mailbox_size: usize,
    ) -> (Self, HttpResolver) {
        let (mailbox_tx, mailbox_rx) = mpsc::channel(mailbox_size);

        let actor = Self {
            client,
            mailbox_rx,
            handler: handler::Handler::new(ingress_tx),
            in_flight: Pool::default(),
        };

        let handle = HttpResolver { mailbox_tx };

        (actor, handle)
    }

    /// Run the actor loop, processing messages and managing in-flight requests.
    pub async fn run(mut self) {
        info!("HttpResolver actor started");

        loop {
            select! {
                // Handle completed futures from the pool
                _ = self.in_flight.next_completed() => {
                    // Future completed - work is already done inside the future
                },
                // Handle new fetch requests from the mailbox
                msg = self.mailbox_rx.next() => {
                    let Some(ResolverMessage::Fetch(key)) = msg else {
                        // Mailbox closed
                        warn!("mailbox closed");
                        break;
                    };
                    // Create a future for this fetch and add it to the pool
                    let future = Self::process_fetch(
                        key,
                        self.client.clone(),
                        self.handler.clone(),
                    );
                    self.in_flight.push(future);
                },
            };
        }

        info!("HttpResolver actor stopped");
    }

    /// Process a single fetch request by making a REST request and forwarding to marshal.
    async fn process_fetch(
        key: handler::Request<Block>,
        client: Client<Sequential>,
        handler: handler::Handler<Block>,
    ) {
        match key {
            handler::Request::Block(digest) => {
                Self::fetch_block_by_digest(digest, client, handler).await;
            }
            handler::Request::Finalized { height } => {
                Self::fetch_finalized_by_height(height, client, handler).await;
            }
            handler::Request::Notarized { round } => {
                // For notarized blocks, we don't have a direct query - skip for now
                warn!(?round, "notarized block request (not implemented for HTTP)");
            }
        }
    }

    /// Fetch a block by digest from the indexer and forward to marshal.
    async fn fetch_block_by_digest(
        digest: Digest,
        client: Client<Sequential>,
        mut handler: handler::Handler<Block>,
    ) {
        debug!(?digest, "fetching block by digest from indexer");

        match client.block_get(Query::Digest(digest)).await {
            Ok(alto_client::consensus::Payload::Block(block)) => {
                // Deliver to marshal
                let key = handler::Request::Block(digest);
                let value = Bytes::from(block.encode().to_vec());
                if !handler.deliver(key, value).await {
                    warn!(?digest, "failed to deliver block to marshal");
                }
            }
            Ok(_) => {
                warn!(?digest, "wrong payload returned for block by digest");
            }
            Err(e) => {
                warn!(?digest, error=?e, "failed to fetch block by digest");
            }
        }
    }

    /// Fetch a finalized block by height from the indexer and forward to marshal.
    async fn fetch_finalized_by_height(
        height: Height,
        client: Client<Sequential>,
        mut handler: handler::Handler<Block>,
    ) {
        debug!(height = height.get(), "fetching finalized block by height from indexer");

        match client.block_get(Query::Index(height.get())).await {
            Ok(alto_client::consensus::Payload::Finalized(finalized)) => {
                // Deliver the block to marshal
                let key = handler::Request::Finalized { height };
                let finalization = finalized.proof.clone();
                let block = finalized.block.clone();
                let value = Bytes::from((finalization, block).encode().to_vec());
                if !handler.deliver(key, value).await {
                    warn!(height = height.get(), "failed to deliver finalized block to marshal");
                }

                info!(height = height.get(), "fetched and delivered finalized block");
            }
            Ok(_) => {
                // No payload - should be impossible
                warn!(height = height.get(), "wrong payload returned for finalized block by height");
            }
            Err(e) => {
                warn!(height = height.get(), error=?e, "failed to fetch finalized block by height");
            }
        }
    }
}

/// Engine that runs marshal standalone for following consensus.
pub struct FollowerEngine<E>
where
    E: Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
{
    context: E,
    buffer_mailbox: buffered::Mailbox<PublicKey, Block>,
    marshal: marshal::Actor<
        E,
        Block,
        ConstantProvider<Scheme, commonware_consensus::types::Epoch>,
        immutable::Archive<E, Digest, Finalization>,
        immutable::Archive<E, Digest, Block>,
        FixedEpocher,
        Sequential,
    >,
    marshal_mailbox: marshal::Mailbox<Scheme, Block>,
}

impl<E> FollowerEngine<E>
where
    E: Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
{
    /// Create a new follower engine.
    pub async fn new(context: E, identity: Identity, mailbox_size: usize) -> Self {
        // Create a dummy public key for the buffer (not used for actual networking)
        // Use a valid ed25519 public key (all zeros is not valid, so we decode from hex)
        let dummy_key =
            PublicKey::decode([0u8; 32].as_slice()).expect("failed to decode dummy public key");

        // Create the buffer (we won't use it for networking, just need its mailbox)
        let (_buffer, buffer_mailbox) = buffered::Engine::new(
            context.with_label("buffer"),
            buffered::Config {
                public_key: dummy_key,
                mailbox_size,
                deque_size: DEQUE_SIZE,
                priority: false,
                codec_config: (),
            },
        );

        // Create the buffer pool
        let buffer_pool = PoolRef::new(BUFFER_POOL_PAGE_SIZE, BUFFER_POOL_CAPACITY);

        // Initialize finalizations by height archive
        let scheme = Scheme::certificate_verifier(NAMESPACE, identity);
        let finalizations_by_height = immutable::Archive::init(
            context.with_label("finalizations_by_height"),
            immutable::Config {
                metadata_partition: "follower-finalizations-by-height-metadata".to_string(),
                freezer_table_partition: "follower-finalizations-by-height-freezer-table"
                    .to_string(),
                freezer_table_initial_size: FINALIZED_FREEZER_TABLE_INITIAL_SIZE,
                freezer_table_resize_frequency: FREEZER_TABLE_RESIZE_FREQUENCY,
                freezer_table_resize_chunk_size: FREEZER_TABLE_RESIZE_CHUNK_SIZE,
                freezer_key_partition: "follower-finalizations-by-height-freezer-key-journal"
                    .to_string(),
                freezer_key_buffer_pool: buffer_pool.clone(),
                freezer_key_write_buffer: WRITE_BUFFER,
                freezer_value_partition: "follower-finalizations-by-height-freezer-value-journal"
                    .to_string(),
                freezer_value_write_buffer: WRITE_BUFFER,
                freezer_value_target_size: FREEZER_JOURNAL_TARGET_SIZE,
                freezer_value_compression: FREEZER_JOURNAL_COMPRESSION,
                ordinal_partition: "follower-finalizations-by-height-ordinal".to_string(),
                ordinal_write_buffer: WRITE_BUFFER,
                items_per_section: IMMUTABLE_ITEMS_PER_SECTION,
                codec_config: scheme.certificate_codec_config(),
                replay_buffer: REPLAY_BUFFER,
            },
        )
        .await
        .expect("failed to initialize finalizations by height archive");
        info!("restored finalizations by height archive");

        // Initialize finalized blocks archive
        let finalized_blocks = immutable::Archive::init(
            context.with_label("finalized_blocks"),
            immutable::Config {
                metadata_partition: "follower-finalized_blocks-metadata".to_string(),
                freezer_table_partition: "follower-finalized_blocks-freezer-table".to_string(),
                freezer_table_initial_size: BLOCKS_FREEZER_TABLE_INITIAL_SIZE,
                freezer_table_resize_frequency: FREEZER_TABLE_RESIZE_FREQUENCY,
                freezer_table_resize_chunk_size: FREEZER_TABLE_RESIZE_CHUNK_SIZE,
                freezer_key_partition: "follower-finalized-blocks-freezer-key-journal".to_string(),
                freezer_key_buffer_pool: buffer_pool.clone(),
                freezer_key_write_buffer: WRITE_BUFFER,
                freezer_value_partition: "follower-finalized-blocks-freezer-value-journal"
                    .to_string(),
                freezer_value_write_buffer: WRITE_BUFFER,
                freezer_value_target_size: FREEZER_JOURNAL_TARGET_SIZE,
                freezer_value_compression: None,
                ordinal_partition: "follower-finalized-blocks-ordinal".to_string(),
                ordinal_write_buffer: WRITE_BUFFER,
                items_per_section: IMMUTABLE_ITEMS_PER_SECTION,
                codec_config: (),
                replay_buffer: REPLAY_BUFFER,
            },
        )
        .await
        .expect("failed to initialize finalized blocks archive");
        info!("restored finalized blocks archive");

        // Create the certificate verifier provider
        let provider = ConstantProvider::new(scheme);
        let epocher = FixedEpocher::new(EPOCH_LENGTH);

        // Create marshal
        let (marshal, marshal_mailbox, _) = marshal::Actor::init(
            context.with_label("marshal"),
            finalizations_by_height,
            finalized_blocks,
            marshal::Config {
                provider,
                epocher,
                partition_prefix: "follower".to_string(),
                mailbox_size,
                view_retention_timeout: VIEW_RETENTION_TIMEOUT,
                prunable_items_per_section: PRUNABLE_ITEMS_PER_SECTION,
                replay_buffer: REPLAY_BUFFER,
                key_write_buffer: WRITE_BUFFER,
                value_write_buffer: WRITE_BUFFER,
                block_codec_config: (),
                max_repair: MAX_REPAIR,
                buffer_pool,
                strategy: Sequential,
            },
        )
        .await;

        Self {
            context,
            buffer_mailbox,
            marshal,
            marshal_mailbox,
        }
    }

    /// Get a clone of the marshal mailbox for submitting certificates.
    pub fn mailbox(&self) -> marshal::Mailbox<Scheme, Block> {
        self.marshal_mailbox.clone()
    }

    /// Start the follower engine with marshal.
    pub fn start(
        self,
        ingress_rx: mpsc::Receiver<handler::Message<Block>>,
        resolver: HttpResolver,
    ) -> Handle<()> {
        let context = self.context.clone();
        context.spawn(move |_| async move {
            // Create a reporter that logs finalized blocks
            let app = FollowerApplication;

            // Start marshal with the HTTP resolver
            self.marshal
                .start(app, self.buffer_mailbox, (ingress_rx, resolver))
                .await
                .expect("marshal failed");
        })
    }
}

/// Reporter application for follower nodes.
///
/// This receives notifications about finalized blocks from marshal.
#[derive(Clone)]
struct FollowerApplication;

impl commonware_consensus::Reporter for FollowerApplication {
    type Activity = Update<Block>;

    async fn report(&mut self, activity: Self::Activity) {
        if let Update::Block(block, ack_rx) = activity {
            info!(
                height = block.height.get(),
                "finalized block processed by marshal"
            );
            ack_rx.acknowledge();
        }
    }
}

/// Certificate feeder that consumes from exoware/indexer and feeds to marshal.
struct CertificateFeeder<E: Clock> {
    context: E,
    client: Client<Sequential>,
    scheme: Scheme,
    marshal_mailbox: marshal::Mailbox<Scheme, Block>,
    /// Whether the floor has been set (happens on first finalization received)
    floor_set: bool,
    /// Whether to auto-checkpoint (set floor on first block)
    auto_checkpoint: bool,
}

impl<E: Clock> CertificateFeeder<E> {
    fn new(
        context: E,
        client: Client<Sequential>,
        identity: Identity,
        marshal_mailbox: marshal::Mailbox<Scheme, Block>,
        auto_checkpoint: bool,
    ) -> Self {
        let scheme = Scheme::certificate_verifier(NAMESPACE, identity);
        Self {
            context,
            client,
            scheme,
            marshal_mailbox,
            floor_set: false,
            auto_checkpoint,
        }
    }

    /// Start feeding certificates from the WebSocket stream to marshal.
    async fn run(mut self) {
        loop {
            match self.client.listen().await {
                Ok(mut stream) => {
                    info!("connected to certificate stream");

                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(message) => {
                                if let Err(e) = self.handle_message(message).await {
                                    error!(error = ?e, "failed to handle message");
                                }
                            }
                            Err(e) => {
                                error!(error = ?e, "stream error");
                                break;
                            }
                        }
                    }

                    warn!("certificate stream disconnected, reconnecting...");
                }
                Err(e) => {
                    error!(error = ?e, "failed to connect to certificate stream");
                }
            }

            // Wait before reconnecting
            self.context.sleep(Duration::from_secs(1)).await;
        }
    }

    async fn handle_message(&mut self, message: Message) -> Result<(), String> {
        match message {
            Message::Finalization(finalized) => {
                let height = finalized.block.height;
                let view = finalized.proof.view();

                debug!(height = height.get(), view = view.get(), "received finalization");

                // Verify the finalization proof
                if !finalized.verify(&self.scheme, &Sequential) {
                    return Err(format!(
                        "invalid finalization signature for height {}",
                        height.get()
                    ));
                }

                // On first finalization, set the floor to this height so marshal
                // doesn't try to backfill blocks before it.
                if !self.floor_set && self.auto_checkpoint {
                    info!(height = height.get(), "setting checkpoint floor on first finalization");
                    self.marshal_mailbox.set_floor(height).await;
                    self.floor_set = true;
                }

                // Report the finalization to marshal so it can request missing ancestors.
                self.marshal_mailbox
                    .report(Activity::Finalization(finalized.proof.clone()))
                    .await;

                info!(height = height.get(), view = view.get(), "processed finalization");
            }
            Message::Notarization(notarized) => {
                let height = notarized.block.height;
                let view = notarized.proof.view();

                debug!(height = height.get(), view = view.get(), "received notarization");
                // Notarizations are ignored - we only follow finalized state
            }
            Message::Seed(seed) => {
                debug!(view = seed.view().get(), "received seed");
                // Seeds are not needed for following finalized state
            }
        }
        Ok(())
    }
}

fn main() {
    // Parse arguments
    let matches = Command::new("follower")
        .about("Follower node for an alto chain (non-validator)")
        .arg(Arg::new("config").long("config").required(true))
        .get_matches();

    // Load config
    let config_file = matches.get_one::<String>("config").unwrap();
    let config_file = std::fs::read_to_string(config_file).expect("Could not read config file");
    let config: Config = serde_yaml::from_str(&config_file).expect("Could not parse config file");

    // Parse identity
    let identity_bytes =
        from_hex_formatted(&config.identity).expect("Could not parse identity hex");
    let identity =
        Identity::decode(identity_bytes.as_ref()).expect("Could not decode identity public key");

    // Initialize runtime
    let cfg = tokio::Config::default()
        .with_tcp_nodelay(Some(true))
        .with_worker_threads(config.worker_threads)
        .with_storage_directory(PathBuf::from(config.directory))
        .with_catch_panics(false);
    let executor = tokio::Runner::new(cfg);

    // Start runtime
    executor.start(|context| async move {
        // Configure telemetry
        let log_level = Level::from_str(&config.log_level).expect("Invalid log level");
        tokio::telemetry::init(
            context.with_label("telemetry"),
            tokio::telemetry::Logging {
                level: log_level,
                json: false,
            },
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                config.metrics_port,
            )),
            None,
        );

        info!(source = %config.source, "starting follower node");

        // Create the alto client for connecting to exoware/indexer
        let client = Client::new(&config.source, identity, Sequential);

        // Wait for the source to be healthy
        loop {
            match client.health().await {
                Ok(_) => {
                    info!("connected to certificate source");
                    break;
                }
                Err(e) => {
                    warn!(error = ?e, "waiting for certificate source to be available...");
                    context.sleep(Duration::from_secs(1)).await;
                }
            }
        }

        // Create the follower engine with marshal
        let engine = FollowerEngine::new(context.clone(), identity, config.mailbox_size).await;
        let marshal_mailbox = engine.mailbox();

        // Create the ingress channel for delivering blocks to marshal
        let (ingress_tx, ingress_rx) = mpsc::channel(config.mailbox_size);

        // Create the HTTP resolver actor for backfilling
        // The actor processes fetch requests in parallel using a futures pool
        // and delivers results to marshal
        let (resolver_actor, resolver) = HttpResolverActor::new(
            client.clone(),
            ingress_tx.clone(),
            config.mailbox_size,
        );

        // Spawn the resolver actor
        let resolver_handle = context.clone().spawn(|_| resolver_actor.run());

        // Start the follower engine (marshal)
        let engine_handle = engine.start(ingress_rx, resolver.clone());

        // Create and start the certificate feeder
        // The feeder will set the checkpoint floor on the first finalization received
        // (if auto_checkpoint is enabled), ensuring we start from exactly where we connect.
        let feeder = CertificateFeeder::new(
            context.clone(),
            client,
            identity,
            marshal_mailbox,
            config.auto_checkpoint,
        );
        let feeder_handle = context.spawn(|_| feeder.run());

        // Wait for any of the handles to finish (which indicates an error)
        futures::future::select(
            futures::future::select(engine_handle, feeder_handle),
            resolver_handle,
        )
        .await;
        error!("follower stopped unexpectedly");
    });
}
