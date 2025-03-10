use super::{
    archive::Wrapped,
    buffer::Buffer,
    coordinator::Coordinator,
    handler::Handler,
    ingress::{Mailbox, Message},
    Config,
};
use crate::{
    actors::syncer::{
        handler,
        key::{self, MultiIndex, Value},
    },
    Indexer,
};
use alto_types::{Block, Finalization, Finalized, Notarized};
use bytes::Bytes;
use commonware_cryptography::{bls12381, ed25519::PublicKey, sha256::Digest};
use commonware_macros::select;
use commonware_p2p::{utils::requester, Receiver, Recipients, Sender};
use commonware_resolver::{p2p, Resolver};
use commonware_runtime::{Blob, Clock, Handle, Metrics, Spawner, Storage};
use commonware_storage::{
    archive::{
        self,
        translator::{EightCap, TwoCap},
        Archive, Identifier,
    },
    journal::{self, variable::Journal},
    metadata::{self, Metadata},
};
use commonware_utils::array::FixedBytes;
use futures::{
    channel::{mpsc, oneshot},
    lock::Mutex,
    StreamExt,
};
use governor::{clock::Clock as GClock, Quota};
use prometheus_client::metrics::gauge::Gauge;
use rand::Rng;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};
use tracing::{debug, info, warn};

/// Application actor.
pub struct Actor<B: Blob, R: Rng + Spawner + Metrics + Clock + GClock + Storage<B>, I: Indexer> {
    context: R,
    public_key: PublicKey,
    public: bls12381::PublicKey,
    participants: Vec<PublicKey>,
    mailbox: mpsc::Receiver<Message>,
    mailbox_size: usize,
    backfill_quota: Quota,
    activity_timeout: u64,
    indexer: Option<I>,

    // Blocks verified stored by view<>digest
    verified: Archive<TwoCap, Digest, B, R>,
    // Blocks notarized stored by view<>digest
    notarized: Archive<TwoCap, Digest, B, R>,

    // Finalizations stored by height
    finalized: Archive<EightCap, Digest, B, R>,
    // Blocks finalized stored by height
    //
    // We store this separately because we may not have the finalization for a block
    blocks: Archive<EightCap, Digest, B, R>,

    // Finalizer storage
    finalizer: Metadata<B, R, FixedBytes<1>>,

    // Latest height metric
    finalized_height: Gauge,
    // Indexed height metric
    contiguous_height: Gauge,
}

impl<B: Blob, R: Rng + Spawner + Metrics + Clock + GClock + Storage<B>, I: Indexer> Actor<B, R, I> {
    /// Create a new application actor.
    pub async fn init(context: R, config: Config<I>) -> (Self, Mailbox) {
        // Initialize verified blocks
        let verified_journal = Journal::init(
            context.with_label("verified_journal"),
            journal::variable::Config {
                partition: format!("{}-verifications", config.partition_prefix),
            },
        )
        .await
        .expect("Failed to initialize verified journal");
        let verified_archive = Archive::init(
            context.with_label("verified_archive"),
            verified_journal,
            archive::Config {
                translator: TwoCap,
                section_mask: 0xffff_ffff_ffff_f000u64,
                pending_writes: 0,
                replay_concurrency: 4,
                compression: Some(3),
            },
        )
        .await
        .expect("Failed to initialize verified archive");

        // Initialize notarized blocks
        let notarized_journal = Journal::init(
            context.with_label("notarized_journal"),
            journal::variable::Config {
                partition: format!("{}-notarizations", config.partition_prefix),
            },
        )
        .await
        .expect("Failed to initialize notarized journal");
        let notarized_archive = Archive::init(
            context.with_label("notarized_archive"),
            notarized_journal,
            archive::Config {
                translator: TwoCap,
                section_mask: 0xffff_ffff_ffff_f000u64,
                pending_writes: 0,
                replay_concurrency: 4,
                compression: Some(3),
            },
        )
        .await
        .expect("Failed to initialize notarized archive");

        // Initialize finalizations
        let finalized_journal = Journal::init(
            context.with_label("finalized_journal"),
            journal::variable::Config {
                partition: format!("{}-finalizations", config.partition_prefix),
            },
        )
        .await
        .expect("Failed to initialize finalized journal");
        let finalized_archive = Archive::init(
            context.with_label("finalized_archive"),
            finalized_journal,
            archive::Config {
                translator: EightCap,
                section_mask: 0xffff_ffff_ffff_0000u64,
                pending_writes: 0,
                replay_concurrency: 4,
                compression: Some(3),
            },
        )
        .await
        .expect("Failed to initialize finalized archive");

        // Initialize blocks
        let block_journal = Journal::init(
            context.with_label("block_journal"),
            journal::variable::Config {
                partition: format!("{}-blocks", config.partition_prefix),
            },
        )
        .await
        .expect("Failed to initialize block journal");
        let block_archive = Archive::init(
            context.with_label("block_archive"),
            block_journal,
            archive::Config {
                translator: EightCap,
                section_mask: 0xffff_ffff_ffff_0000u64,
                pending_writes: 0,
                replay_concurrency: 4,
                compression: Some(3),
            },
        )
        .await
        .expect("Failed to initialize finalized archive");

        // Initialize finalizer metadata
        let finalizer_metadata = Metadata::init(
            context.with_label("finalizer_metadata"),
            metadata::Config {
                partition: format!("{}-finalizer_metadata", config.partition_prefix),
            },
        )
        .await
        .expect("Failed to initialize finalizer metadata");

        // Create metrics
        let finalized_height = Gauge::default();
        context.register(
            "finalized_height",
            "Finalized height of application",
            finalized_height.clone(),
        );
        let contiguous_height = Gauge::default();
        context.register(
            "contiguous_height",
            "Contiguous height of application",
            contiguous_height.clone(),
        );

        // Initialize mailbox
        let (sender, mailbox) = mpsc::channel(config.mailbox_size);
        (
            Self {
                context,
                public_key: config.public_key,
                public: config.identity.into(),
                participants: config.participants,
                mailbox,
                mailbox_size: config.mailbox_size,
                backfill_quota: config.backfill_quota,
                activity_timeout: config.activity_timeout,
                indexer: config.indexer,

                verified: verified_archive,
                notarized: notarized_archive,

                finalized: finalized_archive,
                blocks: block_archive,

                finalizer: finalizer_metadata,

                finalized_height,
                contiguous_height,
            },
            Mailbox::new(sender),
        )
    }

    pub fn start(
        mut self,
        broadcast_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        backfill_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
    ) -> Handle<()> {
        self.context.spawn_ref()(self.run(broadcast_network, backfill_network))
    }

    /// Run the application actor.
    async fn run(
        mut self,
        mut broadcast_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        backfill_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
    ) {
        // Initialize resolver
        let coordinator = Coordinator::new(self.participants.clone());
        let (handler_sender, mut handler_receiver) = mpsc::channel(self.mailbox_size);
        let handler = Handler::new(handler_sender);
        let (resolver_engine, mut resolver) = p2p::Engine::new(
            self.context.with_label("resolver"),
            p2p::Config {
                coordinator,
                consumer: handler.clone(),
                producer: handler,
                mailbox_size: self.mailbox_size,
                requester_config: requester::Config {
                    public_key: self.public_key,
                    rate_limit: self.backfill_quota,
                    initial: Duration::from_secs(1),
                    timeout: Duration::from_secs(2),
                },
                fetch_retry_timeout: Duration::from_millis(100), // prevent busy loop
                priority_requests: false,
                priority_responses: false,
            },
        );
        resolver_engine.start(backfill_network);

        // Process all finalized blocks in order (fetching any that are missing)
        let last_view_processed = Arc::new(Mutex::new(0));
        let verified = Wrapped::new(self.verified);
        let notarized = Wrapped::new(self.notarized);
        let finalized = Wrapped::new(self.finalized);
        let blocks = Wrapped::new(self.blocks);
        let (mut finalizer_sender, mut finalizer_receiver) = mpsc::channel::<()>(1);
        self.context.with_label("finalizer").spawn({
            let mut resolver = resolver.clone();
            let last_view_processed = last_view_processed.clone();
            let verified = verified.clone();
            let notarized = notarized.clone();
            let finalized = finalized.clone();
            let blocks = blocks.clone();
            move |_| async move {
                // Initialize last indexed from metadata store
                let latest_key = FixedBytes::new([0u8]);
                let mut last_indexed = if let Some(bytes) = self.finalizer.get(&latest_key) {
                    u64::from_be_bytes(bytes.to_vec().try_into().unwrap())
                } else {
                    0
                };

                // Index all finalized blocks
                //
                // If using state sync, this is not necessary.
                let mut requested_blocks = BTreeSet::new();
                loop {
                    // Check if the next block is available
                    let next = last_indexed + 1;
                    let block = blocks
                        .get(Identifier::Index(next))
                        .await
                        .expect("Failed to get finalized block");
                    if let Some(block) = block {
                        // Update metadata
                        self.finalizer
                            .put(latest_key.clone(), next.to_be_bytes().to_vec().into());
                        self.finalizer
                            .sync()
                            .await
                            .expect("Failed to sync finalizer");

                        // In an application that maintains state, you would compute the state transition function here.

                        // Cancel any outstanding requests (by height and by digest)
                        resolver
                            .cancel(MultiIndex::new(Value::Finalized(next)))
                            .await;
                        let block =
                            Block::deserialize(&block).expect("Failed to deserialize block");
                        resolver
                            .cancel(MultiIndex::new(Value::Digest(block.digest())))
                            .await;

                        // Update the latest indexed
                        self.contiguous_height.set(next as i64);
                        last_indexed = next;
                        info!(height = next, "indexed finalized block");

                        // Update last view processed (if we have a finalization for this block)
                        let finalization = finalized
                            .get(Identifier::Index(next))
                            .await
                            .expect("Failed to get finalization");
                        if let Some(finalization) = finalization {
                            let finalization = Finalization::deserialize(None, &finalization)
                                .expect("Failed to deserialize finalization");
                            *last_view_processed.lock().await = finalization.view;
                        }
                        continue;
                    }

                    // Try to connect to our latest handled block (may not exist finalizations for some heights)
                    let (_, start_next) = blocks.next_gap(next).await;
                    if let Some(start_next) = start_next {
                        if last_indexed > 0 {
                            // Get gapped block
                            let gapped_block = blocks
                                .get(Identifier::Index(start_next))
                                .await
                                .expect("Failed to get finalized block")
                                .expect("Gapped block missing");
                            let gapped_block = Block::deserialize(&gapped_block)
                                .expect("Failed to deserialize block");

                            // Attempt to repair one block from other sources
                            let target_block = gapped_block.parent;
                            let verified = verified
                                .get(Identifier::Key(&target_block))
                                .await
                                .expect("Failed to get verified block");
                            if let Some(verified) = verified {
                                let verified = Block::deserialize(&verified)
                                    .expect("Failed to deserialize block");
                                blocks
                                    .put(verified.height, target_block, verified.serialize().into())
                                    .await
                                    .expect("Failed to insert finalized block");
                                debug!(height = verified.height, "repaired block from verified");
                                continue;
                            }
                            let notarization = notarized
                                .get(Identifier::Key(&target_block))
                                .await
                                .expect("Failed to get notarized block");
                            if let Some(notarization) = notarization {
                                let notarization = Notarized::deserialize(None, &notarization)
                                    .expect("Failed to deserialize block");
                                blocks
                                    .put(
                                        notarization.block.height,
                                        target_block,
                                        notarization.block.serialize().into(),
                                    )
                                    .await
                                    .expect("Failed to insert finalized block");
                                debug!(
                                    height = notarization.block.height,
                                    "repaired block from notarizations"
                                );
                                continue;
                            }

                            // Request the parent block digest
                            resolver
                                .fetch(MultiIndex::new(Value::Digest(target_block)))
                                .await;
                        }

                        // Enqueue next items (by index)
                        let range = next..std::cmp::min(start_next, next + 20);
                        debug!(
                            range.start,
                            range.end, "requesting missing finalized blocks"
                        );
                        for height in range {
                            // Check if we've already requested
                            if requested_blocks.contains(&height) {
                                continue;
                            }

                            // Request the block
                            let key = MultiIndex::new(Value::Finalized(height));
                            resolver.fetch(key).await;
                            requested_blocks.insert(height);
                        }
                    };

                    // If not finalized, wait for some message from someone that finalized store was updated
                    debug!(height = next, "waiting to index finalized block");
                    let _ = finalizer_receiver.next().await;
                }
            }
        });

        // Handle messages
        let mut buffer = Buffer::new(10);
        let mut waiters: HashMap<Digest, Vec<oneshot::Sender<Block>>> = HashMap::new();
        let mut latest_view = 0;
        let mut outstanding_notarize = BTreeSet::new();
        loop {
            // Clear dead waiters
            waiters.retain(|_, waiters| {
                waiters.retain(|waiter| !waiter.is_canceled());
                !waiters.is_empty()
            });

            // Cancel useless requests
            let mut to_cancel = Vec::new();
            outstanding_notarize.retain(|view| {
                if *view < latest_view {
                    to_cancel.push(MultiIndex::new(Value::Notarized(*view)));
                    false
                } else {
                    true
                }
            });
            for view in to_cancel {
                resolver.cancel(view).await;
            }

            // Select messages
            select! {
                // Handle mailbox before resolver messages
                mailbox_message = self.mailbox.next() => {
                    let message = mailbox_message.expect("Mailbox closed");
                    match message {
                        Message::Broadcast { payload } => {
                            broadcast_network
                                .0
                                .send(Recipients::All, payload.serialize().into(), true)
                                .await
                                .expect("Failed to broadcast");
                        }
                        Message::Verified { view, payload } => {
                            verified
                                .put(view, payload.digest(), payload.serialize().into())
                                .await
                                .expect("Failed to insert verified block");
                        }
                        Message::Notarized { proof, seed } => {
                            // Upload seed to indexer (if available)
                            if let Some(indexer) = self.indexer.as_ref() {
                                self.context.with_label("indexer").spawn({
                                    let indexer = indexer.clone();
                                    let view = proof.view;
                                    move |_| async move {
                                        let seed = seed.serialize().into();
                                        let result = indexer.seed_upload(seed).await;
                                        if let Err(e) = result {
                                            warn!(?e, "failed to upload seed");
                                            return;
                                        }
                                        debug!(view, "seed uploaded to indexer");
                                    }
                                });
                            }

                            // Check if in buffer
                            let mut block = None;
                            if let Some(buffered) = buffer.get(&proof.payload) {
                                block = Some(buffered.clone());
                            }

                            // Check if in verified blocks
                            if block.is_none() {
                                if let Some(verified) = verified.get(Identifier::Key(&proof.payload)).await.expect("Failed to get verified block") {
                                    block = Some(Block::deserialize(&verified).expect("Failed to deserialize block"));
                                }
                            }

                            // If found, store notarization
                            if let Some(block) = block {
                                let view = proof.view;
                                let height = block.height;
                                let digest = proof.payload.clone();
                                let notarization = Notarized::new(proof, block);
                                let notarization: Bytes = notarization.serialize().into();
                                notarized
                                    .put(view, digest, notarization.clone())
                                    .await
                                    .expect("Failed to insert notarized block");
                                debug!(view, height, "notarized block stored");

                                // Upload to indexer (if available)
                                if let Some(indexer) = self.indexer.as_ref() {
                                    self.context.with_label("indexer").spawn({
                                        let indexer = indexer.clone();
                                        move |_| async move {
                                            let result = indexer
                                                .notarization_upload(notarization)
                                                .await;
                                            if let Err(e) = result {
                                                warn!(?e, "failed to upload notarization");
                                                return;
                                            }
                                            debug!(view, "notarization uploaded to indexer");
                                        }
                                    });
                                }
                                continue;
                            }

                            // Fetch from network
                            //
                            // We don't worry about retaining the proof because any peer must provide
                            // it to us when serving the notarization.
                            debug!(view = proof.view, "notarized block missing");
                            outstanding_notarize.insert(proof.view);
                            resolver.fetch(MultiIndex::new(Value::Notarized(proof.view))).await;
                        }
                        Message::Finalized { proof, seed } => {
                            // Upload seed to indexer (if available)
                            if let Some(indexer) = self.indexer.as_ref() {
                                self.context.with_label("indexer").spawn({
                                    let indexer = indexer.clone();
                                    let view = proof.view;
                                    move |_| async move {
                                        let seed = seed.serialize().into();
                                        let result = indexer.seed_upload(seed).await;
                                        if let Err(e) = result {
                                            warn!(?e, "failed to upload seed");
                                            return;
                                        }
                                        debug!(view, "seed uploaded to indexer");
                                    }
                                });
                            }

                            // Check if in buffer
                            let mut block = None;
                            if let Some(buffered) = buffer.get(&proof.payload){
                                block = Some(buffered.clone());
                            }

                            // Check if in verified
                            if block.is_none() {
                                if let Some(verified) = verified.get(Identifier::Key(&proof.payload)).await.expect("Failed to get verified block") {
                                    block = Some(Block::deserialize(&verified).expect("Failed to deserialize block"));
                                }
                            }

                            // Check if in notarized
                            if block.is_none() {
                                if let Some(notarized) = notarized.get(Identifier::Key(&proof.payload)).await.expect("Failed to get notarized block") {
                                    block = Some(Notarized::deserialize(None, &notarized).expect("Failed to deserialize block").block);
                                }
                            }

                            // If found, store finalization
                            if let Some(block) = block {
                                let view = proof.view;
                                let digest = proof.payload.clone();
                                let height = block.height;
                                finalized
                                    .put(height, proof.payload.clone(), proof.serialize().into())
                                    .await
                                    .expect("Failed to insert finalization");
                                blocks
                                    .put(height, digest, block.serialize().into())
                                    .await
                                    .expect("Failed to insert finalized block");
                                debug!(view, height, "finalized block stored");

                                // Prune blocks
                                let last_view_processed = *last_view_processed.lock().await;
                                let min_view = last_view_processed.saturating_sub(self.activity_timeout);
                                verified
                                    .prune(min_view)
                                    .await
                                    .expect("Failed to prune verified block");
                                notarized
                                    .prune(min_view)
                                    .await
                                    .expect("Failed to prune notarized block");

                                // Notify finalizer
                                let _ = finalizer_sender.try_send(());

                                // Update latest
                                latest_view = view;

                                // Update metrics
                                self.finalized_height.set(height as i64);

                                // Upload to indexer (if available)
                                if let Some(indexer) = self.indexer.as_ref() {
                                    self.context.with_label("indexer").spawn({
                                        let indexer = indexer.clone();
                                        let finalization = Finalized::new(proof, block).serialize().into();
                                        move |_| async move {
                                            let result = indexer
                                                .finalization_upload(finalization)
                                                .await;
                                            if let Err(e) = result {
                                                warn!(?e, "failed to upload finalization");
                                                return;
                                            }
                                            debug!(height, "finalization uploaded to indexer");
                                        }
                                    });
                                }
                                continue;
                            }

                            // Fetch from network
                            warn!(view = proof.view, digest = ?proof.payload, "finalized block missing");
                            resolver.fetch(MultiIndex::new(Value::Digest(proof.payload))).await;
                        }
                        Message::Get { view, payload, response } => {
                            // Check if in buffer
                            let buffered = buffer.get(&payload);
                            if let Some(buffered) = buffered {
                                debug!(height = buffered.height, "found block in buffer");
                                let _ = response.send(buffered.clone());
                                continue;
                            }

                            // Check verified blocks
                            let block = verified.get(Identifier::Key(&payload)).await.expect("Failed to get verified block");
                            if let Some(block) = block {
                                let block = Block::deserialize(&block).expect("Failed to deserialize block");
                                debug!(height = block.height, "found block in verified");
                                let _ = response.send(block);
                                continue;
                            }

                            // Check if in notarized blocks
                            let notarization = notarized.get(Identifier::Key(&payload)).await.expect("Failed to get notarized block");
                            if let Some(notarization) = notarization {
                                let notarization = Notarized::deserialize(None, &notarization).expect("Failed to deserialize block");
                                let block = notarization.block;
                                debug!(height = block.height, "found block in notarized");
                                let _ = response.send(block);
                                continue;
                            }

                            // Check if in finalized blocks
                            let block = blocks.get(Identifier::Key(&payload)).await.expect("Failed to get finalized block");
                            if let Some(block) = block {
                                let block = Block::deserialize(&block).expect("Failed to deserialize block");
                                debug!(height = block.height, "found block in finalized");
                                let _ = response.send(block);
                                continue;
                            }

                            // Fetch from network if notarized (view is non-nil)
                            if let Some(view) = view {
                                debug!(view, ?payload, "required block missing");
                                resolver.fetch(MultiIndex::new(Value::Notarized(view))).await;
                            }

                            // Register waiter
                            debug!(view, ?payload, "registering waiter");
                            waiters.entry(payload).or_default().push(response);
                        }
                    }
                },
                // Handle incoming broadcasts
                broadcast_message = broadcast_network.1.recv() => {
                    let (sender, message) = broadcast_message.expect("Broadcast closed");
                    let Some(block) = Block::deserialize(&message) else {
                        warn!(?sender, "failed to deserialize block");
                        continue;
                    };
                    debug!(?sender, digest=?block.digest(), height=block.height, "received broadcast");
                    buffer.add(sender, block.clone());

                    // Notify waiters
                    if let Some(waiters) = waiters.remove(&block.digest()) {
                        debug!(?block.height, "waiter resolved via broadcast");
                        for waiter in waiters {
                            let _ = waiter.send(block.clone());
                        }
                    }
                },
                // Handle resolver messages last
                handler_message = handler_receiver.next() => {
                    let message = handler_message.expect("Handler closed");
                    match message {
                        handler::Message::Produce { key, response } => {
                            match key.to_value() {
                                key::Value::Notarized(view) => {
                                    let notarization = notarized.get(Identifier::Index(view)).await.expect("Failed to get notarized block");
                                    if let Some(notarized) = notarization {
                                        let _ = response.send(notarized);
                                    } else {
                                        debug!(view, "notarization missing on request");
                                    }
                                },
                                key::Value::Finalized(height) => {
                                    // Get finalization
                                    let finalization = finalized.get(Identifier::Index(height)).await.expect("Failed to get finalization");
                                    let Some(finalization) = finalization else {
                                        debug!(height, "finalization missing on request");
                                        continue;
                                    };
                                    let finalization = Finalization::deserialize(None, &finalization).expect("Failed to deserialize finalization");

                                    // Get block
                                    let block = blocks.get(Identifier::Index(height)).await.expect("Failed to get finalized block");
                                    let Some(block) = block else {
                                        debug!(height, "finalized block missing on request");
                                        continue;
                                    };
                                    let block = Block::deserialize(&block).expect("Failed to deserialize block");

                                    // Send finalization
                                    let payload = Finalized::new(finalization, block);
                                    let _ = response.send(payload.serialize().into());
                                },
                                key::Value::Digest(digest) => {
                                    // Check buffer
                                    if let Some(block) = buffer.get(&digest) {
                                        let _ = response.send(block.serialize().into());
                                        continue;
                                    }

                                    // Get verified block
                                    let block = verified.get(Identifier::Key(&digest)).await.expect("Failed to get verified block");
                                    if let Some(block) = block {
                                        let _ = response.send(block);
                                        continue;
                                    }

                                    // Get notarized block
                                    let notarization = notarized.get(Identifier::Key(&digest)).await.expect("Failed to get notarized block");
                                    if let Some(notarized) = notarization {
                                        let notarization = Notarized::deserialize(None, &notarized).expect("Failed to deserialize notarization");
                                        let _ = response.send(notarization.block.serialize().into());
                                        continue;
                                    }

                                    // Get block
                                    let block = blocks.get(Identifier::Key(&digest)).await.expect("Failed to get finalized block");
                                    if let Some(block) = block {
                                        let _ = response.send(block);
                                        continue;
                                    };

                                    // No record of block
                                    debug!(?digest, "block missing on request");
                                }
                            }
                        }
                        handler::Message::Deliver { key, value, response } => {
                            match key.to_value() {
                                key::Value::Notarized(view) => {
                                    // Parse notarization
                                    let Some(notarization) = Notarized::deserialize(Some(&self.public), &value) else {
                                        let _ = response.send(false);
                                        continue;
                                    };

                                    // Ensure the received payload is for the correct view
                                    if notarization.proof.view != view {
                                        let _ = response.send(false);
                                        continue;
                                    }

                                    // Persist the notarization
                                    debug!(view, "received notarization");
                                    let _ = response.send(true);
                                    notarized
                                        .put(view, notarization.block.digest(), value)
                                        .await
                                        .expect("Failed to insert notarized block");

                                    // Notify waiters
                                    if let Some(waiters) = waiters.remove(&notarization.block.digest()) {
                                        debug!(view, ?notarization.block.height, "waiter resolved via notarization");
                                        for waiter in waiters {
                                            let _ = waiter.send(notarization.block.clone());
                                        }
                                    }
                                },
                                key::Value::Finalized(height) => {
                                    // Parse finalization
                                    let Some(finalization) = Finalized::deserialize(Some(&self.public), &value) else {
                                        let _ = response.send(false);
                                        continue;
                                    };

                                    // Ensure the received payload is for the correct height
                                    if finalization.block.height != height {
                                        let _ = response.send(false);
                                        continue;
                                    }

                                    // Indicate the finalization was valid
                                    debug!(height, "received finalization");
                                    let _ = response.send(true);

                                    // Persist the finalization
                                    finalized
                                        .put(height, finalization.block.digest(), finalization.proof.serialize().into())
                                        .await
                                        .expect("Failed to insert finalization");

                                    // Persist the block
                                    blocks
                                        .put(height, finalization.block.digest(), finalization.block.serialize().into())
                                        .await
                                        .expect("Failed to insert finalized block");

                                    // Notify waiters
                                    if let Some(waiters) = waiters.remove(&finalization.block.digest()) {
                                        debug!(?finalization.block.height, "waiter resolved via finalization");
                                        for waiter in waiters {
                                            let _ = waiter.send(finalization.block.clone());
                                        }
                                    }

                                    // Notify finalizer
                                    let _ = finalizer_sender.try_send(());
                                },
                                key::Value::Digest(digest) => {
                                    // Parse block
                                    let block = Block::deserialize(&value).expect("Failed to deserialize block");

                                    // Ensure the received payload is for the correct digest
                                    if block.digest() != digest {
                                        let _ = response.send(false);
                                        continue;
                                    }

                                    // Persist the block
                                    debug!(?digest, height = block.height, "received block");
                                    let _ = response.send(true);
                                    blocks
                                        .put(block.height, digest.clone(), value)
                                        .await
                                        .expect("Failed to insert finalized block");

                                    // Notify waiters
                                    if let Some(waiters) = waiters.remove(&digest) {
                                        debug!(?block.height, "waiter resolved via block");
                                        for waiter in waiters {
                                            let _ = waiter.send(block.clone());
                                        }
                                    }

                                    // Notify finalizer
                                    let _ = finalizer_sender.try_send(());
                                }
                            }
                        }
                    }
                },
            }
        }
    }
}
