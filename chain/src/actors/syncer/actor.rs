use super::{
    archive::Wrapped,
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
use alto_types::{Block, Finalized, Notarized, NAMESPACE};
use commonware_broadcast::{buffered, Broadcaster};
use commonware_codec::{DecodeExt, Encode};
use commonware_consensus::threshold_simplex::types::{Finalization, Seedable, Viewable};
use commonware_cryptography::{bls12381, ed25519::PublicKey, sha256::Digest, Digestible};
use commonware_macros::select;
use commonware_p2p::{utils::requester, Receiver, Sender};
use commonware_resolver::{p2p, Resolver};
use commonware_runtime::{Clock, Handle, Metrics, Spawner, Storage};
use commonware_storage::{
    archive::{self, Archive, Identifier},
    index::translator::{EightCap, TwoCap},
    journal::{self, variable::Journal},
    metadata::{self, Metadata},
};
use commonware_utils::array::FixedBytes;
use futures::{channel::mpsc, lock::Mutex, StreamExt};
use governor::{clock::Clock as GClock, Quota};
use prometheus_client::metrics::gauge::Gauge;
use rand::Rng;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tracing::{debug, info, warn};

/// Application actor.
pub struct Actor<R: Rng + Spawner + Metrics + Clock + GClock + Storage, I: Indexer> {
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
    verified: Archive<TwoCap, Digest, R>,
    // Blocks notarized stored by view<>digest
    notarized: Archive<TwoCap, Digest, R>,

    // Finalizations stored by height
    finalized: Archive<EightCap, Digest, R>,
    // Blocks finalized stored by height
    //
    // We store this separately because we may not have the finalization for a block
    blocks: Archive<EightCap, Digest, R>,

    // Finalizer storage
    finalizer: Metadata<R, FixedBytes<1>>,

    // Latest height metric
    finalized_height: Gauge,
    // Indexed height metric
    contiguous_height: Gauge,
}

impl<R: Rng + Spawner + Metrics + Clock + GClock + Storage, I: Indexer> Actor<R, I> {
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
                section_mask: 0xffff_ffff_fff0_0000u64,
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
                section_mask: 0xffff_ffff_fff0_0000u64,
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
        backfill_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        buffer: buffered::Mailbox<Digest, Block>,
    ) -> Handle<()> {
        self.context.spawn_ref()(self.run(backfill_network, buffer))
    }

    /// Run the application actor.
    async fn run(
        mut self,
        backfill_network: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
        mut buffer: buffered::Mailbox<Digest, Block>,
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
                    public_key: self.public_key.clone(),
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
        let verified = Wrapped::<_, _, _, Block>::new(self.verified);
        let notarized = Wrapped::<_, _, _, Notarized>::new(self.notarized);
        let finalized = Wrapped::<_, _, _, Finalization<Digest>>::new(self.finalized);
        let blocks = Wrapped::<_, _, _, Block>::new(self.blocks);
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
                            *last_view_processed.lock().await = finalization.view();
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

                            // Attempt to repair one block from other sources
                            let target_block = gapped_block.parent;
                            let verified = verified
                                .get(Identifier::Key(&target_block))
                                .await
                                .expect("Failed to get verified block");
                            if let Some(verified) = verified {
                                let height = verified.height;
                                blocks
                                    .put(height, target_block, verified)
                                    .await
                                    .expect("Failed to insert finalized block");
                                debug!(height, "repaired block from verified");
                                continue;
                            }
                            let notarization = notarized
                                .get(Identifier::Key(&target_block))
                                .await
                                .expect("Failed to get notarized block");
                            if let Some(notarization) = notarization {
                                let height = notarization.block.height;
                                blocks
                                    .put(height, target_block, notarization.block)
                                    .await
                                    .expect("Failed to insert finalized block");
                                debug!(height, "repaired block from notarizations");
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
        let mut latest_view = 0;
        let mut outstanding_notarize = BTreeSet::new();
        loop {
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
                            buffer.broadcast(payload).await;
                        }
                        Message::Verified { view, payload } => {
                            verified
                                .put(view, payload.digest(), payload)
                                .await
                                .expect("Failed to insert verified block");
                        }
                        Message::Notarization { notarization } => {
                            // Upload seed to indexer (if available)
                            let view = notarization.view();
                            if let Some(indexer) = self.indexer.as_ref() {
                                self.context.with_label("indexer").spawn({
                                    let indexer = indexer.clone();
                                    let seed = notarization.seed();
                                    move |_| async move {
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
                            let proposal = &notarization.proposal;
                            let mut block = None;
                            if let Some(buffered) = buffer.get(proposal.payload).await {
                                block = Some(buffered.clone());
                            }

                            // Check if in verified blocks
                            if block.is_none() {
                                if let Some(verified) = verified.get(Identifier::Key(&proposal.payload)).await.expect("Failed to get verified block") {
                                    block = Some(verified);
                                }
                            }

                            // If found, store notarization
                            if let Some(block) = block {
                                let height = block.height;
                                let digest = proposal.payload;
                                let notarization = Notarized::new(notarization.clone(), block);

                                // Upload to indexer (if available)
                                if let Some(indexer) = self.indexer.as_ref() {
                                    self.context.with_label("indexer").spawn({
                                        let indexer = indexer.clone();
                                        let notarization = notarization.clone();
                                        move |_| async move {
                                            let result = indexer
                                                .notarized_upload(notarization)
                                                .await;
                                            if let Err(e) = result {
                                                warn!(?e, "failed to upload notarization");
                                                return;
                                            }
                                            debug!(view, "notarization uploaded to indexer");
                                        }
                                    });
                                }

                                // Persist the notarization
                                notarized
                                    .put(view, digest, notarization)
                                    .await
                                    .expect("Failed to insert notarized block");
                                debug!(view, height, "notarized block stored");
                                continue;
                            }

                            // Fetch from network
                            //
                            // We don't worry about retaining the proof because any peer must provide
                            // it to us when serving the notarization.
                            debug!(view, "notarized block missing");
                            outstanding_notarize.insert(view);
                            resolver.fetch(MultiIndex::new(Value::Notarized(view))).await;
                        }
                        Message::Finalization { finalization } => {
                            // Upload seed to indexer (if available)
                            let view = finalization.view();
                            if let Some(indexer) = self.indexer.as_ref() {
                                self.context.with_label("indexer").spawn({
                                    let indexer = indexer.clone();
                                    let seed = finalization.seed();
                                    move |_| async move {
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
                            let proposal = &finalization.proposal;
                            let mut block = None;
                            if let Some(buffered) = buffer.get(proposal.payload).await{
                                block = Some(buffered.clone());
                            }

                            // Check if in verified
                            if block.is_none() {
                                if let Some(verified) = verified.get(Identifier::Key(&proposal.payload)).await.expect("Failed to get verified block") {
                                    block = Some(verified);
                                }
                            }

                            // Check if in notarized
                            if block.is_none() {
                                if let Some(notarized) = notarized.get(Identifier::Key(&proposal.payload)).await.expect("Failed to get notarized block") {
                                    block = Some(notarized.block);
                                }
                            }

                            // If found, store finalization
                            if let Some(block) = block {
                                let digest = proposal.payload;
                                let height = block.height;

                                // Upload to indexer (if available)
                                if let Some(indexer) = self.indexer.as_ref() {
                                    self.context.with_label("indexer").spawn({
                                        let indexer = indexer.clone();
                                        let finalized = Finalized::new(finalization.clone(), block.clone());
                                        move |_| async move {
                                            let result = indexer
                                                .finalized_upload(finalized)
                                                .await;
                                            if let Err(e) = result {
                                                warn!(?e, "failed to upload finalization");
                                                return;
                                            }
                                            debug!(height, "finalization uploaded to indexer");
                                        }
                                    });
                                }

                                // Persist the finalization
                                finalized
                                    .put(height, proposal.payload, finalization)
                                    .await
                                    .expect("Failed to insert finalization");
                                blocks
                                    .put(height, digest, block)
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

                                continue;
                            }

                            // Fetch from network
                            warn!(view, digest = ?proposal.payload, "finalized block missing");
                            resolver.fetch(MultiIndex::new(Value::Digest(proposal.payload))).await;
                        }
                        Message::Get { view, payload, response } => {
                            // Check if in buffer
                            let buffered = buffer.get(payload).await;
                            if let Some(buffered) = buffered {
                                debug!(height = buffered.height, "found block in buffer");
                                let _ = response.send(buffered.clone());
                                continue;
                            }

                            // Check verified blocks
                            let block = verified.get(Identifier::Key(&payload)).await.expect("Failed to get verified block");
                            if let Some(block) = block {
                                debug!(height = block.height, "found block in verified");
                                let _ = response.send(block);
                                continue;
                            }

                            // Check if in notarized blocks
                            let notarization = notarized.get(Identifier::Key(&payload)).await.expect("Failed to get notarized block");
                            if let Some(notarization) = notarization {
                                let block = notarization.block;
                                debug!(height = block.height, "found block in notarized");
                                let _ = response.send(block);
                                continue;
                            }

                            // Check if in finalized blocks
                            let block = blocks.get(Identifier::Key(&payload)).await.expect("Failed to get finalized block");
                            if let Some(block) = block {
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
                            buffer.wait_prebuilt(payload, response).await;
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
                                        let _ = response.send(notarized.encode().into());
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

                                    // Get block
                                    let block = blocks.get(Identifier::Index(height)).await.expect("Failed to get finalized block");
                                    let Some(block) = block else {
                                        debug!(height, "finalized block missing on request");
                                        continue;
                                    };

                                    // Send finalization
                                    let payload = Finalized::new(finalization, block);
                                    let _ = response.send(payload.encode().into());
                                },
                                key::Value::Digest(digest) => {
                                    // Check buffer
                                    if let Some(block) = buffer.get(digest).await {
                                        let _ = response.send(block.encode().into());
                                        continue;
                                    }

                                    // Get verified block
                                    let block = verified.get(Identifier::Key(&digest)).await.expect("Failed to get verified block");
                                    if let Some(block) = block {
                                        let _ = response.send(block.encode().into());
                                        continue;
                                    }

                                    // Get notarized block
                                    let notarization = notarized.get(Identifier::Key(&digest)).await.expect("Failed to get notarized block");
                                    if let Some(notarized) = notarization {
                                        let _ = response.send(notarized.block.encode().into());
                                        continue;
                                    }

                                    // Get block
                                    let block = blocks.get(Identifier::Key(&digest)).await.expect("Failed to get finalized block");
                                    if let Some(block) = block {
                                        let _ = response.send(block.encode().into());
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
                                    let Ok(notarization) = Notarized::decode(value.as_ref()) else {
                                        let _ = response.send(false);
                                        continue;
                                    };
                                    if !notarization.verify(NAMESPACE, self.public.as_ref()) {
                                        let _ = response.send(false);
                                        continue;
                                    }

                                    // Ensure the received payload is for the correct view
                                    if notarization.proof.view() != view {
                                        let _ = response.send(false);
                                        continue;
                                    }

                                    // Persist the notarization
                                    debug!(view, "received notarization");
                                    let _ = response.send(true);
                                    notarized
                                        .put(view, notarization.block.digest(), notarization)
                                        .await
                                        .expect("Failed to insert notarized block");
                                },
                                key::Value::Finalized(height) => {
                                    // Parse finalization
                                    let Ok(finalization) = Finalized::decode(value.as_ref()) else {
                                        let _ = response.send(false);
                                        continue;
                                    };
                                    if !finalization.verify(NAMESPACE, self.public.as_ref()) {
                                        let _ = response.send(false);
                                        continue;
                                    }

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
                                        .put(height, finalization.block.digest(), finalization.proof)
                                        .await
                                        .expect("Failed to insert finalization");

                                    // Persist the block
                                    blocks
                                        .put(height, finalization.block.digest(), finalization.block)
                                        .await
                                        .expect("Failed to insert finalized block");

                                    // Notify finalizer
                                    let _ = finalizer_sender.try_send(());
                                },
                                key::Value::Digest(digest) => {
                                    // Parse block
                                    let block = Block::decode(value.as_ref()).expect("Failed to deserialize block");

                                    // Ensure the received payload is for the correct digest
                                    if block.digest() != digest {
                                        let _ = response.send(false);
                                        continue;
                                    }

                                    // Persist the block
                                    debug!(?digest, height = block.height, "received block");
                                    let _ = response.send(true);
                                    blocks
                                        .put(block.height, digest, block)
                                        .await
                                        .expect("Failed to insert finalized block");

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
