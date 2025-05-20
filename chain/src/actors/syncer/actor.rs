use super::{
    coordinator::Coordinator,
    handler::Handler,
    ingress::{Mailbox, Message, Orchestration, Orchestrator},
    Config,
};
use crate::{
    actors::syncer::{
        handler,
        key::{self, MultiIndex, Value},
    },
    Indexer,
};
use alto_types::{Block, Finalization, Finalized, Identity, Notarized, NAMESPACE};
use commonware_broadcast::{buffered, Broadcaster};
use commonware_codec::{DecodeExt, Encode};
use commonware_consensus::threshold_simplex::types::{Seedable, Viewable};
use commonware_cryptography::{ed25519::PublicKey, sha256::Digest, Digestible};
use commonware_macros::select;
use commonware_p2p::{utils::requester, Receiver, Recipients, Sender};
use commonware_resolver::{p2p, Resolver};
use commonware_runtime::{Clock, Handle, Metrics, Spawner, Storage};
use commonware_storage::{
    archive::{self, Archive, Identifier},
    index::translator::{EightCap, TwoCap},
    metadata::{self, Metadata},
};
use commonware_utils::array::FixedBytes;
use futures::{channel::mpsc, StreamExt};
use governor::{clock::Clock as GClock, Quota};
use prometheus_client::metrics::gauge::Gauge;
use rand::Rng;
use std::{collections::BTreeSet, time::Duration};
use tracing::{debug, info, warn};

const REPLAY_BUFFER: usize = 8 * 1024 * 1024;
const REPLAY_CONCURRENCY: usize = 4;
const WRITE_BUFFER: usize = 1024 * 1024;

/// Application actor.
pub struct Actor<R: Rng + Spawner + Metrics + Clock + GClock + Storage, I: Indexer> {
    context: R,
    public_key: PublicKey,
    identity: Identity,
    participants: Vec<PublicKey>,
    mailbox: mpsc::Receiver<Message>,
    mailbox_size: usize,
    backfill_quota: Quota,
    activity_timeout: u64,
    indexer: Option<I>,

    // Blocks verified stored by view<>digest
    verified: Archive<TwoCap, R, Digest, Block>,
    // Blocks notarized stored by view<>digest
    notarized: Archive<TwoCap, R, Digest, Notarized>,

    // Finalizations stored by height
    finalized: Archive<EightCap, R, Digest, Finalization>,
    // Blocks finalized stored by height
    //
    // We store this separately because we may not have the finalization for a block
    blocks: Archive<EightCap, R, Digest, Block>,

    // Finalizer storage
    finalizer_metadata: Metadata<R, FixedBytes<1>>,

    // Latest height metric
    finalized_height: Gauge,
    // Indexed height metric
    contiguous_height: Gauge,
}

impl<R: Rng + Spawner + Metrics + Clock + GClock + Storage, I: Indexer> Actor<R, I> {
    /// Create a new application actor.
    pub async fn init(context: R, config: Config<I>) -> (Self, Mailbox) {
        // Initialize verified blocks
        let verified_archive = Archive::init(
            context.with_label("verified_archive"),
            archive::Config {
                partition: format!("{}-verifications", config.partition_prefix),
                translator: TwoCap,
                section_mask: 0xffff_ffff_ffff_f000u64,
                pending_writes: 0,
                replay_concurrency: REPLAY_CONCURRENCY,
                compression: Some(3),
                codec_config: (),
                replay_buffer: REPLAY_BUFFER,
                write_buffer: WRITE_BUFFER,
            },
        )
        .await
        .expect("Failed to initialize verified archive");

        // Initialize notarized blocks
        let notarized_archive = Archive::init(
            context.with_label("notarized_archive"),
            archive::Config {
                partition: format!("{}-notarizations", config.partition_prefix),
                translator: TwoCap,
                section_mask: 0xffff_ffff_ffff_f000u64,
                pending_writes: 0,
                replay_concurrency: REPLAY_CONCURRENCY,
                compression: Some(3),
                codec_config: (),
                replay_buffer: REPLAY_BUFFER,
                write_buffer: WRITE_BUFFER,
            },
        )
        .await
        .expect("Failed to initialize notarized archive");

        // Initialize finalizations
        let finalized_archive = Archive::init(
            context.with_label("finalized_archive"),
            archive::Config {
                partition: format!("{}-finalizations", config.partition_prefix),
                translator: EightCap,
                section_mask: 0xffff_ffff_fff0_0000u64,
                pending_writes: 0,
                replay_concurrency: REPLAY_CONCURRENCY,
                compression: Some(3),
                codec_config: (),
                replay_buffer: REPLAY_BUFFER,
                write_buffer: WRITE_BUFFER,
            },
        )
        .await
        .expect("Failed to initialize finalized archive");

        // Initialize blocks
        let block_archive = Archive::init(
            context.with_label("block_archive"),
            archive::Config {
                partition: format!("{}-blocks", config.partition_prefix),
                translator: EightCap,
                section_mask: 0xffff_ffff_fff0_0000u64,
                pending_writes: 0,
                replay_concurrency: REPLAY_CONCURRENCY,
                compression: Some(3),
                codec_config: (),
                replay_buffer: REPLAY_BUFFER,
                write_buffer: WRITE_BUFFER,
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
                identity: config.identity,
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

                finalizer_metadata,

                finalized_height,
                contiguous_height,
            },
            Mailbox::new(sender),
        )
    }

    pub fn start(
        mut self,
        buffer: buffered::Mailbox<PublicKey, Digest, Digest, Block>,
        backfill: (
            impl Sender<PublicKey = PublicKey>,
            impl Receiver<PublicKey = PublicKey>,
        ),
    ) -> Handle<()> {
        self.context.spawn_ref()(self.run(buffer, backfill))
    }

    /// Run the application actor.
    async fn run(
        mut self,
        mut buffer: buffered::Mailbox<PublicKey, Digest, Digest, Block>,
        backfill: (
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
        resolver_engine.start(backfill);

        // Process all finalized blocks in order (fetching any that are missing)
        let (mut finalizer_sender, mut finalizer_receiver) = mpsc::channel::<()>(1);
        let (orchestrator_sender, mut orchestrator_receiver) = mpsc::channel(2); // buffer to send processed while moving forward
        let mut orchestor = Orchestrator::new(orchestrator_sender);
        self.context
            .with_label("finalizer")
            .spawn(move |_| async move {
                // Initialize last indexed from metadata store
                let latest_key = FixedBytes::new([0u8]);
                let mut last_indexed = if let Some(bytes) = self.finalizer_metadata.get(&latest_key)
                {
                    u64::from_be_bytes(bytes.to_vec().try_into().unwrap())
                } else {
                    0
                };

                // Index all finalized blocks
                //
                // If using state sync, this is not necessary.
                loop {
                    // Check if the next block is available
                    let next = last_indexed + 1;
                    if let Some(block) = orchestor.get(next).await {
                        // Update metadata
                        self.finalizer_metadata
                            .put(latest_key.clone(), next.to_be_bytes().to_vec());
                        self.finalizer_metadata
                            .sync()
                            .await
                            .expect("Failed to sync finalizer");

                        // In an application that maintains state, you would compute the state transition function here.

                        // Update the latest indexed
                        self.contiguous_height.set(next as i64);
                        last_indexed = next;
                        info!(height = next, "indexed finalized block");

                        // Update last view processed (if we have a finalization for this block)
                        orchestor.processed(next, block.digest()).await;
                        continue;
                    }

                    // Try to connect to our latest handled block (may not exist finalizations for some heights)
                    if orchestor.repair(next).await {
                        continue;
                    }

                    // If nothing to do, wait for some message from someone that the finalized store was updated
                    debug!(height = next, "waiting to index finalized block");
                    let _ = finalizer_receiver.next().await;
                }
            });

        // Handle messages
        let mut latest_view = 0;
        let mut requested_blocks = BTreeSet::new();
        let mut last_view_processed: u64 = 0;
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
                // Handle consensus before finalizer or backfiller
                mailbox_message = self.mailbox.next() => {
                    let message = mailbox_message.expect("Mailbox closed");
                    match message {
                        Message::Broadcast { payload } => {
                            let ack = buffer.broadcast(Recipients::All, payload).await;
                            drop(ack);
                        }
                        Message::Verified { view, payload } => {
                            match self.verified
                                .put(view, payload.digest(), payload)
                                .await {
                                    Ok(_) => {
                                        debug!(view, "verified block stored");
                                    },
                                    Err(archive::Error::AlreadyPrunedTo(_)) => {
                                        debug!(view, "verified block already pruned");
                                    }
                                    Err(e) => {
                                        panic!("Failed to insert verified block: {e}");
                                    }
                                };
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
                            let mut block =  buffer.get(None, proposal.payload, Some(proposal.payload)).await.into_iter().next();

                            // Check if in verified blocks
                            if block.is_none() {
                                block = self.verified.get(Identifier::Key(&proposal.payload)).await.expect("Failed to get verified block");
                            }

                            // If found, store notarization
                            if let Some(block) = block {
                                let height = block.height;
                                let digest = proposal.payload;
                                let notarization = Notarized::new(notarization, block);

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
                                match self.notarized
                                    .put(view, digest, notarization)
                                    .await {
                                    Ok(_) => {
                                        debug!(view, height, "notarized block stored");
                                    },
                                    Err(archive::Error::AlreadyPrunedTo(_)) => {
                                        debug!(view, "notarized already pruned");
                                    },
                                    Err(e) => {
                                        panic!("Failed to insert notarized block: {e}");
                                    }
                                };
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
                            let mut block = buffer.get(None, proposal.payload, Some(proposal.payload)).await.into_iter().next();

                            // Check if in verified
                            if block.is_none() {
                                block = self.verified.get(Identifier::Key(&proposal.payload)).await.expect("Failed to get verified block");
                            }

                            // Check if in notarized
                            if block.is_none() {
                                block = self.notarized.get(Identifier::Key(&proposal.payload)).await.expect("Failed to get notarized block").map(|notarized| notarized.block);
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
                                self.finalized
                                    .put(height, proposal.payload, finalization)
                                    .await
                                    .expect("Failed to insert finalization");
                                self.blocks
                                    .put(height, digest, block)
                                    .await
                                    .expect("Failed to insert finalized block");
                                debug!(view, height, "finalized block stored");

                                // Prune blocks
                                let min_view = last_view_processed.saturating_sub(self.activity_timeout);
                                self.verified
                                    .prune(min_view)
                                    .await
                                    .expect("Failed to prune verified block");
                                self.notarized
                                    .prune(min_view)
                                    .await
                                    .expect("Failed to prune notarized block");
                                info!(min_view, "pruned verified and notarized archives");

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
                            let buffered = buffer.get(None, payload, Some(payload)).await.into_iter().next();
                            if let Some(buffered) = buffered {
                                debug!(height = buffered.height, "found block in buffer");
                                let _ = response.send(buffered);
                                continue;
                            }

                            // Check verified blocks
                            let block = self.verified.get(Identifier::Key(&payload)).await.expect("Failed to get verified block");
                            if let Some(block) = block {
                                debug!(height = block.height, "found block in verified");
                                let _ = response.send(block);
                                continue;
                            }

                            // Check if in notarized blocks
                            let notarization = self.notarized.get(Identifier::Key(&payload)).await.expect("Failed to get notarized block");
                            if let Some(notarization) = notarization {
                                let block = notarization.block;
                                debug!(height = block.height, "found block in notarized");
                                let _ = response.send(block);
                                continue;
                            }

                            // Check if in finalized blocks
                            let block = self.blocks.get(Identifier::Key(&payload)).await.expect("Failed to get finalized block");
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
                            buffer.subscribe_prepared(None, payload, Some(payload), response).await;
                        }
                    }
                },
                // Handle finalizer messages next
                orchestrator_message = orchestrator_receiver.next() => {
                    let orchestrator_message = orchestrator_message.expect("Orchestrator closed");
                    match orchestrator_message {
                        Orchestration::Get { next, result } => {
                            // Check if in blocks
                            let block = self.blocks.get(Identifier::Index(next)).await.expect("Failed to get finalized block");
                            result.send(block).expect("Failed to send block");
                        }
                        Orchestration::Processed { next, digest } => {
                            // Cancel any outstanding requests (by height and by digest)
                            resolver.cancel(MultiIndex::new(Value::Finalized(next))).await;
                            resolver.cancel(MultiIndex::new(Value::Digest(digest))).await;

                            // If finalization exists, mark as last_view_processed
                            let finalization = self.finalized.get(Identifier::Index(next)).await.expect("Failed to get finalized block");
                            if let Some(finalization) = finalization {
                                last_view_processed = finalization.view();
                            }

                            // Drain requested blocks less than next
                            requested_blocks.retain(|height| *height > next);
                        }
                        Orchestration::Repair { next, result } => {
                            // Find next gap
                            let (_, start_next) = self.blocks.next_gap(next);
                            let Some(start_next) = start_next else {
                                result.send(false).expect("Failed to send repair result");
                                continue;
                            };

                            // If we are at some height greater than genesis, attempt to repair the parent
                            if next > 0 {
                                // Get gapped block
                                let gapped_block = self.blocks.get(Identifier::Index(start_next)).await.expect("Failed to get finalized block").expect("Gapped block missing");

                                // Attempt to repair one block from other sources
                                let target_block = gapped_block.parent;
                                let verified = self.verified.get(Identifier::Key(&target_block)).await.expect("Failed to get verified block");
                                if let Some(verified) = verified {
                                    let height = verified.height;
                                    self.blocks.put(height, target_block, verified).await.expect("Failed to insert finalized block");
                                    debug!(height, "repaired block from verified");
                                    result.send(true).expect("Failed to send repair result");
                                    continue;
                                }
                                let notarization = self.notarized.get(Identifier::Key(&target_block)).await.expect("Failed to get notarized block");
                                if let Some(notarization) = notarization {
                                    let height = notarization.block.height;
                                    self.blocks.put(height, target_block, notarization.block).await.expect("Failed to insert finalized block");
                                    debug!(height, "repaired block from notarizations");
                                    result.send(true).expect("Failed to send repair result");
                                    continue;
                                }

                                // Request the parent block digest
                                resolver.fetch(MultiIndex::new(Value::Digest(target_block))).await;
                            }

                            // Enqueue next items (by index)
                            let range = next..std::cmp::min(start_next, next + 20);
                            debug!(range.start, range.end, "requesting missing finalized blocks");
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
                            result.send(false).expect("Failed to send repair result");
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
                                    let notarization = self.notarized.get(Identifier::Index(view)).await.expect("Failed to get notarized block");
                                    if let Some(notarized) = notarization {
                                        let _ = response.send(notarized.encode().into());
                                    } else {
                                        debug!(view, "notarization missing on request");
                                    }
                                },
                                key::Value::Finalized(height) => {
                                    // Get finalization
                                    let finalization = self.finalized.get(Identifier::Index(height)).await.expect("Failed to get finalization");
                                    let Some(finalization) = finalization else {
                                        debug!(height, "finalization missing on request");
                                        continue;
                                    };

                                    // Get block
                                    let block = self.blocks.get(Identifier::Index(height)).await.expect("Failed to get finalized block");
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
                                    let block = buffer.get(None, digest, Some(digest)).await.into_iter().next();
                                    if let Some(block) = block {
                                        let _ = response.send(block.encode().into());
                                        continue;
                                    }

                                    // Get verified block
                                    let block = self.verified.get(Identifier::Key(&digest)).await.expect("Failed to get verified block");
                                    if let Some(block) = block {
                                        let _ = response.send(block.encode().into());
                                        continue;
                                    }

                                    // Get notarized block
                                    let notarization = self.notarized.get(Identifier::Key(&digest)).await.expect("Failed to get notarized block");
                                    if let Some(notarized) = notarization {
                                        let _ = response.send(notarized.block.encode().into());
                                        continue;
                                    }

                                    // Get block
                                    let block = self.blocks.get(Identifier::Key(&digest)).await.expect("Failed to get finalized block");
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
                                    if !notarization.verify(NAMESPACE, &self.identity) {
                                        let _ = response.send(false);
                                        continue;
                                    }

                                    // Ensure the received payload is for the correct view
                                    if notarization.proof.view() != view {
                                        let _ = response.send(false);
                                        continue;
                                    }

                                    // Persist the notarization
                                    let _ = response.send(true);
                                    match self.notarized
                                        .put(view, notarization.block.digest(), notarization)
                                        .await {
                                        Ok(_) => {
                                            debug!(view, "notarized stored");
                                        },
                                        Err(archive::Error::AlreadyPrunedTo(_)) => {
                                            debug!(view, "notarized already pruned");

                                        }
                                        Err(e) => {
                                            panic!("Failed to insert notarized block: {e}");
                                        }
                                    };
                                },
                                key::Value::Finalized(height) => {
                                    // Parse finalization
                                    let Ok(finalization) = Finalized::decode(value.as_ref()) else {
                                        let _ = response.send(false);
                                        continue;
                                    };
                                    if !finalization.verify(NAMESPACE, &self.identity) {
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
                                    self.finalized
                                        .put(height, finalization.block.digest(), finalization.proof)
                                        .await
                                        .expect("Failed to insert finalization");

                                    // Persist the block
                                    self.blocks
                                        .put(height, finalization.block.digest(), finalization.block)
                                        .await
                                        .expect("Failed to insert finalized block");

                                    // Notify finalizer
                                    let _ = finalizer_sender.try_send(());
                                },
                                key::Value::Digest(digest) => {
                                    // Parse block
                                    let Ok(block) = Block::decode(value.as_ref()) else {
                                        let _ = response.send(false);
                                        continue;
                                    };

                                    // Ensure the received payload is for the correct digest
                                    if block.digest() != digest {
                                        let _ = response.send(false);
                                        continue;
                                    }

                                    // Persist the block
                                    debug!(?digest, height = block.height, "received block");
                                    let _ = response.send(true);
                                    self.blocks
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
