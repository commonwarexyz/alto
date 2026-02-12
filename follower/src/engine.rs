use crate::{
    resolver::HttpResolver,
    store::{CaughtUpFlag, DeferredBlocks, DeferredCertificates},
    NoopReceiver, NoopSender,
};
use alto_types::{Block, Scheme, EPOCH_LENGTH};
use commonware_broadcast::buffered;
use commonware_consensus::{
    marshal::{self, ingress::handler, Update},
    types::{FixedEpocher, Height, ViewDelta},
    Reporter,
};
use commonware_cryptography::{
    certificate::{ConstantProvider, Scheme as CertScheme},
    ed25519::{PrivateKey, PublicKey},
    Signer,
};
use commonware_math::algebra::Random;
use commonware_parallel::Sequential;
use commonware_runtime::{buffer::paged::CacheRef, Handle, Metrics, Spawner, Storage};
use commonware_storage::archive::immutable;
use commonware_utils::{channel::mpsc, Acknowledgement, NZUsize, NZU16, NZU64};
use governor::clock::Clock as GClock;
use rand::{CryptoRng, Rng};
use std::{
    num::NonZero,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::SystemTime,
};
use tracing::info;

const PRUNABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(4_096);
const IMMUTABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(262_144);
const FREEZER_TABLE_RESIZE_FREQUENCY: u8 = 4;
const FREEZER_TABLE_RESIZE_CHUNK_SIZE: u32 = 2u32.pow(16); // 3MB
const FREEZER_JOURNAL_TARGET_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
const FREEZER_JOURNAL_COMPRESSION: Option<u8> = Some(3);
const REPLAY_BUFFER: NonZero<usize> = NZUsize!(8 * 1024 * 1024); // 8MB
const WRITE_BUFFER: NonZero<usize> = NZUsize!(1024 * 1024); // 1MB
const BUFFER_POOL_PAGE_SIZE: NonZero<u16> = NZU16!(4_096); // 4KB
const BUFFER_POOL_CAPACITY: NonZero<usize> = NZUsize!(8_192); // 32MB
pub const DEFAULT_MAX_REPAIR: NonZero<usize> = NZUsize!(256);
const VIEW_RETENTION_TIMEOUT: ViewDelta = ViewDelta::new(2560);
const DEQUE_SIZE: usize = 10;
const BACKFILL_LOG_INTERVAL: u64 = 1000;
const BLOCKS_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21); // 100MB
const FINALIZED_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21); // 100MB

/// The engine that drives the follower's [marshal::Actor].
///
/// Unlike the validator's engine, this does not run consensus. Instead, it
/// relies on a [CertificateFeeder](crate::feeder::CertificateFeeder) to feed
/// certificates from a trusted source and an [HttpResolverActor](crate::resolver::HttpResolverActor)
/// to backfill missing blocks.
#[allow(clippy::type_complexity)]
pub struct Engine<E>
where
    E: commonware_runtime::Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
{
    context: E,
    caught_up: CaughtUpFlag,
    buffer: buffered::Engine<E, PublicKey, Block>,
    buffer_mailbox: buffered::Mailbox<PublicKey, Block>,
    marshal: marshal::Actor<
        E,
        Block,
        ConstantProvider<Scheme, commonware_consensus::types::Epoch>,
        DeferredCertificates<E>,
        DeferredBlocks<E>,
        FixedEpocher,
        Sequential,
    >,
    marshal_mailbox: marshal::Mailbox<Scheme, Block>,
}

impl<E> Engine<E>
where
    E: commonware_runtime::Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
{
    /// Create a new [Engine].
    pub async fn new(mut context: E, scheme: Scheme, mailbox_size: usize, max_repair: NonZero<usize>) -> Self {
        // Create the buffer
        //
        // The follower does not participate in p2p broadcast, so we use a dummy
        // key and noop sender/receiver. The buffer is still required by marshal.
        let dummy_key = PrivateKey::random(&mut context).public_key();

        let (buffer, buffer_mailbox) = buffered::Engine::new(
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
        let page_cache = CacheRef::new(BUFFER_POOL_PAGE_SIZE, BUFFER_POOL_CAPACITY);

        // Initialize finalizations by height
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
                freezer_key_page_cache: page_cache.clone(),
                freezer_value_partition: "follower-finalizations-by-height-freezer-value-journal"
                    .to_string(),
                freezer_value_target_size: FREEZER_JOURNAL_TARGET_SIZE,
                freezer_value_compression: FREEZER_JOURNAL_COMPRESSION,
                ordinal_partition: "follower-finalizations-by-height-ordinal".to_string(),
                items_per_section: IMMUTABLE_ITEMS_PER_SECTION,
                freezer_key_write_buffer: WRITE_BUFFER,
                freezer_value_write_buffer: WRITE_BUFFER,
                ordinal_write_buffer: WRITE_BUFFER,
                codec_config: scheme.certificate_codec_config(),
                replay_buffer: REPLAY_BUFFER,
            },
        )
        .await
        .expect("failed to initialize finalizations by height archive");
        info!("restored finalizations by height archive");

        // Initialize finalized blocks
        let finalized_blocks = immutable::Archive::init(
            context.with_label("finalized_blocks"),
            immutable::Config {
                metadata_partition: "follower-finalized-blocks-metadata".to_string(),
                freezer_table_partition: "follower-finalized-blocks-freezer-table".to_string(),
                freezer_table_initial_size: BLOCKS_FREEZER_TABLE_INITIAL_SIZE,
                freezer_table_resize_frequency: FREEZER_TABLE_RESIZE_FREQUENCY,
                freezer_table_resize_chunk_size: FREEZER_TABLE_RESIZE_CHUNK_SIZE,
                freezer_key_partition: "follower-finalized-blocks-freezer-key-journal".to_string(),
                freezer_key_page_cache: page_cache.clone(),
                freezer_value_partition: "follower-finalized-blocks-freezer-value-journal"
                    .to_string(),
                freezer_value_target_size: FREEZER_JOURNAL_TARGET_SIZE,
                freezer_value_compression: None,
                ordinal_partition: "follower-finalized-blocks-ordinal".to_string(),
                items_per_section: IMMUTABLE_ITEMS_PER_SECTION,
                freezer_key_write_buffer: WRITE_BUFFER,
                freezer_value_write_buffer: WRITE_BUFFER,
                ordinal_write_buffer: WRITE_BUFFER,
                codec_config: (),
                replay_buffer: REPLAY_BUFFER,
            },
        )
        .await
        .expect("failed to initialize finalized blocks archive");
        info!("restored finalized blocks archive");

        // Wrap archives with deferred sync
        let caught_up: CaughtUpFlag = Arc::new(AtomicBool::new(false));
        let finalizations_by_height =
            DeferredCertificates::new(finalizations_by_height, caught_up.clone());
        let finalized_blocks = DeferredBlocks::new(finalized_blocks, caught_up.clone());

        // Create marshal
        let provider = ConstantProvider::new(scheme);
        let epocher = FixedEpocher::new(EPOCH_LENGTH);

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
                max_repair,
                page_cache,
                strategy: Sequential,
            },
        )
        .await;

        // Return the engine
        Self {
            context,
            caught_up,
            buffer,
            buffer_mailbox,
            marshal,
            marshal_mailbox,
        }
    }

    /// Returns a clone of the [marshal::Mailbox] for feeding certificates.
    pub fn mailbox(&self) -> marshal::Mailbox<Scheme, Block> {
        self.marshal_mailbox.clone()
    }

    /// Start the [Engine].
    pub fn start(
        self,
        ingress_rx: mpsc::Receiver<handler::Message<Block>>,
        resolver: HttpResolver,
    ) -> (Handle<()>, Handle<()>) {
        let context = self.context.clone();

        // Start the buffer
        let buffer_handle = self.buffer.start((NoopSender, NoopReceiver));

        // Start marshal
        let buffer_mailbox = self.buffer_mailbox;
        let marshal = self.marshal;
        let caught_up = self.caught_up;
        let backfill_start = context.current();
        let engine_handle = context.spawn(move |_| async move {
            let app = Application {
                tip: Height::zero(),
                caught_up,
                backfill_last_time: backfill_start,
                backfill_last_height: Height::zero(),
            };
            marshal
                .start(app, buffer_mailbox, (ingress_rx, resolver))
                .await
                .expect("marshal failed");
        });

        (engine_handle, buffer_handle)
    }
}

/// Reporter that acknowledges finalized blocks from [marshal::Actor].
///
/// Tracks the latest finalized tip and signals when the follower has caught up,
/// transitioning the deferred archive wrappers from batched to per-block sync.
#[derive(Clone)]
struct Application {
    tip: Height,
    caught_up: CaughtUpFlag,
    backfill_last_time: SystemTime,
    backfill_last_height: Height,
}

impl Reporter for Application {
    type Activity = Update<Block>;

    async fn report(&mut self, activity: Self::Activity) {
        match activity {
            Update::Tip(_, height, _) => {
                self.tip = height;
            }
            Update::Block(block, ack) => {
                let height = block.height;
                if !self.caught_up.load(Ordering::Relaxed) {
                    if height.get() - self.backfill_last_height.get() >= BACKFILL_LOG_INTERVAL {
                        let now = SystemTime::now();
                        let elapsed = now
                            .duration_since(self.backfill_last_time)
                            .unwrap_or_default()
                            .as_secs_f64();
                        let blocks = (height.get() - self.backfill_last_height.get()) as f64;
                        let bps = if elapsed > 0.0 {
                            blocks / elapsed
                        } else {
                            0.0
                        };
                        let remaining = self.tip.get().saturating_sub(height.get());
                        let eta_secs = if bps > 0.0 {
                            remaining as f64 / bps
                        } else {
                            0.0
                        };
                        let eta = eta_secs as u64;
                        let hours = eta / 3600;
                        let minutes = (eta % 3600) / 60;
                        let seconds = eta % 60;
                        info!(
                            height = height.get(),
                            tip = self.tip.get(),
                            remaining,
                            blocks_per_sec = format!("{bps:.0}"),
                            eta = format!("{hours}h{minutes:02}m{seconds:02}s"),
                            "backfilling",
                        );
                        self.backfill_last_height = height;
                        self.backfill_last_time = now;
                    }
                    if height >= self.tip && self.tip > Height::zero() {
                        info!(height = height.get(), "caught up to tip, enabling sync");
                        self.caught_up.store(true, Ordering::Relaxed);
                    }
                } else {
                    info!(height = height.get(), "reported block");
                }
                ack.acknowledge();
            }
        }
    }
}
