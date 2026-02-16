use crate::{resolver::Resolver, throughput::Throughput, NoopReceiver, NoopSender};
use alto_types::{Block, Finalization, Scheme, EPOCH_LENGTH};
use commonware_broadcast::buffered;
use commonware_consensus::{
    marshal::{self, ingress::handler, Update},
    types::{FixedEpocher, ViewDelta},
    Reporter,
};
use commonware_cryptography::{
    certificate::{ConstantProvider, Scheme as CertScheme},
    ed25519::{PrivateKey, PublicKey},
    sha256::Digest,
    Signer,
};
use commonware_math::algebra::Random;
use commonware_parallel::Sequential;
use commonware_runtime::{
    buffer::paged::CacheRef, spawn_cell, Clock, ContextCell, Handle, Metrics, Spawner, Storage,
};
use commonware_storage::archive::immutable;
use commonware_utils::{channel::mpsc, Acknowledgement, NZUsize, NZU16, NZU64};
use futures::future::try_join_all;
use governor::clock::Clock as GClock;
use rand::{CryptoRng, Rng};
use std::num::NonZero;
use tracing::{error, info, warn};

const PRUNABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(4_096);
const IMMUTABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(262_144);
const FREEZER_TABLE_RESIZE_FREQUENCY: u8 = 4;
const FREEZER_TABLE_RESIZE_CHUNK_SIZE: u32 = 2u32.pow(16); // 3MB
const FREEZER_JOURNAL_TARGET_SIZE: u64 = 1024 * 1024 * 1024; // 1GB
const FREEZER_JOURNAL_COMPRESSION: Option<u8> = Some(3);
const REPLAY_BUFFER: NonZero<usize> = NZUsize!(8 * 1024 * 1024); // 8MB
const WRITE_BUFFER: NonZero<usize> = NZUsize!(1024 * 1024); // 1MB
const PAGE_CACHE_PAGE_SIZE: NonZero<u16> = NZU16!(4_096); // 4KB
const PAGE_CACHE_CAPACITY: NonZero<usize> = NZUsize!(8_192); // 32MB
pub const DEFAULT_MAX_REPAIR: NonZero<usize> = NZUsize!(256);
const VIEW_RETENTION_TIMEOUT: ViewDelta = ViewDelta::new(2560);
const DEQUE_SIZE: usize = 10;
const BLOCKS_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21); // 100MB
const FINALIZED_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21); // 100MB
const THROUGHPUT_WINDOW: std::time::Duration = std::time::Duration::from_secs(30);

/// The engine that drives the follower's [marshal::Actor].
///
/// Unlike the validator's engine, this does not run consensus. Instead, it
/// relies on a [Feeder](crate::feeder::Feeder) to feed certificates from a
/// trusted source and an [Actor](crate::resolver::Actor) to backfill missing
/// blocks.
#[allow(clippy::type_complexity)]
pub struct Engine<E>
where
    E: commonware_runtime::Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
{
    context: ContextCell<E>,
    buffer: buffered::Engine<E, PublicKey, Block>,
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
}

impl<E> Engine<E>
where
    E: commonware_runtime::Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
{
    /// Create a new [Engine].
    pub async fn new(
        mut context: E,
        scheme: Scheme,
        mailbox_size: usize,
        max_repair: NonZero<usize>,
    ) -> (Self, marshal::Mailbox<Scheme, Block>) {
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

        // Create the page cache
        let page_cache = CacheRef::new(PAGE_CACHE_PAGE_SIZE, PAGE_CACHE_CAPACITY);

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

        // Create marshal
        let provider = ConstantProvider::new(scheme);
        let epocher = FixedEpocher::new(EPOCH_LENGTH);
        let (marshal, mailbox, _) = marshal::Actor::init(
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

        // Return the engine and marshal mailbox
        let engine = Self {
            context: ContextCell::new(context),
            buffer,
            buffer_mailbox,
            marshal,
        };
        (engine, mailbox)
    }

    /// Start the [Engine].
    pub fn start(
        mut self,
        marshal: (mpsc::Receiver<handler::Message<Block>>, Resolver),
    ) -> Handle<()> {
        spawn_cell!(self.context, self.run(marshal).await)
    }

    async fn run(mut self, marshal: (mpsc::Receiver<handler::Message<Block>>, Resolver)) {
        // Start the buffer
        let buffer_handle = self.buffer.start((NoopSender, NoopReceiver));

        // Start marshal
        let marshal_handle = self.marshal.start(
            Application::new(self.context.take()),
            self.buffer_mailbox,
            marshal,
        );

        // Wait for any actor to finish
        if let Err(e) = try_join_all(vec![buffer_handle, marshal_handle]).await {
            error!(?e, "engine failed");
        } else {
            warn!("engine stopped");
        }
    }
}

/// Reporter that acknowledges finalized blocks from [marshal::Actor]
/// and logs throughput over a 30-second sliding window.
#[derive(Clone)]
struct Application<E: Clock> {
    context: E,
    throughput: Throughput,
}

impl<E: Clock> Application<E> {
    fn new(context: E) -> Self {
        Self {
            context,
            throughput: Throughput::new(THROUGHPUT_WINDOW),
        }
    }
}

impl<E: Clock> Reporter for Application<E> {
    type Activity = Update<Block>;

    async fn report(&mut self, activity: Self::Activity) {
        if let Update::Block(block, ack_rx) = activity {
            let bps = self.throughput.record(self.context.current());
            info!(
                height = block.height.get(),
                bps = format!("{bps:.2}"),
                "reported block"
            );
            ack_rx.acknowledge();
        }
    }
}
