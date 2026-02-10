use crate::{resolver::HttpResolver, NoopReceiver, NoopSender};
use alto_types::{Block, Finalization, Scheme, EPOCH_LENGTH};
use commonware_broadcast::buffered;
use commonware_consensus::{
    marshal::{self, ingress::handler, Update},
    types::{FixedEpocher, ViewDelta},
    Reporter,
};
use commonware_cryptography::{
    certificate::{ConstantProvider, Scheme as CertScheme},
    ed25519::PublicKey,
    sha256::Digest,
    Signer,
};
use commonware_parallel::Sequential;
use commonware_runtime::{buffer::paged::CacheRef, Handle, Metrics, Spawner, Storage};
use commonware_storage::archive::immutable;
use commonware_utils::{channel::mpsc, Acknowledgement, NZU16, NZU64, NZUsize};
use governor::clock::Clock as GClock;
use rand::{CryptoRng, Rng};
use std::num::NonZero;
use tracing::info;

const PRUNABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(4_096);
const IMMUTABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(262_144);
const FREEZER_TABLE_RESIZE_FREQUENCY: u8 = 4;
const FREEZER_TABLE_RESIZE_CHUNK_SIZE: u32 = 2u32.pow(16);
const FREEZER_JOURNAL_TARGET_SIZE: u64 = 1024 * 1024 * 1024;
const FREEZER_JOURNAL_COMPRESSION: Option<u8> = Some(3);
const REPLAY_BUFFER: NonZero<usize> = NZUsize!(8 * 1024 * 1024);
const WRITE_BUFFER: NonZero<usize> = NZUsize!(1024 * 1024);
const BUFFER_POOL_PAGE_SIZE: NonZero<u16> = NZU16!(4_096);
const BUFFER_POOL_CAPACITY: NonZero<usize> = NZUsize!(8_192);
const MAX_REPAIR: NonZero<usize> = NZUsize!(20);
const VIEW_RETENTION_TIMEOUT: ViewDelta = ViewDelta::new(2560);
const DEQUE_SIZE: usize = 10;
const BLOCKS_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21);
const FINALIZED_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21);

#[allow(clippy::type_complexity)]
pub struct FollowerEngine<E>
where
    E: commonware_runtime::Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
{
    context: E,
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
    marshal_mailbox: marshal::Mailbox<Scheme, Block>,
}

impl<E> FollowerEngine<E>
where
    E: commonware_runtime::Clock + GClock + Rng + CryptoRng + Spawner + Storage + Metrics,
{
    pub async fn new(mut context: E, scheme: Scheme, mailbox_size: usize) -> Self {
        let seed: u64 = context.gen();
        let dummy_key = commonware_cryptography::ed25519::PrivateKey::from_seed(seed).public_key();

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

        let page_cache = CacheRef::new(BUFFER_POOL_PAGE_SIZE, BUFFER_POOL_CAPACITY);

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
                max_repair: MAX_REPAIR,
                page_cache,
                strategy: Sequential,
            },
        )
        .await;

        Self {
            context,
            buffer,
            buffer_mailbox,
            marshal,
            marshal_mailbox,
        }
    }

    pub fn mailbox(&self) -> marshal::Mailbox<Scheme, Block> {
        self.marshal_mailbox.clone()
    }

    pub fn buffer(&self) -> buffered::Mailbox<PublicKey, Block> {
        self.buffer_mailbox.clone()
    }

    pub fn start(
        self,
        ingress_rx: mpsc::Receiver<handler::Message<Block>>,
        resolver: HttpResolver,
    ) -> (Handle<()>, Handle<()>) {
        let context = self.context.clone();

        let buffer_handle = self.buffer.start((NoopSender, NoopReceiver));

        let buffer_mailbox = self.buffer_mailbox;
        let marshal = self.marshal;
        let engine_handle = context.spawn(move |_| async move {
            let app = FollowerApplication;
            marshal
                .start(app, buffer_mailbox, (ingress_rx, resolver))
                .await
                .expect("marshal failed");
        });

        (engine_handle, buffer_handle)
    }
}

#[derive(Clone)]
struct FollowerApplication;

impl Reporter for FollowerApplication {
    type Activity = Update<Block>;

    async fn report(&mut self, activity: Self::Activity) {
        if let Update::Block(block, ack_rx) = activity {
            info!(height = block.height.get(), "reported block");
            ack_rx.acknowledge();
        }
    }
}
