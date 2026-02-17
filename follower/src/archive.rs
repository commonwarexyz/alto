//! Archive storage for the follower.
//!
//! The follower stores two collections of finalized data:
//!
//!   - Certificates (finalizations keyed by height)
//!   - Blocks (finalized blocks keyed by digest)
//!
//! When pruning is enabled (`pruning_depth` is `Some`), both collections use
//! [prunable::Archive] so old sections can be reclaimed. When pruning is
//! disabled, [immutable::Archive] is used instead (optimized for append-only
//! workloads with freezer-backed storage).
//!
//! [Certificates] and [Blocks] are enum wrappers that implement the
//! [marshal::store::Certificates] and [marshal::store::Blocks] traits,
//! respectively, by delegating to whichever archive variant was initialized.
//! UFCS is required because both the [Archive] trait and the marshal store
//! traits share method names (`put`, `sync`, `get`, `prune`).

use alto_types::{Block, Finalization, Scheme};
use commonware_consensus::{marshal, types::Height};
use commonware_cryptography::{certificate::Scheme as CertScheme, sha256::Digest, Committable};
use commonware_runtime::{buffer::paged::CacheRef, BufferPooler, Clock, Metrics, Storage};
use commonware_storage::{
    archive::{self, immutable, prunable, Archive, Identifier},
    translator::FourCap,
};
use commonware_utils::{NZUsize, NZU16, NZU64};
use std::num::NonZero;

// Shared constants (also used by marshal config in engine.rs)
pub(crate) const PRUNABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(4_096);
pub(crate) const REPLAY_BUFFER: NonZero<usize> = NZUsize!(8 * 1024 * 1024); // 8MB
pub(crate) const WRITE_BUFFER: NonZero<usize> = NZUsize!(1024 * 1024); // 1MB

// Finalized archive constants (shared by both prunable and immutable paths)
const FINALIZED_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(262_144);
const FINALIZED_COMPRESSION: Option<u8> = Some(3);
const PAGE_CACHE_PAGE_SIZE: NonZero<u16> = NZU16!(4_096); // 4KB
const PAGE_CACHE_CAPACITY: NonZero<usize> = NZUsize!(8_192); // 32MB

// Immutable-only constants (freezer table sizing)
const FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(14); // 1MB
const FREEZER_TABLE_RESIZE_FREQUENCY: u8 = 4;
const FREEZER_TABLE_RESIZE_CHUNK_SIZE: u32 = 2u32.pow(16); // ~3MB
const FREEZER_JOURNAL_TARGET_SIZE: u64 = 1_073_741_824; // 1GB

/// Initialize the certificate and block archives based on the pruning config.
///
/// Returns the two archive wrappers plus the shared page cache (needed by
/// marshal for its own prunable internal stores).
pub(crate) async fn init<E>(
    context: &mut E,
    scheme: &Scheme,
    pruning_depth: Option<u64>,
) -> (Certificates<E>, Blocks<E>, CacheRef)
where
    E: BufferPooler + Storage + Metrics + Clock,
{
    let page_cache = CacheRef::from_pooler(context, PAGE_CACHE_PAGE_SIZE, PAGE_CACHE_CAPACITY);

    if pruning_depth.is_some() {
        let fbh = prunable::Archive::init(
            context.with_label("finalizations_by_height"),
            prunable::Config {
                translator: FourCap,
                key_partition: "follower-finalizations-by-height-key".to_string(),
                key_page_cache: page_cache.clone(),
                value_partition: "follower-finalizations-by-height-value".to_string(),
                compression: FINALIZED_COMPRESSION,
                codec_config: scheme.certificate_codec_config(),
                items_per_section: FINALIZED_ITEMS_PER_SECTION,
                key_write_buffer: WRITE_BUFFER,
                value_write_buffer: WRITE_BUFFER,
                replay_buffer: REPLAY_BUFFER,
            },
        )
        .await
        .expect("failed to initialize finalizations by height archive");
        let fb = prunable::Archive::init(
            context.with_label("finalized_blocks"),
            prunable::Config {
                translator: FourCap,
                key_partition: "follower-finalized-blocks-key".to_string(),
                key_page_cache: page_cache.clone(),
                value_partition: "follower-finalized-blocks-value".to_string(),
                compression: None,
                codec_config: (),
                items_per_section: FINALIZED_ITEMS_PER_SECTION,
                key_write_buffer: WRITE_BUFFER,
                value_write_buffer: WRITE_BUFFER,
                replay_buffer: REPLAY_BUFFER,
            },
        )
        .await
        .expect("failed to initialize finalized blocks archive");
        (
            Certificates::Prunable(fbh),
            Blocks::Prunable(fb),
            page_cache,
        )
    } else {
        let fbh = immutable::Archive::init(
            context.with_label("finalizations_by_height"),
            immutable::Config {
                metadata_partition: "follower-finalizations-by-height-metadata".to_string(),
                freezer_table_partition: "follower-finalizations-by-height-freezer-table"
                    .to_string(),
                freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
                freezer_table_resize_frequency: FREEZER_TABLE_RESIZE_FREQUENCY,
                freezer_table_resize_chunk_size: FREEZER_TABLE_RESIZE_CHUNK_SIZE,
                freezer_key_partition: "follower-finalizations-by-height-key".to_string(),
                freezer_key_page_cache: page_cache.clone(),
                freezer_value_partition: "follower-finalizations-by-height-value".to_string(),
                freezer_value_target_size: FREEZER_JOURNAL_TARGET_SIZE,
                freezer_value_compression: FINALIZED_COMPRESSION,
                ordinal_partition: "follower-finalizations-by-height-ordinal".to_string(),
                items_per_section: FINALIZED_ITEMS_PER_SECTION,
                freezer_key_write_buffer: WRITE_BUFFER,
                freezer_value_write_buffer: WRITE_BUFFER,
                ordinal_write_buffer: WRITE_BUFFER,
                replay_buffer: REPLAY_BUFFER,
                codec_config: scheme.certificate_codec_config(),
            },
        )
        .await
        .expect("failed to initialize finalizations by height archive");
        let fb = immutable::Archive::init(
            context.with_label("finalized_blocks"),
            immutable::Config {
                metadata_partition: "follower-finalized-blocks-metadata".to_string(),
                freezer_table_partition: "follower-finalized-blocks-freezer-table".to_string(),
                freezer_table_initial_size: FREEZER_TABLE_INITIAL_SIZE,
                freezer_table_resize_frequency: FREEZER_TABLE_RESIZE_FREQUENCY,
                freezer_table_resize_chunk_size: FREEZER_TABLE_RESIZE_CHUNK_SIZE,
                freezer_key_partition: "follower-finalized-blocks-key".to_string(),
                freezer_key_page_cache: page_cache.clone(),
                freezer_value_partition: "follower-finalized-blocks-value".to_string(),
                freezer_value_target_size: FREEZER_JOURNAL_TARGET_SIZE,
                freezer_value_compression: None,
                ordinal_partition: "follower-finalized-blocks-ordinal".to_string(),
                items_per_section: FINALIZED_ITEMS_PER_SECTION,
                freezer_key_write_buffer: WRITE_BUFFER,
                freezer_value_write_buffer: WRITE_BUFFER,
                ordinal_write_buffer: WRITE_BUFFER,
                replay_buffer: REPLAY_BUFFER,
                codec_config: (),
            },
        )
        .await
        .expect("failed to initialize finalized blocks archive");
        (
            Certificates::Immutable(fbh),
            Blocks::Immutable(fb),
            page_cache,
        )
    }
}

/// Wrapper over [immutable::Archive] and [prunable::Archive] for finalization
/// certificates. Implements [marshal::store::Certificates].
pub(crate) enum Certificates<E: BufferPooler + Storage + Metrics + Clock> {
    Immutable(immutable::Archive<E, Digest, Finalization>),
    Prunable(prunable::Archive<FourCap, E, Digest, Finalization>),
}

impl<E: BufferPooler + Storage + Metrics + Clock> marshal::store::Certificates for Certificates<E> {
    type Commitment = Digest;
    type Scheme = Scheme;
    type Error = archive::Error;

    async fn put(
        &mut self,
        height: Height,
        commitment: Digest,
        finalization: Finalization,
    ) -> Result<(), Self::Error> {
        match self {
            Self::Immutable(a) => Archive::put(a, height.get(), commitment, finalization).await,
            Self::Prunable(a) => Archive::put(a, height.get(), commitment, finalization).await,
        }
    }

    async fn sync(&mut self) -> Result<(), Self::Error> {
        match self {
            Self::Immutable(a) => Archive::sync(a).await,
            Self::Prunable(a) => Archive::sync(a).await,
        }
    }

    async fn get(&self, id: Identifier<'_, Digest>) -> Result<Option<Finalization>, Self::Error> {
        match self {
            Self::Immutable(a) => Archive::get(a, id).await,
            Self::Prunable(a) => Archive::get(a, id).await,
        }
    }

    async fn prune(&mut self, min: Height) -> Result<(), Self::Error> {
        match self {
            Self::Immutable(_) => Ok(()),
            Self::Prunable(a) => prunable::Archive::prune(a, min.get()).await,
        }
    }

    fn last_index(&self) -> Option<Height> {
        match self {
            Self::Immutable(a) => Archive::last_index(a).map(Height::new),
            Self::Prunable(a) => Archive::last_index(a).map(Height::new),
        }
    }
}

/// Wrapper over [immutable::Archive] and [prunable::Archive] for finalized
/// blocks. Implements [marshal::store::Blocks].
pub(crate) enum Blocks<E: BufferPooler + Storage + Metrics + Clock> {
    Immutable(immutable::Archive<E, Digest, Block>),
    Prunable(prunable::Archive<FourCap, E, Digest, Block>),
}

impl<E: BufferPooler + Storage + Metrics + Clock> marshal::store::Blocks for Blocks<E> {
    type Block = Block;
    type Error = archive::Error;

    async fn put(&mut self, block: Block) -> Result<(), Self::Error> {
        let height = block.height.get();
        let commitment = block.commitment();
        match self {
            Self::Immutable(a) => Archive::put(a, height, commitment, block).await,
            Self::Prunable(a) => Archive::put(a, height, commitment, block).await,
        }
    }

    async fn sync(&mut self) -> Result<(), Self::Error> {
        match self {
            Self::Immutable(a) => Archive::sync(a).await,
            Self::Prunable(a) => Archive::sync(a).await,
        }
    }

    async fn get(&self, id: Identifier<'_, Digest>) -> Result<Option<Block>, Self::Error> {
        match self {
            Self::Immutable(a) => Archive::get(a, id).await,
            Self::Prunable(a) => Archive::get(a, id).await,
        }
    }

    async fn prune(&mut self, min: Height) -> Result<(), Self::Error> {
        match self {
            Self::Immutable(_) => Ok(()),
            Self::Prunable(a) => prunable::Archive::prune(a, min.get()).await,
        }
    }

    fn missing_items(&self, start: Height, max: usize) -> Vec<Height> {
        match self {
            Self::Immutable(a) => Archive::missing_items(a, start.get(), max)
                .into_iter()
                .map(Height::new)
                .collect(),
            Self::Prunable(a) => Archive::missing_items(a, start.get(), max)
                .into_iter()
                .map(Height::new)
                .collect(),
        }
    }

    fn next_gap(&self, value: Height) -> (Option<Height>, Option<Height>) {
        let (a, b) = match self {
            Self::Immutable(a) => Archive::next_gap(a, value.get()),
            Self::Prunable(a) => Archive::next_gap(a, value.get()),
        };
        (a.map(Height::new), b.map(Height::new))
    }
}
