use alto_types::Block;
use bytes::{Buf, BufMut};
use commonware_codec::{self, FixedSize, Read, Write};
use commonware_cryptography::{sha256::Digest, Digestible};
use commonware_utils::{sync::Mutex, PrioritySet};
use std::{collections::BTreeMap, sync::Arc};

/// What the backfiller should do next for a digest.
pub enum Decision {
    /// The block is already uploaded, so the queue entry can be skipped.
    Skip,
    /// The live certificate path is still handling this digest, so the backfiller
    /// should wait instead of racing it.
    Wait,
    /// The backfiller should proceed with a block upload attempt.
    Proceed,
}

/// Entry stored in the backfill queue.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub height: u64,
    pub digest: Digest,
}

impl FixedSize for Entry {
    const SIZE: usize = u64::SIZE + Digest::SIZE;
}

impl Write for Entry {
    fn write(&self, buf: &mut impl BufMut) {
        self.height.write(buf);
        self.digest.write(buf);
    }
}

impl Read for Entry {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, commonware_codec::Error> {
        let height = u64::read_cfg(buf, &())?;
        let digest = Digest::read_cfg(buf, &())?;
        Ok(Self { height, digest })
    }
}

/// Tracks block uploads and the oldest finalized height that still needs them.
///
/// Duplicate finalize notifications may enqueue multiple queue entries for the
/// same digest until one upload succeeds. The queue itself remains the durable
/// source of truth for pending work; this state only keeps recent upload and
/// cache metadata around it.
pub struct State {
    // Successfully uploaded digests stay in the dedupe set until contiguous
    // queue retirement has advanced far enough that older heights are no longer
    // needed for dedupe.
    uploaded: PrioritySet<Digest, u64>,
    // Conservative height watermark derived from contiguous queue-floor
    // progress. Heights below this can be forgotten from the uploaded set.
    acked_through: u64,
    // Highest finalized height observed from the live application stream.
    latest_finalized: u64,
    // Blocks cached for the live certificate upload path and the backfiller.
    cached_blocks: BTreeMap<Digest, Block>,
    // Number of in-flight certificate uploads per digest so the backfiller can
    // wait for the live path instead of racing it.
    certificate_uploads: BTreeMap<Digest, usize>,
}

impl State {
    pub fn new() -> Self {
        Self {
            uploaded: PrioritySet::new(),
            acked_through: 0,
            latest_finalized: 0,
            cached_blocks: BTreeMap::new(),
            certificate_uploads: BTreeMap::new(),
        }
    }

    fn is_uploaded(&self, digest: &Digest) -> bool {
        self.uploaded.contains(digest)
    }

    pub fn record(&mut self, block: &Block) -> Option<Entry> {
        let entry = Entry {
            height: block.height.get(),
            digest: block.digest(),
        };
        self.observe_finalization(entry.height);
        if self.is_uploaded(&entry.digest) {
            return None;
        }
        self.cache_block(block.clone());
        Some(entry)
    }

    pub fn advance_queue_floor(&mut self, height: u64) {
        self.acked_through = self.acked_through.max(height);
        self.prune();
    }

    pub fn mark_uploaded(&mut self, digest: Digest, height: u64) {
        self.cached_blocks.remove(&digest);
        self.uploaded.put(digest, height);
        self.prune();
    }

    pub fn cache_block(&mut self, block: Block) {
        self.cached_blocks.entry(block.digest()).or_insert(block);
    }

    pub fn cached_block(&self, digest: &Digest) -> Option<Block> {
        self.cached_blocks.get(digest).cloned()
    }

    pub fn should_upload(&self, digest: &Digest) -> Decision {
        if self.is_uploaded(digest) {
            Decision::Skip
        } else if self.certificate_uploads.contains_key(digest) {
            Decision::Wait
        } else {
            Decision::Proceed
        }
    }

    pub fn start_certificate_upload(&mut self, digest: Digest) {
        *self.certificate_uploads.entry(digest).or_default() += 1;
    }

    pub fn finish_certificate_upload(&mut self, digest: &Digest, uploaded_height: Option<u64>) {
        let count = self
            .certificate_uploads
            .get_mut(digest)
            .expect("missing in-flight certificate upload");
        *count -= 1;
        if *count == 0 {
            self.certificate_uploads.remove(digest);
        }
        if let Some(height) = uploaded_height {
            self.cached_blocks.remove(digest);
            self.uploaded.put(*digest, height);
        }
        self.prune();
    }

    fn observe_finalization(&mut self, height: u64) {
        self.latest_finalized = self.latest_finalized.max(height);
        self.prune();
    }

    fn prune(&mut self) {
        while let Some((_, &height)) = self.uploaded.peek() {
            if height >= self.acked_through {
                break;
            }
            self.uploaded.pop();
        }

        let mut cached_prune_before = self.latest_finalized;
        for digest in self.certificate_uploads.keys() {
            let Some(block) = self.cached_blocks.get(digest) else {
                continue;
            };
            cached_prune_before = cached_prune_before.min(block.height.get());
        }
        self.cached_blocks
            .retain(|_, block| block.height.get() >= cached_prune_before);
    }
}

/// State shared by the live certificate path and the backfiller path.
pub type SharedState = Arc<Mutex<State>>;

#[cfg(test)]
mod tests {
    use super::*;
    use alto_types::{Context, EPOCH};
    use commonware_consensus::types::{Height, Round, View};
    use commonware_cryptography::{ed25519, Digestible, Hasher, Sha256, Signer};

    fn test_block(view: u64, height: u64, label: &[u8]) -> Block {
        Block::new(
            Context {
                round: Round::new(EPOCH, View::new(view)),
                leader: ed25519::PrivateKey::from_seed(view).public_key(),
                parent: (
                    View::new(view.saturating_sub(1)),
                    Sha256::hash(format!("parent-{view}").as_bytes()),
                ),
            },
            Sha256::hash(label),
            Height::new(height),
            height,
        )
    }

    #[test]
    fn test_upload_state_prunes_only_after_queue_floor_progress() {
        let mut uploads = State::new();

        let digest_10 = Sha256::hash(b"view-10");
        let digest_11 = Sha256::hash(b"view-11");
        let digest_12 = Sha256::hash(b"view-12");

        uploads.mark_uploaded(digest_11, 11);
        uploads.mark_uploaded(digest_12, 12);

        assert!(uploads.is_uploaded(&digest_11));
        assert!(uploads.is_uploaded(&digest_12));

        uploads.mark_uploaded(digest_10, 10);
        uploads.advance_queue_floor(10);

        assert!(uploads.is_uploaded(&digest_10));
        assert!(uploads.is_uploaded(&digest_11));
        assert!(uploads.is_uploaded(&digest_12));

        uploads.advance_queue_floor(12);

        assert!(!uploads.is_uploaded(&digest_10));
        assert!(!uploads.is_uploaded(&digest_11));
        assert!(uploads.is_uploaded(&digest_12));
    }

    #[test]
    fn test_upload_state_allows_duplicate_pending_entries() {
        let mut uploads = State::new();
        let block = test_block(10, 10, b"view-10");
        let entry = Entry {
            height: block.height.get(),
            digest: block.digest(),
        };

        assert!(uploads.record(&block).is_some());
        assert!(uploads.record(&block).is_some());
        uploads.mark_uploaded(entry.digest, entry.height);
        assert!(uploads.record(&block).is_none());
    }

    #[test]
    fn test_upload_state_does_not_recache_already_uploaded_blocks() {
        let mut uploads = State::new();
        let block = test_block(7, 7, b"view-7");
        let digest = block.digest();

        assert!(uploads.record(&block).is_some());
        uploads.mark_uploaded(digest, block.height.get());
        assert!(uploads.cached_block(&digest).is_none());
        assert!(uploads.record(&block).is_none());
        assert!(uploads.cached_block(&digest).is_none());
    }

    #[test]
    fn test_upload_state_eventually_drops_cached_block_after_failed_certificate_upload() {
        let mut uploads = State::new();
        let block = test_block(7, 7, b"view-7");
        let digest = block.digest();

        uploads.start_certificate_upload(digest);
        uploads.cache_block(block);
        uploads.finish_certificate_upload(&digest, None);

        assert!(uploads.cached_block(&digest).is_some());

        uploads.observe_finalization(8);
        assert!(uploads.cached_block(&digest).is_none());
    }

    #[test]
    fn test_upload_state_prunes_cached_block_after_newer_finalization() {
        let mut uploads = State::new();
        let block = test_block(7, 7, b"view-7");
        let digest = block.digest();

        uploads.cache_block(block);
        assert!(uploads.cached_block(&digest).is_some());

        uploads.observe_finalization(8);
        assert!(uploads.cached_block(&digest).is_none());
    }
}
