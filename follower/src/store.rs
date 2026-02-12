use alto_types::{Block, Finalization, Scheme};
use commonware_consensus::{
    marshal::store::{Blocks, Certificates},
    types::Height,
    Heightable,
};
use commonware_cryptography::{sha256::Digest, Committable};
use commonware_runtime::{Clock, Metrics, Storage};
use commonware_storage::archive::{immutable, Archive, Error, Identifier};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

const DEFERRED_SYNC_INTERVAL: u64 = 1024;

pub type CaughtUpFlag = Arc<AtomicBool>;

pub struct DeferredBlocks<E: Storage + Metrics + Clock> {
    inner: immutable::Archive<E, Digest, Block>,
    caught_up: CaughtUpFlag,
    pending: u64,
}

impl<E: Storage + Metrics + Clock> DeferredBlocks<E> {
    pub fn new(inner: immutable::Archive<E, Digest, Block>, caught_up: CaughtUpFlag) -> Self {
        Self {
            inner,
            caught_up,
            pending: 0,
        }
    }
}

impl<E: Storage + Metrics + Clock> Blocks for DeferredBlocks<E> {
    type Block = Block;
    type Error = Error;

    async fn put(&mut self, block: Block) -> Result<(), Error> {
        let index = block.height().get();
        let key = block.commitment();
        if self.caught_up.load(Ordering::Relaxed) {
            return Archive::put_sync(&mut self.inner, index, key, block).await;
        }
        Archive::put(&mut self.inner, index, key, block).await?;
        self.pending += 1;
        if self.pending >= DEFERRED_SYNC_INTERVAL {
            Archive::sync(&mut self.inner).await?;
            self.pending = 0;
        }
        Ok(())
    }

    async fn get(&self, id: Identifier<'_, Digest>) -> Result<Option<Block>, Error> {
        Archive::get(&self.inner, id).await
    }

    async fn prune(&mut self, _: Height) -> Result<(), Error> {
        Ok(())
    }

    fn missing_items(&self, start: Height, max: usize) -> Vec<Height> {
        Archive::missing_items(&self.inner, start.get(), max)
            .into_iter()
            .map(Height::new)
            .collect()
    }

    fn next_gap(&self, value: Height) -> (Option<Height>, Option<Height>) {
        let (a, b) = Archive::next_gap(&self.inner, value.get());
        (a.map(Height::new), b.map(Height::new))
    }
}

pub struct DeferredCertificates<E: Storage + Metrics + Clock> {
    inner: immutable::Archive<E, Digest, Finalization>,
    caught_up: CaughtUpFlag,
    pending: u64,
}

impl<E: Storage + Metrics + Clock> DeferredCertificates<E> {
    pub fn new(
        inner: immutable::Archive<E, Digest, Finalization>,
        caught_up: CaughtUpFlag,
    ) -> Self {
        Self {
            inner,
            caught_up,
            pending: 0,
        }
    }
}

impl<E: Storage + Metrics + Clock> Certificates for DeferredCertificates<E> {
    type Commitment = Digest;
    type Scheme = Scheme;
    type Error = Error;

    async fn put(
        &mut self,
        height: Height,
        commitment: Digest,
        finalization: Finalization,
    ) -> Result<(), Error> {
        if self.caught_up.load(Ordering::Relaxed) {
            return Archive::put_sync(&mut self.inner, height.get(), commitment, finalization)
                .await;
        }
        Archive::put(&mut self.inner, height.get(), commitment, finalization).await?;
        self.pending += 1;
        if self.pending >= DEFERRED_SYNC_INTERVAL {
            Archive::sync(&mut self.inner).await?;
            self.pending = 0;
        }
        Ok(())
    }

    async fn get(&self, id: Identifier<'_, Digest>) -> Result<Option<Finalization>, Error> {
        Archive::get(&self.inner, id).await
    }

    async fn prune(&mut self, _: Height) -> Result<(), Error> {
        Ok(())
    }

    fn last_index(&self) -> Option<Height> {
        Archive::last_index(&self.inner).map(Height::new)
    }
}
