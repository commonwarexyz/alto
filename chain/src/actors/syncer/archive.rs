use commonware_codec::Codec;
use commonware_runtime::{Metrics, RwLock, Storage};
use commonware_storage::archive::{self, Archive, Identifier, Translator};
use commonware_utils::Array;
use std::{marker::PhantomData, sync::Arc};

/// Archive wrapper that handles all locking.
#[derive(Clone)]
pub struct Wrapped<T, K, R, V>
where
    T: Translator,
    K: Array,
    R: Storage + Metrics,
    V: Codec,
{
    inner: Arc<RwLock<Archive<T, K, R>>>,
    _phantom: PhantomData<V>,
}

impl<T, K, R, V> Wrapped<T, K, R, V>
where
    T: Translator,
    K: Array,
    R: Storage + Metrics,
    V: Codec,
{
    /// Creates a new `Wrapped` from an existing `Archive`.
    pub fn new(archive: Archive<T, K, R>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(archive)),
            _phantom: PhantomData,
        }
    }

    /// Retrieves a value from the archive by identifier.
    pub async fn get(&self, identifier: Identifier<'_, K>) -> Result<Option<V>, archive::Error> {
        let archive = self.inner.read().await;
        let Some(result) = archive.get(identifier).await? else {
            return Ok(None);
        };
        Ok(Some(V::decode_cfg(result.as_ref(), &()).unwrap()))
    }

    /// Inserts a value into the archive with the given index and key.
    pub async fn put(&self, index: u64, key: K, data: V) -> Result<(), archive::Error> {
        let mut archive = self.inner.write().await;
        archive.put(index, key, data.encode().into()).await?;
        Ok(())
    }

    /// Prunes entries from the archive up to the specified minimum index.
    pub async fn prune(&self, min_index: u64) -> Result<(), archive::Error> {
        let mut archive = self.inner.write().await;
        archive.prune(min_index).await?;
        Ok(())
    }

    /// Retrieves the next gap in the archive.
    pub async fn next_gap(&self, start: u64) -> (Option<u64>, Option<u64>) {
        let archive = self.inner.read().await;
        archive.next_gap(start)
    }
}
