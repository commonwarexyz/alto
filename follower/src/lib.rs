use alto_client::consensus::{Message, Payload};
use alto_types::{Finalized, Notarized};
use commonware_cryptography::ed25519::PublicKey;
use commonware_p2p::Recipients;
use commonware_runtime::IoBufMut;
use commonware_utils::{NZU16, NZU64, NZUsize};
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    future::Future,
    num::NonZero,
    time::SystemTime,
};

pub mod engine;
pub mod feeder;
pub mod resolver;

pub use alto_client::{IndexQuery, Query};

// Storage constants
pub const PRUNABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(4_096);
pub const IMMUTABLE_ITEMS_PER_SECTION: NonZero<u64> = NZU64!(262_144);
pub const FREEZER_TABLE_RESIZE_FREQUENCY: u8 = 4;
pub const FREEZER_TABLE_RESIZE_CHUNK_SIZE: u32 = 2u32.pow(16);
pub const FREEZER_JOURNAL_TARGET_SIZE: u64 = 1024 * 1024 * 1024;
pub const FREEZER_JOURNAL_COMPRESSION: Option<u8> = Some(3);
pub const REPLAY_BUFFER: NonZero<usize> = NZUsize!(8 * 1024 * 1024);
pub const WRITE_BUFFER: NonZero<usize> = NZUsize!(8 * 1024 * 1024);
pub const BUFFER_POOL_PAGE_SIZE: NonZero<u16> = NZU16!(4_096);
pub const BUFFER_POOL_CAPACITY: NonZero<usize> = NZUsize!(8_192);
pub const MAX_REPAIR: NonZero<usize> = NZUsize!(50);
pub const VIEW_RETENTION_TIMEOUT: commonware_consensus::types::ViewDelta =
    commonware_consensus::types::ViewDelta::new(2560);
pub const DEQUE_SIZE: usize = 10;
pub const BLOCKS_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21);
pub const FINALIZED_FREEZER_TABLE_INITIAL_SIZE: u32 = 2u32.pow(21);

#[derive(Deserialize, Serialize)]
pub struct Config {
    pub source: String,
    pub identity: String,
    pub directory: String,
    pub worker_threads: usize,
    pub log_level: String,
    pub metrics_port: u16,
    pub mailbox_size: usize,
    pub auto_checkpoint: bool,
}

pub trait Source: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn health(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn block_get(
        &self,
        query: Query,
    ) -> impl Future<Output = Result<Payload, Self::Error>> + Send;
    fn finalized_get(
        &self,
        query: IndexQuery,
    ) -> impl Future<Output = Result<Finalized, Self::Error>> + Send;
    fn notarized_get(
        &self,
        query: IndexQuery,
    ) -> impl Future<Output = Result<Notarized, Self::Error>> + Send;
    fn listen(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<Message, Self::Error>> + Send + Unpin,
            Self::Error,
        >,
    > + Send;
}

impl<S: commonware_parallel::Strategy> Source for alto_client::Client<S> {
    type Error = alto_client::Error;

    fn health(&self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.health()
    }

    fn block_get(
        &self,
        query: Query,
    ) -> impl Future<Output = Result<Payload, Self::Error>> + Send {
        self.block_get(query)
    }

    fn finalized_get(
        &self,
        query: IndexQuery,
    ) -> impl Future<Output = Result<Finalized, Self::Error>> + Send {
        self.finalized_get(query)
    }

    fn notarized_get(
        &self,
        query: IndexQuery,
    ) -> impl Future<Output = Result<Notarized, Self::Error>> + Send {
        self.notarized_get(query)
    }

    fn listen(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<Message, Self::Error>> + Send + Unpin,
            Self::Error,
        >,
    > + Send {
        self.listen()
    }
}

#[derive(Clone)]
pub struct NoopSender;

pub struct NoopCheckedSender;

impl commonware_p2p::CheckedSender for NoopCheckedSender {
    type PublicKey = PublicKey;
    type Error = std::io::Error;

    async fn send(
        self,
        _message: impl Into<IoBufMut> + Send,
        _priority: bool,
    ) -> Result<Vec<Self::PublicKey>, Self::Error> {
        Ok(Vec::new())
    }
}

impl commonware_p2p::LimitedSender for NoopSender {
    type PublicKey = PublicKey;
    type Checked<'a> = NoopCheckedSender;

    async fn check(
        &mut self,
        _recipients: Recipients<Self::PublicKey>,
    ) -> Result<Self::Checked<'_>, SystemTime> {
        Err(SystemTime::now())
    }
}

#[derive(Debug)]
pub struct NoopReceiver;

impl commonware_p2p::Receiver for NoopReceiver {
    type Error = std::io::Error;
    type PublicKey = PublicKey;

    async fn recv(
        &mut self,
    ) -> Result<commonware_p2p::Message<Self::PublicKey>, Self::Error> {
        std::future::pending().await
    }
}

#[cfg(test)]
mod tests;
