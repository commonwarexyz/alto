use alto_client::consensus::{Message, Payload};
use alto_types::{Finalized, Notarized};
use commonware_cryptography::ed25519::PublicKey;
use commonware_p2p::Recipients;
use commonware_runtime::IoBufMut;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, future::Future, time::SystemTime};

pub mod engine;
pub mod feeder;
pub mod resolver;
pub mod backfill;

pub use alto_client::{IndexQuery, Query};

/// Configuration for the follower binary.
#[derive(Deserialize, Serialize)]
pub struct Config {
    pub source: String,
    pub identity: String,
    pub directory: String,
    pub worker_threads: usize,
    pub log_level: String,
    pub metrics_port: u16,
    pub mailbox_size: usize,
    pub max_repair: usize,
    pub backfill_concurrency: usize,
    pub tip: bool,
}

/// Abstraction over the certificate source (HTTP client) used by the
/// [feeder::CertificateFeeder] and [resolver::HttpResolverActor].
pub trait Source: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn health(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn block(&self, query: Query) -> impl Future<Output = Result<Payload, Self::Error>> + Send;
    fn finalized(
        &self,
        query: IndexQuery,
    ) -> impl Future<Output = Result<Finalized, Self::Error>> + Send;
    fn finalized_unverified(
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

    fn block(&self, query: Query) -> impl Future<Output = Result<Payload, Self::Error>> + Send {
        self.block_get(query)
    }

    fn finalized(
        &self,
        query: IndexQuery,
    ) -> impl Future<Output = Result<Finalized, Self::Error>> + Send {
        self.finalized_get(query)
    }

    fn finalized_unverified(
        &self,
        query: IndexQuery,
    ) -> impl Future<Output = Result<Finalized, Self::Error>> + Send {
        self.finalized_get_unverified(query)
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

/// Noop p2p sender used by the follower's buffer engine.
///
/// The follower does not participate in p2p broadcast, so all send
/// operations are dropped.
#[derive(Clone)]
pub(crate) struct NoopSender;

pub(crate) struct NoopCheckedSender;

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

/// Noop p2p receiver used by the follower's buffer engine.
///
/// The follower does not participate in p2p broadcast, so recv blocks
/// forever (via [std::future::pending]).
#[derive(Debug)]
pub(crate) struct NoopReceiver;

impl commonware_p2p::Receiver for NoopReceiver {
    type Error = std::io::Error;
    type PublicKey = PublicKey;

    async fn recv(&mut self) -> Result<commonware_p2p::Message<Self::PublicKey>, Self::Error> {
        std::future::pending().await
    }
}

#[cfg(test)]
mod tests;
