use alto_types::Identity;
use commonware_cryptography::ed25519::PublicKey;
use governor::Quota;

mod actor;
mod coordinator;
pub mod handler;
mod key;
pub use actor::Actor;
mod ingress;
pub use ingress::Mailbox;

use crate::Indexer;

/// Configuration for the syncer.
pub struct Config<I: Indexer> {
    pub partition_prefix: String,

    pub public_key: PublicKey,

    /// Network identity
    pub identity: Identity,

    pub participants: Vec<PublicKey>,

    /// Number of messages from consensus to hold in our backlog
    /// before blocking.
    pub mailbox_size: usize,

    pub backfill_quota: Quota,

    pub activity_timeout: u64,

    pub indexer: Option<I>,
}
