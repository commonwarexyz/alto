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

/// Configuration for the [Actor].
pub struct Config<I: Indexer> {
    /// The public key of the validator.
    pub public_key: PublicKey,

    /// The identity of the network.
    pub identity: Identity,

    /// The public keys of the participants in the network.
    pub participants: Vec<PublicKey>,

    /// The prefix to use for all partitions.
    pub partition_prefix: String,

    /// The initial size of the freezer table for blocks.
    pub blocks_freezer_table_initial_size: u32,

    /// The initial size of the freezer table for finalizations.
    pub finalized_freezer_table_initial_size: u32,

    /// Number of messages from consensus to hold in our backlog
    /// before blocking.
    pub mailbox_size: usize,

    /// The rate limit for backfilling.
    pub backfill_quota: Quota,

    /// The timeout for pruning consensus activity.
    pub activity_timeout: u64,

    /// The indexer to invoke when storing blocks and finalizations.
    pub indexer: Option<I>,
}
