use commonware_cryptography::{bls12381::primitives::group, ed25519::PublicKey};
use governor::Quota;

mod actor;
mod archive;
mod buffer;
mod coordinator;
pub mod handler;
mod key;
pub use actor::Actor;
mod ingress;
pub use ingress::Mailbox;

/// Configuration for the syncer.
pub struct Config {
    pub partition_prefix: String,

    pub public_key: PublicKey,

    /// Network identity
    pub identity: group::Public,

    pub participants: Vec<PublicKey>,

    /// Number of messages from consensus to hold in our backlog
    /// before blocking.
    pub mailbox_size: usize,

    pub backfill_quota: Quota,

    pub activity_timeout: u64,
}
