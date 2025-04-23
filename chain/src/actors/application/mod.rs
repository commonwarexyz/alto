use commonware_cryptography::{
    bls12381::primitives::{group, poly::Poly},
    ed25519::PublicKey,
};

mod actor;
pub use actor::Actor;
mod ingress;
pub use ingress::Mailbox;
mod supervisor;
pub use supervisor::Supervisor;

/// Configuration for the application.
pub struct Config {
    /// Participants active in consensus.
    pub participants: Vec<PublicKey>,

    pub identity: Poly<group::Public>,

    pub share: group::Share,

    /// Number of messages from consensus to hold in our backlog
    /// before blocking.
    pub mailbox_size: usize,
}
