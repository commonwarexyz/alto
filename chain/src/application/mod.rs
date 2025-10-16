mod actor;
pub use actor::Actor;
mod ingress;
pub use ingress::Mailbox;

/// Configuration for the application.
pub struct Config {
    /// Number of messages from consensus to hold in our backlog
    /// before blocking.
    pub mailbox_size: usize,
}
