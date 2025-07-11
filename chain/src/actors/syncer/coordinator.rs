use commonware_cryptography::PublicKey;
use commonware_resolver::p2p;

#[derive(Clone)]
pub struct Coordinator<P: PublicKey> {
    participants: Vec<P>,
}

impl<P: PublicKey> Coordinator<P> {
    pub fn new(participants: Vec<P>) -> Self {
        Self { participants }
    }
}

impl<P: PublicKey> p2p::Coordinator for Coordinator<P> {
    type PublicKey = P;

    fn peers(&self) -> &Vec<Self::PublicKey> {
        &self.participants
    }

    fn peer_set_id(&self) -> u64 {
        0
    }
}
