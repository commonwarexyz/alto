use crate::Seed;
use commonware_codec::Encode;
use commonware_consensus::threshold_simplex::types::Finalization as TFinalization;
use commonware_cryptography::{bls12381::primitives::variant::MinSig, sha256::Digest};
use commonware_utils::modulo;

pub type Finalization = TFinalization<MinSig, Digest>;

/// The leader for a given seed is determined by the modulo of the seed with the number of participants.
pub fn leader_index(seed: &Seed, participants: usize) -> usize {
    let signature = seed.signature.encode().freeze();
    modulo(&signature, participants as u64) as usize
}
