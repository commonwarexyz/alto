use commonware_consensus::threshold_simplex::types::{
    Activity as CActivity, Finalization as CFinalization, Notarization as CNotarization,
    Seed as CSeed,
};
use commonware_cryptography::{
    bls12381::primitives::variant::{MinSig, Variant},
    sha256::Digest,
};
use commonware_utils::modulo;

pub type Seed = CSeed<MinSig>;
pub type Notarization = CNotarization<MinSig, Digest>;
pub type Finalization = CFinalization<MinSig, Digest>;
pub type Activity = CActivity<MinSig, Digest>;

pub type Identity = <MinSig as Variant>::Public;
pub type Evaluation = Identity;
pub type Signature = <MinSig as Variant>::Signature;

/// The leader for a given seed is determined by the modulo of the seed with the number of participants.
pub fn leader_index(seed: &[u8], participants: usize) -> usize {
    modulo(seed, participants as u64) as usize
}
