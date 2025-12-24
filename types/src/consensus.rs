use commonware_coding::ReedSolomon;
use commonware_consensus::simplex::scheme::bls12381_threshold;
use commonware_consensus::simplex::types::{
    Activity as CActivity, Finalization as CFinalization, Notarization as CNotarization,
};
use commonware_consensus::types::CodingCommitment;
use commonware_cryptography::Sha256;
use commonware_cryptography::{
    bls12381::primitives::variant::{MinSig, Variant},
    ed25519,
};

pub use commonware_consensus::simplex::scheme::bls12381_threshold::Seedable;

pub type Scheme = bls12381_threshold::Scheme<PublicKey, MinSig>;
pub type CodingScheme = ReedSolomon<Sha256>;
pub type Seed = bls12381_threshold::Seed<MinSig>;
pub type Notarization = CNotarization<Scheme, CodingCommitment>;
pub type Finalization = CFinalization<Scheme, CodingCommitment>;
pub type Activity = CActivity<Scheme, CodingCommitment>;

pub type PublicKey = ed25519::PublicKey;
pub type Identity = <MinSig as Variant>::Public;
pub type Evaluation = Identity;
pub type Signature = <MinSig as Variant>::Signature;
