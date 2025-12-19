use commonware_consensus::simplex::scheme::bls12381_threshold;
use commonware_consensus::simplex::types::{
    Activity as CActivity, Finalization as CFinalization, Notarization as CNotarization,
};
use commonware_cryptography::{
    bls12381::primitives::variant::{MinSig, Variant},
    ed25519,
    sha256::Digest,
};

pub use commonware_consensus::simplex::scheme::bls12381_threshold::Seedable;

pub type Scheme = bls12381_threshold::Scheme<PublicKey, MinSig>;
pub type Seed = bls12381_threshold::Seed<MinSig>;
pub type Notarization = CNotarization<Scheme, Digest>;
pub type Finalization = CFinalization<Scheme, Digest>;
pub type Activity = CActivity<Scheme, Digest>;

pub type PublicKey = ed25519::PublicKey;
pub type Identity = <MinSig as Variant>::Public;
pub type Evaluation = Identity;
pub type Signature = <MinSig as Variant>::Signature;
