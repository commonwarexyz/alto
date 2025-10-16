use commonware_consensus::threshold_simplex::signing_scheme::bls12381_threshold;
use commonware_consensus::threshold_simplex::types::{
    Activity as CActivity, Finalization as CFinalization, Notarization as CNotarization,
};
use commonware_cryptography::{
    bls12381::primitives::variant::{MinSig, Variant},
    sha256::Digest,
};

pub use commonware_consensus::threshold_simplex::signing_scheme::bls12381_threshold::Seedable;

pub type SigningScheme = bls12381_threshold::Scheme<MinSig>;
pub type Seed = bls12381_threshold::Seed<MinSig>;
pub type Notarization = CNotarization<SigningScheme, Digest>;
pub type Finalization = CFinalization<SigningScheme, Digest>;
pub type Activity = CActivity<SigningScheme, Digest>;

pub type Identity = <MinSig as Variant>::Public;
pub type Evaluation = Identity;
pub type Signature = <MinSig as Variant>::Signature;
