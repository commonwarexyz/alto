use commonware_consensus::simplex::scheme::bls12381_threshold;
use commonware_consensus::simplex::types::{
    Activity as CActivity, Finalization as CFinalization, Notarization as CNotarization,
};
use commonware_cryptography::{
    bls12381::primitives::variant::{MinSig, Variant},
    ed25519,
    sha256::Digest,
};
use commonware_parallel::Sequential;

pub use commonware_consensus::simplex::scheme::bls12381_threshold::Seedable;

/// BLS12-381 threshold signing scheme parameterized by execution strategy.
pub type SchemeOf<S> = bls12381_threshold::Scheme<PublicKey, MinSig, S>;

/// Notarization type parameterized by execution strategy.
pub type NotarizationOf<S> = CNotarization<SchemeOf<S>, Digest>;

/// Finalization type parameterized by execution strategy.
pub type FinalizationOf<S> = CFinalization<SchemeOf<S>, Digest>;

/// Activity type parameterized by execution strategy.
pub type ActivityOf<S> = CActivity<SchemeOf<S>, Digest>;

/// Default scheme using sequential execution (for backwards compatibility).
pub type Scheme = SchemeOf<Sequential>;

/// Default seed type.
pub type Seed = bls12381_threshold::Seed<MinSig>;

/// Default notarization type using sequential execution.
pub type Notarization = NotarizationOf<Sequential>;

/// Default finalization type using sequential execution.
pub type Finalization = FinalizationOf<Sequential>;

/// Default activity type using sequential execution.
pub type Activity = ActivityOf<Sequential>;

pub type PublicKey = ed25519::PublicKey;
pub type Identity = <MinSig as Variant>::Public;
pub type Evaluation = Identity;
pub type Signature = <MinSig as Variant>::Signature;
