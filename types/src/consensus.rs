use commonware_consensus::minimmit::{
    scheme::bls12381_threshold,
    types::{
        Activity as CActivity, Context as CContext, Finalization as CFinalization,
        MNotarization as CNotarization,
    },
};
use commonware_cryptography::{
    bls12381::primitives::variant::{MinSig, Variant},
    ed25519,
    sha256::Digest,
};

pub type Context = CContext<Digest, PublicKey>;

pub type Scheme = bls12381_threshold::Scheme<PublicKey, MinSig>;
pub type Notarization = CNotarization<Scheme, Digest>;
pub type Finalization = CFinalization<Scheme, Digest>;
pub type Activity = CActivity<Scheme, Digest>;

pub type PublicKey = ed25519::PublicKey;
pub type Identity = <MinSig as Variant>::Public;
pub type Signature = <MinSig as Variant>::Signature;
