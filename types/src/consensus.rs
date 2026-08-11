use commonware_consensus::simplex::scheme::bls12381_threshold::vrf;
use commonware_consensus::simplex::types::{
    Activity as CActivity, Context as CContext, Finalization as CFinalization,
    Notarization as CNotarization,
};
pub use commonware_consensus::types::coding::Commitment;
use commonware_cryptography::{
    bls12381::primitives::variant::{MinSig, Variant},
    ed25519,
};

pub type Context = CContext<Commitment, PublicKey>;

pub type Scheme = vrf::Scheme<PublicKey, MinSig>;
pub type Seed = vrf::Seed<MinSig>;
pub use vrf::Seedable;
pub type Notarization = CNotarization<Scheme, Commitment>;
pub type Finalization = CFinalization<Scheme, Commitment>;
pub type Activity = CActivity<Scheme, Commitment>;

pub type PublicKey = ed25519::PublicKey;
pub type Identity = <MinSig as Variant>::Public;
pub type Signature = <MinSig as Variant>::Signature;
