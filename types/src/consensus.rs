use commonware_consensus::simplex::{
    scheme::{fn_dsa, Scheme as SimplexScheme},
    types::{
        Activity as CActivity, Context as CContext, Finalization as CFinalization,
        Notarization as CNotarization,
    },
};
use commonware_cryptography::{
    certificate::Verifier as CertificateVerifier, fn_dsa::EllipsoidalFalcon512, sha256::Digest,
};
use commonware_utils::ordered::{BiMap, Set};

/// Individually verified experimental ellipsoidal Falcon certificates.
///
/// Each validator's identity key is also its consensus signing key.
pub type ConsensusScheme = fn_dsa::Scheme<PublicKey, EllipsoidalFalcon512>;

/// Consensus schemes whose certificates verify against the network's [Identity].
pub trait Scheme:
    SimplexScheme<Digest, PublicKey = PublicKey> + CertificateVerifier + Sized
{
    fn certificate_verifier(namespace: &[u8], identity: Identity) -> Self;
}

impl Scheme for ConsensusScheme {
    fn certificate_verifier(namespace: &[u8], identity: Identity) -> Self {
        let participants: Vec<_> = identity.into_iter().map(|key| (key.clone(), key)).collect();
        let participants = BiMap::try_from(participants).expect("participant set keys are unique");
        Self::verifier(namespace, participants)
    }
}

pub type Context = CContext<Digest, PublicKey>;
pub type Notarization<S> = CNotarization<S, Digest>;
pub type Finalization<S> = CFinalization<S, Digest>;
pub type Activity<S> = CActivity<S, Digest>;

/// Validator identity key, which also signs consensus messages.
pub type PublicKey = commonware_cryptography::fn_dsa::PublicKey<EllipsoidalFalcon512>;
/// Validator identity key, which also signs consensus messages.
pub type PrivateKey = commonware_cryptography::fn_dsa::PrivateKey<EllipsoidalFalcon512>;

/// Public material from which [Scheme::certificate_verifier] builds a verifier: the ordered
/// participant set, because every certificate signature verifies against its signer's key.
pub type Identity = Set<PublicKey>;
