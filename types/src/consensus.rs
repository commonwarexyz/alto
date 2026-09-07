use commonware_codec::Read;
use commonware_consensus::simplex::{
    elector::{Random, RandomVersion},
    scheme::{
        bls12381_threshold::{standard, vrf},
        Scheme as SimplexScheme,
    },
    types::{
        Activity as CActivity, Context as CContext, Finalization as CFinalization,
        Notarization as CNotarization,
    },
};
use commonware_cryptography::{
    bls12381::primitives::variant::{MinSig, Variant},
    certificate::Verifier as CertificateVerifier,
    ed25519,
    sha256::{Digest, Sha256},
};
use serde::{Deserialize, Serialize};

/// Native one-signature threshold certificates used by stable leaders.
pub type StandardScheme = standard::Scheme<PublicKey, MinSig>;

/// Vote-and-seed threshold certificates used by rotating leaders.
pub type VrfScheme = vrf::Scheme<PublicKey, MinSig>;

/// Certificate-seeded leader election used with [VrfScheme].
pub type RotatingElector = Random<Sha256>;

/// Leader election configuration for rotating leaders.
///
/// The seed-to-leader mapping is consensus-critical: every validator and the explorer (which
/// derives the leader of each view from the seed) must use this exact configuration.
/// [RandomVersion::V1] hashes the seed signature before reduction and therefore produces a
/// different schedule than the elector shipped before commonware v2026.9.0 (now
/// [RandomVersion::V0]), so a rotating-leader network must be redeployed rather than upgraded in
/// place.
pub const ROTATING_ELECTOR: RotatingElector = Random::new(RandomVersion::V1);

/// Certificate construction used by a consensus network.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificateMode {
    /// One threshold signature over each vote. Stable round-robin does not require a VRF.
    Standard,
    /// Vote and round-seed threshold signatures for certificate-seeded rotating leaders.
    Vrf,
}

impl CertificateMode {
    /// Every mode, in the order offered on the command line.
    pub const ALL: [Self; 2] = [Self::Standard, Self::Vrf];

    /// Name used on the command line and in configuration files (the serde spelling).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Vrf => "vrf",
        }
    }
}

impl std::str::FromStr for CertificateMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|mode| mode.as_str() == s)
            .ok_or_else(|| format!("unknown certificate mode: {s}"))
    }
}

/// Alto's common surface over its two statically selected consensus schemes.
///
/// Consensus engines remain monomorphized over the concrete implementation. This trait only
/// centralizes construction and optional seed extraction used by shared Alto components.
pub trait Scheme:
    SimplexScheme<Digest, PublicKey = PublicKey>
    + CertificateVerifier<Certificate: Read<Cfg = ()>>
    + Sized
{
    fn certificate_verifier(namespace: &[u8], identity: Identity) -> Self;

    fn verify_seed(&self, seed: &Seed) -> bool;

    fn vote_signature(
        certificate: &<Self as CertificateVerifier>::Certificate,
    ) -> Option<Signature>;

    fn seed_signature(
        certificate: &<Self as CertificateVerifier>::Certificate,
    ) -> Option<Signature>;
}

impl Scheme for StandardScheme {
    fn certificate_verifier(namespace: &[u8], identity: Identity) -> Self {
        standard::Scheme::<PublicKey, MinSig>::certificate_verifier(namespace, identity)
    }

    fn verify_seed(&self, _seed: &Seed) -> bool {
        false
    }

    fn vote_signature(
        certificate: &<Self as CertificateVerifier>::Certificate,
    ) -> Option<Signature> {
        certificate.get().copied()
    }

    fn seed_signature(
        _certificate: &<Self as CertificateVerifier>::Certificate,
    ) -> Option<Signature> {
        None
    }
}

impl Scheme for VrfScheme {
    fn certificate_verifier(namespace: &[u8], identity: Identity) -> Self {
        vrf::Scheme::<PublicKey, MinSig>::certificate_verifier(namespace, identity)
    }

    fn verify_seed(&self, seed: &Seed) -> bool {
        seed.verify(self)
    }

    fn vote_signature(
        certificate: &<Self as CertificateVerifier>::Certificate,
    ) -> Option<Signature> {
        certificate
            .get()
            .map(|certificate| certificate.vote_signature)
    }

    fn seed_signature(
        certificate: &<Self as CertificateVerifier>::Certificate,
    ) -> Option<Signature> {
        certificate
            .get()
            .map(|certificate| certificate.seed_signature)
    }
}

pub type Context = CContext<Digest, PublicKey>;
pub type Seed = vrf::Seed<MinSig>;
pub type Notarization<S> = CNotarization<S, Digest>;
pub type Finalization<S> = CFinalization<S, Digest>;
pub type Activity<S> = CActivity<S, Digest>;

pub type PublicKey = ed25519::PublicKey;
pub type Identity = <MinSig as Variant>::Public;
pub type Signature = <MinSig as Variant>::Signature;

pub trait Seedable {
    /// Returns the certificate-derived round seed when the selected scheme carries one.
    fn seed(&self) -> Option<Seed>;
}

impl<S: Scheme> Seedable for Notarization<S> {
    fn seed(&self) -> Option<Seed> {
        S::seed_signature(&self.certificate)
            .map(|signature| Seed::new(self.proposal.round, signature))
    }
}

impl<S: Scheme> Seedable for Finalization<S> {
    fn seed(&self) -> Option<Seed> {
        S::seed_signature(&self.certificate)
            .map(|signature| Seed::new(self.proposal.round, signature))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_consensus::{
        simplex::types::{Notarization as CNotarization, Notarize, Proposal},
        types::{Epoch, Round, View},
    };
    use commonware_cryptography::{sha256, Digest as _};
    use commonware_parallel::Sequential;
    use commonware_utils::non_empty;
    use rand::{rngs::StdRng, SeedableRng};

    fn standard_schemes(seed: u64) -> Vec<StandardScheme> {
        let mut rng = StdRng::seed_from_u64(seed);
        standard::fixture::<MinSig, _>(&mut rng, b"test", 4).schemes
    }

    fn vrf_schemes(seed: u64) -> Vec<VrfScheme> {
        let mut rng = StdRng::seed_from_u64(seed);
        vrf::fixture::<MinSig, _>(&mut rng, b"test", 4).schemes
    }

    fn proposal(view: u64) -> Proposal<Digest> {
        Proposal::new(
            Round::new(Epoch::new(1), View::new(view)),
            View::new(view - 1),
            sha256::Digest::EMPTY,
        )
    }

    fn notarization<S: Scheme>(schemes: &[S], view: u64) -> Notarization<S> {
        let proposal = proposal(view);
        let votes: Vec<_> = schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        CNotarization::from_notarizes(&schemes[0], non_empty![@&votes], &Sequential).unwrap()
    }

    #[test]
    fn concrete_schemes_verify_and_expose_only_vrf_seeds() {
        let standard = standard_schemes(15);
        let standard_notarization = notarization(&standard, 9);
        assert!(standard_notarization.seed().is_none());
        assert!(standard_notarization.verify(
            &mut StdRng::seed_from_u64(16),
            &standard[0],
            &Sequential,
        ));

        let vrf = vrf_schemes(17);
        let vrf_notarization = notarization(&vrf, 9);
        assert!(vrf_notarization.seed().is_some());
        assert!(vrf_notarization.verify(&mut StdRng::seed_from_u64(18), &vrf[0], &Sequential,));
    }
}
