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
    bls12381::primitives::{
        group::Share,
        sharing::Sharing,
        variant::{MinSig, Variant},
    },
    certificate::Verifier as CertificateVerifier,
    ed25519,
    sha256::{Digest, Sha256},
};
use commonware_utils::ordered::Set;
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

/// Alto's common surface over its two statically selected consensus schemes.
///
/// Consensus engines remain monomorphized over the concrete implementation. This trait only
/// centralizes construction and optional seed extraction used by shared Alto components.
pub trait Scheme:
    SimplexScheme<Digest, PublicKey = PublicKey>
    + CertificateVerifier<Certificate: Read<Cfg = ()>>
    + Sized
{
    fn signer(
        namespace: &[u8],
        participants: Set<PublicKey>,
        polynomial: Sharing<MinSig>,
        share: Share,
    ) -> Option<Self>;

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
    fn signer(
        namespace: &[u8],
        participants: Set<PublicKey>,
        polynomial: Sharing<MinSig>,
        share: Share,
    ) -> Option<Self> {
        standard::Scheme::<PublicKey, MinSig>::signer(namespace, participants, polynomial, share)
    }

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
    fn signer(
        namespace: &[u8],
        participants: Set<PublicKey>,
        polynomial: Sharing<MinSig>,
        share: Share,
    ) -> Option<Self> {
        vrf::Scheme::<PublicKey, MinSig>::signer(namespace, participants, polynomial, share)
    }

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
    use commonware_codec::{DecodeExt, Encode, EncodeSize, FixedSize};
    use commonware_consensus::{
        simplex::types::{Notarization as CNotarization, Notarize, Proposal},
        types::{Epoch, Round, View},
    };
    use commonware_cryptography::{sha256, Digest as _};
    use commonware_parallel::Sequential;
    use commonware_utils::non_empty;
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn rotating_elector_is_pinned_to_v1() {
        // The validator and the explorer WASM both derive leaders from this constant; changing
        // the version changes the leader schedule of every rotating-leader network.
        assert_eq!(format!("{ROTATING_ELECTOR:?}"), "V1");
    }

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
    fn standard_threshold_votes_use_one_signature_on_wire() {
        let schemes = standard_schemes(13);
        let proposal = proposal(9);
        let vote = Notarize::sign(&schemes[0], proposal.clone()).unwrap();
        let notarization = notarization(&schemes, 9);
        let signature_size = <Signature as FixedSize>::SIZE;

        assert_eq!(vote.attestation.signature.encode_size(), signature_size);
        assert_eq!(notarization.certificate.encode_size(), signature_size);
        assert_eq!(
            vote.encode_size(),
            proposal.encode_size() + vote.attestation.signer.encode_size() + signature_size
        );
        assert_eq!(
            notarization.encode_size(),
            proposal.encode_size() + signature_size
        );

        let encoded_vote = vote.encode();
        let encoded_notarization = notarization.encode();
        assert_eq!(
            Notarize::<StandardScheme, Digest>::decode(encoded_vote.clone()).unwrap(),
            vote
        );
        assert_eq!(
            Notarization::<StandardScheme>::decode(encoded_notarization.clone()).unwrap(),
            notarization
        );
        assert!(Notarize::<VrfScheme, Digest>::decode(encoded_vote).is_err());
        assert!(Notarization::<VrfScheme>::decode(encoded_notarization).is_err());
    }

    #[test]
    fn vrf_threshold_votes_retain_two_signatures_on_wire() {
        let schemes = vrf_schemes(14);
        let proposal = proposal(9);
        let vote = Notarize::sign(&schemes[0], proposal.clone()).unwrap();
        let notarization = notarization(&schemes, 9);
        let certificate_size = 2 * <Signature as FixedSize>::SIZE;

        assert_eq!(vote.attestation.signature.encode_size(), certificate_size);
        assert_eq!(notarization.certificate.encode_size(), certificate_size);
        assert_eq!(
            vote.encode_size(),
            proposal.encode_size() + vote.attestation.signer.encode_size() + certificate_size
        );
        assert_eq!(
            notarization.encode_size(),
            proposal.encode_size() + certificate_size
        );

        let encoded_vote = vote.encode();
        let encoded_notarization = notarization.encode();
        assert_eq!(
            Notarize::<VrfScheme, Digest>::decode(encoded_vote.clone()).unwrap(),
            vote
        );
        assert_eq!(
            Notarization::<VrfScheme>::decode(encoded_notarization.clone()).unwrap(),
            notarization
        );
        assert!(Notarize::<StandardScheme, Digest>::decode(encoded_vote).is_err());
        assert!(Notarization::<StandardScheme>::decode(encoded_notarization).is_err());
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
