use commonware_consensus::simplex::{
    scheme::bls12381_threshold::{standard, vrf},
    types::{
        Activity as CActivity, Context as CContext, Finalization as CFinalization,
        Notarization as CNotarization, Subject,
    },
};
use commonware_cryptography::{
    bls12381::{
        certificate::threshold::Certificate as StandardCertificate,
        primitives::{
            group::Share,
            sharing::Sharing,
            variant::{MinSig, Variant},
        },
    },
    certificate::{
        Attestation, Scheme as CertificateScheme, Verification, Verifier as CertificateVerifier,
    },
    ed25519,
    sha256::Digest,
};
use commonware_parallel::Strategy;
use commonware_utils::{ordered::Set, N3f1, Participant};
use rand::CryptoRng;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

type StandardScheme = standard::Scheme<PublicKey, MinSig>;
type VrfScheme = vrf::Scheme<PublicKey, MinSig>;
type ConsensusSignature = vrf::Signature<MinSig>;

/// Certificate construction used by a consensus network.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificateMode {
    /// One threshold signature over each vote. Stable round-robin does not require a VRF.
    Standard,
    /// Vote and round-seed threshold signatures for certificate-seeded rotating leaders.
    Vrf,
}

#[derive(Clone, Debug)]
enum Inner {
    Standard(StandardScheme),
    Vrf(VrfScheme),
}

/// Alto's mode-aware threshold certificate scheme.
///
/// Consensus framing retains the existing fixed-size vote/seed pair. Standard certificates put
/// the vote signature in both fields; mode-aware assembly and verification require them to match,
/// while cryptographic verification and recovery delegate to Commonware's one-signature standard
/// threshold scheme.
#[derive(Clone, Debug)]
pub struct Scheme(Inner);

impl Scheme {
    pub fn signer(
        mode: CertificateMode,
        namespace: &[u8],
        participants: Set<PublicKey>,
        polynomial: Sharing<MinSig>,
        share: Share,
    ) -> Option<Self> {
        match mode {
            CertificateMode::Standard => {
                StandardScheme::signer(namespace, participants, polynomial, share)
                    .map(Inner::Standard)
            }
            CertificateMode::Vrf => {
                VrfScheme::signer(namespace, participants, polynomial, share).map(Inner::Vrf)
            }
        }
        .map(Self)
    }

    pub fn verifier(
        mode: CertificateMode,
        namespace: &[u8],
        participants: Set<PublicKey>,
        polynomial: Sharing<MinSig>,
    ) -> Self {
        match mode {
            CertificateMode::Standard => Self(Inner::Standard(StandardScheme::verifier(
                namespace,
                participants,
                polynomial,
            ))),
            CertificateMode::Vrf => Self(Inner::Vrf(VrfScheme::verifier(
                namespace,
                participants,
                polynomial,
            ))),
        }
    }

    pub fn certificate_verifier(
        mode: CertificateMode,
        namespace: &[u8],
        identity: Identity,
    ) -> Self {
        match mode {
            CertificateMode::Standard => Self(Inner::Standard(
                StandardScheme::certificate_verifier(namespace, identity),
            )),
            CertificateMode::Vrf => Self(Inner::Vrf(VrfScheme::certificate_verifier(
                namespace, identity,
            ))),
        }
    }

    #[doc(hidden)]
    pub fn from_vrf(inner: VrfScheme) -> Self {
        Self(Inner::Vrf(inner))
    }

    pub fn identity(&self) -> &Identity {
        match &self.0 {
            Inner::Standard(scheme) => scheme.identity(),
            Inner::Vrf(scheme) => scheme.identity(),
        }
    }

    pub fn verify_seed(&self, seed: &Seed) -> bool {
        match &self.0 {
            Inner::Standard(_) => false,
            Inner::Vrf(scheme) => seed.verify(scheme),
        }
    }

    /// Extracts the round-seed signature after a caller has verified a VRF certificate.
    ///
    /// This checks the mode's canonical framing but does not authenticate the certificate.
    pub fn verified_vrf_seed_signature(
        certificate: &<Self as CertificateVerifier>::Certificate,
    ) -> Option<Signature> {
        let signature = certificate.get()?;
        (signature.vote_signature != signature.seed_signature).then_some(signature.seed_signature)
    }
}

fn standard_pair(signature: Signature) -> ConsensusSignature {
    ConsensusSignature {
        vote_signature: signature,
        seed_signature: signature,
    }
}

fn standard_signature(signature: &ConsensusSignature) -> Option<Signature> {
    (signature.vote_signature == signature.seed_signature).then_some(signature.vote_signature)
}

fn wrap_vrf_attestation(attestation: Attestation<VrfScheme>) -> Attestation<Scheme> {
    Attestation {
        signer: attestation.signer,
        signature: attestation.signature,
    }
}

fn wrap_standard_attestation(attestation: Attestation<StandardScheme>) -> Attestation<Scheme> {
    let signature = *attestation
        .signature
        .get()
        .expect("locally constructed standard signature must decode");
    Attestation {
        signer: attestation.signer,
        signature: standard_pair(signature).into(),
    }
}

impl CertificateVerifier for Scheme {
    type Subject<'a, D: commonware_cryptography::Digest> = Subject<'a, D>;
    type Faults = N3f1;
    type PublicKey = PublicKey;
    type Certificate = vrf::Certificate<MinSig>;

    fn verify_certificate<R, D>(
        &self,
        rng: &mut R,
        subject: Subject<'_, D>,
        certificate: &Self::Certificate,
        strategy: &impl Strategy,
    ) -> bool
    where
        R: CryptoRng,
        D: commonware_cryptography::Digest,
    {
        match &self.0 {
            Inner::Standard(scheme) => {
                let Some(signature) = certificate.get().and_then(standard_signature) else {
                    return false;
                };
                let certificate = StandardCertificate::new(signature);
                scheme.verify_certificate(rng, subject, &certificate, strategy)
            }
            Inner::Vrf(scheme) => scheme.verify_certificate(rng, subject, certificate, strategy),
        }
    }

    fn verify_certificates<'a, R, D, I>(
        &self,
        rng: &mut R,
        certificates: I,
        strategy: &impl Strategy,
    ) -> bool
    where
        R: CryptoRng,
        D: commonware_cryptography::Digest,
        I: Iterator<Item = (Subject<'a, D>, &'a Self::Certificate)>,
    {
        match &self.0 {
            Inner::Standard(scheme) => {
                let Some(certificates) = certificates
                    .map(|(subject, certificate)| {
                        let signature = certificate.get().and_then(standard_signature)?;
                        Some((subject, StandardCertificate::new(signature)))
                    })
                    .collect::<Option<Vec<_>>>()
                else {
                    return false;
                };
                scheme.verify_certificates(
                    rng,
                    certificates
                        .iter()
                        .map(|(subject, certificate)| (*subject, certificate)),
                    strategy,
                )
            }
            Inner::Vrf(scheme) => scheme.verify_certificates(rng, certificates, strategy),
        }
    }

    fn is_batchable() -> bool {
        true
    }

    fn certificate_codec_config(&self) {}

    fn certificate_codec_config_unbounded() {}
}

impl CertificateScheme for Scheme {
    type Signature = ConsensusSignature;

    fn me(&self) -> Option<Participant> {
        match &self.0 {
            Inner::Standard(scheme) => scheme.me(),
            Inner::Vrf(scheme) => scheme.me(),
        }
    }

    fn participants(&self) -> &Set<Self::PublicKey> {
        match &self.0 {
            Inner::Standard(scheme) => scheme.participants(),
            Inner::Vrf(scheme) => scheme.participants(),
        }
    }

    fn sign<D: commonware_cryptography::Digest>(
        &self,
        subject: Subject<'_, D>,
    ) -> Option<Attestation<Self>> {
        match &self.0 {
            Inner::Standard(scheme) => scheme.sign(subject).map(wrap_standard_attestation),
            Inner::Vrf(scheme) => scheme.sign(subject).map(wrap_vrf_attestation),
        }
    }

    fn verify_attestation<R, D>(
        &self,
        rng: &mut R,
        subject: Subject<'_, D>,
        attestation: &Attestation<Self>,
        strategy: &impl Strategy,
    ) -> bool
    where
        R: CryptoRng,
        D: commonware_cryptography::Digest,
    {
        match &self.0 {
            Inner::Standard(scheme) => {
                let Some(signature) = attestation.signature.get().and_then(standard_signature)
                else {
                    return false;
                };
                let attestation = Attestation::<StandardScheme> {
                    signer: attestation.signer,
                    signature: signature.into(),
                };
                scheme.verify_attestation(rng, subject, &attestation, strategy)
            }
            Inner::Vrf(scheme) => {
                let attestation = Attestation::<VrfScheme> {
                    signer: attestation.signer,
                    signature: attestation.signature.clone(),
                };
                scheme.verify_attestation(rng, subject, &attestation, strategy)
            }
        }
    }

    fn verify_attestations<R, D, I>(
        &self,
        rng: &mut R,
        subject: Subject<'_, D>,
        attestations: I,
        strategy: &impl Strategy,
    ) -> Verification<Self>
    where
        R: CryptoRng,
        D: commonware_cryptography::Digest,
        I: IntoIterator<Item = Attestation<Self>>,
        I::IntoIter: Send,
    {
        match &self.0 {
            Inner::Standard(scheme) => {
                let (attestations, rejected) =
                    strategy.map_partition_collect_vec(attestations.into_iter(), |attestation| {
                        let signer = attestation.signer;
                        let converted = attestation
                            .signature
                            .get()
                            .and_then(standard_signature)
                            .map(|signature| Attestation::<StandardScheme> {
                                signer,
                                signature: signature.into(),
                            });
                        (signer, converted)
                    });
                let verification = scheme.verify_attestations(rng, subject, attestations, strategy);
                let mut rejected: BTreeSet<_> = rejected.into_iter().collect();
                rejected.extend(verification.invalid);
                Verification::new(
                    verification
                        .verified
                        .into_iter()
                        .map(wrap_standard_attestation)
                        .collect(),
                    rejected.into_iter().collect(),
                )
            }
            Inner::Vrf(scheme) => {
                let attestations = attestations.into_iter().map(|attestation| Attestation {
                    signer: attestation.signer,
                    signature: attestation.signature,
                });
                let verification = scheme.verify_attestations(rng, subject, attestations, strategy);
                Verification::new(
                    verification
                        .verified
                        .into_iter()
                        .map(wrap_vrf_attestation)
                        .collect(),
                    verification.invalid,
                )
            }
        }
    }

    fn assemble<I>(&self, attestations: I, strategy: &impl Strategy) -> Option<Self::Certificate>
    where
        I: IntoIterator<Item = Attestation<Self>>,
        I::IntoIter: Send,
    {
        match &self.0 {
            Inner::Standard(scheme) => {
                let attestations = attestations
                    .into_iter()
                    .map(|attestation| {
                        let signature = attestation.signature.get().and_then(standard_signature)?;
                        Some(Attestation::<StandardScheme> {
                            signer: attestation.signer,
                            signature: signature.into(),
                        })
                    })
                    .collect::<Option<Vec<_>>>()?;
                let certificate = scheme.assemble(attestations, strategy)?;
                let signature = *certificate.get()?;
                Some(standard_pair(signature).into())
            }
            Inner::Vrf(scheme) => {
                let attestations = attestations.into_iter().map(|attestation| Attestation {
                    signer: attestation.signer,
                    signature: attestation.signature,
                });
                scheme.assemble(attestations, strategy)
            }
        }
    }

    fn is_attributable() -> bool {
        false
    }
}

pub type Context = CContext<Digest, PublicKey>;
pub type Seed = vrf::Seed<MinSig>;
pub type Notarization = CNotarization<Scheme, Digest>;
pub type Finalization = CFinalization<Scheme, Digest>;
pub type Activity = CActivity<Scheme, Digest>;

pub type PublicKey = ed25519::PublicKey;
pub type Identity = <MinSig as Variant>::Public;
pub type Signature = <MinSig as Variant>::Signature;

pub trait Seedable {
    /// Returns the embedded VRF seed, if present. The containing certificate must be verified
    /// before the seed is used as authenticated network data.
    fn seed(&self) -> Option<Seed>;
}

impl Seedable for Notarization {
    fn seed(&self) -> Option<Seed> {
        Scheme::verified_vrf_seed_signature(&self.certificate)
            .map(|signature| Seed::new(self.proposal.round, signature))
    }
}

impl Seedable for Finalization {
    fn seed(&self) -> Option<Seed> {
        Scheme::verified_vrf_seed_signature(&self.certificate)
            .map(|signature| Seed::new(self.proposal.round, signature))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_codec::Encode;
    use commonware_consensus::{
        simplex::types::{verify_certificates, Notarization, Notarize, Proposal},
        types::{Epoch, Round, View},
    };
    use commonware_cryptography::{certificate::mocks::Fixture, sha256, Digest as _};
    use commonware_parallel::{Manual, Sequential};
    use rand::{rngs::StdRng, SeedableRng};
    use std::{
        future::Future,
        num::NonZeroUsize,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    #[derive(Clone, Debug, Default)]
    struct CountingStrategy {
        partition_calls: Arc<AtomicUsize>,
    }

    impl CountingStrategy {
        fn partition_calls(&self) -> usize {
            self.partition_calls.load(Ordering::Relaxed)
        }
    }

    impl Strategy for CountingStrategy {
        fn manual(&self) -> Manual<Self> {
            Manual::new(self.clone(), NonZeroUsize::MIN)
        }

        fn spawn<F, T>(&self, f: F) -> impl Future<Output = T> + Send + 'static
        where
            F: FnOnce(Self) -> T + Send + 'static,
            T: Send + 'static,
        {
            let strategy = self.clone();
            async move { f(strategy) }
        }

        fn run<R, SEQ, PAR>(&self, len: usize, serial: SEQ, parallel: PAR) -> R
        where
            R: Send,
            SEQ: FnOnce() -> R + Send,
            PAR: FnOnce() -> R + Send,
        {
            Sequential.run(len, serial, parallel)
        }

        fn try_run<R, E, SEQ, PAR>(&self, len: usize, serial: SEQ, parallel: PAR) -> Result<R, E>
        where
            R: Send,
            E: Send,
            SEQ: FnOnce() -> Result<R, E> + Send,
            PAR: FnOnce() -> Result<R, E> + Send,
        {
            Sequential.try_run(len, serial, parallel)
        }

        fn fold_init<I, INIT, T, R, ID, F, RD>(
            &self,
            iter: I,
            init: INIT,
            identity: ID,
            fold_op: F,
            reduce_op: RD,
        ) -> R
        where
            I: IntoIterator<IntoIter: Send, Item: Send> + Send,
            INIT: Fn() -> T + Send + Sync,
            T: Send,
            R: Send,
            ID: Fn() -> R + Send + Sync,
            F: Fn(R, &mut T, I::Item) -> R + Send + Sync,
            RD: Fn(R, R) -> R + Send + Sync,
        {
            Sequential.fold_init(iter, init, identity, fold_op, reduce_op)
        }

        fn try_fold<I, R, E, ID, F, RD>(
            &self,
            iter: I,
            identity: ID,
            fold_op: F,
            reduce_op: RD,
        ) -> Result<R, E>
        where
            I: IntoIterator<IntoIter: Send, Item: Send> + Send,
            R: Send,
            E: Send,
            ID: Fn() -> R + Send + Sync,
            F: Fn(R, I::Item) -> Result<R, E> + Send + Sync,
            RD: Fn(R, R) -> R + Send + Sync,
        {
            Sequential.try_fold(iter, identity, fold_op, reduce_op)
        }

        fn map_partition_collect_vec<I, F, K, U>(&self, iter: I, map_op: F) -> (Vec<U>, Vec<K>)
        where
            I: IntoIterator<IntoIter: Send, Item: Send> + Send,
            F: Fn(I::Item) -> (K, Option<U>) + Send + Sync,
            K: Send,
            U: Send,
        {
            self.partition_calls.fetch_add(1, Ordering::Relaxed);
            Sequential.map_partition_collect_vec(iter, map_op)
        }

        fn join<A, B, RA, RB>(&self, a: A, b: B) -> (RA, RB)
        where
            A: FnOnce() -> RA + Send,
            B: FnOnce() -> RB + Send,
            RA: Send,
            RB: Send,
        {
            Sequential.join(a, b)
        }

        fn sort_by<T, C>(&self, items: &mut [T], compare: C)
        where
            T: Send,
            C: Fn(&T, &T) -> std::cmp::Ordering + Send + Sync,
        {
            Sequential.sort_by(items, compare);
        }
    }

    fn schemes(mode: CertificateMode, seed: u64) -> Vec<Scheme> {
        let mut rng = StdRng::seed_from_u64(seed);
        let Fixture { schemes, .. } = vrf::fixture::<MinSig, _>(&mut rng, b"test", 4);
        schemes
            .iter()
            .map(|scheme| {
                Scheme::signer(
                    mode,
                    b"test",
                    scheme.participants().clone(),
                    scheme.polynomial().clone(),
                    scheme.share().unwrap().clone(),
                )
                .unwrap()
            })
            .collect()
    }

    fn notarization(schemes: &[Scheme], view: u64) -> crate::Notarization {
        let proposal = Proposal::new(
            Round::new(Epoch::new(1), View::new(view)),
            View::new(view - 1),
            sha256::Digest::EMPTY,
        );
        let votes: Vec<_> = schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        Notarization::from_notarizes(&schemes[0], &votes, &Sequential).unwrap()
    }

    #[test]
    fn standard_threshold_certificates_do_not_export_vrf_seeds() {
        let schemes = schemes(CertificateMode::Standard, 1);
        let notarization = notarization(&schemes, 9);
        let vrf_verifier =
            Scheme::certificate_verifier(CertificateMode::Vrf, b"test", *schemes[0].identity());

        assert!(notarization.seed().is_none());
        assert!(notarization.verify(&mut StdRng::seed_from_u64(2), &schemes[0], &Sequential));
        assert!(!notarization.verify(&mut StdRng::seed_from_u64(2), &vrf_verifier, &Sequential,));
    }

    #[test]
    fn standard_threshold_certificates_reject_noncanonical_second_signature() {
        let schemes = schemes(CertificateMode::Standard, 3);
        let mut certificate = notarization(&schemes, 9);
        let other = notarization(&schemes, 10);
        let signature = certificate.certificate.get().unwrap().vote_signature;
        let other_signature = other.certificate.get().unwrap().seed_signature;
        certificate.certificate = ConsensusSignature {
            vote_signature: signature,
            seed_signature: other_signature,
        }
        .into();

        assert!(!certificate.verify(&mut StdRng::seed_from_u64(4), &schemes[0], &Sequential,));
    }

    #[test]
    fn standard_threshold_certificates_batch_verify() {
        let schemes = schemes(CertificateMode::Standard, 5);
        let first = notarization(&schemes, 9);
        let mut second = notarization(&schemes, 10);
        let second_vote = second.certificate.get().unwrap().vote_signature;
        let first_vote = first.certificate.get().unwrap().vote_signature;
        second.certificate = ConsensusSignature {
            vote_signature: second_vote,
            seed_signature: first_vote,
        }
        .into();
        let certificates = [
            (
                Subject::Notarize {
                    proposal: &first.proposal,
                },
                &first.certificate,
            ),
            (
                Subject::Notarize {
                    proposal: &second.proposal,
                },
                &second.certificate,
            ),
        ];

        assert_eq!(
            verify_certificates(
                &mut StdRng::seed_from_u64(6),
                &schemes[0],
                &certificates,
                &Sequential,
            ),
            vec![true, false]
        );
    }

    #[test]
    fn standard_threshold_batch_uses_strategy_for_adapter_decoding() {
        let schemes = schemes(CertificateMode::Standard, 11);
        let proposal = Proposal::new(
            Round::new(Epoch::new(1), View::new(9)),
            View::new(8),
            sha256::Digest::EMPTY,
        );
        let attestations = schemes
            .iter()
            .map(|scheme| {
                scheme
                    .sign(Subject::Notarize {
                        proposal: &proposal,
                    })
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let strategy = CountingStrategy::default();

        let verification = schemes[0].verify_attestations(
            &mut StdRng::seed_from_u64(12),
            Subject::Notarize {
                proposal: &proposal,
            },
            attestations,
            &strategy,
        );

        assert_eq!(verification.verified.len(), schemes.len());
        assert!(verification.invalid.is_empty());
        assert!(
            strategy.partition_calls() >= 2,
            "the adapter and standard verifier must both use the parallel strategy"
        );
    }

    #[test]
    fn vrf_mode_preserves_legacy_certificate_bytes() {
        let mut rng = StdRng::seed_from_u64(7);
        let Fixture { schemes, .. } = vrf::fixture::<MinSig, _>(&mut rng, b"test", 4);
        let wrapped: Vec<_> = schemes
            .iter()
            .map(|scheme| {
                Scheme::signer(
                    CertificateMode::Vrf,
                    b"test",
                    scheme.participants().clone(),
                    scheme.polynomial().clone(),
                    scheme.share().unwrap().clone(),
                )
                .unwrap()
            })
            .collect();
        let proposal = Proposal::new(
            Round::new(Epoch::new(1), View::new(9)),
            View::new(8),
            sha256::Digest::EMPTY,
        );
        let legacy_votes: Vec<_> = schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        let wrapped_votes: Vec<_> = wrapped
            .iter()
            .map(|scheme| Notarize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        let legacy = Notarization::from_notarizes(&schemes[0], &legacy_votes, &Sequential).unwrap();
        let adapted =
            Notarization::from_notarizes(&wrapped[0], &wrapped_votes, &Sequential).unwrap();

        assert_eq!(legacy.certificate.encode(), adapted.certificate.encode());
    }

    #[test]
    fn vrf_threshold_certificates_retain_round_seed() {
        let schemes = schemes(CertificateMode::Vrf, 9);
        let notarization = notarization(&schemes, 9);
        let standard_verifier = Scheme::certificate_verifier(
            CertificateMode::Standard,
            b"test",
            *schemes[0].identity(),
        );

        assert!(notarization.seed().is_some());
        assert!(notarization.verify(&mut StdRng::seed_from_u64(8), &schemes[0], &Sequential));
        assert!(!notarization.verify(
            &mut StdRng::seed_from_u64(8),
            &standard_verifier,
            &Sequential,
        ));
    }
}
