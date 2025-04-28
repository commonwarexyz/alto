//! Common types used throughout `alto`.

mod block;
pub use block::{Block, Finalized, Notarized};
mod consensus;
use commonware_utils::hex;
pub use consensus::leader_index;
pub mod wasm;

pub const NAMESPACE: &[u8] = b"_ALTO";

#[repr(u8)]
pub enum Kind {
    Seed = 0,
    Notarization = 1,
    Finalization = 2,
}

impl Kind {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Seed),
            1 => Some(Self::Notarization),
            2 => Some(Self::Finalization),
            _ => None,
        }
    }

    pub fn to_hex(&self) -> String {
        match self {
            Self::Seed => hex(&[0]),
            Self::Notarization => hex(&[1]),
            Self::Finalization => hex(&[2]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_codec::{DecodeExt, Encode};
    use commonware_consensus::threshold_simplex::types::{
        Finalization, Finalize, Notarization, Notarize, Proposal,
    };
    use commonware_cryptography::{
        bls12381::{
            dkg::ops,
            primitives::{ops::threshold_signature_recover, poly},
        },
        hash, Digestible,
    };
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn test_notarized() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let (public, shares) = ops::generate_shares(&mut rng, None, 4, 3);

        // Create a block
        let digest = hash(b"hello world");
        let block = Block::new(digest, 10, 100);
        let proposal = Proposal::new(11, 8, block.digest());

        // Create a notarization
        let partials = shares
            .iter()
            .map(|share| Notarize::sign(NAMESPACE, share, proposal.clone()))
            .collect::<Vec<_>>();
        let proposal_partials = partials
            .iter()
            .map(|partial| partial.proposal_signature.clone())
            .collect::<Vec<_>>();
        let proposal_recovered = threshold_signature_recover(3, &proposal_partials).unwrap();
        let seed_partials = partials
            .into_iter()
            .map(|partial| partial.seed_signature)
            .collect::<Vec<_>>();
        let seed_recovered = threshold_signature_recover(3, &seed_partials).unwrap();
        let notarization = Notarization::new(proposal, proposal_recovered, seed_recovered);
        let notarized = Notarized::new(notarization, block.clone());

        // Serialize and deserialize
        let encoded = notarized.encode();
        let decoded = Notarized::decode(encoded).expect("failed to decode notarized");
        assert_eq!(notarized, decoded);

        // Verify notarized
        let public_key = poly::public(&public);
        assert!(notarized.verify(NAMESPACE, public_key));
    }

    #[test]
    fn test_finalized() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let (public, shares) = ops::generate_shares(&mut rng, None, 4, 3);

        // Create a block
        let digest = hash(b"hello world");
        let block = Block::new(digest, 10, 100);
        let proposal = Proposal::new(11, 8, block.digest());

        // Create a finalization
        let partials = shares
            .iter()
            .map(|share| Notarize::sign(NAMESPACE, share, proposal.clone()))
            .collect::<Vec<_>>();
        let seed_partials = partials
            .into_iter()
            .map(|partial| partial.seed_signature)
            .collect::<Vec<_>>();
        let seed_recovered = threshold_signature_recover(3, &seed_partials).unwrap();
        let finalize_partials = shares
            .iter()
            .map(|share| Finalize::sign(NAMESPACE, share, proposal.clone()))
            .collect::<Vec<_>>();
        let finalize_partials = finalize_partials
            .into_iter()
            .map(|partial| partial.proposal_signature)
            .collect::<Vec<_>>();
        let finalize_recovered = threshold_signature_recover(3, &finalize_partials).unwrap();
        let finalized = Finalization::new(proposal, finalize_recovered, seed_recovered);
        let finalized = Finalized::new(finalized, block.clone());

        // Serialize and deserialize
        let encoded = finalized.encode();
        let decoded = Finalized::decode(encoded).expect("failed to decode finalized");
        assert_eq!(finalized, decoded);

        // Verify finalized
        let public_key = poly::public(&public);
        assert!(finalized.verify(NAMESPACE, public_key));
    }
}
