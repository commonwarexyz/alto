//! Common types used throughout `alto`.

mod block;
pub use block::{Block, Finalized, Notarized};
mod consensus;
use commonware_utils::hex;
pub use consensus::{
    Activity, Evaluation, Finalization, Identity, Notarization, Seed, Seedable, Signature,
    SigningScheme,
};
pub mod wasm;

pub const NAMESPACE: &[u8] = b"_ALTO";
pub const EPOCH: u64 = 0;

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
    use commonware_consensus::{
        threshold_simplex::types::{Finalization, Finalize, Notarization, Notarize, Proposal},
        types::Round,
    };
    use commonware_cryptography::{
        bls12381::{dkg::ops, primitives::variant::MinSig},
        Digestible, Hasher, Sha256,
    };
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn test_notarized() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let n = 4;
        let (polynomial, shares) = ops::generate_shares::<_, MinSig>(&mut rng, None, n, 3);
        let participants = vec![(); n as usize];
        let schemes: Vec<_> = shares
            .into_iter()
            .map(|share| SigningScheme::new(&participants, &polynomial, share))
            .collect();

        // Create a block
        let digest = Sha256::hash(b"hello world");
        let block = Block::new(digest, 10, 100);
        let proposal = Proposal::new(Round::new(EPOCH, 11), 8, block.digest());

        // Create a notarization
        let notarizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, NAMESPACE, proposal.clone()))
            .collect();
        let notarization = Notarization::from_notarizes(&schemes[0], &notarizes).unwrap();
        let notarized = Notarized::new(notarization, block.clone());

        // Serialize and deserialize
        let encoded = notarized.encode();
        let decoded = Notarized::decode(encoded).expect("failed to decode notarized");
        assert_eq!(notarized, decoded);

        // Verify notarized
        assert!(notarized.verify(&schemes[0], NAMESPACE));
    }

    #[test]
    fn test_finalized() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let n = 4;
        let (polynomial, shares) = ops::generate_shares::<_, MinSig>(&mut rng, None, n, 3);
        let participants = vec![(); n as usize];
        let schemes: Vec<_> = shares
            .into_iter()
            .map(|share| SigningScheme::new(&participants, &polynomial, share))
            .collect();

        // Create a block
        let digest = Sha256::hash(b"hello world");
        let block = Block::new(digest, 10, 100);
        let proposal = Proposal::new(Round::new(EPOCH, 11), 8, block.digest());

        // Create a finalization
        let finalizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Finalize::sign(scheme, NAMESPACE, proposal.clone()))
            .collect();
        let finalization = Finalization::from_finalizes(&schemes[0], &finalizes, None).unwrap();
        let finalized = Finalized::new(finalization, block.clone());

        // Serialize and deserialize
        let encoded = finalized.encode();
        let decoded = Finalized::decode(encoded).expect("failed to decode finalized");
        assert_eq!(finalized, decoded);

        // Verify finalized
        assert!(finalized.verify(&schemes[0], NAMESPACE));
    }
}
