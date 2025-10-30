//! Common types used throughout `alto`.

mod block;
pub use block::{Block, Finalized, Notarized};
mod consensus;
use commonware_utils::hex;
pub use consensus::{
    Activity, Evaluation, Finalization, Identity, Notarization, PublicKey, Scheme, Seed, Seedable,
    Signature,
};
pub mod wasm;

/// The unique namespace prefix used in all signing operations to prevent signature replay attacks.
pub const NAMESPACE: &[u8] = b"_ALTO";

/// The epoch number used in [commonware_consensus::simplex].
///
/// Because alto does not implement reconfiguration (validator set changes and resharing), we hardcode the epoch to 0.
///
/// For an example of how to implement reconfiguration and resharing, see [commonware-reshare](https://github.com/commonwarexyz/monorepo/tree/main/examples/reshare).
pub const EPOCH: u64 = 0;
/// The epoch length used in [commonware_consensus::simplex].
///
/// Because alto does not implement reconfiguration (validator set changes and resharing), we hardcode the epoch length to u64::MAX (to
/// stay in the first epoch forever).
///
/// For an example of how to implement reconfiguration and resharing, see [commonware-reshare](https://github.com/commonwarexyz/monorepo/tree/main/examples/reshare).
pub const EPOCH_LENGTH: u64 = u64::MAX;

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
        simplex::types::{Finalization, Finalize, Notarization, Notarize, Proposal},
        types::Round,
    };
    use commonware_cryptography::{
        bls12381::{dkg::ops, primitives::variant::MinSig},
        ed25519, Digestible, Hasher, PrivateKeyExt, Sha256, Signer,
    };
    use commonware_utils::set::Ordered;
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn test_notarized() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let n = 4;
        let participants = (0..n)
            .map(|_| ed25519::PrivateKey::from_rng(&mut rng).public_key())
            .collect::<Ordered<_>>();
        let (polynomial, shares) = ops::generate_shares::<_, MinSig>(&mut rng, None, n, 3);
        let schemes: Vec<_> = shares
            .into_iter()
            .map(|share| Scheme::new(participants.clone(), &polynomial, share))
            .collect();

        // Create a block
        let digest = Sha256::hash(b"hello world");
        let block = Block::new(digest, 10, 100);
        let proposal = Proposal::new(Round::new(EPOCH, 11), 8, block.digest());

        // Create a notarization
        let notarizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, NAMESPACE, proposal.clone()).unwrap())
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
        let participants = (0..n)
            .map(|_| ed25519::PrivateKey::from_rng(&mut rng).public_key())
            .collect::<Ordered<_>>();
        let schemes: Vec<_> = shares
            .into_iter()
            .map(|share| Scheme::new(participants.clone(), &polynomial, share))
            .collect();

        // Create a block
        let digest = Sha256::hash(b"hello world");
        let block = Block::new(digest, 10, 100);
        let proposal = Proposal::new(Round::new(EPOCH, 11), 8, block.digest());

        // Create a finalization
        let finalizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Finalize::sign(scheme, NAMESPACE, proposal.clone()).unwrap())
            .collect();
        let finalization = Finalization::from_finalizes(&schemes[0], &finalizes).unwrap();
        let finalized = Finalized::new(finalization, block.clone());

        // Serialize and deserialize
        let encoded = finalized.encode();
        let decoded = Finalized::decode(encoded).expect("failed to decode finalized");
        assert_eq!(finalized, decoded);

        // Verify finalized
        assert!(finalized.verify(&schemes[0], NAMESPACE));
    }
}
