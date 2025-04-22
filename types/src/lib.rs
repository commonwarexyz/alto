//! Common types used throughout `alto`.

mod block;
pub use block::{Block, Finalized, Notarized};
mod consensus;
pub use consensus::leader_index;
pub mod wasm;

// We don't use functions here to guard against silent changes.
pub const NAMESPACE: &[u8] = b"_ALTO";

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_codec::{DecodeExt, Encode};
    use commonware_consensus::threshold_simplex::types::{Finalization, Notarization, Proposal};
    use commonware_cryptography::{hash, Bls12381, Digestible, Signer};
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn test_notarized() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let mut network = Bls12381::new(&mut rng);

        // Create a block
        let digest = hash(b"hello world");
        let block = Block::new(digest, 10, 100);
        let proposal = Proposal::new(11, 8, block.digest());

        // Create a notarization
        let notarization = Notarization::sign(NAMESPACE, &mut network, proposal.clone());
        let notarized = Notarized::new(notarization, block.clone());

        // Serialize and deserialize
        let encoded = notarized.encode();
        let decoded = Notarized::decode(encoded).expect("failed to decode notarized");
        assert_eq!(notarized, decoded);

        // Verify notarized
        let public_key = network.public_key();
        assert!(notarized.verify(public_key.as_ref()));
    }

    #[test]
    fn test_finalized() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let mut network = Bls12381::new(&mut rng);

        // Create a block
        let digest = hash(b"hello world");
        let block = Block::new(digest, 10, 100);
        let proposal = Proposal::new(11, 8, block.digest());

        // Create a notarization
        let finalization = Finalization::sign(NAMESPACE, &mut network, proposal.clone());
        let finalized = Finalized::new(finalization, block);

        // Serialize and deserialize
        let encoded = finalized.encode();
        let decoded = Finalized::decode(encoded).expect("failed to decode finalized");
        assert_eq!(finalized, decoded);

        // Verify finalized
        let public_key = network.public_key();
        assert!(finalized.verify(public_key.as_ref()));
    }
}
