//! Common types used throughout `alto`.

mod block;
pub use block::{Block, Finalized, Notarized};
mod consensus;
pub use consensus::{Finalization, Kind, Notarization, Nullification, Seed};

// We don't use functions here to guard against silent changes.
pub const NAMESPACE: &[u8] = b"_ALTO";
pub const P2P_NAMESPACE: &[u8] = b"_ALTO_P2P";
pub const SEED_NAMESPACE: &[u8] = b"_ALTO_SEED";
pub const NOTARIZE_NAMESPACE: &[u8] = b"_ALTO_NOTARIZE";
pub const NULLIFY_NAMESPACE: &[u8] = b"_ALTO_NULLIFY";
pub const FINALIZE_NAMESPACE: &[u8] = b"_ALTO_FINALIZE";

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_cryptography::{hash, Bls12381, Scheme};
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn test_seed() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let mut network = Bls12381::new(&mut rng);

        // Create seed
        let view = 0;
        let seed_payload = Seed::payload(view);
        let seed_signature = network.sign(Some(SEED_NAMESPACE), &seed_payload);
        let seed = Seed::new(view, seed_signature);

        // Check seed serialization
        let serialized = seed.serialize();
        let deserialized = Seed::deserialize(Some(&network.public_key()), &serialized).unwrap();
        assert_eq!(seed.view, deserialized.view);
    }

    #[test]
    fn test_seed_manipulated() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let mut network = Bls12381::new(&mut rng);

        // Create seed
        let view = 0;
        let seed_payload = Seed::payload(view);
        let seed_signature = network.sign(Some(SEED_NAMESPACE), &seed_payload);
        let mut seed = Seed::new(view, seed_signature);

        // Modify contents
        seed.view = 1;

        // Serialize seed
        let serialized = seed.serialize();

        // Deserialize seed
        assert!(Seed::deserialize(Some(&network.public_key()), &serialized).is_none());

        // Deserialize seed with no public key
        assert!(Seed::deserialize(None, &serialized).is_some());
    }

    #[test]
    fn test_nullification() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let mut network = Bls12381::new(&mut rng);

        // Create nullification
        let view = 0;
        let nullify_payload = Nullification::payload(view);
        let nullify_signature = network.sign(Some(NULLIFY_NAMESPACE), &nullify_payload);
        let nullification = Nullification::new(view, nullify_signature);

        // Check nullification serialization
        let serialized = nullification.serialize();
        let deserialized =
            Nullification::deserialize(Some(&network.public_key()), &serialized).unwrap();
        assert_eq!(nullification.view, deserialized.view);
    }

    #[test]
    fn test_nullification_manipulated() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let mut network = Bls12381::new(&mut rng);

        // Create nullification
        let view = 0;
        let nullify_payload = Nullification::payload(view);
        let nullify_signature = network.sign(Some(NULLIFY_NAMESPACE), &nullify_payload);
        let mut nullification = Nullification::new(view, nullify_signature);

        // Modify contents
        nullification.view = 1;

        // Serialize nullification
        let serialized = nullification.serialize();

        // Deserialize nullification
        assert!(Nullification::deserialize(Some(&network.public_key()), &serialized).is_none());

        // Deserialize nullification with no public key
        assert!(Nullification::deserialize(None, &serialized).is_some());
    }

    #[test]
    fn test_notarization_finalization() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let mut network = Bls12381::new(&mut rng);

        // Create block
        let parent_digest = hash(&[0; 32]);
        let height = 0;
        let timestamp = 1;
        let block = Block::new(parent_digest, height, timestamp);
        let block_digest = block.digest();

        // Check block serialization
        let serialized = block.serialize();
        let deserialized = Block::deserialize(&serialized).unwrap();
        assert_eq!(block_digest, deserialized.digest());
        assert_eq!(block.parent, deserialized.parent);
        assert_eq!(block.height, deserialized.height);
        assert_eq!(block.timestamp, deserialized.timestamp);

        // Create notarization
        let view = 0;
        let parent_view = 0;
        let block_payload = Notarization::payload(view, parent_view, &block_digest);
        let block_signature = network.sign(Some(NOTARIZE_NAMESPACE), &block_payload);
        let notarization = Notarization::new(view, parent_view, block_digest, block_signature);

        // Check notarization serialization
        let serialized = notarization.serialize();
        let deserialized =
            Notarization::deserialize(Some(&network.public_key()), &serialized).unwrap();
        assert_eq!(notarization.view, deserialized.view);
        assert_eq!(notarization.parent, deserialized.parent);
        assert_eq!(notarization.payload, deserialized.payload);

        // Create finalization
        let finalize_payload = Finalization::payload(view, parent_view, &notarization.payload);
        let finalize_signature = network.sign(Some(FINALIZE_NAMESPACE), &finalize_payload);
        let finalization =
            Finalization::new(view, parent_view, notarization.payload, finalize_signature);

        // Check finalization serialization
        let serialized = finalization.serialize();
        let deserialized =
            Finalization::deserialize(Some(&network.public_key()), &serialized).unwrap();
        assert_eq!(finalization.view, deserialized.view);
        assert_eq!(finalization.parent, deserialized.parent);
        assert_eq!(finalization.payload, deserialized.payload);
    }

    #[test]
    fn test_notarization_finalization_manipulated() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let mut network = Bls12381::new(&mut rng);

        // Create block
        let parent_digest = hash(&[0; 32]);
        let height = 0;
        let timestamp = 1;
        let block = Block::new(parent_digest, height, timestamp);

        // Create notarization
        let view = 0;
        let parent_view = 0;
        let block_payload = Notarization::payload(view, parent_view, &block.digest());
        let block_signature = network.sign(Some(NOTARIZE_NAMESPACE), &block_payload);

        // Create incorrect notarization proof
        let notarization =
            Notarization::new(view + 1, parent_view, block.digest(), block_signature);

        // Check notarization serialization
        let serialized = notarization.serialize();
        let result = Notarization::deserialize(Some(&network.public_key()), &serialized);
        assert!(result.is_none());

        // Check notarization serialization with no public key
        let result = Notarization::deserialize(None, &serialized);
        assert!(result.is_some());

        // Create finalization
        let finalize_payload = Finalization::payload(view, parent_view, &block.digest());
        let finalize_signature = network.sign(Some(FINALIZE_NAMESPACE), &finalize_payload);

        // Create incorrect finalization proof
        let finalization =
            Finalization::new(view + 1, parent_view, block.digest(), finalize_signature);

        // Check finalization serialization
        let serialized = finalization.serialize();
        let result = Finalization::deserialize(Some(&network.public_key()), &serialized);
        assert!(result.is_none());

        // Check finalization serialization with no public key
        let result = Finalization::deserialize(None, &serialized);
        assert!(result.is_some());
    }
}
