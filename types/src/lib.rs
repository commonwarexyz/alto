mod block;
pub use block::Block;
mod consensus;
pub use consensus::{
    Finalization, Notarization, Nullification, Seed, FINALIZE_NAMESPACE, NAMESPACE,
    NOTARIZE_NAMESPACE, NULLIFY_NAMESPACE, SEED_NAMESPACE,
};
pub mod client;

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_cryptography::{hash, sha256::Digest, Bls12381, Ed25519, Scheme};
    use rand::{rngs::StdRng, SeedableRng};

    fn test_digest(value: usize) -> Digest {
        hash(&value.to_be_bytes())
    }

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
        let deserialized = Seed::deserialize(&network.public_key(), &serialized).unwrap();
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
        assert!(Seed::deserialize(&network.public_key(), &serialized).is_none());
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
        let deserialized = Nullification::deserialize(&network.public_key(), &serialized).unwrap();
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
        assert!(Nullification::deserialize(&network.public_key(), &serialized).is_none());
    }

    #[test]
    fn test_notarization_finalization() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let mut network = Bls12381::new(&mut rng);

        // Create batch proofs
        let mut batch_proofs = Vec::new();
        for i in 0..10 {
            let sequencer = Ed25519::new(&mut rng).public_key();
            let batch_digest = test_digest(i);
            let batch_payload = Proof::payload(&sequencer, 0, &batch_digest, 0);
            let batch_signature = network.sign(Some(BATCH_NAMESPACE), &batch_payload);
            let batch_proof = Proof::new(sequencer, 0, batch_digest, 0, batch_signature);
            batch_proofs.push(batch_proof);
        }

        // Create block
        let parent_digest = hash(&[0; 32]);
        let height = 0;
        let block = Block::new(parent_digest, height, batch_proofs.clone());
        let block_digest = block.digest();

        // Check block serialization
        let serialized = block.serialize();
        let deserialized = Block::deserialize(&network.public_key(), &serialized).unwrap();
        assert_eq!(block_digest, deserialized.digest());
        for (batch_proof, deserialized) in batch_proofs.iter().zip(deserialized.batches.iter()) {
            assert_eq!(batch_proof.digest(), deserialized.digest());
            assert_eq!(batch_proof.sequencer, deserialized.sequencer);
            assert_eq!(batch_proof.height, deserialized.height);
            assert_eq!(batch_proof.batch, deserialized.batch);
            assert_eq!(batch_proof.epoch, deserialized.epoch);
            assert_eq!(batch_proof.signature, deserialized.signature);
        }

        // Create notarization
        let view = 0;
        let parent_view = 0;
        let block_payload = Notarization::payload(view, parent_view, &block_digest);
        let block_signature = network.sign(Some(NOTARIZE_NAMESPACE), &block_payload);
        let notarization = Notarization::new(view, parent_view, block_digest, block_signature);

        // Check notarization serialization
        let serialized = notarization.serialize();
        let deserialized = Notarization::deserialize(&network.public_key(), &serialized).unwrap();
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
        let deserialized = Finalization::deserialize(&network.public_key(), &serialized).unwrap();
        assert_eq!(finalization.view, deserialized.view);
        assert_eq!(finalization.parent, deserialized.parent);
        assert_eq!(finalization.payload, deserialized.payload);
    }

    #[test]
    fn test_notarization_finalization_manipulated() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let mut network = Bls12381::new(&mut rng);

        // Create batch proofs
        let mut batch_proofs = Vec::new();
        for i in 0..10 {
            let sequencer = Ed25519::new(&mut rng).public_key();
            let batch_digest = test_digest(i);
            let batch_payload = Proof::payload(&sequencer, 0, &batch_digest, 0);
            let batch_signature = network.sign(Some(BATCH_NAMESPACE), &batch_payload);
            let batch_proof = Proof::new(sequencer, 0, batch_digest, 0, batch_signature);
            batch_proofs.push(batch_proof);
        }

        // Create block
        let parent_digest = hash(&[0; 32]);
        let height = 0;
        let block = Block::new(parent_digest, height, batch_proofs.clone());

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
        let result = Notarization::deserialize(&network.public_key(), &serialized);
        assert!(result.is_none());

        // Create finalization
        let finalize_payload = Finalization::payload(view, parent_view, &block.digest());
        let finalize_signature = network.sign(Some(FINALIZE_NAMESPACE), &finalize_payload);

        // Create incorrect finalization proof
        let finalization =
            Finalization::new(view + 1, parent_view, block.digest(), finalize_signature);

        // Check finalization serialization
        let serialized = finalization.serialize();
        let result = Finalization::deserialize(&network.public_key(), &serialized);
        assert!(result.is_none());
    }
}
