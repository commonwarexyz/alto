//! Common types used throughout `alto`.

use commonware_consensus::types::Epoch;
use commonware_formatting::hex;
use commonware_utils::NZU64;
use std::num::NonZero;

mod block;
pub use block::{Block, Finalized, Notarized};

mod consensus;
pub use consensus::{
    Activity, CertificateMode, Context, Finalization, Identity, Notarization, PublicKey,
    RotatingElector, Scheme, Seed, Seedable, Signature, StandardScheme, VrfScheme,
    ROTATING_ELECTOR,
};

pub mod wasm;

/// The unique namespace prefix used in all signing operations to prevent signature replay attacks.
pub const NAMESPACE: &[u8] = b"_ALTO";

/// The epoch number used in [commonware_consensus::simplex].
///
/// Because alto does not implement reconfiguration (validator set changes and resharing), we hardcode the epoch to 0.
///
/// For an example of how to implement reconfiguration and resharing, see [commonware-reshare](https://github.com/commonwarexyz/monorepo/tree/main/examples/reshare).
pub const EPOCH: Epoch = Epoch::zero();

/// The epoch length used in [commonware_consensus::simplex].
///
/// Because alto does not implement reconfiguration (validator set changes and resharing), we hardcode the epoch length to u64::MAX (to
/// stay in the first epoch forever).
///
/// For an example of how to implement reconfiguration and resharing, see [commonware-reshare](https://github.com/commonwarexyz/monorepo/tree/main/examples/reshare).
pub const EPOCH_LENGTH: NonZero<u64> = NZU64!(u64::MAX);

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
    use bytes::Bytes;
    use commonware_codec::{Decode, Encode, EncodeSize, Read};
    use commonware_consensus::{
        simplex::{
            scheme::bls12381_threshold::vrf as bls12381_threshold,
            types::{Finalization, Finalize, Notarization, Notarize, Proposal},
        },
        types::{Height, Round, View},
    };
    use commonware_cryptography::{
        bls12381::primitives::variant::MinSig, certificate::mocks::Fixture, ed25519, sha256,
        Digest, Digestible, Hasher, Sha256, Signer,
    };
    use commonware_parallel::Sequential;
    use commonware_utils::non_empty;
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn block_data_above_one_mib_round_trips_and_is_committed_by_digest() {
        let context = Context {
            round: Round::new(EPOCH, View::new(9)),
            leader: ed25519::PrivateKey::from_seed(0).public_key(),
            parent: (View::new(8), sha256::Digest::EMPTY),
        };
        let parent = Sha256::hash(&[b"parent"]);
        let empty = Block::new(context.clone(), parent, Height::new(10), 100, Bytes::new());
        let data = Bytes::from(vec![0xa5; 1024 * 1024 + 1]);
        let block = Block::new(context, parent, Height::new(10), 100, data.clone());

        assert_eq!(
            block.encode_size(),
            empty.encode_size() - Bytes::new().encode_size() + data.encode_size()
        );
        assert_eq!(block.encode_inline_size(), block.encode_size() - data.len());
        assert_ne!(block.digest(), empty.digest());
        assert_eq!(
            Block::decode_cfg(block.encode(), &Block::unbounded_codec_config()).unwrap(),
            block
        );
        assert_eq!(
            Block::decode_cfg(block.encode(), &Block::codec_config(1024 * 1024 + 1)).unwrap(),
            block
        );
        // Validators reject payloads larger than the configured block size before caching them
        // Smaller payloads (e.g. the empty genesis block) still decode
        assert!(Block::decode_cfg(block.encode(), &Block::codec_config(1024 * 1024)).is_err());
        assert_eq!(
            Block::decode_cfg(empty.encode(), &Block::codec_config(1024)).unwrap(),
            empty
        );
        assert_eq!(
            Block::decode_cfg(empty.encode(), &Block::codec_config(0)).unwrap(),
            empty
        );

        let mut encoded = block.encode().to_vec();
        let suffix = [1, 2, 3, 4];
        encoded.extend_from_slice(&suffix);
        let mut reader = encoded.as_slice();
        assert_eq!(
            Block::read_cfg(&mut reader, &Block::unbounded_codec_config()).unwrap(),
            block
        );
        assert_eq!(reader, suffix);
    }

    #[test]
    fn test_notarized() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let n = 4;
        let Fixture { schemes, .. } =
            bls12381_threshold::fixture::<MinSig, _>(&mut rng, NAMESPACE, n);
        let schemes = schemes;

        // Create a block with context
        let context = Context {
            round: Round::new(EPOCH, View::new(9)),
            leader: ed25519::PrivateKey::from_seed(0).public_key(),
            parent: (View::new(8), sha256::Digest::EMPTY),
        };
        let digest = Sha256::hash(&[b"hello world"]);
        let block = Block::new(
            context,
            digest,
            Height::new(10),
            100,
            Bytes::from_static(b"random junk bytes"),
        );
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(9)),
            View::new(8),
            block.digest(),
        );

        // Create a notarization
        let notarizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        let notarization =
            Notarization::from_notarizes(&schemes[0], non_empty![@&notarizes], &Sequential)
                .unwrap();
        let notarized = Notarized::new(notarization, block.clone());

        // Serialize and deserialize
        let encoded = notarized.encode();
        let decoded = Notarized::decode_cfg(encoded, &Block::unbounded_codec_config())
            .expect("failed to decode notarized");
        assert_eq!(notarized, decoded);

        // Verify notarized
        assert!(notarized.verify(&schemes[0], &Sequential));
    }

    #[test]
    fn test_finalized() {
        // Create network key
        let mut rng = StdRng::seed_from_u64(0);
        let n = 4;
        let Fixture { schemes, .. } =
            bls12381_threshold::fixture::<MinSig, _>(&mut rng, NAMESPACE, n);
        let schemes = schemes;

        // Create a block with context
        let context = Context {
            round: Round::new(EPOCH, View::new(9)),
            leader: ed25519::PrivateKey::from_seed(0).public_key(),
            parent: (View::new(8), sha256::Digest::EMPTY),
        };
        let digest = Sha256::hash(&[b"hello world"]);
        let block = Block::new(context, digest, Height::new(10), 100, Bytes::new());
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(9)),
            View::new(8),
            block.digest(),
        );

        // Create a finalization
        let finalizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Finalize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        let finalization =
            Finalization::from_finalizes(&schemes[0], non_empty![@&finalizes], &Sequential)
                .unwrap();
        let finalized = Finalized::new(finalization, block.clone());

        // Serialize and deserialize
        let encoded = finalized.encode();
        let decoded = Finalized::decode_cfg(encoded, &Block::unbounded_codec_config())
            .expect("failed to decode finalized");
        assert_eq!(finalized, decoded);

        // Verify finalized
        assert!(finalized.verify(&schemes[0], &Sequential));
    }
}
