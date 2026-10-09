//! Common types used throughout `alto`.

use commonware_codec::{Decode, Encode, Input};
use commonware_consensus::types::Epoch;
use commonware_cryptography::{Hasher, Sha256};
use commonware_utils::NZU64;
use std::num::NonZero;

mod block;
pub use block::{Block, Finalized, Notarized};

mod consensus;
pub use consensus::{
    Activity, ConsensusScheme, Context, Finalization, Identity, Notarization, PrivateKey,
    PublicKey, Scheme,
};

/// Browser bindings for the explorer, which verifies certificates against the network's
/// [Identity].
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

/// Largest participant set accepted by [decode_identity].
pub const MAX_PARTICIPANTS: usize = 10_000;

/// Decodes the network [Identity]: the participant set, which must be non-empty, sorted, unique,
/// and contain at most [MAX_PARTICIPANTS] keys.
pub fn decode_identity(bytes: impl Input) -> Result<Identity, commonware_codec::Error> {
    Identity::decode_cfg(bytes, &((1..=MAX_PARTICIPANTS).into(), ()))
}

/// Returns the name of the host that runs the validator identified by `public_key`.
///
/// Deployment instance names, configuration file names, and host lookups use this name, while
/// peer lists keep the full hex-encoded key. An FN-DSA-512 key encodes to 897 bytes, too long for
/// instance or file names, so the name is the hex encoding of the first 16 bytes of the SHA-256
/// digest of the encoded key.
pub fn host_name(public_key: &PublicKey) -> String {
    let digest = Sha256::hash(&[public_key.encode().as_ref()]);
    commonware_formatting::hex(&digest.as_ref()[..16])
}

/// Kind of certified artifact in an indexer stream frame, encoded as the frame's first byte.
#[repr(u8)]
pub enum Kind {
    Notarization = 1,
    Finalization = 2,
}

impl Kind {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Notarization),
            2 => Some(Self::Finalization),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use commonware_codec::{Copying, EncodeSize, Read};
    use commonware_consensus::{
        simplex::types::{Finalize, Notarize, Proposal},
        types::{Height, Round, View},
    };
    use commonware_cryptography::{sha256, Digest, Digestible, Signer};
    use commonware_parallel::Sequential;
    use commonware_utils::{
        non_empty,
        ordered::{BiMap, Set},
    };

    #[test]
    fn block_data_above_one_mib_round_trips_and_is_committed_by_digest() {
        let context = Context {
            round: Round::new(EPOCH, View::new(9)),
            leader: PrivateKey::from_seed(0).public_key(),
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

        // Validators reject payloads larger than the configured block size before caching them.
        // Smaller payloads (e.g. the empty genesis block) still decode.
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
        let mut reader = Copying(encoded.as_slice());
        assert_eq!(
            Block::read_cfg(&mut reader, &Block::unbounded_codec_config()).unwrap(),
            block
        );
        assert_eq!(reader.0, suffix);
    }

    /// Returns one signer per participant and the participant set, with each identity key
    /// doubling as its signing key.
    fn fixture(n: u64) -> (Vec<ConsensusScheme>, Identity) {
        let keys: Vec<PrivateKey> = (0..n).map(PrivateKey::from_seed).collect();
        let identity = Set::from_iter_dedup(keys.iter().map(|key| key.public_key()));
        let participants: Vec<_> = identity
            .iter()
            .map(|key| (key.clone(), key.clone()))
            .collect();
        let participants = BiMap::try_from(participants).unwrap();
        let schemes = keys
            .into_iter()
            .map(|key| ConsensusScheme::signer(NAMESPACE, participants.clone(), key).unwrap())
            .collect();
        (schemes, identity)
    }

    #[test]
    fn fn_dsa_certified_blocks_round_trip_and_verify_against_participants() {
        let (schemes, identity) = fixture(4);
        let verifier = ConsensusScheme::certificate_verifier(NAMESPACE, identity);
        let (_, other_identity) = fixture(5);
        let other_verifier = ConsensusScheme::certificate_verifier(NAMESPACE, other_identity);

        let context = Context {
            round: Round::new(EPOCH, View::new(9)),
            leader: PrivateKey::from_seed(0).public_key(),
            parent: (View::new(8), sha256::Digest::EMPTY),
        };
        let block = Block::new(
            context,
            Sha256::hash(&[b"hello world"]),
            Height::new(10),
            100,
            Bytes::from_static(b"random junk bytes"),
        );
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(9)),
            View::new(8),
            block.digest(),
        );

        // Notarization
        let notarizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Notarize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        let notarization = Notarization::<ConsensusScheme>::from_notarizes(
            &schemes[0],
            non_empty![@&notarizes],
            &Sequential,
        )
        .unwrap();
        let notarized = Notarized::new(notarization, block.clone());
        let decoded = Notarized::<ConsensusScheme>::decode_cfg(
            notarized.encode(),
            &Block::unbounded_codec_config(),
        )
        .unwrap();
        assert_eq!(decoded, notarized);
        assert!(decoded.verify(&verifier, &Sequential));
        assert!(!decoded.verify(&other_verifier, &Sequential));

        // Finalization
        let finalizes: Vec<_> = schemes
            .iter()
            .map(|scheme| Finalize::sign(scheme, proposal.clone()).unwrap())
            .collect();
        let finalization = Finalization::<ConsensusScheme>::from_finalizes(
            &schemes[0],
            non_empty![@&finalizes],
            &Sequential,
        )
        .unwrap();
        let finalized = Finalized::new(finalization, block);
        let decoded = Finalized::<ConsensusScheme>::decode_cfg(
            finalized.encode(),
            &Block::unbounded_codec_config(),
        )
        .unwrap();
        assert_eq!(decoded, finalized);
        assert!(decoded.verify(&verifier, &Sequential));
        assert!(!decoded.verify(&other_verifier, &Sequential));
    }

    #[test]
    fn host_names_are_short_and_distinct() {
        let first = host_name(&PrivateKey::from_seed(0).public_key());
        let second = host_name(&PrivateKey::from_seed(1).public_key());
        assert_eq!(first.len(), 32);
        assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(first, second);
    }

    #[test]
    fn identities_decode_only_bounded_sorted_participant_sets() {
        let (_, identity) = fixture(4);
        assert_eq!(decode_identity(identity.encode()).unwrap(), identity);

        // Empty, unsorted, and trailing-byte encodings are rejected.
        assert!(decode_identity(Identity::default().encode()).is_err());
        let mut keys: Vec<_> = identity.iter().cloned().collect();
        keys.reverse();
        assert!(decode_identity(keys.encode()).is_err());
        let mut trailing = identity.encode().to_vec();
        trailing.push(0);
        assert!(decode_identity(trailing).is_err());

        // A length prefix above the bound is rejected before reading any key.
        assert!(decode_identity((MAX_PARTICIPANTS + 1).encode()).is_err());
    }

    /// The explorer's WASM check (`explorer/scripts/check-wasm.mjs`) verifies these
    /// artifacts. Run with `ALTO_UPDATE_FIXTURES=1` to rewrite the fixture after a deliberate
    /// format change.
    #[test]
    fn explorer_fn_dsa_fixture_is_current() {
        use commonware_formatting::hex;

        let (schemes, identity) = fixture(4);
        let context = Context {
            round: Round::new(EPOCH, View::new(9)),
            leader: identity[1].clone(),
            parent: (View::new(8), Sha256::hash(&[b"parent"])),
        };
        let block = Block::new(
            context,
            Sha256::hash(&[b"parent"]),
            Height::new(10),
            1_786_513_323_130,
            Bytes::from_static(b"explorer fixture"),
        );
        let proposal = Proposal::new(
            Round::new(EPOCH, View::new(9)),
            View::new(8),
            block.digest(),
        );

        // Certificates from a quorum that skips the second participant.
        let quorum = [&schemes[0], &schemes[2], &schemes[3]];
        let notarizes: Vec<_> = quorum
            .iter()
            .map(|scheme| Notarize::sign(*scheme, proposal.clone()).unwrap())
            .collect();
        let notarization = Notarization::<ConsensusScheme>::from_notarizes(
            &schemes[0],
            non_empty![@&notarizes],
            &Sequential,
        )
        .unwrap();
        let finalizes: Vec<_> = quorum
            .iter()
            .map(|scheme| Finalize::sign(*scheme, proposal.clone()).unwrap())
            .collect();
        let finalization = Finalization::<ConsensusScheme>::from_finalizes(
            &schemes[0],
            non_empty![@&finalizes],
            &Sequential,
        )
        .unwrap();

        // Offset of the first signature: proposal, signer bitmap, then the signature count.
        let signature_offset = proposal.encode_size()
            + notarization.certificate.signers.encode_size()
            + notarization.certificate.signatures.len().encode_size();
        // Both certificates have the same signers, so their blocks start at the same offset.
        let block_offset = notarization.encode_size();
        assert_eq!(finalization.encode_size(), block_offset);
        let artifact = |name: &str, encoded: Vec<u8>, certificate: Vec<u8>| {
            format!(
                "  \"{name}\": {{\n    \"bytes\": \"{}\",\n    \"view\": 9,\n    \"signature\": \"{}\",\n    \"block\": {{\n      \"leader\": \"{}\",\n      \"parent\": \"{}\",\n      \"height\": 10,\n      \"timestamp\": {},\n      \"digest\": \"{}\"\n    }}\n  }}",
                hex(&encoded),
                hex(&Sha256::hash(&[&certificate])),
                hex(&block.context.leader.encode()),
                hex(&block.parent),
                block.timestamp,
                hex(&block.digest()),
            )
        };
        let fixture = format!(
            "{{\n  \"identity\": \"{}\",\n  \"signatureOffset\": {signature_offset},\n  \"blockOffset\": {block_offset},\n{},\n{}\n}}\n",
            hex(&identity.encode()),
            artifact(
                "notarization",
                Notarized::new(notarization.clone(), block.clone()).encode().to_vec(),
                notarization.certificate.encode().to_vec(),
            ),
            artifact(
                "finalization",
                Finalized::new(finalization.clone(), block.clone()).encode().to_vec(),
                finalization.certificate.encode().to_vec(),
            ),
        );

        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../explorer/scripts/fn_dsa_fixture.json");
        if std::env::var_os("ALTO_UPDATE_FIXTURES").is_some() {
            std::fs::write(&path, fixture).unwrap();
            return;
        }
        let current = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            current == fixture,
            "{} is stale; rerun with ALTO_UPDATE_FIXTURES=1",
            path.display()
        );
    }
}
