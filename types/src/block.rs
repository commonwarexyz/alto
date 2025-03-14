use crate::{Finalization, Notarization};
use bytes::{Buf, BufMut};
use commonware_cryptography::{bls12381::PublicKey, sha256::Digest, Hasher, Sha256};
use commonware_utils::{Array, SizedSerialize};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block {
    /// The parent block's digest.
    pub parent: Digest,

    /// The height of the block in the blockchain.
    pub height: u64,

    /// The timestamp of the block (in milliseconds since the Unix epoch).
    pub timestamp: u64,

    /// Pre-computed digest of the block.
    digest: Digest,
}

impl Block {
    fn compute_digest(parent: &Digest, height: u64, timestamp: u64) -> Digest {
        let mut hasher = Sha256::new();
        hasher.update(parent);
        hasher.update(&height.to_be_bytes());
        hasher.update(&timestamp.to_be_bytes());
        hasher.finalize()
    }

    pub fn new(parent: Digest, height: u64, timestamp: u64) -> Self {
        let digest = Self::compute_digest(&parent, height, timestamp);
        Self {
            parent,
            height,
            timestamp,
            digest,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::SERIALIZED_LEN);
        bytes.extend_from_slice(&self.parent);
        bytes.put_u64(self.height);
        bytes.put_u64(self.timestamp);
        bytes
    }

    pub fn deserialize(mut bytes: &[u8]) -> Option<Self> {
        // Parse the block
        if bytes.len() != Self::SERIALIZED_LEN {
            return None;
        }
        let parent = Digest::read_from(&mut bytes).ok()?;
        let height = bytes.get_u64();
        let timestamp = bytes.get_u64();

        // Return block
        let digest = Self::compute_digest(&parent, height, timestamp);
        Some(Self {
            parent,
            height,
            timestamp,
            digest,
        })
    }

    pub fn digest(&self) -> Digest {
        self.digest
    }
}

impl SizedSerialize for Block {
    const SERIALIZED_LEN: usize =
        Digest::SERIALIZED_LEN + u64::SERIALIZED_LEN + u64::SERIALIZED_LEN;
}

pub struct Notarized {
    pub proof: Notarization,
    pub block: Block,
}

impl Notarized {
    pub fn new(proof: Notarization, block: Block) -> Self {
        Self { proof, block }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let block = self.block.serialize();
        let mut bytes = Vec::with_capacity(Notarization::SERIALIZED_LEN + block.len());
        bytes.extend_from_slice(&self.proof.serialize());
        bytes.extend_from_slice(&block);
        bytes
    }

    pub fn deserialize(public: Option<&PublicKey>, bytes: &[u8]) -> Option<Self> {
        // Deserialize the proof and block
        let (proof, block) = bytes.split_at_checked(Notarization::SERIALIZED_LEN)?;
        let proof = Notarization::deserialize(public, proof)?;
        let block = Block::deserialize(block)?;

        // Ensure the proof is for the block
        if proof.payload != block.digest() {
            return None;
        }
        Some(Self { proof, block })
    }
}

pub struct Finalized {
    pub proof: Finalization,
    pub block: Block,
}

impl Finalized {
    pub fn new(proof: Finalization, block: Block) -> Self {
        Self { proof, block }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let block = self.block.serialize();
        let mut bytes = Vec::with_capacity(Finalization::SERIALIZED_LEN + block.len());
        bytes.extend_from_slice(&self.proof.serialize());
        bytes.extend_from_slice(&block);
        bytes
    }

    pub fn deserialize(public: Option<&PublicKey>, bytes: &[u8]) -> Option<Self> {
        // Deserialize the proof and block
        let (proof, block) = bytes.split_at_checked(Finalization::SERIALIZED_LEN)?;
        let proof = Finalization::deserialize(public, proof)?;
        let block = Block::deserialize(block)?;

        // Ensure the proof is for the block
        if proof.payload != block.digest() {
            return None;
        }
        Some(Self { proof, block })
    }
}
