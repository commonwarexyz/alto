use bytes::{Buf, BufMut};
use commonware_codec::{Error, FixedSize, Read, ReadExt, Write};
use commonware_consensus::threshold_simplex::types::Notarization;
use commonware_cryptography::{bls12381::PublicKey, sha256::Digest, Digestible, Hasher, Sha256};

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
}

impl Write for Block {
    fn write(&self, writer: &mut impl BufMut) {
        self.parent.write(writer);
        self.height.write(writer);
        self.timestamp.write(writer);
    }
}

impl Read for Block {
    fn read_cfg(reader: &mut impl Buf, cfg: &()) -> Result<Self, Error> {
        let parent = Digest::read(reader)?;
        let height = u64::read(reader)?;
        let timestamp = u64::read(reader)?;

        // Return block
        let digest = Self::compute_digest(&parent, height, timestamp);
        Some(Self {
            parent,
            height,
            timestamp,
            digest,
        })
    }
}

impl FixedSize for Block {
    const SIZE: usize = Digest::SIZE + u64::SIZE + u64::SIZE;
}

impl Digestible<Digest> for Block {
    fn digest(&self) -> Digest {
        self.digest
    }
}

pub struct Notarized {
    pub proof: Notarization<Digest>,
    pub block: Block,
}

impl Notarized {
    pub fn new(proof: Notarization<Digest>, block: Block) -> Self {
        Self { proof, block }
    }
}

impl Write for Notarized {
    pub fn serialize(&self) -> Vec<u8> {
        let block = self.block.serialize();
        let mut bytes = Vec::with_capacity(Notarization::SERIALIZED_LEN + block.len());
        bytes.extend_from_slice(&self.proof.serialize());
        bytes.extend_from_slice(&block);
        bytes
    }
}

impl Read for Notarized {
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

impl FixedSize for Notarized {
    const SIZE: usize = Notarization::SIZE + Block::SIZE;
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
