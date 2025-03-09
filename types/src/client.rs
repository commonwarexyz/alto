use crate::{Block, Finalization, Notarization};
use commonware_cryptography::bls12381;
use commonware_utils::{hex, SizedSerialize};

#[repr(u8)]
pub enum Kind {
    Seed = 0,
    Notarization = 1,
    Nullification = 2,
    Finalization = 3,
}

impl Kind {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Seed),
            1 => Some(Self::Notarization),
            2 => Some(Self::Nullification),
            3 => Some(Self::Finalization),
            _ => None,
        }
    }

    pub fn to_hex(&self) -> String {
        match self {
            Self::Seed => hex(&[0]),
            Self::Notarization => hex(&[1]),
            Self::Nullification => hex(&[2]),
            Self::Finalization => hex(&[3]),
        }
    }
}

pub struct NotarizationPayload {
    pub proof: Notarization,
    pub block: Block,
}

impl NotarizationPayload {
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

    pub fn deserialize(public: Option<&bls12381::PublicKey>, bytes: &[u8]) -> Option<Self> {
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

pub struct FinalizationPayload {
    pub proof: Finalization,
    pub block: Block,
}

impl FinalizationPayload {
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

    pub fn deserialize(public: Option<&bls12381::PublicKey>, bytes: &[u8]) -> Option<Self> {
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
