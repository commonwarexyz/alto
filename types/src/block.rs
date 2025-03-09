use crate::broadcast::Proof;
use bytes::{Buf, BufMut};
use commonware_cryptography::bls12381;
use commonware_cryptography::{sha256::Digest, Hasher, Sha256};
use commonware_storage::bmt::Builder;
use commonware_utils::{Array, SizedSerialize};

const BLOCK_HEADER_LEN: usize = Digest::SERIALIZED_LEN + u64::SERIALIZED_LEN + u16::SERIALIZED_LEN;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block {
    /// The parent block's digest.
    pub parent: Digest,

    /// The height of the block in the blockchain.
    pub height: u64,

    /// The batches of transactions referenced by the block.
    ///
    /// Max batches is u16::MAX.
    ///
    /// TODO: must only move forward for a given sequencer and should
    /// include one proof for each sequencer.
    pub batches: Vec<Proof>,

    /// Pre-computed digest of the block.
    digest: Digest,
}

impl Block {
    fn compute_digest(parent: &Digest, height: u64, batch_root: &Digest) -> Digest {
        let mut hasher = Sha256::new();
        hasher.update(parent);
        hasher.update(&height.to_be_bytes());
        hasher.update(batch_root);
        hasher.finalize()
    }

    fn get_root(batches: &[Proof]) -> Digest {
        let mut bmt = Builder::<Sha256>::new(batches.len());
        for batch in batches {
            bmt.add(&batch.digest());
        }
        bmt.build().root()
    }

    pub fn new(parent: Digest, height: u64, batches: Vec<Proof>) -> Self {
        // Generate batch root
        assert!(batches.len() <= u16::MAX as usize);
        let batch_root = Self::get_root(&batches);

        // Generate digest
        let digest = Self::compute_digest(&parent, height, &batch_root);
        Self {
            parent,
            height,
            batches,
            digest,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            Digest::SERIALIZED_LEN
                + u64::SERIALIZED_LEN
                + u16::SERIALIZED_LEN
                + self.batches.len() * Proof::SERIALIZED_LEN,
        );
        bytes.extend_from_slice(&self.parent);
        bytes.put_u64(self.height);
        bytes.put_u16(self.batches.len() as u16);
        for batch in &self.batches {
            bytes.extend_from_slice(&batch.serialize());
        }
        bytes
    }

    pub fn deserialize(public: &bls12381::PublicKey, mut bytes: &[u8]) -> Option<Self> {
        // Get header
        if bytes.len() < BLOCK_HEADER_LEN {
            return None;
        }
        let parent = Digest::read_from(&mut bytes).ok()?;
        let height = bytes.get_u64();
        let batch_len = bytes.get_u16() as usize;

        // Verify length of remaining is correct
        if bytes.remaining() != batch_len * Proof::SERIALIZED_LEN {
            return None;
        }

        // Read remaining transactions
        let mut batches = Vec::with_capacity(batch_len);
        for _ in 0..batch_len {
            let (batch, rest) = bytes.split_at(Proof::SERIALIZED_LEN);
            let batch = Proof::deserialize(public, batch)?;
            batches.push(batch);
            bytes = rest;
        }

        // Get root
        let batch_root = Self::get_root(&batches);

        // Return block
        let digest = Self::compute_digest(&parent, height, &batch_root);
        Some(Self {
            parent,
            height,
            batches,
            digest,
        })
    }

    pub fn digest(&self) -> Digest {
        self.digest.clone()
    }
}
