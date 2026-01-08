use crate::consensus::{FinalizationOf, NotarizationOf, SchemeOf};
use bytes::{Buf, BufMut};
use commonware_codec::{varint::UInt, EncodeSize, Error, Read, ReadExt, Write};
use commonware_consensus::{types::Height, Heightable};
use commonware_cryptography::{sha256::Digest, Committable, Digestible, Hasher, Sha256};
use commonware_parallel::{Sequential, Strategy};
use rand::rngs::OsRng;
use std::marker::PhantomData;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block {
    /// The parent block's digest.
    pub parent: Digest,

    /// The height of the block in the blockchain.
    pub height: Height,

    /// The timestamp of the block (in milliseconds since the Unix epoch).
    pub timestamp: u64,

    /// Pre-computed digest of the block.
    digest: Digest,
}

impl Block {
    fn compute_digest(parent: &Digest, height: Height, timestamp: u64) -> Digest {
        let mut hasher = Sha256::new();
        hasher.update(parent);
        hasher.update(&height.get().to_be_bytes());
        hasher.update(&timestamp.to_be_bytes());
        hasher.finalize()
    }

    pub fn new(parent: Digest, height: Height, timestamp: u64) -> Self {
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
        UInt(self.timestamp).write(writer);
    }
}

impl Read for Block {
    type Cfg = ();

    fn read_cfg(reader: &mut impl Buf, _: &Self::Cfg) -> Result<Self, Error> {
        let parent = Digest::read(reader)?;
        let height = Height::read(reader)?;
        let timestamp = UInt::read(reader)?.into();

        // Pre-compute the digest
        let digest = Self::compute_digest(&parent, height, timestamp);
        Ok(Self {
            parent,
            height,
            timestamp,

            digest,
        })
    }
}

impl EncodeSize for Block {
    fn encode_size(&self) -> usize {
        self.parent.encode_size() + self.height.encode_size() + UInt(self.timestamp).encode_size()
    }
}

impl Digestible for Block {
    type Digest = Digest;

    fn digest(&self) -> Digest {
        self.digest
    }
}

impl Committable for Block {
    type Commitment = Digest;

    fn commitment(&self) -> Digest {
        self.digest
    }
}

/// A notarized block, containing the notarization proof and the block.
#[derive(Clone, Debug)]
pub struct NotarizedOf<S: Strategy> {
    pub proof: NotarizationOf<S>,
    pub block: Block,
    _phantom: PhantomData<S>,
}

impl<S: Strategy> PartialEq for NotarizedOf<S> {
    fn eq(&self, other: &Self) -> bool {
        self.proof == other.proof && self.block == other.block
    }
}

impl<S: Strategy> Eq for NotarizedOf<S> {}

impl<S: Strategy> NotarizedOf<S> {
    pub fn new(proof: NotarizationOf<S>, block: Block) -> Self {
        Self {
            proof,
            block,
            _phantom: PhantomData,
        }
    }

    pub fn verify(&self, scheme: &SchemeOf<S>) -> bool {
        self.proof.verify(&mut OsRng, scheme)
    }
}

impl<S: Strategy> Write for NotarizedOf<S> {
    fn write(&self, buf: &mut impl BufMut) {
        self.proof.write(buf);
        self.block.write(buf);
    }
}

impl<S: Strategy> Read for NotarizedOf<S> {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &Self::Cfg) -> Result<Self, Error> {
        let proof = NotarizationOf::<S>::read(buf)?;
        let block = Block::read(buf)?;

        // Ensure the proof is for the block
        if proof.proposal.payload != block.digest() {
            return Err(Error::Invalid(
                "types::Notarized",
                "Proof payload does not match block digest",
            ));
        }
        Ok(Self {
            proof,
            block,
            _phantom: PhantomData,
        })
    }
}

impl<S: Strategy> EncodeSize for NotarizedOf<S> {
    fn encode_size(&self) -> usize {
        self.proof.encode_size() + self.block.encode_size()
    }
}

/// Default Notarized type using sequential execution.
pub type Notarized = NotarizedOf<Sequential>;

/// A finalized block, containing the finalization proof and the block.
#[derive(Clone, Debug)]
pub struct FinalizedOf<S: Strategy> {
    pub proof: FinalizationOf<S>,
    pub block: Block,
    _phantom: PhantomData<S>,
}

impl<S: Strategy> PartialEq for FinalizedOf<S> {
    fn eq(&self, other: &Self) -> bool {
        self.proof == other.proof && self.block == other.block
    }
}

impl<S: Strategy> Eq for FinalizedOf<S> {}

impl<S: Strategy> FinalizedOf<S> {
    pub fn new(proof: FinalizationOf<S>, block: Block) -> Self {
        Self {
            proof,
            block,
            _phantom: PhantomData,
        }
    }

    pub fn verify(&self, scheme: &SchemeOf<S>) -> bool {
        self.proof.verify(&mut OsRng, scheme)
    }
}

impl<S: Strategy> Write for FinalizedOf<S> {
    fn write(&self, buf: &mut impl BufMut) {
        self.proof.write(buf);
        self.block.write(buf);
    }
}

impl<S: Strategy> Read for FinalizedOf<S> {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &Self::Cfg) -> Result<Self, Error> {
        let proof = FinalizationOf::<S>::read(buf)?;
        let block = Block::read(buf)?;

        // Ensure the proof is for the block
        if proof.proposal.payload != block.digest() {
            return Err(Error::Invalid(
                "types::Finalized",
                "Proof payload does not match block digest",
            ));
        }
        Ok(Self {
            proof,
            block,
            _phantom: PhantomData,
        })
    }
}

impl<S: Strategy> EncodeSize for FinalizedOf<S> {
    fn encode_size(&self) -> usize {
        self.proof.encode_size() + self.block.encode_size()
    }
}

/// Default Finalized type using sequential execution.
pub type Finalized = FinalizedOf<Sequential>;

impl commonware_consensus::Block for Block {
    fn parent(&self) -> Digest {
        self.parent
    }
}

impl Heightable for Block {
    fn height(&self) -> Height {
        self.height
    }
}
