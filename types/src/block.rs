use bytes::{Buf, BufMut};
use commonware_codec::{varint::UInt, EncodeSize, Error, Read, ReadExt, Write};
use commonware_consensus::{
    threshold_simplex::types::{Finalization, Notarization},
    types::CodingCommitment,
};
use commonware_cryptography::{
    bls12381::primitives::variant::{MinSig, Variant},
    sha256::Digest,
    Committable, Digestible, Hasher, Sha256,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block {
    /// The parent block's [CodingCommitment].
    pub parent: CodingCommitment,

    /// The height of the block in the blockchain.
    pub height: u64,

    /// The timestamp of the block (in milliseconds since the Unix epoch).
    pub timestamp: u64,

    /// Pre-computed digest of the block.
    digest: Digest,
}

impl Block {
    fn compute_digest(parent: &CodingCommitment, height: u64, timestamp: u64) -> Digest {
        let mut hasher = Sha256::new();
        hasher.update(parent);
        hasher.update(&height.to_be_bytes());
        hasher.update(&timestamp.to_be_bytes());
        hasher.finalize()
    }

    pub fn new(parent: CodingCommitment, height: u64, timestamp: u64) -> Self {
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
        UInt(self.height).write(writer);
        UInt(self.timestamp).write(writer);
    }
}

impl Read for Block {
    type Cfg = ();

    fn read_cfg(reader: &mut impl Buf, _: &Self::Cfg) -> Result<Self, Error> {
        let parent = CodingCommitment::read(reader)?;
        let height = UInt::read(reader)?.into();
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
        self.parent.encode_size()
            + UInt(self.height).encode_size()
            + UInt(self.timestamp).encode_size()
    }
}

impl Digestible for Block {
    type Digest = Digest;

    fn digest(&self) -> Digest {
        self.digest
    }
}

impl Committable for Block {
    type Commitment = CodingCommitment;

    fn commitment(&self) -> Self::Commitment {
        Default::default()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notarized {
    pub proof: Notarization<MinSig, CodingCommitment>,
    pub block: Block,
}

impl Notarized {
    pub fn new(proof: Notarization<MinSig, CodingCommitment>, block: Block) -> Self {
        Self { proof, block }
    }

    pub fn verify(&self, namespace: &[u8], identity: &<MinSig as Variant>::Public) -> bool {
        self.proof.verify(namespace, identity)
    }
}

impl Write for Notarized {
    fn write(&self, buf: &mut impl BufMut) {
        self.proof.write(buf);
        self.block.write(buf);
    }
}

impl Read for Notarized {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &Self::Cfg) -> Result<Self, Error> {
        let proof = Notarization::<MinSig, CodingCommitment>::read(buf)?;
        let block = Block::read(buf)?;

        // Ensure the proof is for the block
        if proof.proposal.payload != block.commitment() {
            return Err(Error::Invalid(
                "types::Notarized",
                "Proof payload does not match block digest",
            ));
        }
        Ok(Self { proof, block })
    }
}

impl EncodeSize for Notarized {
    fn encode_size(&self) -> usize {
        self.proof.encode_size() + self.block.encode_size()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finalized {
    pub proof: Finalization<MinSig, CodingCommitment>,
    pub block: Block,
}

impl Finalized {
    pub fn new(proof: Finalization<MinSig, CodingCommitment>, block: Block) -> Self {
        Self { proof, block }
    }

    pub fn verify(&self, namespace: &[u8], identity: &<MinSig as Variant>::Public) -> bool {
        self.proof.verify(namespace, identity)
    }
}

impl Write for Finalized {
    fn write(&self, buf: &mut impl BufMut) {
        self.proof.write(buf);
        self.block.write(buf);
    }
}

impl Read for Finalized {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &Self::Cfg) -> Result<Self, Error> {
        let proof = Finalization::<MinSig, CodingCommitment>::read(buf)?;
        let block = Block::read(buf)?;

        // Ensure the proof is for the block
        if proof.proposal.payload != block.commitment() {
            return Err(Error::Invalid(
                "types::Finalized",
                "Proof payload does not match block digest",
            ));
        }
        Ok(Self { proof, block })
    }
}

impl EncodeSize for Finalized {
    fn encode_size(&self) -> usize {
        self.proof.encode_size() + self.block.encode_size()
    }
}

impl commonware_consensus::Block for Block {
    fn parent(&self) -> Self::Commitment {
        self.parent
    }

    fn height(&self) -> u64 {
        self.height
    }
}
