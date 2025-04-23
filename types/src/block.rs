use bytes::{Buf, BufMut};
use commonware_codec::{Error, FixedSize, Read, ReadExt, Write};
use commonware_consensus::threshold_simplex::types::{Finalization, Notarization};
use commonware_cryptography::{
    bls12381::primitives::group::Public, sha256::Digest, Digestible, Hasher, Sha256,
};

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
    fn read_cfg(reader: &mut impl Buf, _: &()) -> Result<Self, Error> {
        let parent = Digest::read(reader)?;
        let height = u64::read(reader)?;
        let timestamp = u64::read(reader)?;

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

impl FixedSize for Block {
    const SIZE: usize = Digest::SIZE + u64::SIZE + u64::SIZE;
}

impl Digestible<Digest> for Block {
    fn digest(&self) -> Digest {
        self.digest
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notarized {
    pub proof: Notarization<Digest>,
    pub block: Block,
}

impl Notarized {
    pub fn new(proof: Notarization<Digest>, block: Block) -> Self {
        Self { proof, block }
    }

    pub fn verify(&self, namespace: &[u8], public_key: &Public) -> bool {
        self.proof.verify(namespace, public_key)
    }
}

impl Write for Notarized {
    fn write(&self, buf: &mut impl BufMut) {
        self.proof.write(buf);
        self.block.write(buf);
    }
}

impl Read for Notarized {
    fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, Error> {
        let proof = Notarization::<Digest>::read(buf)?;
        let block = Block::read(buf)?;

        // Ensure the proof is for the block
        if proof.proposal.payload != block.digest() {
            return Err(Error::Invalid(
                "types::Notarized",
                "Proof payload does not match block digest",
            ));
        }
        Ok(Self { proof, block })
    }
}

impl FixedSize for Notarized {
    const SIZE: usize = Notarization::<Digest>::SIZE + Block::SIZE;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finalized {
    pub proof: Finalization<Digest>,
    pub block: Block,
}

impl Finalized {
    pub fn new(proof: Finalization<Digest>, block: Block) -> Self {
        Self { proof, block }
    }

    pub fn verify(&self, namespace: &[u8], public_key: &Public) -> bool {
        self.proof.verify(namespace, public_key)
    }
}

impl Write for Finalized {
    fn write(&self, buf: &mut impl BufMut) {
        self.proof.write(buf);
        self.block.write(buf);
    }
}

impl Read for Finalized {
    fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, Error> {
        let proof = Finalization::<Digest>::read(buf)?;
        let block = Block::read(buf)?;

        // Ensure the proof is for the block
        if proof.proposal.payload != block.digest() {
            return Err(Error::Invalid(
                "types::Finalized",
                "Proof payload does not match block digest",
            ));
        }
        Ok(Self { proof, block })
    }
}

impl FixedSize for Finalized {
    const SIZE: usize = Finalization::<Digest>::SIZE + Block::SIZE;
}
