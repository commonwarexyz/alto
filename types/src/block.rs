use crate::consensus::{Finalization, Notarization, PublicKey, Scheme};
use crate::EPOCH;
use bytes::{Buf, BufMut};
use commonware_codec::{varint::UInt, DecodeExt, EncodeSize, Error, Read, ReadExt, Write};
use commonware_consensus::{
    simplex::types::Context,
    types::{Height, Round, View},
    Heightable,
};
use commonware_cryptography::Signer as _;
use commonware_cryptography::{sha256::Digest, Committable, Digestible, Hasher, Sha256};
use commonware_parallel::Strategy;
use rand::rngs::OsRng;

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

    /// Consensus context associated with this block.
    pub context: Context<Digest, PublicKey>,
}

impl Block {
    fn compute_digest(parent: &Digest, height: Height, timestamp: u64) -> Digest {
        let mut hasher = Sha256::new();
        hasher.update(parent);
        hasher.update(&height.get().to_be_bytes());
        hasher.update(&timestamp.to_be_bytes());
        hasher.finalize()
    }

    fn default_context(parent: Digest, height: Height) -> Context<Digest, PublicKey> {
        let signer = commonware_cryptography::ed25519::PrivateKey::decode([7u8; 32].as_ref())
            .expect("static signer seed must decode");
        Context {
            round: Round::new(EPOCH, View::new(height.get().saturating_sub(1))),
            leader: signer.public_key(),
            parent: (View::new(height.get().saturating_sub(1)), parent),
        }
    }

    pub fn new(parent: Digest, height: Height, timestamp: u64) -> Self {
        let context = Self::default_context(parent, height);
        Self::new_with_context(parent, height, timestamp, context)
    }

    pub fn new_with_context(
        parent: Digest,
        height: Height,
        timestamp: u64,
        context: Context<Digest, PublicKey>,
    ) -> Self {
        let digest = Self::compute_digest(&parent, height, timestamp);
        Self {
            parent,
            height,
            timestamp,
            digest,
            context,
        }
    }
}

impl Write for Block {
    fn write(&self, writer: &mut impl BufMut) {
        self.parent.write(writer);
        self.height.write(writer);
        UInt(self.timestamp).write(writer);
        self.context.write(writer);
    }
}

impl Read for Block {
    type Cfg = ();

    fn read_cfg(reader: &mut impl Buf, _: &Self::Cfg) -> Result<Self, Error> {
        let parent = Digest::read(reader)?;
        let height = Height::read(reader)?;
        let timestamp = UInt::read(reader)?.into();
        let context = Context::read(reader)?;

        // Pre-compute the digest
        let digest = Self::compute_digest(&parent, height, timestamp);
        Ok(Self {
            parent,
            height,
            timestamp,
            digest,
            context,
        })
    }
}

impl EncodeSize for Block {
    fn encode_size(&self) -> usize {
        self.parent.encode_size()
            + self.height.encode_size()
            + UInt(self.timestamp).encode_size()
            + self.context.encode_size()
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notarized {
    pub proof: Notarization,
    pub block: Block,
}

impl Notarized {
    pub fn new(proof: Notarization, block: Block) -> Self {
        Self { proof, block }
    }

    pub fn verify(&self, scheme: &Scheme, strategy: &impl Strategy) -> bool {
        self.proof.verify(&mut OsRng, scheme, strategy)
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
        let proof = Notarization::read(buf)?;
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

impl EncodeSize for Notarized {
    fn encode_size(&self) -> usize {
        self.proof.encode_size() + self.block.encode_size()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finalized {
    pub proof: Finalization,
    pub block: Block,
}

impl Finalized {
    pub fn new(proof: Finalization, block: Block) -> Self {
        Self { proof, block }
    }

    pub fn verify(&self, scheme: &Scheme, strategy: &impl Strategy) -> bool {
        self.proof.verify(&mut OsRng, scheme, strategy)
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
        let proof = Finalization::read(buf)?;
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

impl EncodeSize for Finalized {
    fn encode_size(&self) -> usize {
        self.proof.encode_size() + self.block.encode_size()
    }
}

impl commonware_consensus::Block for Block {
    fn parent(&self) -> Digest {
        self.parent
    }
}

impl commonware_consensus::CertifiableBlock for Block {
    type Context = Context<Digest, PublicKey>;

    fn context(&self) -> Self::Context {
        self.context.clone()
    }
}

impl Heightable for Block {
    fn height(&self) -> Height {
        self.height
    }
}
