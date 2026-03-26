use crate::consensus::{Context, Finalization, Notarization, Scheme};
use bytes::{Buf, BufMut};
use commonware_codec::{varint::UInt, Encode, EncodeSize, Error, Read, ReadExt, Write};
use commonware_consensus::{types::Height, CertifiableBlock, Heightable};
use commonware_cryptography::{sha256::Digest, Digestible, Hasher, Sha256};
use commonware_parallel::Strategy;
use commonware_storage::mmr::Location;
use commonware_utils::range::NonEmptyRange;
use rand::rngs::OsRng;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block {
    /// The consensus context when this block was proposed.
    pub context: Context,

    /// The parent block's digest.
    pub parent: Digest,

    /// The height of the block in the blockchain.
    pub height: Height,

    /// The timestamp of the block (in milliseconds since the Unix epoch).
    pub timestamp: u64,

    /// The state root from merkleizing the database after executing this block.
    pub state_root: Digest,

    /// The database range (inactivity floor .. size) for QMDB sync targeting.
    pub range: NonEmptyRange<Location>,

    /// Key indices in `[0, 2^16)` that this block writes to.
    pub slots: Vec<u16>,

    /// Pre-computed digest of the block.
    digest: Digest,
}

impl Block {
    fn compute_digest(
        context: &Context,
        parent: &Digest,
        height: Height,
        timestamp: u64,
        state_root: &Digest,
        range: &NonEmptyRange<Location>,
        slots: &[u16],
    ) -> Digest {
        let mut hasher = Sha256::new();
        hasher.update(&context.encode());
        hasher.update(parent);
        hasher.update(&height.get().to_be_bytes());
        hasher.update(&timestamp.to_be_bytes());
        hasher.update(state_root);
        hasher.update(&range.start().as_u64().to_be_bytes());
        hasher.update(&range.end().as_u64().to_be_bytes());
        for &s in slots {
            hasher.update(&s.to_be_bytes());
        }
        hasher.finalize()
    }

    pub fn new(
        context: Context,
        parent: Digest,
        height: Height,
        timestamp: u64,
        state_root: Digest,
        range: NonEmptyRange<Location>,
        slots: Vec<u16>,
    ) -> Self {
        let digest = Self::compute_digest(
            &context,
            &parent,
            height,
            timestamp,
            &state_root,
            &range,
            &slots,
        );
        Self {
            context,
            parent,
            height,
            timestamp,
            state_root,
            range,
            slots,
            digest,
        }
    }
}

impl Write for Block {
    fn write(&self, writer: &mut impl BufMut) {
        self.context.write(writer);
        self.parent.write(writer);
        self.height.write(writer);
        UInt(self.timestamp).write(writer);
        self.state_root.write(writer);
        self.range.write(writer);
        self.slots.write(writer);
    }
}

impl Read for Block {
    type Cfg = ();

    fn read_cfg(reader: &mut impl Buf, _: &Self::Cfg) -> Result<Self, Error> {
        let context = Context::read(reader)?;
        let parent = Digest::read(reader)?;
        let height = Height::read(reader)?;
        let timestamp = UInt::read(reader)?.0;
        let state_root = Digest::read(reader)?;
        let range = NonEmptyRange::<Location>::read(reader)?;
        let slots = Vec::<u16>::read_cfg(reader, &((0..=(1 << 14)).into(), ()))?;

        let digest = Self::compute_digest(
            &context,
            &parent,
            height,
            timestamp,
            &state_root,
            &range,
            &slots,
        );
        Ok(Self {
            context,
            parent,
            height,
            timestamp,
            state_root,
            range,
            slots,
            digest,
        })
    }
}

impl EncodeSize for Block {
    fn encode_size(&self) -> usize {
        self.context.encode_size()
            + self.parent.encode_size()
            + self.height.encode_size()
            + UInt(self.timestamp).encode_size()
            + self.state_root.encode_size()
            + self.range.encode_size()
            + self.slots.encode_size()
    }
}

impl Digestible for Block {
    type Digest = Digest;

    fn digest(&self) -> Digest {
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

impl Heightable for Block {
    fn height(&self) -> Height {
        self.height
    }
}

impl CertifiableBlock for Block {
    type Context = Context;

    fn context(&self) -> Self::Context {
        self.context.clone()
    }
}
