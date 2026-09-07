use crate::consensus::{Context, Finalization, Notarization, Scheme};
use bytes::{Buf, BufMut, Bytes};
use commonware_codec::{
    varint::UInt, BufsMut, Encode, EncodeSize, Error, RangeCfg, Read, ReadExt, Write,
};
use commonware_consensus::{types::Height, CertifiableBlock, Heightable};
use commonware_cryptography::{sha256::Digest, Digestible, Hasher, Sha256};
use commonware_parallel::Strategy;
use commonware_utils::sys_rng;

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

    /// Opaque data appended to the encoded block.
    pub data: Bytes,

    /// Pre-computed digest of the block.
    digest: Digest,
}

impl Block {
    fn compute_digest(
        context: &Context,
        parent: &Digest,
        height: Height,
        timestamp: u64,
        data: &[u8],
    ) -> Digest {
        let mut hasher = Sha256::default();
        hasher
            .update(&context.encode())
            .update(parent)
            .update(&height.get().to_be_bytes())
            .update(&timestamp.to_be_bytes())
            .update(data);
        let (_, digest) = hasher.finalize();
        digest
    }

    pub fn new(
        context: Context,
        parent: Digest,
        height: Height,
        timestamp: u64,
        data: Bytes,
    ) -> Self {
        assert!(
            u32::try_from(data.len()).is_ok(),
            "block data exceeds codec maximum"
        );
        let digest = Self::compute_digest(&context, &parent, height, timestamp, &data);
        Self {
            context,
            parent,
            height,
            timestamp,
            data,
            digest,
        }
    }

    /// Codec configuration that rejects blocks whose payload exceeds `block_size` bytes.
    ///
    /// Validators decode every block they receive with this configuration so an oversized payload
    /// is rejected before it is cached or verified. Smaller payloads (such as the empty genesis
    /// block) still decode. The exact-size consensus rule is enforced when a block is verified.
    pub fn codec_config(block_size: u32) -> RangeCfg<usize> {
        let block_size =
            usize::try_from(block_size).expect("block size is unsupported on this platform");
        RangeCfg::from(..=block_size)
    }

    /// Codec configuration that accepts any payload size.
    ///
    /// For consumers that do not know the network's block size and instead trust the certificate
    /// that accompanies a block.
    pub fn unbounded_codec_config() -> RangeCfg<usize> {
        RangeCfg::from(..)
    }
}

impl Write for Block {
    fn write(&self, writer: &mut impl BufMut) {
        self.context.write(writer);
        self.parent.write(writer);
        self.height.write(writer);
        UInt(self.timestamp).write(writer);
        self.data.write(writer);
    }

    fn write_bufs(&self, writer: &mut impl BufsMut) {
        self.context.write_bufs(writer);
        self.parent.write_bufs(writer);
        self.height.write_bufs(writer);
        UInt(self.timestamp).write_bufs(writer);
        self.data.write_bufs(writer);
    }
}

impl Read for Block {
    /// Accepted payload sizes (see [Block::codec_config]).
    type Cfg = RangeCfg<usize>;

    fn read_cfg(reader: &mut impl Buf, cfg: &Self::Cfg) -> Result<Self, Error> {
        let context = Context::read(reader)?;
        let parent = Digest::read(reader)?;
        let height = Height::read(reader)?;
        let timestamp = UInt::read(reader)?.0;
        let data = Bytes::read_cfg(reader, cfg)?;

        let digest = Self::compute_digest(&context, &parent, height, timestamp, &data);
        Ok(Self {
            context,
            parent,
            height,
            timestamp,
            data,
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
            + self.data.encode_size()
    }

    fn encode_inline_size(&self) -> usize {
        self.context.encode_inline_size()
            + self.parent.encode_inline_size()
            + self.height.encode_inline_size()
            + UInt(self.timestamp).encode_inline_size()
            + self.data.encode_inline_size()
    }
}

impl Digestible for Block {
    type Digest = Digest;

    fn digest(&self) -> Digest {
        self.digest
    }
}

#[derive(Clone, Debug)]
pub struct Notarized<S: Scheme> {
    pub proof: Notarization<S>,
    pub block: Block,
}

impl<S: Scheme> PartialEq for Notarized<S> {
    fn eq(&self, other: &Self) -> bool {
        self.proof == other.proof && self.block == other.block
    }
}

impl<S: Scheme> Eq for Notarized<S> {}

impl<S: Scheme> Notarized<S> {
    pub fn new(proof: Notarization<S>, block: Block) -> Self {
        Self { proof, block }
    }

    pub fn verify(&self, scheme: &S, strategy: &impl Strategy) -> bool {
        self.proof.verify(&mut sys_rng(), scheme, strategy)
    }
}

impl<S: Scheme> Write for Notarized<S> {
    fn write(&self, buf: &mut impl BufMut) {
        self.proof.write(buf);
        self.block.write(buf);
    }

    fn write_bufs(&self, buf: &mut impl BufsMut) {
        self.proof.write_bufs(buf);
        self.block.write_bufs(buf);
    }
}

impl<S: Scheme> Read for Notarized<S> {
    /// Accepted block payload sizes (see [Block::codec_config]).
    type Cfg = RangeCfg<usize>;

    fn read_cfg(buf: &mut impl Buf, cfg: &Self::Cfg) -> Result<Self, Error> {
        let proof = Notarization::<S>::read(buf)?;
        let block = Block::read_cfg(buf, cfg)?;

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

impl<S: Scheme> EncodeSize for Notarized<S> {
    fn encode_size(&self) -> usize {
        self.proof.encode_size() + self.block.encode_size()
    }

    fn encode_inline_size(&self) -> usize {
        self.proof.encode_inline_size() + self.block.encode_inline_size()
    }
}

#[derive(Clone, Debug)]
pub struct Finalized<S: Scheme> {
    pub proof: Finalization<S>,
    pub block: Block,
}

impl<S: Scheme> PartialEq for Finalized<S> {
    fn eq(&self, other: &Self) -> bool {
        self.proof == other.proof && self.block == other.block
    }
}

impl<S: Scheme> Eq for Finalized<S> {}

impl<S: Scheme> Finalized<S> {
    pub fn new(proof: Finalization<S>, block: Block) -> Self {
        Self { proof, block }
    }

    pub fn verify(&self, scheme: &S, strategy: &impl Strategy) -> bool {
        self.proof.verify(&mut sys_rng(), scheme, strategy)
    }
}

impl<S: Scheme> Write for Finalized<S> {
    fn write(&self, buf: &mut impl BufMut) {
        self.proof.write(buf);
        self.block.write(buf);
    }

    fn write_bufs(&self, buf: &mut impl BufsMut) {
        self.proof.write_bufs(buf);
        self.block.write_bufs(buf);
    }
}

impl<S: Scheme> Read for Finalized<S> {
    /// Accepted block payload sizes (see [Block::codec_config]).
    type Cfg = RangeCfg<usize>;

    fn read_cfg(buf: &mut impl Buf, cfg: &Self::Cfg) -> Result<Self, Error> {
        let proof = Finalization::<S>::read(buf)?;
        let block = Block::read_cfg(buf, cfg)?;

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

impl<S: Scheme> EncodeSize for Finalized<S> {
    fn encode_size(&self) -> usize {
        self.proof.encode_size() + self.block.encode_size()
    }

    fn encode_inline_size(&self) -> usize {
        self.proof.encode_inline_size() + self.block.encode_inline_size()
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
