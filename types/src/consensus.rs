use bytes::{Buf, BufMut};
use commonware_codec::{Error, FixedSize, Read, ReadExt, Write};
use commonware_consensus::threshold_simplex::types::{Finalization, Notarization};
use commonware_cryptography::{sha256::Digest, Digestible};
use commonware_utils::modulo;

use crate::Block;

/// The leader for a given seed is determined by the modulo of the seed with the number of participants.
pub fn leader_index(seed: &[u8], participants: usize) -> usize {
    modulo(seed, participants as u64) as usize
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

pub struct Finalized {
    pub proof: Finalization<Digest>,
    pub block: Block,
}

impl Finalized {
    pub fn new(proof: Finalization<Digest>, block: Block) -> Self {
        Self { proof, block }
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
