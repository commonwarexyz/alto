use crate::{Block, Commitment, PublicKey};
use commonware_coding::{Config, ReedSolomon};
use commonware_consensus::marshal::coding::{
    types::{CodedBlock as CommonwareCodedBlock, StoredCodedBlock as CommonwareStoredCodedBlock},
    Coding,
};
use commonware_cryptography::{sha256::Digest, Digest as _, Sha256};
use commonware_utils::NZU16;

/// Reed-Solomon scheme used to disseminate Alto blocks.
pub type CodingScheme = ReedSolomon<Sha256>;

/// An Alto block together with its erasure-coding commitment.
pub type CodedBlock = CommonwareCodedBlock<Block, CodingScheme, Sha256>;

/// Storage representation of an erasure-coded Alto block.
pub type StoredCodedBlock = CommonwareStoredCodedBlock<Block, CodingScheme, Sha256>;

/// Marshal variant used by Alto validators.
pub type MarshalCoding = Coding<Block, CodingScheme, Sha256, PublicKey>;

const GENESIS_PARENT_CODING_CONFIG: Config = Config {
    minimum_shards: NZU16!(1),
    extra_shards: NZU16!(1),
};

/// Returns the synthetic parent commitment embedded in the genesis context.
pub fn genesis_parent_commitment() -> Commitment {
    Commitment::from((
        Digest::EMPTY,
        Digest::EMPTY,
        Digest::EMPTY,
        GENESIS_PARENT_CODING_CONFIG,
    ))
}
