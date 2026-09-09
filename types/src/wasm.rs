use crate::{
    Block, Finalized, Identity, Notarized, Scheme, Seed, Signature, StandardScheme, VrfScheme,
    EPOCH, NAMESPACE, ROTATING_ELECTOR,
};
use commonware_codec::{Decode, DecodeExt, Encode};
use commonware_consensus::{
    types::{Round, View},
    Viewable,
};
use commonware_cryptography::{bls12381::primitives::variant::MinSig, Digestible};
use commonware_parallel::Sequential;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

#[derive(Deserialize, Serialize)]
pub struct SeedJs {
    pub view: u64,
    pub signature: Vec<u8>,
}

#[derive(Serialize)]
pub struct BlockJs {
    pub leader: Vec<u8>,
    pub parent: Vec<u8>,
    pub height: u64,
    pub timestamp: u64,
    pub digest: Vec<u8>,
}

impl From<&Block> for BlockJs {
    fn from(block: &Block) -> Self {
        Self {
            leader: block.context.leader.encode().to_vec(),
            parent: block.parent.to_vec(),
            height: block.height.get(),
            timestamp: block.timestamp,
            digest: block.digest().to_vec(),
        }
    }
}

#[derive(Serialize)]
pub struct CertifiedBlockJs {
    pub view: u64,
    pub signature: Vec<u8>,
    pub block: BlockJs,
}

#[wasm_bindgen]
pub fn parse_seed(identity: Vec<u8>, bytes: Vec<u8>) -> JsValue {
    let identity = Identity::decode(identity.as_ref()).expect("invalid identity");
    let verifier = VrfScheme::certificate_verifier(NAMESPACE, identity);

    let Ok(seed) = Seed::decode(bytes.as_ref()) else {
        return JsValue::NULL;
    };
    if !seed.verify(&verifier) {
        return JsValue::NULL;
    }
    let seed_js = SeedJs {
        view: seed.view().get(),
        signature: seed.signature.encode().to_vec(),
    };
    serde_wasm_bindgen::to_value(&seed_js).unwrap_or(JsValue::NULL)
}

#[wasm_bindgen]
pub fn parse_notarized(identity: Vec<u8>, bytes: Vec<u8>, standard: bool) -> JsValue {
    let identity = Identity::decode(identity.as_ref()).expect("invalid identity");
    if standard {
        parse_notarized_with::<StandardScheme>(identity, bytes)
    } else {
        parse_notarized_with::<VrfScheme>(identity, bytes)
    }
}

fn parse_notarized_with<S: Scheme>(identity: Identity, bytes: Vec<u8>) -> JsValue {
    let verifier = S::certificate_verifier(NAMESPACE, identity);

    let Ok(notarized) =
        Notarized::<S>::decode_cfg(bytes.as_ref(), &Block::unbounded_codec_config())
    else {
        return JsValue::NULL;
    };
    if !notarized.verify(&verifier, &Sequential) {
        return JsValue::NULL;
    }
    let Some(signature) = S::vote_signature(&notarized.proof.certificate) else {
        return JsValue::NULL;
    };
    let notarized_js = CertifiedBlockJs {
        view: notarized.proof.view().get(),
        signature: signature.encode().to_vec(),
        block: (&notarized.block).into(),
    };
    serde_wasm_bindgen::to_value(&notarized_js).unwrap_or(JsValue::NULL)
}

#[wasm_bindgen]
pub fn parse_finalized(identity: Vec<u8>, bytes: Vec<u8>, standard: bool) -> JsValue {
    let identity = Identity::decode(identity.as_ref()).expect("invalid identity");
    if standard {
        parse_finalized_with::<StandardScheme>(identity, bytes)
    } else {
        parse_finalized_with::<VrfScheme>(identity, bytes)
    }
}

fn parse_finalized_with<S: Scheme>(identity: Identity, bytes: Vec<u8>) -> JsValue {
    let verifier = S::certificate_verifier(NAMESPACE, identity);
    let Ok(finalized) =
        Finalized::<S>::decode_cfg(bytes.as_ref(), &Block::unbounded_codec_config())
    else {
        return JsValue::NULL;
    };
    if !finalized.verify(&verifier, &Sequential) {
        return JsValue::NULL;
    }
    let Some(signature) = S::vote_signature(&finalized.proof.certificate) else {
        return JsValue::NULL;
    };
    let finalized_js = CertifiedBlockJs {
        view: finalized.proof.view().get(),
        signature: signature.encode().to_vec(),
        block: (&finalized.block).into(),
    };
    serde_wasm_bindgen::to_value(&finalized_js).unwrap_or(JsValue::NULL)
}

#[wasm_bindgen]
pub fn parse_block(bytes: Vec<u8>) -> JsValue {
    let Ok(block) = Block::decode_cfg(bytes.as_ref(), &Block::unbounded_codec_config()) else {
        return JsValue::NULL;
    };
    let block_js = BlockJs::from(&block);
    serde_wasm_bindgen::to_value(&block_js).unwrap_or(JsValue::NULL)
}

/// Returns the index of the leader elected by `seed`, i.e. the leader of the view after the
/// seed's view.
#[wasm_bindgen]
pub fn leader_index(seed: JsValue, participants: usize) -> usize {
    let Ok(seed) = serde_wasm_bindgen::from_value::<SeedJs>(seed) else {
        return 0;
    };

    let Ok(signature) = Signature::decode(seed.signature.as_ref()) else {
        return 0;
    };

    // The seed of view `v` selects the leader of view `v + 1`.
    let elected = Round::new(EPOCH, View::new(seed.view.saturating_add(1)));
    ROTATING_ELECTOR
        .select_leader::<MinSig>(
            elected,
            u32::try_from(participants).expect("too many participants"),
            Some(signature),
        )
        .get() as usize
}
