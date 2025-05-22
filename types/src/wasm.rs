use crate::{
    leader_index as compute_leader_index, Block, Finalized, Identity, Notarized, Seed, NAMESPACE,
};
use commonware_codec::{DecodeExt, Encode};
use commonware_consensus::threshold_simplex::types::Viewable;
use commonware_cryptography::Digestible;
use serde::Serialize;
use wasm_bindgen::prelude::*;

#[derive(Serialize)]
pub struct SeedJs {
    pub view: u64,
    pub signature: Vec<u8>,
}

#[derive(Serialize)]
pub struct ProofJs {
    pub view: u64,
    pub parent: u64,
    pub payload: Vec<u8>,
    pub signature: Vec<u8>,
}

#[derive(Serialize)]
pub struct BlockJs {
    pub parent: Vec<u8>,
    pub height: u64,
    pub timestamp: u64,
    pub digest: Vec<u8>,
}

#[derive(Serialize)]
pub struct NotarizedJs {
    pub proof: ProofJs,
    pub block: BlockJs,
}

#[derive(Serialize)]
pub struct FinalizedJs {
    pub proof: ProofJs,
    pub block: BlockJs,
}

#[wasm_bindgen]
pub fn parse_seed(identity: Vec<u8>, bytes: Vec<u8>) -> JsValue {
    let identity = Identity::decode(identity.as_ref()).expect("invalid identity");
    let Ok(seed) = Seed::decode(bytes.as_ref()) else {
        return JsValue::NULL;
    };
    if !seed.verify(NAMESPACE, &identity) {
        return JsValue::NULL;
    }
    let seed_js = SeedJs {
        view: seed.view(),
        signature: seed.signature.encode().to_vec(),
    };
    serde_wasm_bindgen::to_value(&seed_js).unwrap_or(JsValue::NULL)
}

#[wasm_bindgen]
pub fn parse_notarized(identity: Vec<u8>, bytes: Vec<u8>) -> JsValue {
    let identity = Identity::decode(identity.as_ref()).expect("invalid identity");
    let Ok(notarized) = Notarized::decode(bytes.as_ref()) else {
        return JsValue::NULL;
    };
    if !notarized.verify(NAMESPACE, &identity) {
        return JsValue::NULL;
    }
    let notarized_js = NotarizedJs {
        proof: ProofJs {
            view: notarized.proof.view(),
            parent: notarized.proof.proposal.parent,
            payload: notarized.proof.proposal.payload.to_vec(),
            signature: notarized.proof.proposal_signature.encode().to_vec(),
        },
        block: BlockJs {
            parent: notarized.block.parent.to_vec(),
            height: notarized.block.height,
            timestamp: notarized.block.timestamp,
            digest: notarized.block.digest().to_vec(),
        },
    };
    serde_wasm_bindgen::to_value(&notarized_js).unwrap_or(JsValue::NULL)
}

#[wasm_bindgen]
pub fn parse_finalized(identity: Vec<u8>, bytes: Vec<u8>) -> JsValue {
    let identity = Identity::decode(identity.as_ref()).expect("invalid identity");
    let Ok(finalized) = Finalized::decode(bytes.as_ref()) else {
        return JsValue::NULL;
    };
    if !finalized.verify(NAMESPACE, &identity) {
        return JsValue::NULL;
    }
    let finalized_js = FinalizedJs {
        proof: ProofJs {
            view: finalized.proof.view(),
            parent: finalized.proof.proposal.parent,
            payload: finalized.proof.proposal.payload.to_vec(),
            signature: finalized.proof.proposal_signature.encode().to_vec(),
        },
        block: BlockJs {
            parent: finalized.block.parent.to_vec(),
            height: finalized.block.height,
            timestamp: finalized.block.timestamp,
            digest: finalized.block.digest().to_vec(),
        },
    };
    serde_wasm_bindgen::to_value(&finalized_js).unwrap_or(JsValue::NULL)
}

#[wasm_bindgen]
pub fn parse_block(bytes: Vec<u8>) -> JsValue {
    let Ok(block) = Block::decode(bytes.as_ref()) else {
        return JsValue::NULL;
    };
    let block_js = BlockJs {
        parent: block.parent.to_vec(),
        height: block.height,
        timestamp: block.timestamp,
        digest: block.digest().to_vec(),
    };
    serde_wasm_bindgen::to_value(&block_js).unwrap_or(JsValue::NULL)
}

#[wasm_bindgen]
pub fn leader_index(signature: Vec<u8>, participants: usize) -> usize {
    compute_leader_index(&signature, participants)
}
