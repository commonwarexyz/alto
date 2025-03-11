use commonware_cryptography::bls12381::PublicKey;
use serde::Serialize;
use wasm_bindgen::prelude::*;

use crate::{Block, Finalized, Notarized, Seed};

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
pub fn parse_seed(public_key: Option<Vec<u8>>, bytes: Vec<u8>) -> JsValue {
    let mut public = None;
    if let Some(pk) = public_key {
        public = Some(PublicKey::try_from(pk).expect("invalid public key"));
    }
    match Seed::deserialize(public.as_ref(), &bytes) {
        Some(s) => {
            let seed_js = SeedJs {
                view: s.view,
                signature: s.signature.to_vec(),
            };
            serde_wasm_bindgen::to_value(&seed_js).unwrap_or(JsValue::NULL)
        }
        None => JsValue::NULL,
    }
}

#[wasm_bindgen]
pub fn parse_notarized(public_key: Option<Vec<u8>>, bytes: Vec<u8>) -> JsValue {
    let mut public = None;
    if let Some(pk) = public_key {
        public = Some(PublicKey::try_from(pk).expect("invalid public key"));
    }
    match Notarized::deserialize(public.as_ref(), &bytes) {
        Some(n) => {
            let notarized_js = NotarizedJs {
                proof: ProofJs {
                    view: n.proof.view,
                    parent: n.proof.parent,
                    payload: n.proof.payload.to_vec(),
                    signature: n.proof.signature.to_vec(),
                },
                block: BlockJs {
                    parent: n.block.parent.to_vec(),
                    height: n.block.height,
                    timestamp: n.block.timestamp,
                    digest: n.block.digest().to_vec(),
                },
            };
            serde_wasm_bindgen::to_value(&notarized_js).unwrap_or(JsValue::NULL)
        }
        None => JsValue::NULL,
    }
}

#[wasm_bindgen]
pub fn parse_finalized(public_key: Option<Vec<u8>>, bytes: Vec<u8>) -> JsValue {
    let mut public = None;
    if let Some(pk) = public_key {
        public = Some(PublicKey::try_from(pk).expect("invalid public key"));
    }
    match Finalized::deserialize(public.as_ref(), &bytes) {
        Some(f) => {
            let finalized_js = FinalizedJs {
                proof: ProofJs {
                    view: f.proof.view,
                    parent: f.proof.parent,
                    payload: f.proof.payload.to_vec(),
                    signature: f.proof.signature.to_vec(),
                },
                block: BlockJs {
                    parent: f.block.parent.to_vec(),
                    height: f.block.height,
                    timestamp: f.block.timestamp,
                    digest: f.block.digest().to_vec(),
                },
            };
            serde_wasm_bindgen::to_value(&finalized_js).unwrap_or(JsValue::NULL)
        }
        None => JsValue::NULL,
    }
}

#[wasm_bindgen]
pub fn parse_block(bytes: Vec<u8>) -> JsValue {
    match Block::deserialize(&bytes) {
        Some(b) => {
            let block_js = BlockJs {
                parent: b.parent.to_vec(),
                height: b.height,
                timestamp: b.timestamp,
                digest: b.digest().to_vec(),
            };
            serde_wasm_bindgen::to_value(&block_js).unwrap_or(JsValue::NULL)
        }
        None => JsValue::NULL,
    }
}
