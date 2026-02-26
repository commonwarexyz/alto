use crate::{Block, Finalized, Identity, Notarized, Scheme, NAMESPACE};
use commonware_codec::{DecodeExt, Encode};
use commonware_consensus::Viewable;
use commonware_cryptography::Digestible;
use commonware_parallel::Sequential;
use serde::Serialize;
use wasm_bindgen::prelude::*;

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
pub fn parse_notarized(identity: Vec<u8>, bytes: Vec<u8>) -> JsValue {
    let identity = Identity::decode(identity.as_ref()).expect("invalid identity");
    let certificate_verifier = Scheme::certificate_verifier(NAMESPACE, identity);

    let Ok(notarized) = Notarized::decode(bytes.as_ref()) else {
        return JsValue::NULL;
    };

    // Certificate-verifier mode can only verify threshold certificates.
    // Minimmit m-notarizations are often aggregated and require full committee
    // metadata (participants + polynomial) for signature verification.
    if notarized.proof.certificate.is_threshold()
        && !notarized.verify(&certificate_verifier, &Sequential)
    {
        return JsValue::NULL;
    }
    let view = notarized.proof.view();
    let certificate = notarized.proof.certificate;
    let notarized_js = NotarizedJs {
        proof: ProofJs {
            view: view.get(),
            parent: notarized.proof.proposal.parent.get(),
            payload: notarized.proof.proposal.payload.to_vec(),
            signature: certificate.vote_signature().unwrap().encode().to_vec(),
        },
        block: BlockJs {
            parent: notarized.block.parent.to_vec(),
            height: notarized.block.height.get(),
            timestamp: notarized.block.timestamp,
            digest: notarized.block.digest().to_vec(),
        },
    };
    serde_wasm_bindgen::to_value(&notarized_js).unwrap_or(JsValue::NULL)
}

#[wasm_bindgen]
pub fn parse_finalized(identity: Vec<u8>, bytes: Vec<u8>) -> JsValue {
    let identity = Identity::decode(identity.as_ref()).expect("invalid identity");
    let certificate_verifier = Scheme::certificate_verifier(NAMESPACE, identity);
    let Ok(finalized) = Finalized::decode(bytes.as_ref()) else {
        return JsValue::NULL;
    };
    if !finalized.verify(&certificate_verifier, &Sequential) {
        return JsValue::NULL;
    }
    let view = finalized.proof.view();
    let certificate = finalized.proof.certificate;
    let finalized_js = FinalizedJs {
        proof: ProofJs {
            view: view.get(),
            parent: finalized.proof.proposal.parent.get(),
            payload: finalized.proof.proposal.payload.to_vec(),
            signature: certificate.vote_signature().unwrap().encode().to_vec(),
        },
        block: BlockJs {
            parent: finalized.block.parent.to_vec(),
            height: finalized.block.height.get(),
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
        height: block.height.get(),
        timestamp: block.timestamp,
        digest: block.digest().to_vec(),
    };
    serde_wasm_bindgen::to_value(&block_js).unwrap_or(JsValue::NULL)
}

#[wasm_bindgen]
pub fn leader_index(view: u64, participants: usize) -> usize {
    if participants == 0 {
        return 0;
    }
    ((view.saturating_sub(1)) as usize) % participants
}
