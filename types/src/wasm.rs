use crate::{Block, Finalized, Identity, Notarized, Scheme, Seed, Signature, EPOCH, NAMESPACE};
use commonware_codec::{DecodeExt, Encode};
use commonware_consensus::{
    simplex::elector::Random,
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
    let certificate_verifier = Scheme::certificate_verifier(NAMESPACE, identity);

    let Ok(seed) = Seed::decode(bytes.as_ref()) else {
        return JsValue::NULL;
    };
    if !seed.verify(&certificate_verifier) {
        return JsValue::NULL;
    }
    let seed_js = SeedJs {
        view: seed.view().get(),
        signature: seed.signature.encode().to_vec(),
    };
    serde_wasm_bindgen::to_value(&seed_js).unwrap_or(JsValue::NULL)
}

#[wasm_bindgen]
pub fn parse_notarized(identity: Vec<u8>, bytes: Vec<u8>) -> JsValue {
    let identity = Identity::decode(identity.as_ref()).expect("invalid identity");
    let certificate_verifier = Scheme::certificate_verifier(NAMESPACE, identity);

    let Ok(notarized) = Notarized::decode(bytes.as_ref()) else {
        return JsValue::NULL;
    };
    if !notarized.verify(&certificate_verifier, &Sequential) {
        return JsValue::NULL;
    }
    let notarized_js = NotarizedJs {
        proof: ProofJs {
            view: notarized.proof.view().get(),
            parent: notarized.proof.proposal.parent.get(),
            payload: notarized.proof.proposal.payload.to_vec(),
            signature: notarized.proof.certificate.vote_signature.encode().to_vec(),
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
    let finalized_js = FinalizedJs {
        proof: ProofJs {
            view: finalized.proof.view().get(),
            parent: finalized.proof.proposal.parent.get(),
            payload: finalized.proof.proposal.payload.to_vec(),
            signature: finalized.proof.certificate.vote_signature.encode().to_vec(),
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
pub fn leader_index(seed: JsValue, participants: usize) -> usize {
    let Ok(seed) = serde_wasm_bindgen::from_value::<SeedJs>(seed) else {
        return 0;
    };

    let Ok(signature) = Signature::decode(seed.signature.as_ref()) else {
        return 0;
    };

    let round = Round::new(EPOCH, View::new(seed.view));
    let seed = Seed::new(round, signature);

    Random::select_leader::<MinSig>(
        round,
        u32::try_from(participants).expect("too many participants"),
        (round.view().get() != 1).then_some(seed.signature),
    )
    .get() as usize
}
