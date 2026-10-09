use crate::{decode_identity, Block, ConsensusScheme, Finalized, Notarized, Scheme, NAMESPACE};
use commonware_codec::{Copying, Decode, Encode};
use commonware_consensus::Viewable;
use commonware_cryptography::{Digestible, Hasher, Sha256};
use commonware_parallel::Sequential;
use serde::Serialize;
use std::{cell::RefCell, rc::Rc};
use wasm_bindgen::prelude::*;

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

/// A verified notarized or finalized block.
#[derive(Serialize)]
pub struct CertifiedBlockJs {
    pub view: u64,
    /// The SHA-256 digest of the encoded certificate, which carries one ellipsoidal Falcon-512
    /// signature per signer.
    pub signature: Vec<u8>,
    pub block: BlockJs,
}

/// Returns the SHA-256 digest that identifies a verified certificate.
fn certificate_digest(
    certificate: &<ConsensusScheme as commonware_cryptography::certificate::Verifier>::Certificate,
) -> Vec<u8> {
    Sha256::hash(&[certificate.encode().as_ref()]).to_vec()
}

/// A verifier and the encoded identity it was built from.
struct CachedVerifier {
    identity: Vec<u8>,
    verifier: Rc<ConsensusScheme>,
}

thread_local! {
    /// The verifier for the most recently used identity.
    ///
    /// The explorer verifies every artifact against the same identity, and building a verifier
    /// decodes every participant's public key, which can cost more than verifying the
    /// certificate itself.
    static VERIFIER: RefCell<Option<CachedVerifier>> = const { RefCell::new(None) };
}

/// Returns the verifier for `identity`, reusing the cached one when it matches.
///
/// Panics if `identity` is not a valid encoded identity.
fn verifier(identity: Vec<u8>) -> Rc<ConsensusScheme> {
    VERIFIER.with_borrow_mut(|cached| {
        if let Some(entry) = cached {
            if entry.identity == identity {
                return entry.verifier.clone();
            }
        }
        let decoded = decode_identity(Copying(identity.as_slice())).expect("invalid identity");
        let verifier = Rc::new(ConsensusScheme::certificate_verifier(NAMESPACE, decoded));
        *cached = Some(CachedVerifier {
            identity,
            verifier: verifier.clone(),
        });
        verifier
    })
}

/// Returns the verified notarized block, or null if it is invalid.
///
/// `identity` is the encoded network identity: the ordered participant set.
#[wasm_bindgen]
pub fn parse_notarized(identity: Vec<u8>, bytes: Vec<u8>) -> JsValue {
    let verifier = verifier(identity);
    let Ok(notarized) =
        Notarized::<ConsensusScheme>::decode_cfg(bytes, &Block::unbounded_codec_config())
    else {
        return JsValue::NULL;
    };
    if !notarized.verify(verifier.as_ref(), &Sequential) {
        return JsValue::NULL;
    }
    let notarized_js = CertifiedBlockJs {
        view: notarized.proof.view().get(),
        signature: certificate_digest(&notarized.proof.certificate),
        block: (&notarized.block).into(),
    };
    serde_wasm_bindgen::to_value(&notarized_js).unwrap_or(JsValue::NULL)
}

/// Returns the verified finalized block, or null if it is invalid.
///
/// Arguments match [parse_notarized].
#[wasm_bindgen]
pub fn parse_finalized(identity: Vec<u8>, bytes: Vec<u8>) -> JsValue {
    let verifier = verifier(identity);
    let Ok(finalized) =
        Finalized::<ConsensusScheme>::decode_cfg(bytes, &Block::unbounded_codec_config())
    else {
        return JsValue::NULL;
    };
    if !finalized.verify(verifier.as_ref(), &Sequential) {
        return JsValue::NULL;
    }
    let finalized_js = CertifiedBlockJs {
        view: finalized.proof.view().get(),
        signature: certificate_digest(&finalized.proof.certificate),
        block: (&finalized.block).into(),
    };
    serde_wasm_bindgen::to_value(&finalized_js).unwrap_or(JsValue::NULL)
}

#[wasm_bindgen]
pub fn parse_block(bytes: Vec<u8>) -> JsValue {
    let Ok(block) = Block::decode_cfg(bytes, &Block::unbounded_codec_config()) else {
        return JsValue::NULL;
    };
    let block_js = BlockJs::from(&block);
    serde_wasm_bindgen::to_value(&block_js).unwrap_or(JsValue::NULL)
}

/// Returns the SHA-256 digest of `bytes`, which the explorer uses to fingerprint an identity.
#[wasm_bindgen]
pub fn sha256(bytes: Vec<u8>) -> Vec<u8> {
    Sha256::hash(&[&bytes]).to_vec()
}
