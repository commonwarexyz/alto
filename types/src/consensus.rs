use crate::{FINALIZE_NAMESPACE, NOTARIZE_NAMESPACE, NULLIFY_NAMESPACE, SEED_NAMESPACE};
use bytes::{Buf, BufMut};
use commonware_cryptography::sha256::Digest;
use commonware_cryptography::{bls12381, Bls12381, Scheme};
use commonware_utils::{hex, Array, SizedSerialize};

// We hardcode the keys here to guard against silent changes.
#[repr(u8)]
pub enum Kind {
    Seed = 0,
    Notarization = 1,
    Nullification = 2,
    Finalization = 3,
}

impl Kind {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Seed),
            1 => Some(Self::Notarization),
            2 => Some(Self::Nullification),
            3 => Some(Self::Finalization),
            _ => None,
        }
    }

    pub fn to_hex(&self) -> String {
        match self {
            Self::Seed => hex(&[0]),
            Self::Notarization => hex(&[1]),
            Self::Nullification => hex(&[2]),
            Self::Finalization => hex(&[3]),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Seed {
    pub view: u64,
    pub signature: bls12381::Signature,
}

impl Seed {
    pub fn payload(view: u64) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(u64::SERIALIZED_LEN);
        bytes.put_u64(view);
        bytes
    }

    fn pack(view: u64, signature: &bls12381::Signature) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::SERIALIZED_LEN);
        bytes.put_u64(view);
        bytes.extend_from_slice(signature);
        bytes
    }

    pub fn new(view: u64, signature: bls12381::Signature) -> Self {
        Self { view, signature }
    }

    pub fn serialize(&self) -> Vec<u8> {
        Self::pack(self.view, &self.signature)
    }

    pub fn deserialize(public: Option<&bls12381::PublicKey>, mut bytes: &[u8]) -> Option<Self> {
        // Check if the length is correct
        if bytes.len() != Self::SERIALIZED_LEN {
            return None;
        }

        // Deserialize the block proof
        let view = bytes.get_u64();
        let signature = bls12381::Signature::read_from(&mut bytes).ok()?;

        // Verify the signature
        if let Some(public) = public {
            let message = Self::payload(view);
            if !Bls12381::verify(Some(SEED_NAMESPACE), &message, public, &signature) {
                return None;
            }
        }
        Some(Self { view, signature })
    }
}

impl SizedSerialize for Seed {
    const SERIALIZED_LEN: usize = u64::SERIALIZED_LEN + bls12381::Signature::SERIALIZED_LEN;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notarization {
    pub view: u64,
    pub parent: u64,
    pub payload: Digest,
    pub signature: bls12381::Signature,
}

impl Notarization {
    pub fn payload(view: u64, parent: u64, payload: &Digest) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(u64::SERIALIZED_LEN + u64::SERIALIZED_LEN + Digest::SERIALIZED_LEN);
        bytes.put_u64(view);
        bytes.put_u64(parent);
        bytes.extend_from_slice(payload);
        bytes
    }

    fn pack(view: u64, parent: u64, payload: &Digest, signature: &bls12381::Signature) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::SERIALIZED_LEN);
        bytes.put_u64(view);
        bytes.put_u64(parent);
        bytes.extend_from_slice(payload);
        bytes.extend_from_slice(signature);
        bytes
    }

    pub fn new(view: u64, parent: u64, payload: Digest, signature: bls12381::Signature) -> Self {
        Self {
            view,
            parent,
            payload,
            signature,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        Self::pack(self.view, self.parent, &self.payload, &self.signature)
    }

    pub fn deserialize(public: Option<&bls12381::PublicKey>, mut bytes: &[u8]) -> Option<Self> {
        // Check if the length is correct
        if bytes.len() != Self::SERIALIZED_LEN {
            return None;
        }

        // Deserialize the block proof
        let view = bytes.get_u64();
        let parent = bytes.get_u64();
        let payload = Digest::read_from(&mut bytes).ok()?;
        let signature = bls12381::Signature::read_from(&mut bytes).ok()?;

        // Verify the signature
        if let Some(public) = public {
            let message = Self::payload(view, parent, &payload);
            if !Bls12381::verify(Some(NOTARIZE_NAMESPACE), &message, public, &signature) {
                return None;
            }
        }
        Some(Self {
            view,
            parent,
            payload,
            signature,
        })
    }
}

impl SizedSerialize for Notarization {
    const SERIALIZED_LEN: usize = u64::SERIALIZED_LEN
        + u64::SERIALIZED_LEN
        + Digest::SERIALIZED_LEN
        + bls12381::Signature::SERIALIZED_LEN;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Nullification {
    pub view: u64,
    pub signature: bls12381::Signature,
}

impl Nullification {
    pub fn payload(view: u64) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(u64::SERIALIZED_LEN);
        bytes.put_u64(view);
        bytes
    }

    fn pack(view: u64, signature: &bls12381::Signature) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::SERIALIZED_LEN);
        bytes.put_u64(view);
        bytes.extend_from_slice(signature);
        bytes
    }

    pub fn new(view: u64, signature: bls12381::Signature) -> Self {
        Self { view, signature }
    }

    pub fn serialize(&self) -> Vec<u8> {
        Self::pack(self.view, &self.signature)
    }

    pub fn deserialize(public: Option<&bls12381::PublicKey>, mut bytes: &[u8]) -> Option<Self> {
        // Check if the length is correct
        if bytes.len() != Self::SERIALIZED_LEN {
            return None;
        }

        // Deserialize the block proof
        let view = bytes.get_u64();
        let signature = bls12381::Signature::read_from(&mut bytes).ok()?;

        // Verify the signature
        if let Some(public) = public {
            let message = Self::payload(view);
            if !Bls12381::verify(Some(NULLIFY_NAMESPACE), &message, public, &signature) {
                return None;
            }
        }
        Some(Self { view, signature })
    }
}

impl SizedSerialize for Nullification {
    const SERIALIZED_LEN: usize = u64::SERIALIZED_LEN + bls12381::Signature::SERIALIZED_LEN;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finalization {
    pub view: u64,
    pub parent: u64,
    pub payload: Digest,
    pub signature: bls12381::Signature,
}

impl Finalization {
    pub fn payload(view: u64, parent: u64, payload: &Digest) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(u64::SERIALIZED_LEN + u64::SERIALIZED_LEN + Digest::SERIALIZED_LEN);
        bytes.put_u64(view);
        bytes.put_u64(parent);
        bytes.extend_from_slice(payload);
        bytes
    }

    fn pack(view: u64, parent: u64, payload: &Digest, signature: &bls12381::Signature) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::SERIALIZED_LEN);
        bytes.put_u64(view);
        bytes.put_u64(parent);
        bytes.extend_from_slice(payload);
        bytes.extend_from_slice(signature);
        bytes
    }

    pub fn new(view: u64, parent: u64, payload: Digest, signature: bls12381::Signature) -> Self {
        Self {
            view,
            parent,
            payload,
            signature,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        Self::pack(self.view, self.parent, &self.payload, &self.signature)
    }

    pub fn deserialize(public: Option<&bls12381::PublicKey>, mut bytes: &[u8]) -> Option<Self> {
        // Check if the length is correct
        if bytes.len() != Self::SERIALIZED_LEN {
            return None;
        }

        // Deserialize the block proof
        let view = bytes.get_u64();
        let parent = bytes.get_u64();
        let payload = Digest::read_from(&mut bytes).ok()?;
        let signature = bls12381::Signature::read_from(&mut bytes).ok()?;

        // Verify the signature
        if let Some(public) = public {
            let message = Self::payload(view, parent, &payload);
            if !Bls12381::verify(Some(FINALIZE_NAMESPACE), &message, public, &signature) {
                return None;
            }
        }
        Some(Self {
            view,
            parent,
            payload,
            signature,
        })
    }
}

impl SizedSerialize for Finalization {
    const SERIALIZED_LEN: usize = u64::SERIALIZED_LEN
        + u64::SERIALIZED_LEN
        + Digest::SERIALIZED_LEN
        + bls12381::Signature::SERIALIZED_LEN;
}
