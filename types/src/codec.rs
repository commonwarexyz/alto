use commonware_codec::{Codec, Error, Reader, Writer};
use commonware_cryptography::{ed25519::PublicKey, Ed25519, Scheme};
use commonware_cryptography::ed25519::PrivateKey;
use serde::Serialize;

pub fn serialize_pk(pk: &PublicKey, writer: &mut impl Writer) {
    // let slice: &[u8] = pk.as_ref();
    // let length = slice.len();
    // length.serialize(&mut *writer).unwrap();
}