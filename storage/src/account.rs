use commonware_codec::{Codec, Error, Reader, SizedCodec, Writer};
use commonware_cryptography::{ed25519::PublicKey, Ed25519, Scheme};
use commonware_cryptography::ed25519::PrivateKey;
use alto_types::Address;
use rand::rngs::OsRng;

pub type Balance = u64;

#[derive(Clone, Debug)]
pub struct Account {
    pub address: Address,
    pub balance: Balance,
}

impl Account {
    pub fn new() -> Self {
        Self {
            address: Address::empty(),
            balance: 0,
        }
    }

    pub fn from_address(address: Address) -> Self {
        Self {
            address,
            balance: 0,
        }
    }
}
impl Codec for Account {
    fn write(&self, writer: &mut impl Writer) {
        writer.write_bytes(self.address.0.as_slice());
        self.balance.write(writer);
    }

    fn read(reader: &mut impl Reader) -> Result<Self, Error> {
        let addr_bytes = <[u8; 32]>::read(reader)?;
        let address = Address::from_bytes(&addr_bytes).unwrap();
        let balance = <u64>::read(reader)?;
        Ok(Self{address, balance})
    }

    fn len_encoded(&self) -> usize {
        Codec::len_encoded(&self.address.0) + Codec::len_encoded(&self.balance)
    }
}