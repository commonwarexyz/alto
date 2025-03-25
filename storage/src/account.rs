use commonware_codec::{Codec, Error, Reader, Writer};
use commonware_cryptography::{ed25519::PublicKey, Ed25519, Scheme};
use commonware_cryptography::ed25519::PrivateKey;
use alto_types::Address;
use rand::rngs::OsRng;

pub type Balance = u64;

#[derive(Clone, Debug)]
pub struct Account {
    pub address: Address,
    pub public_key: PublicKey,
    pub private_key: PrivateKey,
    pub balance: Balance,
}

impl Account {
    pub fn new() -> Self {
        let mut rng = OsRng;
        // generates keypair using random number generator
        let keypair = Ed25519::new(&mut rng);

        let public_key = keypair.public_key();

        let private_key = keypair.private_key();

        // takes addr from pk (truncating or hashing if necessary)
        let address = Address::new(&public_key.as_ref()[..32]);

        Self {
            address,
            public_key,
            private_key,
            balance: 0,
        }
    }
}
impl Codec for Account {
    fn write(&self, writer: &mut impl Writer) {
        let _ = self.address.write(writer);
        self.balance.write(writer);
        let len: u32 = self.public_key.as_ref().len() as u32;
        len.write(writer);
        let _ = self.public_key.as_ref().write(writer);
        let priv_len: u32 = self.private_key.as_ref().len() as u32;
        priv_len.write(writer);
        let _ = self.private_key.as_ref().write(writer);
    }

    fn read(reader: &mut impl Reader) -> Result<Self, Error> {
        let addr = <[u8; 32]>::read(reader)?;
        let balance = <u64>::read(reader)?;
        // setting up public key
        let pk_len = u32::read(reader)?;
        let pk_bytes = reader.read_n_bytes(pk_len as usize)?;
        let public_key = PublicKey::try_from(pk_bytes).unwrap();
        // setting up private key
        let priv_key_len = u32::read(reader)?;
        let priv_key_bytes = reader.read_n_bytes(priv_key_len as usize)?;
        let private_key = PrivateKey::try_from(priv_key_bytes).unwrap();
        let address = Address::try_from(addr).unwrap();

        Ok(Self{ address, public_key, private_key, balance})
    }

    fn len_encoded(&self) -> usize {
        todo!()
    }
}