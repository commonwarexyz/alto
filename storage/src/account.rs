use commonware_cryptography::{ed25519::PublicKey, Ed25519, Scheme};
use alto_types::Address;
use rand::rngs::OsRng;

pub(crate) type Balance = u64;

#[derive(Clone, Debug)]
pub struct Account {
    pub address: Address,
    pub public_key: PublicKey,
    pub balance: Balance,
}

impl Account {
    pub fn new() -> Self {
        let mut rng = OsRng;
        // generates keypair using random number generator
        let keypair = Ed25519::new(&mut rng);

        let public_key = keypair.public_key();

        // takes addr from pk (truncating or hashing if necessary)
        let address = Address::new(&public_key.as_ref()[..32]);

        Self {
            address,
            public_key,
            balance: 0,
        }
    }
}