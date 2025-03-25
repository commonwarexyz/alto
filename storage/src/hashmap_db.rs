use std::collections::HashMap;
use alto_types::Address;
use crate::account::{Account, Balance};
use crate::database::Database;

pub struct HashmapDatabase {
    accounts: HashMap<Address, Account>,
    data: HashMap<String, String>,
}

impl HashmapDatabase {
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
            data: HashMap::new(),
        }
    }
}

impl Database for HashmapDatabase {
    fn get_balance(&self, address: &Address) -> Option<Balance> {
        match self.accounts.get(&address) {
            Some(acct) => Some(acct.balance),
            None => None,
        }
    }

    fn set_balance(&mut self, address: &Address, amt: Balance) -> bool {
        // TODO: Add in public key update later
        let mut updated_account = Account::new();
        updated_account.address = address.clone();
        updated_account.balance = amt;
        self.accounts.insert(address.clone(), updated_account);
        true
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        self.data.insert(String::from_utf8(key.into())?, value.into());
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {

    }
}
