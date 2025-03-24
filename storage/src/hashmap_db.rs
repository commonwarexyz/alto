use std::collections::HashMap;
use alto_types::Address;
use crate::account::{Account, Balance};
use crate::database::Database;

pub struct HashmapDb {
    accts: HashMap<Address, Account>,
}

impl HashmapDb {
    pub fn new() -> Self {
        Self {
            accts: HashMap::new(),
        }
    }
}

impl Database for HashmapDb {
    fn get_balance(&self, address: &Address) -> Option<Balance> {
        match self.accts.get(&address) {
            Some(acct) => Some(acct.balance),
            None => None,
        }
    }

    fn set_balance(&mut self, address: &Address, amt: Balance) -> bool {
        // TODO: Add in public key update later
        let mut updated_account = Account::new();
        updated_account.address = address.clone();
        updated_account.balance = amt;
        self.accts.insert(address.clone(), updated_account);
        true
    }
}
