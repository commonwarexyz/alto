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
    fn get_balance(&self, address: Address) -> Option<Balance> {
        match self.accts.get(&address) {
            Some(acct) => Some(acct.balance),
            None => None,
        }
    }

    fn add_balance(&mut self, address: Address, amt: u64) -> Option<Balance> {
        let curr_balance = self.accts.get(&address);
        match curr_balance {
            Some(account) => {
                let updated_account = account.clone();
                updated_account.balance.checked_add(amt)?;
                self.accts.insert(address, updated_account.clone());
                Some(updated_account.balance)
            },
            None => {
                let new_account = Account::new(amt);
                self.accts.insert(address, new_account.clone());
                Some(new_account.balance)
            },
        }
    }

    fn sub_balance(&mut self, address: Address, amt: u64) -> Option<Balance> {
        let curr_balance = self.accts.get(&address);
        match curr_balance {
            Some(account) => {
                let updated_account = account.clone();
                updated_account.balance.checked_add(amt)?;
                self.accts.insert(address, updated_account.clone());
                Some(updated_account.balance)
            },
            None => {
                let new_account = Account::new(amt);
                self.accts.insert(address, new_account.clone());
                Some(new_account.balance)
            },
        }
    }
}

#[cfg(test)] // Compile these only when running tests
mod tests {
    use super::*; // Import items from the parent module

    #[test] // Mark this function as a test
    fn test_addition() {
        let a = 2;
        let b = 3;
        let result = a + b;

        // Use assertions to validate
        assert_eq!(result, 6); // Test passes if result == 5
    }
}
