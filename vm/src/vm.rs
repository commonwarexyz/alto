use alto_actions::transfer::{Transfer, TransferError};
use alto_storage::account::Account;
use alto_storage::database::Database;
use alto_storage::hashmap_db::HashmapDb;

struct VM {
    state_db: Box<dyn Database>,
}

impl VM {
    pub fn new() {
        Self {
            state_db: Box::new(HashmapDb::new()),
        };
    }

    pub fn execute(&mut self, msg: Transfer) -> Result<(), TransferError> {
        // process the transfer

        // add balance to account
        let from_balance = self.state_db.get_balance(msg.from_address);
        if from_balance.is_none() {
            return Err(TransferError::InvalidFromAddress);
        }
        let to_balance = self.state_db.get_balance(msg.to_address);
        if to_balance.is_none() {
            return Err(TransferError::InvalidToAddress);
        }
        // TODO:
        // make check for sending funds > 0 so reject any negative values or balances.
        // make sure address isn't a duplicate of self.
        // need from balance to be greater than or equal to the transfer amount
        // need the to_balance to not overflow

        let updated_from_balance = from_balance.unwrap().checked_sub(msg.value);
        let updated_to_balance = to_balance.unwrap().checked_add(msg.value);

        match curr_balance {
            Some(account) => {
                let updated_account = account.clone();
                updated_account.balance.checked_add(amt)?;
                self.accts.insert(address, updated_account.clone());
                Some(updated_account.balance)
            }
            None => {
                let new_account = Account::new(amt);
                self.accts.insert(address, new_account.clone());
                Some(new_account.balance)
            }
        }
    }
}
