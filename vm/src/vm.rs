use alto_actions::transfer::{Transfer, TransferError};
use alto_storage::database::Database;
use alto_storage::hashmap_db::HashmapDatabase;
use alto_storage::account::Balance;
use alto_types::Address;

const TEST_FAUCET_ADDRESS: &[u8] = b"0x0123456789abcdef0123456789abcd";
const TEST_FAUCET_BALANCE: Balance = 10_000_000;

struct VM {
    state_db: Box<dyn Database>,
}

impl VM {
    pub fn new() -> Self {
        Self {
            state_db: Box::new(HashmapDatabase::new()),
        }
    }

    pub fn new_test_vm() -> Self {
        let mut state_db = Box::new(HashmapDatabase::new());
        state_db.set_balance(&Self::test_faucet_address(), TEST_FAUCET_BALANCE);

        Self {
            state_db
        }
    }

    // TODO:
    // make check for sending funds > 0 so reject any negative values or balances.
    // make sure address isn't a duplicate of self.
    // need from balance to be greater than or equal to the transfer amount
    // need the to_balance to not overflow
    pub fn execute(&mut self, msg: Transfer) -> Result<(), TransferError> {
        let from_balance = self.state_db.get_balance(&msg.from_address);
        if from_balance.is_none() {
            return Err(TransferError::InvalidFromAddress);
        }


        let mut to_balance = self.state_db.get_balance(&msg.to_address);
        if to_balance.is_none() {
            to_balance = Some(0)
        }

        let updated_from_balance = from_balance.unwrap().checked_sub(msg.value);
        if updated_from_balance.is_none() {
            return Err(TransferError::InsufficientFunds);
        }

        let updated_to_balance = to_balance.unwrap().checked_add(msg.value);
        if updated_to_balance.is_none() {
            return Err(TransferError::TooMuchFunds);
        }

        // TODO: Below doesn't rollback cases. Fix later.
        if !self.state_db.set_balance(&msg.from_address, updated_from_balance.unwrap()) {
           return Err(TransferError::StorageError);
        }
        if !self.state_db.set_balance(&msg.to_address, updated_to_balance.unwrap()) {
            return Err(TransferError::StorageError);
        }

        Ok(())
    }

    pub fn query_balance(&self, address: &Address) -> Option<Balance> {
        self.state_db.get_balance(address)
    }

    pub fn test_faucet_address() -> Address {
        Address::new(TEST_FAUCET_ADDRESS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() -> Result<(), String> {
        let from_address = Address::new(b"0x10000");
        let to_address = Address::new(b"0x20000");
        let faucet_address = VM::test_faucet_address();

        let mut test_vm = VM::new_test_vm();

        // should be empty
        let from_balance1 = test_vm.query_balance(&from_address);
        assert!(from_balance1.is_none());

        let faucet_balance = test_vm.query_balance(&VM::test_faucet_address());
        assert_eq!(faucet_balance, Some(TEST_FAUCET_BALANCE));

        // process faucet transfers to addresses
        let transfer1 = Transfer::new(faucet_address.clone(), from_address.clone(), 100);
        test_vm.execute(transfer1.unwrap()).unwrap();
        let transfer2 = Transfer::new(faucet_address.clone(), to_address.clone(), 200);
        test_vm.execute(transfer2.unwrap()).unwrap();

        // check balances have been updated from faucet transfers
        let from_balance2 = test_vm.query_balance(&from_address);
        assert_eq!(from_balance2, Some(100));
        let from_balance3 = test_vm.query_balance(&to_address);
        assert_eq!(from_balance3, Some(200));

        let transfer3 = Transfer::new(from_address.clone(), to_address.clone(), 50);
        test_vm.execute(transfer3.unwrap()).unwrap();

        // check updated balances
        let from_balance2 = test_vm.query_balance(&from_address);
        assert_eq!(from_balance2, Some(50));
        let from_balance3 = test_vm.query_balance(&to_address);
        assert_eq!(from_balance3, Some(250));

        Ok(())
    }
}
