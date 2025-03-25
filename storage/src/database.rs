use alto_types::Address;
use crate::account::{Balance, Account};
use std::error::Error;

// Define database interface that will be used for all impls
pub trait Database {
    fn get_account(&self, address: &Address) -> Option<Account>;

    fn set_account(&mut self, acc: &Account) -> Result<(), Box<dyn Error>>;

    fn get_balance(&self, address: &Address) -> Option<Balance>;

    fn set_balance(&mut self, address: &Address, amt: u64) -> bool;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn Error>>;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn Error>>;
}