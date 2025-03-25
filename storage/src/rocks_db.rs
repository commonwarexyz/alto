use alto_types::Address;
use rocksdb::{DB, Options};
use crate::account::{Account, Balance};
use crate::database::Database;
use std::path::Path;

const SAL_ROCKS_DB_PATH: &str = "rocksdb";

pub struct RocksDbDatabase {
    db: DB,
}

impl RocksDbDatabase {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db_path = Path::new(SAL_ROCKS_DB_PATH);
        let db = DB::open(&opts, &db_path)?;
        Ok(RocksDbDatabase { db })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        self.db.put(key, value)?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let result = self.db.get(key)?;
        Ok(result)
    }
}

impl Database for RocksDbDatabase {
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
*/