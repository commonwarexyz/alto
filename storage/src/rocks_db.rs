use std::error::Error;
use alto_types::Address;
use rocksdb::{DB, Options};
use commonware_codec::{Codec, ReadBuffer, WriteBuffer};
use crate::account::{Account, Balance};
use crate::database::Database;
use std::path::Path;
use bytes::{BufMut, Bytes};


const SAL_ROCKS_DB_PATH: &str = "rocksdb";
const ACCOUNTS_PREFIX: &[u8] = b"sal_accounts";

const WRITE_BUFFER_CAPACITY: u64 = 500;

pub struct RocksDbDatabase {
    db: DB,
}

impl RocksDbDatabase {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db_path = Path::new(SAL_ROCKS_DB_PATH);
        let db = DB::open(&opts, &db_path)?;
        Ok(RocksDbDatabase { db })
    }

    pub fn key_accounts(addr: &Address) -> Vec<u8> {
        Self::make_multi_key(ACCOUNTS_PREFIX, addr.as_slice())
    }

     fn make_multi_key(prefix: &[u8], sub_id: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(prefix.len() + sub_id.len() + 1);
        key.extend_from_slice(prefix);
        key.push(b':');
        key.extend_from_slice(sub_id);
        key
    }
}

impl Database for RocksDbDatabase {

    fn get_account(&self, address: &Address) -> Result<Option<Account>, Box<dyn Error>> {
        let key = Self::key_accounts(address);
        let result = self.get(key.as_slice())?;
        match result {
            None => Ok(None),
            Some(value) => {
                let bytes = Bytes::copy_from_slice(&value);
                let mut read_buf = ReadBuffer::new(bytes);
                let acc = Account::read(&mut read_buf)?;
                Ok(Some(acc))
            }
        }
    }

    fn set_account(&mut self, acc: &Account) -> Result<(), Box<dyn Error>> {
        let key = Self::key_accounts(&acc.address);
        let mut write_buf = WriteBuffer::new(WRITE_BUFFER_CAPACITY as usize);
        acc.write(&mut write_buf);
        self.put(&key, write_buf.as_ref())?;
        Ok(())
    }

    fn get_balance(&self, address: &Address) -> Option<Balance> {
       let result = self.get_account(address).and_then(|acc| {
           match acc {
               Some(acc) => Ok(acc.balance),
               None => Ok(0),
           }
       });
        match result {
            Some(balance) => Some(balance),
            None => None,
        }
    }

    fn set_balance(&mut self, address: &Address, amt: Balance) -> bool {
        let result = self.get_account(address);
        match result {
            Some(mut acc) => {
                acc.balance = amt;
                let result = self.set_account(&acc);
                result.is_ok()
            },
            None => false,
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn Error>> {
        self.db.put(key, value)?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
        let result = self.db.get(key)?;
        Ok(result)
    }
}