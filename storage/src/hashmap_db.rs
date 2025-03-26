use std::collections::HashMap;
use alto_types::Address;
use crate::account::{Account, Balance};
use crate::database::Database;

pub struct HashmapDatabase {
    data: HashMap<String, String>,
}

impl HashmapDatabase {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }
}

impl Database for HashmapDatabase {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let str_value: String = String::from_utf8(value.into())?;
        let key_value: String = String::from_utf8(key.into())?;

        self.data.insert(key_value, str_value);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let str_key: String = String::from_utf8(key.into()).unwrap();
        self.data.get(&str_key).map_or(
            Ok(None),
            |v| Ok(Some(v.clone().into())))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_hashmap_db() {
        let mut db = HashmapDatabase::new();
        let key = b"key1";
        let value = b"value1";
        db.put(key, value).unwrap();
        let retrieved = db.get(key).unwrap().unwrap();
        assert_eq!(retrieved.as_slice(), value);
    }
}
