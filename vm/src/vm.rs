use std::collections::HashMap;

struct VM {
    state_db: Box<dyn Database>
}

impl VM {
    pub fn new() {
        Self {
            state_db: Box::new(HashMapDb::new())
        }
    }

    pub fn execute(msg: Transfer) {
        // process the transfer
    }
}