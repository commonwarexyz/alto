use alto_storage::database::Database;
use alto_storage::hashmap_db::HashmapDb;
use alto_actions::transfer::Transfer;

struct VM {
    state_db: Box<dyn Database>
}

impl VM {
    pub fn new() {
        Self {
            state_db: Box::new(HashmapDb::new())
        };
    }

    pub fn execute(msg: Transfer) {
        // process the transfer
    }
}