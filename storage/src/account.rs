
pub(crate) type Balance = u64;

#[derive(Clone, Debug)]
pub struct Account {
    pub balance: Balance,
}

impl Account {
    pub fn new(initial_balance: u64) -> Self {
        Self {
            balance: initial_balance,
        }
    }
}