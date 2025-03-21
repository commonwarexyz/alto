
pub struct Transfer {
    pub from_address: Address::new(),
    pub to_address: Address::new(),
    pub value: u64,
}

impl Transfer {
    pub fn new(from: Address, to: Address, value: u64) -> Transfer {
        Self {
            from_address: from,
            to_address: to,
            value: value,
        }
    }
}