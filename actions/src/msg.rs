use alto_types::Address;

pub struct SequencerMsg {
    pub chain_id: Vec<u8>,
    pub data: Vec<u8>,
    pub from_address: Address,
    pub relayer_id: u64,
}

impl SequencerMsg {
    pub fn new() -> Self {
        Self {
            chain_id: vec![],
            data: vec![],
            from_address: Address::new(),
            relayer_id: 0,
        }
    }
    pub fn get_type_id(&self) -> u8 {
        0
    }
}