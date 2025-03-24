use alto_types::Address;

const MAX_MEMO_SIZE: usize = 256;

#[derive(Debug)]
pub struct Transfer {
    pub from_address: Address,
    pub to_address: Address,
    pub value: u64,
    pub memo: Vec<u8>,
}

#[derive(Debug)]
pub enum TransferError {
    DuplicateAddress,
    InvalidToAddress,
    InvalidFromAddress,
    InsufficientFunds,
    TooMuchFunds,
    InvalidMemoSize,
    StorageError,
}

impl Transfer {
    pub fn new(from_address: Address, to_address: Address, value: u64) -> Result<Transfer, TransferError> {
        let empty_memo = b"".to_vec();
        Self::new_with_memo(from_address, to_address, value, empty_memo)
    }

    pub fn new_with_memo(from: Address, to: Address, value: u64, memo: Vec<u8>) -> Result<Self, TransferError> {
        if value == 0 {
           Err(TransferError::InsufficientFunds)
        }
        else if memo.len() > MAX_MEMO_SIZE {
            Err(TransferError::InvalidMemoSize)
        }
        else if from.is_empty() {
            Err(TransferError::InvalidFromAddress)
        }
        else if to.is_empty() {
            Err(TransferError::InvalidToAddress)
        }
        else if from == to {
            Err(TransferError::DuplicateAddress)
        }
        else {
            Ok(Self {
                from_address: from,
                to_address: to,
                value,
                memo,
            })
        }
    }
}