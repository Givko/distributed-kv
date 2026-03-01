pub const OP_SET: u8 = 0;
pub const OP_DELETE: u8 = 1;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Entry {
    pub index: u64,
    pub op: u8,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Entry {
    pub fn set(index: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            index,
            op: OP_SET,
            key,
            value,
        }
    }

    pub fn delete(index: u64, key: Vec<u8>) -> Self {
        Self {
            index,
            op: OP_DELETE,
            key,
            value: Vec::new(),
        }
    }
}
