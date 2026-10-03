pub const OP_SET: u8 = 0;
pub const OP_DELETE: u8 = 1;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Entry<'a> {
    pub index: u64,
    pub op: u8,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> Entry<'a> {
    pub fn set(index: u64, key: &'a [u8], value: &'a [u8]) -> Self {
        Self {
            index,
            op: OP_SET,
            key,
            value,
        }
    }

    pub fn delete(index: u64, key: &'a [u8]) -> Self {
        Self {
            index,
            op: OP_DELETE,
            key,
            value: &[],
        }
    }
}
