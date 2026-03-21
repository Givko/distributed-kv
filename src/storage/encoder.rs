use crate::storage::entry::Entry;
use std::io::{self, Error, ErrorKind};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Encoder;

impl Encoder {
    pub fn decode(data: &[u8]) -> io::Result<(Entry, u64)> {
        //binary format is [index (8 bytes)][op (1 byte)][key length (4 bytes)][key][value length (4 bytes)][value]
        let mut cursor = 0;
        if cursor + 8 > data.len() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Corrupted WAL entry index",
            ));
        }

        let index = u64::from_be_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        if cursor + 1 > data.len() {
            return Err(Error::new(ErrorKind::InvalidData, "Corrupted WAL entry op"));
        }
        let op = data[cursor];
        cursor += 1;
        if cursor + 4 > data.len() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Corrupted WAL entry key length",
            ));
        }
        let key_len = u32::from_be_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if cursor + key_len > data.len() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Corrupted WAL entry key",
            ));
        }
        let key = data[cursor..cursor + key_len].to_vec();
        cursor += key_len;
        if cursor + 4 > data.len() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Corrupted WAL entry value length",
            ));
        }
        let value_len = u32::from_be_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if cursor + value_len > data.len() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Corrupted WAL entry value",
            ));
        }
        let value = data[cursor..cursor + value_len].to_vec();
        cursor += value_len;
        Ok((
            Entry {
                index,
                op,
                key,
                value,
            },
            cursor as u64,
        ))
    }

    pub fn encode(entry: &Entry) -> Vec<u8> {
        let mut encoded =
            Vec::with_capacity(4 + 8 + 1 + 4 + entry.key.len() + 4 + entry.value.len());
        Self::encode_into(entry, &mut encoded);
        encoded
    }

    // Returns the number of bytes written
    pub(super) fn encode_into(entry: &Entry, bytes: &mut Vec<u8>) -> usize {
        let key_len = entry.key.len() as u32;
        let value_len = entry.value.len() as u32;
        let index = entry.index.to_be_bytes();
        let op = &entry.op;
        let key_len_bytes = key_len.to_be_bytes();
        let key = &entry.key;
        let value_len_bytes = value_len.to_be_bytes();
        let value = &entry.value;
        let len: u32 = 8 + 1 + 4 + key_len + 4 + value_len;

        bytes.extend_from_slice(&len.to_be_bytes());
        bytes.extend_from_slice(&index);
        bytes.push(*op);
        bytes.extend_from_slice(&key_len_bytes);
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&value_len_bytes);
        bytes.extend_from_slice(value);

        4 + len as usize
    }

    pub fn decode_all(data: &[u8]) -> io::Result<Vec<Entry>> {
        let mut entries = Vec::new();
        let mut cursor = 0;
        while cursor < data.len() {
            // Read the record length prefix
            if cursor + 4 > data.len() {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Corrupted WAL entry: missing length prefix",
                ));
            }

            let len = u32::from_be_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if cursor + len > data.len() {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Corrupted WAL entry: length prefix too large",
                ));
            }
            let (entry, bytes_read) = Encoder::decode(&data[cursor..cursor + len])?;
            if bytes_read as usize != len {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "WAL entry length mismatch",
                ));
            }
            cursor += len;
            entries.push(entry);
        }
        if cursor != data.len() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Trailing bytes after last WAL entry",
            ));
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::{Entry, OP_DELETE, OP_SET};

    #[tokio::test]
    async fn test_decode_set_entry() {
        // index = 42, op = OP_SET, key = b"foo", value = b"bar"
        let mut body = Vec::new();
        body.extend_from_slice(&42u64.to_be_bytes()); // index
        body.push(OP_SET); // op
        body.extend_from_slice(&(3u32.to_be_bytes())); // key len
        body.extend_from_slice(b"foo"); // key
        body.extend_from_slice(&(3u32.to_be_bytes())); // value len
        body.extend_from_slice(b"bar"); // value
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&body);
        let entries = Encoder::decode_all(&bytes).unwrap();
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.index, 42);
        assert_eq!(entry.op, OP_SET);
        assert_eq!(entry.key, b"foo");
        assert_eq!(entry.value, b"bar");
    }

    #[tokio::test]
    async fn test_decode_delete_entry() {
        // index = 7, op = OP_DELETE, key = b"baz", value = empty
        let mut body = Vec::new();
        body.extend_from_slice(&7u64.to_be_bytes()); // index
        body.push(OP_DELETE); // op
        body.extend_from_slice(&(3u32.to_be_bytes())); // key len
        body.extend_from_slice(b"baz"); // key
        body.extend_from_slice(&(0u32.to_be_bytes())); // value len
        // no value
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&body);
        let entries = Encoder::decode_all(&bytes).unwrap();
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.index, 7);
        assert_eq!(entry.op, OP_DELETE);
        assert_eq!(entry.key, b"baz");
        assert_eq!(entry.value, b"");
    }

    #[tokio::test]
    async fn test_decode_too_short() {
        let bytes = vec![0, 1, 2];
        let err = Encoder::decode_all(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn test_decode_corrupt_key_len() {
        // index + op + key_len (but not enough bytes for key)
        let mut body = Vec::new();
        body.extend_from_slice(&1u64.to_be_bytes());
        body.push(OP_SET);
        body.extend_from_slice(&(10u32.to_be_bytes())); // key_len = 10
        // only 2 bytes of key
        body.extend_from_slice(&[1, 2]);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&body);
        let err = Encoder::decode_all(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn test_decode_corrupt_value_len() {
        // index + op + key_len + key + value_len (but not enough bytes for value)
        let mut body = Vec::new();
        body.extend_from_slice(&1u64.to_be_bytes());
        body.push(OP_SET);
        body.extend_from_slice(&(2u32.to_be_bytes())); // key_len = 2
        body.extend_from_slice(b"ab");
        body.extend_from_slice(&(5u32.to_be_bytes())); // value_len = 5
        body.extend_from_slice(b"xyz"); // only 3 bytes
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&body);
        let err = Encoder::decode_all(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn test_decode_all_multiple_entries() {
        let mut bytes = Vec::new();
        // Entry 1: SET index=1, key="a", value="x"
        let mut body1 = Vec::new();
        body1.extend_from_slice(&1u64.to_be_bytes());
        body1.push(OP_SET);
        body1.extend_from_slice(&(1u32.to_be_bytes()));
        body1.extend_from_slice(b"a");
        body1.extend_from_slice(&(1u32.to_be_bytes()));
        body1.extend_from_slice(b"x");
        bytes.extend_from_slice(&(body1.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&body1);
        // Entry 2: DELETE index=2, key="b"
        let mut body2 = Vec::new();
        body2.extend_from_slice(&2u64.to_be_bytes());
        body2.push(OP_DELETE);
        body2.extend_from_slice(&(1u32.to_be_bytes()));
        body2.extend_from_slice(b"b");
        body2.extend_from_slice(&(0u32.to_be_bytes()));
        bytes.extend_from_slice(&(body2.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&body2);
        // Entry 3: SET index=3, key="c", value="yz"
        let mut body3 = Vec::new();
        body3.extend_from_slice(&3u64.to_be_bytes());
        body3.push(OP_SET);
        body3.extend_from_slice(&(1u32.to_be_bytes()));
        body3.extend_from_slice(b"c");
        body3.extend_from_slice(&(2u32.to_be_bytes()));
        body3.extend_from_slice(b"yz");
        bytes.extend_from_slice(&(body3.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&body3);
        let entries = Encoder::decode_all(&bytes).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], Entry::set(1, b"a".to_vec(), b"x".to_vec()));
        assert_eq!(entries[1], Entry::delete(2, b"b".to_vec()));
        assert_eq!(entries[2], Entry::set(3, b"c".to_vec(), b"yz".to_vec()));
    }

    #[tokio::test]
    async fn test_decode_all_trailing_corrupt_entry() {
        // One valid entry, then incomplete second entry
        let mut bytes = Vec::new();
        let mut body1 = Vec::new();
        body1.extend_from_slice(&1u64.to_be_bytes());
        body1.push(OP_SET);
        body1.extend_from_slice(&(1u32.to_be_bytes()));
        body1.extend_from_slice(b"a");
        body1.extend_from_slice(&(1u32.to_be_bytes()));
        body1.extend_from_slice(b"x");
        bytes.extend_from_slice(&(body1.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&body1);
        // Incomplete entry (only 3 bytes, not enough for length prefix)
        bytes.extend_from_slice(&[1, 2, 3]);
        let err = Encoder::decode_all(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn test_decode_all_with_encode() {
        let entries = vec![
            Entry::set(1, b"foo".to_vec(), b"bar".to_vec()),
            Entry::delete(2, b"baz".to_vec()),
            Entry::set(3, b"x".to_vec(), b"y".to_vec()),
        ];
        let mut bytes = Vec::new();
        for entry in &entries {
            bytes.extend_from_slice(&Encoder::encode(entry));
        }
        let decoded = Encoder::decode_all(&bytes).unwrap();
        assert_eq!(decoded, entries);
    }

    #[tokio::test]
    async fn test_decode_with_encode() {
        let entry = Entry::set(42, b"abc".to_vec(), b"defg".to_vec());
        let bytes = Encoder::encode(&entry);
        let decoded = Encoder::decode_all(&bytes).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], entry);
    }
}
