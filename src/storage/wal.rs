use std::io::{self, Error, ErrorKind};
use std::path::PathBuf;
use std::u64;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub const OP_SET: u8 = 0;
pub const OP_DELETE: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct WalEntry {
    pub index: u64,
    pub op: u8,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl WalEntry {
    pub fn set(index: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        WalEntry { index, op: OP_SET, key, value }
    }

    pub fn delete(index: u64, key: Vec<u8>) -> Self {
        WalEntry { index, op: OP_DELETE, key, value: vec![] }
    }
}

#[async_trait::async_trait]
pub trait WalStorage: Send + Sync {
    async fn append(&self, entry: &WalEntry) -> io::Result<()>;
    async fn read_all(&self) -> io::Result<Vec<WalEntry>>;
}

pub struct Wal {
    magic_number: u32,
    path: PathBuf,
}

impl Wal {
    pub fn new(path: PathBuf) -> Self {
        Self {
            magic_number: 0xDEADBEEF,
            path,
        }
    }

    async fn append_impl(&self, entry: &WalEntry) -> io::Result<()> {
        eprintln!("Appending WAL entry: index={}, op={}, key={:?}, value={:?} to {:?}", 
        entry.index, entry.op, String::from_utf8_lossy(&entry.key), String::from_utf8_lossy(&entry.value), self.path);
        let encoded = self.encode(entry);
        eprintln!("Encoded WAL entry as {} bytes: {:?}", encoded.len(), encoded);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        
        eprintln!("Opened WAL file {:?} for appending", self.path);
        //self.check_magic_number(&mut file).await?;
        eprintln!("WAL file {:?} has valid magic number", self.path);
        file.write_all(&encoded).await?;
        file.flush().await?;
        file.sync_all().await?;
        Ok(())
    }

    // Returns (is_new_file, is_magic_number_correct)
    async fn check_magic_number(&self, file: &mut File) -> io::Result<(bool,bool)> {
        let mut magic_bytes = [0u8; 4];
        file.read_exact(&mut magic_bytes).await?;
        if magic_bytes.iter().all(|&b| b == 0) {
            return Ok((true, true));
        }

        let read_magic_number = u32::from_be_bytes(magic_bytes);
        if read_magic_number != self.magic_number {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid WAL file"));
        }
        Ok((false, true))
    }

    async fn read_all_impl(&self) -> io::Result<Vec<WalEntry>> {
        let mut file = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .await?;

        //self.check_magic_number(&mut file).await?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        let cursor = 0; // Skip the magic number
        let entries = self.decode_all(&buffer[cursor..])?;
        Ok(entries)
    }

    fn decode_all(&self, data: &[u8]) -> io::Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut cursor = 0;
        eprintln!("Decoding WAL entries from {} bytes of data data: {:?}", data.len(), data);
        while cursor < data.len() {
            let (entry, bytes_read) = self.decode(&data[cursor..])?;
            cursor += bytes_read as usize;
            entries.push(entry);
        }
        Ok(entries)
    }

    fn decode (&self, data: &[u8]) -> io::Result<(WalEntry, u64)> {
        //binary format is [index (8 bytes)][op (1 byte)][key length (4 bytes)][key][value length (4 bytes)][value]
        let mut cursor = 0;
        if cursor + 8 > data.len() {
            return Err(Error::new(ErrorKind::InvalidData, "Corrupted WAL entry index"));
        }

        let index = u64::from_be_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        if cursor + 1 > data.len() {
            return Err(Error::new(ErrorKind::InvalidData, "Corrupted WAL entry op"));
        }
        let op = data[cursor];
        cursor += 1;
        if cursor + 4 > data.len() {
            return Err(Error::new(ErrorKind::InvalidData, "Corrupted WAL entry key length"));
        }
        let key_len = u32::from_be_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if cursor + key_len > data.len() {
            return Err(Error::new(ErrorKind::InvalidData, "Corrupted WAL entry key"));
        }
        let key = data[cursor..cursor + key_len].to_vec();
        cursor += key_len;
        if cursor + 4 > data.len() {
            return Err(Error::new(ErrorKind::InvalidData, "Corrupted WAL entry value length"));
        }
        let value_len = u32::from_be_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if cursor + value_len > data.len() {
            return Err(Error::new(ErrorKind::InvalidData, "Corrupted WAL entry value"));
        }
        let value = data[cursor..cursor + value_len].to_vec();
        cursor += value_len;
        Ok((WalEntry { index, op, key, value }, cursor as u64))

    }

    fn encode(&self, entry: &WalEntry) -> Vec<u8> {
        let mut encoded = Vec::new();
        let key_len = entry.key.len() as u32;
        let value_len = entry.value.len() as u32;

        encoded.extend_from_slice(&entry.index.to_be_bytes());
        encoded.push(entry.op);
        encoded.extend_from_slice(&key_len.to_be_bytes());
        encoded.extend_from_slice(&entry.key);
        encoded.extend_from_slice(&value_len.to_be_bytes());
        encoded.extend_from_slice(&entry.value);

        encoded
    }
}

#[async_trait::async_trait]
impl WalStorage for Wal {
    async fn append(&self, entry: &WalEntry) -> io::Result<()> {
        self.append_impl(entry).await
    }

    async fn read_all(&self) -> io::Result<Vec<WalEntry>> {
        self.read_all_impl().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_wal() -> Wal {
        Wal::new(PathBuf::from("/dev/null"))
    }

    #[test]
    fn test_decode_set_entry() {
        // index = 42, op = OP_SET, key = b"foo", value = b"bar"
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&42u64.to_be_bytes()); // index
        bytes.push(OP_SET); // op
        bytes.extend_from_slice(&(3u32.to_be_bytes())); // key len
        bytes.extend_from_slice(b"foo"); // key
        bytes.extend_from_slice(&(3u32.to_be_bytes())); // value len
        bytes.extend_from_slice(b"bar"); // value
        let (entry, used) = make_wal().decode(&bytes).unwrap();
        assert_eq!(used, bytes.len() as u64);
        assert_eq!(entry.index, 42);
        assert_eq!(entry.op, OP_SET);
        assert_eq!(entry.key, b"foo");
        assert_eq!(entry.value, b"bar");
    }

    #[test]
    fn test_decode_delete_entry() {
        // index = 7, op = OP_DELETE, key = b"baz", value = empty
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&7u64.to_be_bytes()); // index
        bytes.push(OP_DELETE); // op
        bytes.extend_from_slice(&(3u32.to_be_bytes())); // key len
        bytes.extend_from_slice(b"baz"); // key
        bytes.extend_from_slice(&(0u32.to_be_bytes())); // value len
        // no value
        let (entry, used) = make_wal().decode(&bytes).unwrap();
        assert_eq!(used, bytes.len() as u64);
        assert_eq!(entry.index, 7);
        assert_eq!(entry.op, OP_DELETE);
        assert_eq!(entry.key, b"baz");
        assert_eq!(entry.value, b"");
    }

    #[test]
    fn test_decode_too_short() {
        let bytes = vec![0, 1, 2];
        let err = make_wal().decode(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_decode_corrupt_key_len() {
        // index + op + key_len (but not enough bytes for key)
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_be_bytes());
        bytes.push(OP_SET);
        bytes.extend_from_slice(&(10u32.to_be_bytes())); // key_len = 10
        // only 2 bytes of key
        bytes.extend_from_slice(&[1, 2]);
        let err = make_wal().decode(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_decode_corrupt_value_len() {
        // index + op + key_len + key + value_len (but not enough bytes for value)
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_be_bytes());
        bytes.push(OP_SET);
        bytes.extend_from_slice(&(2u32.to_be_bytes())); // key_len = 2
        bytes.extend_from_slice(b"ab");
        bytes.extend_from_slice(&(5u32.to_be_bytes())); // value_len = 5
        bytes.extend_from_slice(b"xyz"); // only 3 bytes
        let err = make_wal().decode(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_decode_all_multiple_entries() {
        let wal = super::Wal::new(PathBuf::from("/dev/null"));
        // Entry 1: SET index=1, key="a", value="x"
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_be_bytes());
        bytes.push(OP_SET);
        bytes.extend_from_slice(&(1u32.to_be_bytes()));
        bytes.extend_from_slice(b"a");
        bytes.extend_from_slice(&(1u32.to_be_bytes()));
        bytes.extend_from_slice(b"x");
        // Entry 2: DELETE index=2, key="b"
        bytes.extend_from_slice(&2u64.to_be_bytes());
        bytes.push(OP_DELETE);
        bytes.extend_from_slice(&(1u32.to_be_bytes()));
        bytes.extend_from_slice(b"b");
        bytes.extend_from_slice(&(0u32.to_be_bytes()));
        // Entry 3: SET index=3, key="c", value="yz"
        bytes.extend_from_slice(&3u64.to_be_bytes());
        bytes.push(OP_SET);
        bytes.extend_from_slice(&(1u32.to_be_bytes()));
        bytes.extend_from_slice(b"c");
        bytes.extend_from_slice(&(2u32.to_be_bytes()));
        bytes.extend_from_slice(b"yz");
        let entries = wal.decode_all(&bytes).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], WalEntry::set(1, b"a".to_vec(), b"x".to_vec()));
        assert_eq!(entries[1], WalEntry::delete(2, b"b".to_vec()));
        assert_eq!(entries[2], WalEntry::set(3, b"c".to_vec(), b"yz".to_vec()));
    }

    #[test]
    fn test_decode_all_trailing_corrupt_entry() {
        let wal = super::Wal::new(PathBuf::from("/dev/null"));
        // One valid entry, then incomplete second entry
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_be_bytes());
        bytes.push(OP_SET);
        bytes.extend_from_slice(&(1u32.to_be_bytes()));
        bytes.extend_from_slice(b"a");
        bytes.extend_from_slice(&(1u32.to_be_bytes()));
        bytes.extend_from_slice(b"x");
        // Incomplete entry (only 3 bytes)
        bytes.extend_from_slice(&[1, 2, 3]);
        let err = wal.decode_all(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_decode_all_with_encode() {
        let wal = super::Wal::new(PathBuf::from("/dev/null"));
        let entries = vec![
            WalEntry::set(1, b"foo".to_vec(), b"bar".to_vec()),
            WalEntry::delete(2, b"baz".to_vec()),
            WalEntry::set(3, b"x".to_vec(), b"y".to_vec()),
        ];
        let mut bytes = Vec::new();
        for entry in &entries {
            bytes.extend_from_slice(&wal.encode(entry));
        }
        let decoded = wal.decode_all(&bytes).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn test_decode_with_encode() {
        let wal = super::Wal::new(PathBuf::from("/dev/null"));
        let entry = WalEntry::set(42, b"abc".to_vec(), b"defg".to_vec());
        let bytes = wal.encode(&entry);
        let (decoded, used) = wal.decode(&bytes).unwrap();
        assert_eq!(decoded, entry);
        assert_eq!(used, bytes.len() as u64);
    }
}
