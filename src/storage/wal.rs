use std::io::{self, Error, ErrorKind};
use std::path::PathBuf;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
        let encoded = self.encode(entry);
        let file_exists = fs::metadata(&self.path).await.is_ok();

        if file_exists {
            let mut file = OpenOptions::new().read(true).open(&self.path).await?;
            self.check_magic_number(&mut file).await?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        
        // When creating the file set the magic number in the beginning fo the encoded data
        if !file_exists {
            let mut magic_bytes = Vec::new();
            magic_bytes.extend_from_slice(&self.magic_number.to_be_bytes());
            file.write_all(&magic_bytes).await?;
        }

        file.write_all(&encoded).await?;
        file.flush().await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn check_magic_number(&self, file: &mut File) -> io::Result<()> {
        let mut magic_bytes = [0u8; 4];
        file.read_exact(&mut magic_bytes).await?;
        let read_magic_number = u32::from_be_bytes(magic_bytes);
        if read_magic_number != self.magic_number {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid WAL file"));
        }
        Ok(())
    }

    async fn read_all_impl(&self) -> io::Result<Vec<WalEntry>> {
        let mut file = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .await?;

        self.check_magic_number(&mut file).await?;

        let mut entries = Vec::new();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        let mut cursor = 4; // Skip the magic number
        while cursor < buffer.len() {
            if cursor + 8 > buffer.len() {
                return Err(Error::new(ErrorKind::InvalidData, "Corrupted WAL entry index"));
            }
            let index = u64::from_be_bytes(buffer[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;

            if cursor + 5 > buffer.len() {
                return Err(Error::new(ErrorKind::InvalidData, "Corrupted WAL entry header"));
            }
            let op = buffer[cursor];
            cursor += 1;

            let key_len = u32::from_be_bytes(buffer[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if cursor + key_len > buffer.len() {
                return Err(Error::new(ErrorKind::InvalidData, "Corrupted WAL key length"));
            }
            let key = buffer[cursor..cursor + key_len].to_vec();
            cursor += key_len;
            if cursor + 4 > buffer.len() {
                return Err(Error::new(ErrorKind::InvalidData, "Corrupted WAL value header"));
            }

            let value_len = u32::from_be_bytes(buffer[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if cursor + value_len > buffer.len() {
                return Err(Error::new(ErrorKind::InvalidData, "Corrupted WAL value length"));
            }
            let value = buffer[cursor..cursor + value_len].to_vec();
            cursor += value_len;

            entries.push(WalEntry { index, op, key, value });
        }

        Ok(entries)
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
