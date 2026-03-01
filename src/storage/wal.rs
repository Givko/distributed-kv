use crate::storage::encoder::Encoder;
use crate::storage::entry::Entry;
use std::io;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[async_trait::async_trait]
pub trait WalStorage: Send + Sync {
    async fn append(&mut self, entry: &Entry) -> io::Result<()>;
    async fn read_all(&mut self) -> io::Result<Vec<Entry>>;
}

pub struct Wal {
    file_handle: File,
}

impl Wal {
    pub async fn new(path: PathBuf) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .await
            .expect("Failed to open WAL file");
        Self { file_handle: file }
    }
}

#[async_trait::async_trait]
impl WalStorage for Wal {
    async fn append(&mut self, entry: &Entry) -> io::Result<()> {
        let encoded = Encoder::encode(entry);

        self.file_handle.write_all(&encoded).await?;
        self.file_handle.flush().await?;
        self.file_handle.sync_all().await?;
        Ok(())
    }

    async fn read_all(&mut self) -> io::Result<Vec<Entry>> {
        let mut buffer = Vec::new();
        self.file_handle.rewind().await?;
        self.file_handle.read_to_end(&mut buffer).await?;

        let cursor = 0;
        let entries = Encoder::decode_all(&buffer[cursor..])?;
        self.file_handle.rewind().await?;
        Ok(entries)
    }
}
