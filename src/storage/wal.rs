use std::io;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[async_trait::async_trait]
pub trait WalStorage: Send + Sync {
    async fn append(&mut self, data: &[u8]) -> io::Result<()>;
    async fn read_all(&mut self) -> io::Result<Vec<u8>>;
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

    pub(super) fn from_file(file: File) -> Self {
        Self { file_handle: file }
    }
}

#[async_trait::async_trait]
impl WalStorage for Wal {
    async fn append(&mut self, data: &[u8]) -> io::Result<()> {
        self.file_handle.write_all(data).await?;
        self.file_handle.flush().await?;
        self.file_handle.sync_all().await?;
        Ok(())
    }

    async fn read_all(&mut self) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.file_handle.rewind().await?;
        self.file_handle.read_to_end(&mut buffer).await?;
        self.file_handle.rewind().await?;
        Ok(buffer)
    }
}
