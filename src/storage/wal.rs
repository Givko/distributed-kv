use std::io;
use std::sync::Arc;

use crate::storage::fs::{FileHandle, FileSystem};

#[async_trait::async_trait]
pub trait WalStorage: Send + Sync {
    async fn append(&mut self, data: &[u8]) -> io::Result<()>;
    async fn read_all(&mut self) -> io::Result<Vec<u8>>;
    async fn rotate(&mut self, flush_path: &str) -> io::Result<Box<dyn WalStorage + Send + Sync>>;
    async fn open_read(&self, path: &str) -> io::Result<Box<dyn WalStorage + Send + Sync>>;
    async fn remove(&self) -> io::Result<()>;
}

pub struct Wal {
    file_handle: Box<dyn FileHandle>,
    path: String,
    fs: Arc<dyn FileSystem>,
}

impl Wal {
    pub(super) async fn new(path: &str, fs: Arc<dyn FileSystem>) -> Self {
        let file = fs
            .create_or_append(path)
            .await
            .expect("Failed to open WAL file");
        Self {
            file_handle: file,
            path: path.to_string(),
            fs,
        }
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

    async fn rotate(&mut self, flush_path: &str) -> io::Result<Box<dyn WalStorage + Send + Sync>> {
        self.fs.rename(&self.path, flush_path).await?;
        let original_path = self.path.clone();
        self.path = flush_path.to_string();
        let new_wal = Wal::new(&original_path, self.fs.clone()).await;
        Ok(Box::new(new_wal))
    }

    async fn open_read(&self, path: &str) -> io::Result<Box<dyn WalStorage + Send + Sync>> {
        let file = self.fs.open_read(path).await?;
        Ok(Box::new(Wal {
            file_handle: file,
            path: path.to_string(),
            fs: self.fs.clone(),
        }))
    }

    async fn remove(&self) -> io::Result<()> {
        self.fs.remove_file(&self.path).await
    }
}
