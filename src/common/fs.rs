use std::io;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[async_trait::async_trait]
pub trait FileHandle: Send + Sync {
    async fn write_all(&mut self, data: &[u8]) -> io::Result<()>;
    async fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize>;
    async fn flush(&mut self) -> io::Result<()>;
    async fn sync_all(&mut self) -> io::Result<()>;
    async fn rewind(&mut self) -> io::Result<()>;
}

#[async_trait::async_trait]
pub trait FileSystem: Send + Sync {
    async fn create_or_append(&self, path: &str) -> io::Result<Box<dyn FileHandle>>;
}

pub struct TokioFileHandle {
    file: tokio::fs::File,
}

#[async_trait::async_trait]
impl FileHandle for TokioFileHandle {
    async fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        self.file.write_all(data).await
    }

    async fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.file.read_to_end(buf).await
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.file.flush().await
    }

    async fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all().await
    }

    async fn rewind(&mut self) -> io::Result<()> {
        self.file.rewind().await?;
        Ok(())
    }
}

pub struct TokioFileSystem;

#[async_trait::async_trait]
impl FileSystem for TokioFileSystem {
    async fn create_or_append(&self, path: &str) -> io::Result<Box<dyn FileHandle>> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .await?;
        Ok(Box::new(TokioFileHandle { file }))
    }
}
