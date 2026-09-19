use std::io;
use std::sync::Arc;

use crate::common::fs::{FileHandle, FileSystem};

#[async_trait::async_trait]
pub trait WalStorage: Send + Sync {
    async fn append(&mut self, data: &[u8]) -> io::Result<()>;
    async fn read_all(&mut self) -> io::Result<Vec<u8>>;
    async fn truncate(&mut self, len: usize) -> io::Result<()>;
}

pub struct Wal {
    file_handle: Box<dyn FileHandle>,
}

impl Wal {
    pub async fn new(path: &str, fs: Arc<dyn FileSystem>) -> Self {
        let file = fs
            .create_or_append(path)
            .await
            .expect("Failed to open WAL file");
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

    async fn truncate(&mut self, len: usize) -> io::Result<()> {
        self.file_handle.truncate(len).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::fs::{FileHandle, FileSystem};
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    struct InMemoryFileHandle {
        path: String,
        storage: Arc<StdMutex<HashMap<String, Vec<u8>>>>,
        cursor: io::Cursor<Vec<u8>>,
    }

    #[async_trait::async_trait]
    impl FileHandle for InMemoryFileHandle {
        async fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
            std::io::Write::write_all(&mut self.cursor, data)
        }

        async fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
            std::io::Read::read_to_end(&mut self.cursor, buf)
        }

        async fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        async fn sync_all(&mut self) -> io::Result<()> {
            let mut storage = self.storage.lock().unwrap();
            storage.insert(self.path.clone(), self.cursor.get_ref().clone());
            Ok(())
        }

        async fn rewind(&mut self) -> io::Result<()> {
            std::io::Seek::rewind(&mut self.cursor)
        }

        async fn truncate(&mut self, len: usize) -> io::Result<()> {
            let mut data = self.cursor.get_ref().clone();
            data.truncate(len);
            let position = std::io::Seek::stream_position(&mut self.cursor)?.min(len as u64);
            self.cursor = io::Cursor::new(data);
            std::io::Seek::seek(&mut self.cursor, io::SeekFrom::Start(position))?;
            Ok(())
        }
    }

    struct InMemoryFileSystem {
        storage: Arc<StdMutex<HashMap<String, Vec<u8>>>>,
    }

    impl InMemoryFileSystem {
        fn new() -> Self {
            Self {
                storage: Arc::new(StdMutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl FileSystem for InMemoryFileSystem {
        async fn create_or_append(&self, path: &str) -> io::Result<Box<dyn FileHandle>> {
            let data = {
                let storage = self.storage.lock().unwrap();
                storage.get(path).cloned().unwrap_or_default()
            };
            let len = data.len() as u64;
            let mut cursor = io::Cursor::new(data);
            cursor.set_position(len);
            Ok(Box::new(InMemoryFileHandle {
                path: path.to_string(),
                storage: self.storage.clone(),
                cursor,
            }))
        }
    }

    async fn make_wal() -> Wal {
        let fs = Arc::new(InMemoryFileSystem::new());
        Wal::new("wal.log", fs).await
    }

    #[tokio::test]
    async fn test_append_and_read_all() {
        let mut wal = make_wal().await;
        wal.append(b"hello").await.unwrap();
        let data = wal.read_all().await.unwrap();
        assert_eq!(data, b"hello");
    }

    #[tokio::test]
    async fn test_append_accumulates_data() {
        let mut wal = make_wal().await;
        wal.append(b"aaa").await.unwrap();
        wal.append(b"bbb").await.unwrap();
        let data = wal.read_all().await.unwrap();
        assert_eq!(data, b"aaabbb");
    }

    #[tokio::test]
    async fn test_read_all_is_idempotent() {
        let mut wal = make_wal().await;
        wal.append(b"data").await.unwrap();
        let first = wal.read_all().await.unwrap();
        let second = wal.read_all().await.unwrap();
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn test_read_all_empty_wal() {
        let mut wal = make_wal().await;
        let data = wal.read_all().await.unwrap();
        assert!(data.is_empty());
    }
}
