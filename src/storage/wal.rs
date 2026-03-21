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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::fs::{FileHandle, FileSystem};
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

        async fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
            std::io::Seek::seek(&mut self.cursor, pos)
        }

        async fn rewind(&mut self) -> io::Result<()> {
            std::io::Seek::rewind(&mut self.cursor)
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
        async fn open_read(&self, path: &str) -> io::Result<Box<dyn FileHandle>> {
            let data = {
                let storage = self.storage.lock().unwrap();
                storage
                    .get(path)
                    .cloned()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?
            };
            Ok(Box::new(InMemoryFileHandle {
                path: path.to_string(),
                storage: self.storage.clone(),
                cursor: io::Cursor::new(data),
            }))
        }

        async fn create_or_truncate(&self, path: &str) -> io::Result<Box<dyn FileHandle>> {
            Ok(Box::new(InMemoryFileHandle {
                path: path.to_string(),
                storage: self.storage.clone(),
                cursor: io::Cursor::new(Vec::new()),
            }))
        }

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

        async fn rename(&self, from: &str, to: &str) -> io::Result<()> {
            let mut storage = self.storage.lock().unwrap();
            let data = storage
                .remove(from)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
            storage.insert(to.to_string(), data);
            Ok(())
        }

        async fn remove_file(&self, path: &str) -> io::Result<()> {
            let mut storage = self.storage.lock().unwrap();
            storage.remove(path);
            Ok(())
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

    #[tokio::test]
    async fn test_rotate_returns_empty_new_wal() {
        let mut wal = make_wal().await;
        wal.append(b"old data").await.unwrap();
        let mut new_wal = wal.rotate("wal.log.tmp").await.unwrap();
        let data = new_wal.read_all().await.unwrap();
        assert!(data.is_empty());
    }

    #[tokio::test]
    async fn test_rotate_old_wal_retains_data() {
        let mut wal = make_wal().await;
        wal.append(b"old data").await.unwrap();
        let _ = wal.rotate("wal.log.tmp").await.unwrap();
        // wal now points to wal.log.tmp (the renamed file)
        let data = wal.read_all().await.unwrap();
        assert_eq!(data, b"old data");
    }

    #[tokio::test]
    async fn test_rotate_new_wal_accepts_writes() {
        let mut wal = make_wal().await;
        wal.append(b"old").await.unwrap();
        let mut new_wal = wal.rotate("wal.log.tmp").await.unwrap();
        new_wal.append(b"new").await.unwrap();
        let data = new_wal.read_all().await.unwrap();
        assert_eq!(data, b"new");
    }

    #[tokio::test]
    async fn test_open_read_existing_file() {
        let mut wal = make_wal().await;
        wal.append(b"persisted").await.unwrap();
        // rotate moves data to wal.log.tmp
        let _ = wal.rotate("wal.log.tmp").await.unwrap();
        let mut reader = wal.open_read("wal.log.tmp").await.unwrap();
        let data = reader.read_all().await.unwrap();
        assert_eq!(data, b"persisted");
    }

    #[tokio::test]
    async fn test_open_read_nonexistent_file() {
        let wal = make_wal().await;
        let err = wal.open_read("nonexistent.log").await.err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn test_remove_deletes_file() {
        let fs = Arc::new(InMemoryFileSystem::new());
        let mut wal = Wal::new("wal.log", fs.clone()).await;
        wal.append(b"data").await.unwrap();
        wal.remove().await.unwrap();
        let err = fs.open_read("wal.log").await.err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }
}
