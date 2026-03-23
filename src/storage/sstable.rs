use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use crate::storage::bloom_filter::{BloomFilter, SimpleBloomFilter};
use crate::storage::encoder::Encoder;
use crate::storage::fs::FileSystem;

#[async_trait::async_trait]
pub(super) trait SSTablesStorage {
    async fn flush(
        &mut self,
        data: &[u8],
        sparse_index: &[(Vec<u8>, u64)],
        min_key: Vec<u8>,
        max_key: Vec<u8>,
        bloom_filter: SimpleBloomFilter,
    ) -> io::Result<()>;
    #[allow(dead_code)]
    async fn read(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>>;
}

#[derive(Serialize, Deserialize)]
pub(super) struct SSTableFileMetadata {
    // non decreasing file ID for each new SSTable file
    // This can be used to determine the order of SSTable files and to manage them effectively.
    file_id: u64,
    file_path: String,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    size_in_bytes: u64,

    // Key, offset pairs for faster lookups
    sparse_index: PathBuf,
    bloom_filter: SimpleBloomFilter,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Metadata {
    cur_max_file_id: u64,
    // Using BTreeMap to maintain sorted order of SSTable files based on their file_id
    // This allows for efficient lookups and reading in order from the newest to the oldest SSTable file.
    sstable_file_metadata: BTreeMap<Reverse<u64>, SSTableFileMetadata>,
}

pub(super) struct SSTableStorageManager {
    metadata: Metadata,
    fs: Arc<dyn FileSystem>,
}

impl SSTableStorageManager {
    pub(super) async fn new(fs: Arc<dyn FileSystem>) -> Self {
        let metadata = match fs.open_read("metadata.dat").await {
            Ok(mut file) => {
                let mut buffer = Vec::new();
                if let Err(e) = file.read_to_end(&mut buffer).await {
                    eprintln!("Failed to read metadata file: {}", e);
                    Metadata {
                        cur_max_file_id: 0,
                        sstable_file_metadata: BTreeMap::new(),
                    }
                } else {
                    match serde_json::from_slice(&buffer) {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("Failed to deserialize metadata: {}", e);
                            Metadata {
                                cur_max_file_id: 0,
                                sstable_file_metadata: BTreeMap::new(),
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "Metadata file not found, starting with empty metadata. Error: {}",
                    e
                );
                Metadata {
                    cur_max_file_id: 0,
                    sstable_file_metadata: BTreeMap::new(),
                }
            }
        };
        Self { metadata, fs }
    }

    async fn persist_metadata(&self) -> io::Result<()> {
        let tmp_file_path = "metadata.dat.tmp";
        let mut file = self.fs.create_or_truncate(tmp_file_path).await?;

        // Using json for simplicity, but in a production system, you might want to use a more efficient binary format
        let serialized_state = serde_json::to_string(&self.metadata)?;
        if let Err(e) = file.write_all(serialized_state.as_bytes()).await {
            eprintln!("Failed to write metadata: {}", e);
            return Err(e);
        }
        if let Err(e) = file.sync_all().await {
            eprintln!("Failed to sync metadata file: {}", e);
            return Err(e);
        }
        if let Err(e) = self.fs.rename(tmp_file_path, "metadata.dat").await {
            eprintln!("Failed to rename metadata file: {}", e);
            return Err(e);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl SSTablesStorage for SSTableStorageManager {
    async fn read(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        for metadata in self.metadata.sstable_file_metadata.values() {
            if key < metadata.min_key.as_slice() || key > metadata.max_key.as_slice() {
                eprintln!(
                    "Skipping SSTable file: {} as key is out of range ({} - {})",
                    metadata.file_path,
                    String::from_utf8_lossy(&metadata.min_key),
                    String::from_utf8_lossy(&metadata.max_key)
                );
                continue;
            }
            if !metadata.bloom_filter.contains(key) {
                eprintln!(
                    "Skipping SSTable file: {} as bloom filter indicates key is not present",
                    metadata.file_path
                );
                continue;
            }

            let mut starting_offset = 0;
            let sparse_index: Vec<(Vec<u8>, u64)> = match self
                .fs
                .open_read(metadata.sparse_index.to_str().unwrap_or(""))
                .await
            {
                Ok(mut file) => {
                    let mut buffer = Vec::new();
                    if let Err(e) = file.read_to_end(&mut buffer).await {
                        eprintln!("Failed to read sparse index file: {}", e);
                        Vec::new()
                    } else {
                        match serde_json::from_slice(&buffer) {
                            Ok(index) => index,
                            Err(e) => {
                                eprintln!("Failed to deserialize sparse index: {}", e);
                                Vec::new()
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Sparse index file not found for SSTable file: {}, error: {}. Proceeding without sparse index.",
                        metadata.file_path, e
                    );
                    Vec::new()
                }
            };

            eprintln!(
                "Using sparse index for SSTable file: {}, sparse index size: {} with values: {:?}",
                metadata.file_path,
                sparse_index.len(),
                sparse_index
                    .iter()
                    .map(|(k, v)| (String::from_utf8_lossy(k), v))
                    .collect::<Vec<_>>()
            );
            let mut left = 0;
            let mut right = sparse_index.len() as i32 - 1;
            while left <= right {
                let mid = (left + right) / 2;
                let cur_entry = &sparse_index[mid as usize];
                let cur_key = cur_entry.0.as_slice();
                if cur_key <= key {
                    starting_offset = cur_entry.1;
                }

                if cur_key <= key {
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            }

            eprintln!(
                "Reading from SSTable file: {}, starting offset: {}",
                metadata.file_path, starting_offset
            );

            let mut file = match self.fs.open_read(&metadata.file_path).await {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Failed to open SSTable file: {}", e);
                    return Err(e);
                }
            };

            let bytes = match file.seek(io::SeekFrom::Start(starting_offset)).await {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("Failed to seek in SSTable file: {}", e);
                    return Err(e);
                }
            };
            eprintln!(
                "Seeked to offset {} in SSTable file: {}, bytes seeked: {}",
                starting_offset, metadata.file_path, bytes
            );

            let mut buf = Vec::new();
            let read_bytes = match file.read_to_end(&mut buf).await {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("Failed to read from SSTable file: {}", e);
                    return Err(e);
                }
            };
            eprintln!(
                "read {} bytes from SSTable file: {}",
                read_bytes, metadata.file_path
            );

            //We should et value while decoding when we hit the last key occurence
            //to avoid looping over the entries again
            let decoded = Encoder::decode_all(&buf[..read_bytes])?;
            eprintln!(
                "Decoded {} entries from SSTable file: {}",
                decoded.len(),
                metadata.file_path
            );
            for entry in decoded {
                if entry.key.as_slice() == key {
                    return Ok(Some(entry.value.clone()));
                }
            }
        }
        Ok(None)
    }

    async fn flush(
        &mut self,
        data: &[u8],
        sparse_index: &[(Vec<u8>, u64)],
        min_key: Vec<u8>,
        max_key: Vec<u8>,
        bloom_filter: SimpleBloomFilter,
    ) -> io::Result<()> {
        let file_id = self.metadata.cur_max_file_id + 1;
        let path = format!("sstable_{}.dat", file_id);
        let sparse_index_path = format!("sstable_{}.dat.idx", file_id);

        let sstable_metadata = SSTableFileMetadata {
            file_id,
            file_path: path.clone(),
            min_key,
            max_key,
            size_in_bytes: data.len() as u64,
            sparse_index: PathBuf::from(sparse_index_path.clone()),
            bloom_filter,
        };

        // We want truncate so that retries are idempotent
        let mut file = self
            .fs
            .create_or_truncate(&path)
            .await
            .expect("Failed to open SSTable file");
        if let Err(e) = file.write_all(data).await {
            eprintln!("Failed to write to SSTable file: {}", e);
            return Err(e);
        }
        if let Err(e) = file.sync_all().await {
            eprintln!("Failed to sync SSTable file: {}", e);
            return Err(e);
        }

        // We want truncate so that retries are idempotent
        let mut sparse_file = self
            .fs
            .create_or_truncate(&sparse_index_path)
            .await
            .expect("Failed to create sparse index file");

        //Using JSON for simplicity, but in a production system, you might want to use a more efficient binary format
        if let Err(e) = sparse_file
            .write_all(
                serde_json::to_string(&sparse_index)
                    .expect("Failed to serialize sparse index")
                    .as_bytes(),
            )
            .await
        {
            eprintln!("Failed to write sparse index: {}", e);
            return Err(e);
        }
        if let Err(e) = sparse_file.sync_all().await {
            eprintln!("Failed to sync sparse index file: {}", e);
            return Err(e);
        }

        self.metadata.cur_max_file_id = file_id;
        self.metadata
            .sstable_file_metadata
            .insert(Reverse(file_id), sstable_metadata);
        self.persist_metadata().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::Entry;
    use crate::storage::fs::{FileHandle, FileSystem};
    use crate::storage::lsm_tree::LSMTree;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    /// File handle that owns its own buffer — no shared lock during read/write/seek.
    /// Data is copied from shared storage on open and written back on sync_all.
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

    async fn make_manager() -> (SSTableStorageManager, Arc<InMemoryFileSystem>) {
        let fs = Arc::new(InMemoryFileSystem::new());
        let manager = SSTableStorageManager::new(fs.clone()).await;
        (manager, fs)
    }

    async fn flush_entries(manager: &mut SSTableStorageManager, entries: &[Entry]) {
        let ef = LSMTree::encode_for_flush(entries);
        manager
            .flush(
                &ef.data,
                &ef.sparse_index,
                ef.min_key,
                ef.max_key,
                ef.bloom_filter,
            )
            .await
            .expect("flush should succeed");
    }

    #[tokio::test]
    async fn test_read_returns_none_when_no_sstables() {
        let (manager, _) = make_manager().await;
        assert_eq!(manager.read(b"key").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_read_returns_value_for_existing_key() {
        let (mut manager, _) = make_manager().await;
        flush_entries(
            &mut manager,
            &[
                Entry::set(1, b"apple".to_vec(), b"red".to_vec()),
                Entry::set(2, b"banana".to_vec(), b"yellow".to_vec()),
                Entry::set(3, b"cherry".to_vec(), b"dark_red".to_vec()),
            ],
        )
        .await;

        assert_eq!(manager.read(b"apple").await.unwrap(), Some(b"red".to_vec()));
        assert_eq!(
            manager.read(b"banana").await.unwrap(),
            Some(b"yellow".to_vec())
        );
        assert_eq!(
            manager.read(b"cherry").await.unwrap(),
            Some(b"dark_red".to_vec())
        );
    }

    #[tokio::test]
    async fn test_read_returns_none_for_missing_key_in_range() {
        let (mut manager, _) = make_manager().await;
        flush_entries(
            &mut manager,
            &[
                Entry::set(1, b"apple".to_vec(), b"red".to_vec()),
                Entry::set(2, b"cherry".to_vec(), b"dark_red".to_vec()),
            ],
        )
        .await;

        assert_eq!(manager.read(b"banana").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_read_skips_sstable_when_key_out_of_range() {
        let (mut manager, _) = make_manager().await;
        flush_entries(
            &mut manager,
            &[
                Entry::set(1, b"d".to_vec(), b"v1".to_vec()),
                Entry::set(2, b"f".to_vec(), b"v2".to_vec()),
            ],
        )
        .await;

        assert_eq!(manager.read(b"a").await.unwrap(), None);
        assert_eq!(manager.read(b"z").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_read_newer_sstable_wins() {
        let (mut manager, _) = make_manager().await;

        flush_entries(
            &mut manager,
            &[Entry::set(1, b"key".to_vec(), b"old_value".to_vec())],
        )
        .await;

        flush_entries(
            &mut manager,
            &[Entry::set(2, b"key".to_vec(), b"new_value".to_vec())],
        )
        .await;

        assert_eq!(
            manager.read(b"key").await.unwrap(),
            Some(b"new_value".to_vec())
        );
    }

    #[tokio::test]
    async fn test_read_key_between_sparse_index_entries() {
        let (mut manager, _) = make_manager().await;
        // sparse_index_interval=2: positions 0, 2 are indexed.
        // "banana" at position 1 must be found by scanning from "apple"'s offset.
        flush_entries(
            &mut manager,
            &[
                Entry::set(1, b"apple".to_vec(), b"red".to_vec()),
                Entry::set(2, b"banana".to_vec(), b"yellow".to_vec()),
                Entry::set(3, b"cherry".to_vec(), b"dark_red".to_vec()),
            ],
        )
        .await;

        assert_eq!(
            manager.read(b"banana").await.unwrap(),
            Some(b"yellow".to_vec())
        );
    }

    #[tokio::test]
    async fn test_read_many_entries_finds_last() {
        let (mut manager, _) = make_manager().await;
        flush_entries(
            &mut manager,
            &[
                Entry::set(1, b"a".to_vec(), b"v1".to_vec()),
                Entry::set(2, b"b".to_vec(), b"v2".to_vec()),
                Entry::set(3, b"c".to_vec(), b"v3".to_vec()),
                Entry::set(4, b"d".to_vec(), b"v4".to_vec()),
                Entry::set(5, b"e".to_vec(), b"v5".to_vec()),
                Entry::set(6, b"f".to_vec(), b"v6".to_vec()),
                Entry::set(7, b"g".to_vec(), b"v7".to_vec()),
            ],
        )
        .await;

        assert_eq!(manager.read(b"g").await.unwrap(), Some(b"v7".to_vec()));
    }

    #[tokio::test]
    async fn test_read_falls_back_to_older_sstable() {
        let (mut manager, _) = make_manager().await;

        flush_entries(
            &mut manager,
            &[Entry::set(1, b"aaa".to_vec(), b"v1".to_vec())],
        )
        .await;

        flush_entries(
            &mut manager,
            &[Entry::set(2, b"zzz".to_vec(), b"v2".to_vec())],
        )
        .await;

        assert_eq!(manager.read(b"aaa").await.unwrap(), Some(b"v1".to_vec()));
        assert_eq!(manager.read(b"zzz").await.unwrap(), Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_read_single_entry_sstable() {
        let (mut manager, _) = make_manager().await;
        flush_entries(
            &mut manager,
            &[Entry::set(1, b"only".to_vec(), b"one".to_vec())],
        )
        .await;

        assert_eq!(manager.read(b"only").await.unwrap(), Some(b"one".to_vec()));
        assert_eq!(manager.read(b"other").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_flush_creates_sstable_and_index_files() {
        let (mut manager, fs) = make_manager().await;
        flush_entries(
            &mut manager,
            &[
                Entry::set(1, b"a".to_vec(), b"v1".to_vec()),
                Entry::set(2, b"b".to_vec(), b"v2".to_vec()),
            ],
        )
        .await;

        let storage = fs.storage.lock().unwrap();
        assert!(storage.contains_key("sstable_1.dat"));
        assert!(storage.contains_key("sstable_1.dat.idx"));
        assert!(storage.contains_key("metadata.dat"));
    }

    #[tokio::test]
    async fn test_flush_increments_file_id() {
        let (mut manager, fs) = make_manager().await;
        flush_entries(
            &mut manager,
            &[Entry::set(1, b"a".to_vec(), b"v1".to_vec())],
        )
        .await;
        flush_entries(
            &mut manager,
            &[Entry::set(2, b"b".to_vec(), b"v2".to_vec())],
        )
        .await;

        let storage = fs.storage.lock().unwrap();
        assert!(storage.contains_key("sstable_1.dat"));
        assert!(storage.contains_key("sstable_2.dat"));
        assert_eq!(manager.metadata.cur_max_file_id, 2);
    }

    #[tokio::test]
    async fn test_flush_sstable_data_is_decodable() {
        let entries = [
            Entry::set(1, b"key1".to_vec(), b"val1".to_vec()),
            Entry::set(2, b"key2".to_vec(), b"val2".to_vec()),
        ];
        let (mut manager, fs) = make_manager().await;
        flush_entries(&mut manager, &entries).await;

        let data = {
            let storage = fs.storage.lock().unwrap();
            storage.get("sstable_1.dat").unwrap().clone()
        };
        let decoded = Encoder::decode_all(&data).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].key, b"key1");
        assert_eq!(decoded[0].value, b"val1");
        assert_eq!(decoded[1].key, b"key2");
        assert_eq!(decoded[1].value, b"val2");
    }

    #[tokio::test]
    async fn test_flush_sparse_index_is_deserializable() {
        let entries = [
            Entry::set(1, b"a".to_vec(), b"v1".to_vec()),
            Entry::set(2, b"b".to_vec(), b"v2".to_vec()),
            Entry::set(3, b"c".to_vec(), b"v3".to_vec()),
        ];
        let (mut manager, fs) = make_manager().await;
        flush_entries(&mut manager, &entries).await;

        let idx_data = {
            let storage = fs.storage.lock().unwrap();
            storage.get("sstable_1.dat.idx").unwrap().clone()
        };
        let sparse_index: Vec<(Vec<u8>, u64)> = serde_json::from_slice(&idx_data).unwrap();
        // SPARSE_INDEX_INTERVAL=2: positions 0 and 2 are indexed
        assert_eq!(sparse_index.len(), 2);
        assert_eq!(sparse_index[0].0, b"a");
        assert_eq!(sparse_index[0].1, 0);
        assert_eq!(sparse_index[1].0, b"c");
    }

    #[tokio::test]
    async fn test_flush_stores_min_max_keys_in_metadata() {
        let (mut manager, _) = make_manager().await;
        flush_entries(
            &mut manager,
            &[
                Entry::set(1, b"apple".to_vec(), b"v1".to_vec()),
                Entry::set(2, b"cherry".to_vec(), b"v2".to_vec()),
            ],
        )
        .await;

        let meta = manager
            .metadata
            .sstable_file_metadata
            .values()
            .next()
            .unwrap();
        assert_eq!(meta.min_key, b"apple");
        assert_eq!(meta.max_key, b"cherry");
    }

    #[tokio::test]
    async fn test_flush_metadata_persisted_and_recoverable() {
        let (mut manager, fs) = make_manager().await;
        flush_entries(
            &mut manager,
            &[Entry::set(1, b"key".to_vec(), b"val".to_vec())],
        )
        .await;

        // Create a new manager from the same filesystem — it should recover metadata
        let recovered = SSTableStorageManager::new(fs.clone()).await;
        assert_eq!(recovered.metadata.cur_max_file_id, 1);
        assert_eq!(recovered.metadata.sstable_file_metadata.len(), 1);
        assert_eq!(recovered.read(b"key").await.unwrap(), Some(b"val".to_vec()));
    }

    #[tokio::test]
    async fn test_flush_records_size_in_bytes() {
        let entries = [Entry::set(1, b"k".to_vec(), b"v".to_vec())];
        let ef = LSMTree::encode_for_flush(&entries);
        let (mut manager, _) = make_manager().await;
        flush_entries(&mut manager, &entries).await;

        let meta = manager
            .metadata
            .sstable_file_metadata
            .values()
            .next()
            .unwrap();
        assert_eq!(meta.size_in_bytes, ef.data.len() as u64);
    }

    // ── bloom filter integration ────────────────────────────────────────────

    #[tokio::test]
    async fn test_bloom_filter_stored_in_metadata() {
        let (mut manager, _) = make_manager().await;
        flush_entries(
            &mut manager,
            &[
                Entry::set(1, b"apple".to_vec(), b"red".to_vec()),
                Entry::set(2, b"banana".to_vec(), b"yellow".to_vec()),
            ],
        )
        .await;

        let meta = manager
            .metadata
            .sstable_file_metadata
            .values()
            .next()
            .unwrap();
        assert!(meta.bloom_filter.contains(b"apple"));
        assert!(meta.bloom_filter.contains(b"banana"));
    }

    #[tokio::test]
    async fn test_bloom_filter_rejects_missing_key_in_range() {
        // Key is within min/max range but not in bloom filter — should skip the SSTable
        let (mut manager, _) = make_manager().await;
        flush_entries(
            &mut manager,
            &[
                Entry::set(1, b"apple".to_vec(), b"red".to_vec()),
                Entry::set(2, b"cherry".to_vec(), b"dark_red".to_vec()),
            ],
        )
        .await;

        let meta = manager
            .metadata
            .sstable_file_metadata
            .values()
            .next()
            .unwrap();
        // "banana" is in range [apple, cherry] but was never inserted
        assert!(!meta.bloom_filter.contains(b"banana"));
        assert_eq!(manager.read(b"banana").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_bloom_filter_survives_metadata_recovery() {
        let (mut manager, fs) = make_manager().await;
        flush_entries(
            &mut manager,
            &[Entry::set(1, b"key".to_vec(), b"val".to_vec())],
        )
        .await;

        let recovered = SSTableStorageManager::new(fs.clone()).await;
        let meta = recovered
            .metadata
            .sstable_file_metadata
            .values()
            .next()
            .unwrap();
        assert!(meta.bloom_filter.contains(b"key"));
        assert!(!meta.bloom_filter.contains(b"missing"));
    }

    #[tokio::test]
    async fn test_bloom_filter_per_sstable_is_independent() {
        let (mut manager, _) = make_manager().await;
        flush_entries(
            &mut manager,
            &[Entry::set(1, b"aaa".to_vec(), b"v1".to_vec())],
        )
        .await;
        flush_entries(
            &mut manager,
            &[Entry::set(2, b"zzz".to_vec(), b"v2".to_vec())],
        )
        .await;

        let mut metas = manager.metadata.sstable_file_metadata.values();
        let newer = metas.next().unwrap();
        let older = metas.next().unwrap();

        // Each bloom filter only knows about its own keys
        assert!(newer.bloom_filter.contains(b"zzz"));
        assert!(!newer.bloom_filter.contains(b"aaa"));
        assert!(older.bloom_filter.contains(b"aaa"));
        assert!(!older.bloom_filter.contains(b"zzz"));
    }
}
