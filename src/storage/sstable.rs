use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use crate::storage::bloom_filter::{BloomFilter, SimpleBloomFilter};
use crate::storage::encoder::Encoder;
use crate::storage::fs::FileSystem;

const MAX_BUCKET_SIZE: u8 = 5;
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

    async fn read(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>>;

    #[allow(dead_code)]
    async fn compact(&mut self) -> io::Result<()>;
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
struct SizeBucket {
    avg_size_in_bytes: u64,
    file_count: u64,

    //Containing the file_ids of the SSTables in the bucket
    files: Vec<u64>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Metadata {
    cur_max_file_id: u64,
    max_bucket_size: u8,

    // Using BTreeMap to maintain sorted order of SSTable files based on their file_id
    // This allows for efficient lookups and reading in order from the newest to the oldest SSTable file.
    sstable_file_metadata: BTreeMap<Reverse<u64>, SSTableFileMetadata>,
    size_buckets: Vec<SizeBucket>,
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
                        size_buckets: Vec::new(),
                        max_bucket_size: MAX_BUCKET_SIZE,
                    }
                } else {
                    match serde_json::from_slice(&buffer) {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("Failed to deserialize metadata: {}", e);
                            Metadata {
                                cur_max_file_id: 0,
                                sstable_file_metadata: BTreeMap::new(),
                                size_buckets: Vec::new(),
                                max_bucket_size: MAX_BUCKET_SIZE,
                            }
                        }
                    }
                }
            }
            Err(e) => {
                //Here we might want to abort instead of starting with no data
                //potentially losing all the data
                eprintln!(
                    "Metadata file not found, starting with empty metadata. Error: {}",
                    e
                );
                Metadata {
                    cur_max_file_id: 0,
                    sstable_file_metadata: BTreeMap::new(),
                    size_buckets: Vec::new(),
                    max_bucket_size: MAX_BUCKET_SIZE,
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

    fn put_sstable_in_size_bucket(
        &mut self,
        sstable_metadata: &SSTableFileMetadata,
    ) -> anyhow::Result<()> {
        if let Some(bucket) = self.metadata.size_buckets.iter_mut().find(|b| {
            let half_avg_size = b.avg_size_in_bytes / 2;
            let avg_plus_half = b.avg_size_in_bytes + half_avg_size;
            let avg_minus_half = b.avg_size_in_bytes - half_avg_size;

            let mut fits_in_bucket = false;
            if sstable_metadata.size_in_bytes >= avg_minus_half
                && sstable_metadata.size_in_bytes <= avg_plus_half
            {
                fits_in_bucket = true;
            }
            fits_in_bucket
        }) {
            bucket.file_count += 1;
            bucket.files.push(sstable_metadata.file_id);
            let total_size =
                bucket.avg_size_in_bytes * (bucket.file_count - 1) + sstable_metadata.size_in_bytes;
            bucket.avg_size_in_bytes = total_size / bucket.file_count;
        } else {
            self.metadata.size_buckets.push(SizeBucket {
                avg_size_in_bytes: sstable_metadata.size_in_bytes,
                file_count: 1,
                files: vec![sstable_metadata.file_id],
            });
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
        self.put_sstable_in_size_bucket(&sstable_metadata)
            .expect("unable to put sstable in size bucket");
        self.metadata
            .sstable_file_metadata
            .insert(Reverse(file_id), sstable_metadata);
        self.persist_metadata().await?;
        Ok(())
    }

    async fn compact(&mut self) -> io::Result<()> {
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
            let mut storage = self.storage.lock().expect("storage mutex poisoned");
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
                let storage = self.storage.lock().expect("storage mutex poisoned");
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
                let storage = self.storage.lock().expect("storage mutex poisoned");
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
            let mut storage = self.storage.lock().expect("storage mutex poisoned");
            let data = storage
                .remove(from)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
            storage.insert(to.to_string(), data);
            Ok(())
        }

        async fn remove_file(&self, path: &str) -> io::Result<()> {
            let mut storage = self.storage.lock().expect("storage mutex poisoned");
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
        assert_eq!(manager.read(b"key").await.expect("read should not fail"), None);
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

        assert_eq!(manager.read(b"apple").await.expect("read apple failed"), Some(b"red".to_vec()));
        assert_eq!(
            manager.read(b"banana").await.expect("read banana failed"),
            Some(b"yellow".to_vec())
        );
        assert_eq!(
            manager.read(b"cherry").await.expect("read cherry failed"),
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

        assert_eq!(manager.read(b"banana").await.expect("read banana failed"), None);
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

        assert_eq!(manager.read(b"a").await.expect("read 'a' failed"), None);
        assert_eq!(manager.read(b"z").await.expect("read 'z' failed"), None);
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
            manager.read(b"key").await.expect("read key failed"),
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
            manager.read(b"banana").await.expect("read banana failed"),
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

        assert_eq!(manager.read(b"g").await.expect("read 'g' failed"), Some(b"v7".to_vec()));
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

        assert_eq!(manager.read(b"aaa").await.expect("read aaa failed"), Some(b"v1".to_vec()));
        assert_eq!(manager.read(b"zzz").await.expect("read zzz failed"), Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_read_single_entry_sstable() {
        let (mut manager, _) = make_manager().await;
        flush_entries(
            &mut manager,
            &[Entry::set(1, b"only".to_vec(), b"one".to_vec())],
        )
        .await;

        assert_eq!(manager.read(b"only").await.expect("read 'only' failed"), Some(b"one".to_vec()));
        assert_eq!(manager.read(b"other").await.expect("read 'other' failed"), None);
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

        let storage = fs.storage.lock().expect("storage mutex poisoned");
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

        let storage = fs.storage.lock().expect("storage mutex poisoned");
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
            let storage = fs.storage.lock().expect("storage mutex poisoned");
            storage.get("sstable_1.dat").expect("sstable file missing").clone()
        };
        let decoded = Encoder::decode_all(&data).expect("decode sstable data failed");
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
            let storage = fs.storage.lock().expect("storage mutex poisoned");
            storage.get("sstable_1.dat.idx").expect("sparse index file missing").clone()
        };
        let sparse_index: Vec<(Vec<u8>, u64)> = serde_json::from_slice(&idx_data).expect("deserialize sparse index failed");
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
            .expect("expected at least one sstable metadata");
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
        assert_eq!(recovered.read(b"key").await.expect("recovered read failed"), Some(b"val".to_vec()));
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
            .expect("expected at least one sstable metadata");
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
            .expect("expected at least one sstable metadata");
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
            .expect("expected at least one sstable metadata");
        // "banana" is in range [apple, cherry] but was never inserted
        assert!(!meta.bloom_filter.contains(b"banana"));
        assert_eq!(manager.read(b"banana").await.expect("read banana failed"), None);
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
            .expect("expected at least one sstable metadata");
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
        let newer = metas.next().expect("expected newer sstable metadata");
        let older = metas.next().expect("expected older sstable metadata");

        // Each bloom filter only knows about its own keys
        assert!(newer.bloom_filter.contains(b"zzz"));
        assert!(!newer.bloom_filter.contains(b"aaa"));
        assert!(older.bloom_filter.contains(b"aaa"));
        assert!(!older.bloom_filter.contains(b"zzz"));
    }

    // ── size bucket ─────────────────────────────────────────────────────────

    fn dummy_sstable_meta(file_id: u64, size: u64) -> SSTableFileMetadata {
        SSTableFileMetadata {
            file_id,
            file_path: format!("sstable_{}.dat", file_id),
            min_key: vec![],
            max_key: vec![],
            size_in_bytes: size,
            sparse_index: PathBuf::from(format!("sstable_{}.dat.idx", file_id)),
            bloom_filter: SimpleBloomFilter::new(64),
        }
    }

    #[tokio::test]
    async fn test_size_bucket_first_sstable_creates_new_bucket() {
        let (mut manager, _) = make_manager().await;
        let meta = dummy_sstable_meta(1, 1000);
        manager.put_sstable_in_size_bucket(&meta).expect("bucket assignment failed");

        assert_eq!(manager.metadata.size_buckets.len(), 1);
        assert_eq!(manager.metadata.size_buckets[0].avg_size_in_bytes, 1000);
        assert_eq!(manager.metadata.size_buckets[0].file_count, 1);
        assert_eq!(manager.metadata.size_buckets[0].files, vec![1]);
    }

    #[tokio::test]
    async fn test_size_bucket_similar_size_goes_to_same_bucket() {
        let (mut manager, _) = make_manager().await;
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(1, 1000))
            .expect("bucket assignment for file 1 failed");
        // 1200 is within [500, 1500] (avg=1000 ± 50%)
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(2, 1200))
            .expect("bucket assignment for file 2 failed");

        assert_eq!(manager.metadata.size_buckets.len(), 1);
        assert_eq!(manager.metadata.size_buckets[0].file_count, 2);
        assert_eq!(manager.metadata.size_buckets[0].files, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_size_bucket_very_different_size_creates_new_bucket() {
        let (mut manager, _) = make_manager().await;
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(1, 1000))
            .expect("bucket assignment for file 1 failed");
        // 5000 is outside [500, 1500]
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(2, 5000))
            .expect("bucket assignment for file 2 failed");

        assert_eq!(manager.metadata.size_buckets.len(), 2);
        assert_eq!(manager.metadata.size_buckets[0].avg_size_in_bytes, 1000);
        assert_eq!(manager.metadata.size_buckets[0].files, vec![1]);
        assert_eq!(manager.metadata.size_buckets[1].avg_size_in_bytes, 5000);
        assert_eq!(manager.metadata.size_buckets[1].files, vec![2]);
    }

    #[tokio::test]
    async fn test_size_bucket_avg_is_recalculated() {
        let (mut manager, _) = make_manager().await;
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(1, 1000))
            .expect("bucket assignment for file 1 failed");
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(2, 1000))
            .expect("bucket assignment for file 2 failed");
        // avg after two 1000-byte files = 1000
        assert_eq!(manager.metadata.size_buckets[0].avg_size_in_bytes, 1000);

        // 1400 is within [500, 1500], recalculated avg = (1000*2 + 1400) / 3 = 1133
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(3, 1400))
            .expect("bucket assignment for file 3 failed");
        assert_eq!(manager.metadata.size_buckets[0].file_count, 3);
        assert_eq!(
            manager.metadata.size_buckets[0].avg_size_in_bytes,
            (1000 * 2 + 1400) / 3
        );
    }

    #[tokio::test]
    async fn test_size_bucket_at_upper_boundary() {
        let (mut manager, _) = make_manager().await;
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(1, 1000))
            .expect("bucket assignment for file 1 failed");
        // upper bound = 1000 + 500 = 1500, exactly at boundary should fit
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(2, 1500))
            .expect("bucket assignment for file 2 failed");

        assert_eq!(manager.metadata.size_buckets.len(), 1);
        assert_eq!(manager.metadata.size_buckets[0].file_count, 2);
    }

    #[tokio::test]
    async fn test_size_bucket_at_lower_boundary() {
        let (mut manager, _) = make_manager().await;
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(1, 1000))
            .expect("bucket assignment for file 1 failed");
        // lower bound = 1000 - 500 = 500, exactly at boundary should fit
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(2, 500))
            .expect("bucket assignment for file 2 failed");

        assert_eq!(manager.metadata.size_buckets.len(), 1);
        assert_eq!(manager.metadata.size_buckets[0].file_count, 2);
    }

    #[tokio::test]
    async fn test_size_bucket_just_above_upper_boundary() {
        let (mut manager, _) = make_manager().await;
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(1, 1000))
            .expect("bucket assignment for file 1 failed");
        // 1501 is just above upper bound of 1500 — new bucket
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(2, 1501))
            .expect("bucket assignment for file 2 failed");

        assert_eq!(manager.metadata.size_buckets.len(), 2);
    }

    #[tokio::test]
    async fn test_size_bucket_just_below_lower_boundary() {
        let (mut manager, _) = make_manager().await;
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(1, 1000))
            .expect("bucket assignment for file 1 failed");
        // 499 is just below lower bound of 500 — new bucket
        manager
            .put_sstable_in_size_bucket(&dummy_sstable_meta(2, 499))
            .expect("bucket assignment for file 2 failed");

        assert_eq!(manager.metadata.size_buckets.len(), 2);
    }

    #[tokio::test]
    async fn test_size_bucket_flush_populates_bucket() {
        let (mut manager, _) = make_manager().await;
        flush_entries(
            &mut manager,
            &[Entry::set(1, b"a".to_vec(), b"v1".to_vec())],
        )
        .await;

        assert_eq!(manager.metadata.size_buckets.len(), 1);
        assert_eq!(manager.metadata.size_buckets[0].file_count, 1);
        assert_eq!(manager.metadata.size_buckets[0].files, vec![1]);
    }
}
