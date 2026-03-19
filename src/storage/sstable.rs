use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

#[async_trait::async_trait]
pub(super) trait SSTablesStorage {
    async fn flush(
        &mut self,
        data: &[u8],
        sparse_index: &[(Vec<u8>, u64)],
        min_key: Vec<u8>,
        max_key: Vec<u8>,
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
    sparse_index: Option<Vec<(Vec<u8>, u64)>>,
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
}

impl SSTableStorageManager {
    pub(super) async fn new() -> Self {
        //TODO: Load existing metadata from disk if available
        Self {
            metadata: Metadata {
                cur_max_file_id: 0,
                sstable_file_metadata: BTreeMap::new(),
            },
        }
    }
    async fn persist_metadata(&self) -> io::Result<()> {
        // Implement the logic to persist the metadata to disk
        // This could involve writing the metadata in a specific format, handling updates, etc.
        // For simplicity, we can serialize the metadata using a format like JSON or binary and write it to a file.
        let tmp_file_path = "metadata.dat.tmp";
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(tmp_file_path)
            .await
            .expect("Failed to open metadata file");

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
        if let Err(e) = tokio::fs::rename(tmp_file_path, "metadata.dat").await {
            eprintln!("Failed to rename metadata file: {}", e);
            return Err(e);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl SSTablesStorage for SSTableStorageManager {
    async fn read(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        // Implement the logic to read the value for the given key from the SSTable files
        // This could involve searching through the SSTable files in order, using the sparse index for faster lookups, etc.
        for metadata in self.metadata.sstable_file_metadata.values() {
            if key < metadata.min_key.as_slice() || key > metadata.max_key.as_slice() {
                continue; // Skip files that cannot contain the key
            }
        }
        Ok(None) // Placeholder implementation
    }

    async fn flush(
        &mut self,
        data: &[u8],
        sparse_index: &[(Vec<u8>, u64)],
        min_key: Vec<u8>,
        max_key: Vec<u8>,
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
            sparse_index: None,
        };

        // We want truncate so that retres are idempotent
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
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

        // We want truncate so that retres are idempotent
        let mut sparse_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&sparse_index_path)
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
