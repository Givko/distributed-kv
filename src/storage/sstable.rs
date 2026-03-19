use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::storage::encoder::Encoder;

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
    sparse_index: PathBuf,
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
        let metadata = match OpenOptions::new().read(true).open("metadata.dat").await {
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
        Self { metadata }
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
                eprintln!(
                    "Skipping SSTable file: {} as key is out of range ({} - {})",
                    metadata.file_path,
                    String::from_utf8_lossy(&metadata.min_key),
                    String::from_utf8_lossy(&metadata.max_key)
                );
                continue; // Skip files that cannot contain the key
            }

            // Get starting offset via the sparse index
            // start linear search from that offset until the end
            // we should not stop at the first key match because we might have the key mutliple times
            // se we stop it we have found the last matching key i.e. if nextKey != key return value
            // we can jump by len as the first 8 bytes are the len of the record if the key is not
            // what we are looking for we can jump forward by len - cur_read bytes. Also use borrows to avoid
            // data copy. If we find the key stop if not continue to the next sstable file and the same

            let mut starting_offset = 0;
            let sparse_index: Vec<(Vec<u8>, u64)> = match OpenOptions::new()
                .read(true)
                .open(&metadata.sparse_index)
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
                "Using sparse index for SSTable file: {}, sparse index size: {}",
                metadata.file_path,
                sparse_index.len()
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

            let mut file = match OpenOptions::new()
                .read(true)
                .open(&metadata.file_path)
                .await
            {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Failed to open SSTable file: {}", e);
                    return Err(e);
                }
            };

            match file.seek(tokio::io::SeekFrom::Start(starting_offset)).await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Failed to seek in SSTable file: {}", e);
                    return Err(e);
                }
            };

            let mut buf = Vec::new();
            let read_bytes = match file.read_to_end(&mut buf).await {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("Failed to read from SSTable file: {}", e);
                    return Err(e);
                }
            };

            //We should et value while decoding when we hit the last key occurence
            //to avoid looping over the entries again
            let decoded = Encoder::decode_all(&buf[..read_bytes])?;
            for entry in decoded {
                if entry.key.as_slice() == key {
                    return Ok(Some(entry.value.clone()));
                }
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
            sparse_index: PathBuf::from(sparse_index_path.clone()),
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
