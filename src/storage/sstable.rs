use crate::storage::encoder::Encoder;
use crate::storage::entry::Entry;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io;

#[async_trait::async_trait]
pub(super) trait SSTablesStorage {
    async fn flush(&mut self, entries: &[Entry]) -> io::Result<()>;
}

pub(super) struct SSTableFileMetadata {
    // non decreasing file ID for each new SSTable file
    // This can be used to determine the order of SSTable files and to manage them effectively.
    file_id: u64,
    file_path: String,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    size_in_bytes: u64,
}

pub(super) struct Metadata {
    // Add fields for metadata such as file path, index range, etc.
    number_of_sstable_files: usize,

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
                number_of_sstable_files: 0,
                sstable_file_metadata: BTreeMap::new(),
            },
        }
    }
}

#[async_trait::async_trait]
impl SSTablesStorage for SSTableStorageManager {
    async fn flush(&mut self, entries: &[Entry]) -> io::Result<()> {
        // Implement the logic to write the entries to a new SSTable file
        // This could involve sorting the entries, writing them in a specific format, etc.
        let bytes = Encoder::encode_all(entries);
        Ok(())
    }
}
