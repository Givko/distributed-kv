use crate::storage::entry::Entry;
use std::io;

#[async_trait::async_trait]
pub(super) trait SSTablesStorage {
    fn flush(&mut self, entries: &[Entry]) -> io::Result<()>;
}

pub struct SSTableStorageManager {
    // This struct can manage multiple SSTable files, handle compaction, etc.
}

impl SSTableStorageManager {
    pub fn new() -> Self {
        Self {
            // Initialize any necessary fields
        }
    }
}

#[async_trait::async_trait]
impl SSTablesStorage for SSTableStorageManager {
    fn flush(&mut self, entries: &[Entry]) -> io::Result<()> {
        // Implement the logic to write the entries to a new SSTable file
        // This could involve sorting the entries, writing them in a specific format, etc.
        Ok(())
    }
}
