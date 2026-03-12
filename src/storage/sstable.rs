use crate::storage::entry::Entry;
use std::io;

#[async_trait::async_trait]
pub(super) trait SSTablesStorage {
    async fn flush(entries: &[Entry]) -> io::Result<()>;
}

pub struct SSTableStorageManager;

#[async_trait::async_trait]
impl SSTablesStorage for SSTableStorageManager {
    async fn flush(entries: &[Entry]) -> io::Result<()> {
        // Implement the logic to write the entries to a new SSTable file
        // This could involve sorting the entries, writing them in a specific format, etc.
        Ok(())
    }
}
