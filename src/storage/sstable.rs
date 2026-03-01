use crate::storage::entry::Entry;
use std::io;

pub(super) trait SSTablesStorage {
    fn flush(entries: &[Entry]) -> io::Result<()>;
}
