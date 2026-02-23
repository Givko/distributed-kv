/// The interface the Raft node requires from any storage backend.
///
/// Raft defines this contract — it doesn't know or care what's behind it.
/// Concrete implementations (MemKv, LSM engine, etc.) live in `storage/`
/// and implement this trait there.
pub trait StorageEngine {
    /// Apply a committed command string to the storage state.
    fn apply(&mut self, command: &str);

    /// Look up a key. Returns `None` when the key is not present.
    fn get(&self, key: &str) -> Option<&String>;
}
