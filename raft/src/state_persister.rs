use crate::raft_types::LogEntry;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub entries: Vec<LogEntry>,
    #[serde(default)]
    pub commit_index: u64,
}

#[async_trait::async_trait]
pub trait Persister {
    async fn save_state(&self, state: &PersistentState) -> anyhow::Result<()>;
    async fn load_state(&self) -> anyhow::Result<PersistentState>;
    async fn create_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> anyhow::Result<()>;
}

pub struct FilePersistentStorage {
    id: String,
}

impl FilePersistentStorage {
    pub fn new(id: String) -> Self {
        FilePersistentStorage { id }
    }

    fn get_state_file_path(&self) -> String {
        format!("node_{}_state.json", self.id)
    }
}

#[async_trait::async_trait]
impl Persister for FilePersistentStorage {
    async fn save_state(&self, state: &PersistentState) -> anyhow::Result<()> {
        let serialized_state = serde_json::to_string(state)?;
        let mut file = File::create(self.get_state_file_path()).await?;
        file.write_all(serialized_state.as_bytes()).await?;
        file.flush().await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn create_snapshot(
        &self,
        _last_included_index: u64,
        _last_included_term: u64,
    ) -> anyhow::Result<()> {
        // For simplicity, we won't implement snapshotting in this example.
        Ok(())
    }

    async fn load_state(&self) -> anyhow::Result<PersistentState> {
        let data = match tokio::fs::read_to_string(self.get_state_file_path()).await {
            Ok(data) => data,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                eprint!(
                    "No state file found for node {}, using default state\n",
                    self.id
                );
                return Ok(PersistentState {
                    current_term: 0,
                    voted_for: None,
                    entries: vec![],
                    commit_index: 0,
                });
            }
            Err(error) => return Err(error.into()),
        };
        let state: PersistentState = serde_json::from_str(&data)?;
        Ok(state)
    }
}
