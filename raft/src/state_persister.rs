use crate::raft_types::LogEntry;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub entries: Vec<LogEntry>,
}

#[async_trait::async_trait]
pub trait Persister {
    async fn save_state(&self, state: &PersistentState) -> anyhow::Result<()>;
    async fn load_state(&self) -> anyhow::Result<PersistentState>;
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

    async fn load_state(&self) -> anyhow::Result<PersistentState> {
        let data = tokio::fs::read_to_string(self.get_state_file_path()).await?;
        let state: PersistentState = serde_json::from_str(&data)?;
        Ok(state)
    }
}
