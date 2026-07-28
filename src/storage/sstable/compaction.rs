use std::cmp::Reverse;
use std::collections::{BTreeMap, btree_map};
use std::io;
use std::sync::Arc;

use crate::storage::encoder::Encoder;
use crate::storage::entry::Entry;
use crate::storage::fs::{FileHandle, FileSystem};

use super::{SSTableFileMetadata, SizeBucket};

pub(super) async fn compact_bucket(
    sstable_file_metadata: &BTreeMap<Reverse<u64>, SSTableFileMetadata>,
    fs: &Arc<dyn FileSystem>,
    bucket: &SizeBucket,
) -> io::Result<()> {
    let max_file_id = bucket
        .files
        .iter()
        .max()
        .expect("bucket should have at least one file");
    let _new_sstable_name_tmp = format!("sstable_{}.dat.tmp", max_file_id + 1);

    let mut file_handles: Vec<Box<dyn FileHandle>> = Vec::new();
    for file_id in &bucket.files {
        let metadata = sstable_file_metadata
            .get(&Reverse(*file_id))
            .expect("file metadata should exist");
        let file_handle = fs.open_read(&metadata.file_path).await?;
        file_handles.push(file_handle);
    }

    let mut merged: BTreeMap<Vec<u8>, Entry> = BTreeMap::new();
    for mut file_handle in file_handles {
        let mut buffer = Vec::new();
        file_handle.read_to_end(&mut buffer).await?;

        let decoded = Encoder::decode_all(&buffer)?;
        for entry in decoded {
            match merged.entry(entry.key.clone()) {
                btree_map::Entry::Vacant(e) => {
                    e.insert(entry);
                }
                btree_map::Entry::Occupied(mut e) => {
                    if entry.index > e.get().index {
                        e.insert(entry);
                    }
                }
            }
        }
    }

    Ok(())
}
