# LSM Storage Engine — Implementation Guide

---

## Phase 1: MemTable + WAL (Week 1-2)

### What you're building and why
- **MemTable** — in-memory write buffer using a sorted map. Fast writes because memory is fast
- **WAL** — append-only file on disk. Before any write touches the MemTable, it goes here first
- Together they give you fast writes with crash durability
- On restart: replay WAL → rebuild MemTable exactly as it was

### What to read
- **DDIA Chapter 3** — "Storage and Retrieval", specifically:
  - The "Hash Indexes" section to understand why log-structured storage exists
  - The "SSTables and LSM-Trees" section — this is the core theory for the entire LSM phase
  - Read with one question: *why does sequential disk write beat random write?*
- **LevelDB impl.md** — https://github.com/google/leveldb/blob/main/doc/impl.md
  - Short doc, read it twice
  - Focus on the WAL and MemTable sections

### Diagram before coding
- Write path: `client write → WAL append → fsync → MemTable insert → acknowledge`
- Recovery path: `startup → open WAL → replay entries → MemTable rebuilt → ready`
- Crash scenarios:
  - Crash before fsync → write never acknowledged, safe
  - Crash after fsync before MemTable insert → replayed on restart, safe

### Build steps
1. Build MemTable in isolation — sorted map, three operations: put, get, delete
2. Get must return three distinct states: **value found**, **tombstone found**, **not found** — these are different
3. Build WAL — append-only file, encode operation type + key + value per entry
4. Connect them: WAL write must complete before MemTable write, always
5. Add fsync after every WAL write
6. Implement recovery: on startup check for WAL file, replay all entries into fresh MemTable
7. After successful flush (Phase 2) WAL gets deleted — add a TODO here for now

### Break it
- Write 100 entries → SIGKILL → restart → verify all 100 present
- Corrupt the last few bytes of WAL manually → restart → decide: error or discard corrupted tail?
- Implement that decision explicitly, not accidentally

### Retention note
Write 5 sentences: what WAL does, why fsync matters, what a tombstone is, what recovery looks like

---

## Phase 2: SSTable Flush (Week 3-4)

### What you're building and why
- MemTable cannot grow forever — flush to disk when it exceeds a size threshold
- **SSTable** = Sorted String Table — immutable once written, never modified
- Immutability is what makes concurrent reads safe — nothing changes under you
- **Sparse index** = every Nth key + byte offset stored separately so you can binary search without reading the whole file

### What to read
- **Database Internals Chapter 7** — "Log-Structured Storage"
  - Read the SSTable format section carefully
  - Pay attention to why the index is *sparse* not *dense* — a dense index is just another copy of the data
  - Read the section on size thresholds and flush triggers
- **The original LSM paper** — "The Log-Structured Merge-Tree" by O'Neil et al.
  - Read the first half only — the theory sections
  - Gives you the mathematical intuition for write amplification tradeoffs

### Diagram before coding
- Flush trigger: `MemTable size > threshold → begin flush`
- SSTable file layout: `header | sorted key-value pairs | sparse index section | footer (index offset)`
- Point lookup: `load sparse index → binary search → seek to byte offset → scan forward`
- After flush: `delete WAL → fresh MemTable → fresh WAL`
- Crash after flush but before WAL deletion: WAL replays → duplicate entries → flush again → fine, idempotent

### Build steps
1. **Decide your file format on paper first** — key length prefix + key bytes + value length prefix + value bytes is standard. Decide tombstone encoding now, changing it later is painful
2. Implement flush function — takes MemTable + file path, iterates in sorted order, writes entries sequentially
3. While writing, build sparse index in memory — every Nth entry record key + current byte offset
4. After all entries written, write sparse index to end of file, write index start offset as final 8 bytes
5. Implement SSTable open — load sparse index into memory, keep file handle open
6. Implement point lookup — binary search sparse index, seek, scan forward
7. Implement flush trigger — after every MemTable write check size, if over threshold flush with monotonically increasing sequence number in filename
8. After successful flush: delete WAL, create fresh MemTable and WAL

### Break it
- Trigger 3-4 flushes, verify every key readable across all files
- Truncate an SSTable file manually to simulate partial write — reader must handle this gracefully
- Decide: skip corrupt file, error out, or partial recovery — implement it deliberately

---

## Phase 3: Read Path (Week 5)

### What you're building and why
- A read must check MemTable first, then SSTable files from **newest to oldest**
- MemTable always wins — it has the most recent writes
- Among SSTables, higher sequence number = newer = wins
- A tombstone anywhere in the chain means deleted — even if value exists in older files
- This is **read amplification** — N files means N potential checks. Phases 4 and 5 reduce this

### What to read
- **DDIA Chapter 3** — reread the "Making an LSM-Tree out of SSTables" section
  - Focus specifically on tombstone semantics and merge behaviour
- **RocksDB overview wiki** — https://github.com/facebook/rocksdb/wiki/RocksDB-Overview
  - Read the "Read/Write/Delete" section
  - Understand how they maintain the ordered file list and short-circuit on first hit
- **Database Internals Chapter 7** — the read path section
  - Covers the multi-level lookup and how tombstones propagate

### Diagram before coding
Draw all three correctness cases explicitly:
- Key in SSTable-3 (old), tombstone in SSTable-5 (newer) → **deleted**
- Key in SSTable-3, overwritten in SSTable-7 → **return SSTable-7 value**
- Tombstone in MemTable, value in SSTable-2 → **deleted**

### Build steps
1. Maintain ordered list of SSTable file handles, newest first — prepend on every flush
2. Implement read: check MemTable → iterate SSTable list newest to oldest → first result wins
3. Read function must return three states per file: **value**, **tombstone**, **not found in this file**
4. Tombstone = stop searching, return not found to caller
5. Not found in this file = continue to next file
6. Write a test for each of the three cases above before implementing

### Break it
- Write key 10 times with different values → verify only latest returned
- Delete key → verify not found
- Write key again after deletion → verify new value returned
- Manually insert a conflicting entry in an old SSTable → verify read path isn't confused

---

## Phase 4: Bloom Filters (Week 6)

### What you're building and why
- Without bloom filters: every missing key read scans all N SSTable files — expensive
- Bloom filter = probabilistic data structure attached to each SSTable
  - **No false negatives** — if it says absent, key is definitely absent → skip this file
  - **Occasional false positives** — if it says present, key might not actually be there → one wasted read, acceptable
- Built during flush, serialized to disk, loaded into memory when SSTable is opened
- Must be in memory to be useful — a disk read to check it defeats the purpose

### What to read
- **Bloom filter tutorial** — https://llimllib.github.io/bloomfilter-tutorial
  - Do this first, it's visual and takes 20 minutes
  - Makes the probability math concrete before you touch code
- **DDIA Chapter 3** — the bloom filter sidebar
  - Short, precise on why false positives are acceptable and false negatives are not
- **Database Internals Chapter 7** — bloom filter integration with SSTable
  - How to serialize alongside SSTable and load on open

### Diagram before coding
- Bloom filter as bit array — adding key: hash K times, set K bits
- Checking key: hash K times, are all K bits set?
- False positive: K bit positions all set by other keys → incorrectly says present
- False negative: impossible — a key's own bits can only be set when it was inserted
- Where it lives: built during flush → serialized to disk → loaded on SSTable open → checked before sparse index lookup

### Build steps
1. Build standalone bloom filter struct in complete isolation first
2. Takes capacity (expected keys) and target false positive rate as inputs
3. Compute optimal bit array size and number of hash functions from those inputs — look up the formulas, they are two lines of math
4. Implement add and might-contain
5. Test in isolation: add 10,000 known keys, check 10,000 known keys → zero false negatives
6. Check 10,000 keys never added → measure actual false positive rate against theoretical target
7. Integrate into flush: add every key to bloom filter as you write entries
8. Serialize bloom filter after writing entries, store in SSTable file or alongside it
9. Update SSTable open to deserialize and load bloom filter into memory
10. Update read path: before sparse index lookup, check bloom filter — if definitely absent skip to next file

### Break it
- Write 10,000 keys → look up 10,000 keys never written → count files actually read → verify rate matches target
- Verify every written key still found correctly — must be zero false negatives

---

## Phase 5: Compaction (Week 7-8)

### What you're building and why
- Over time: many SSTable files → reads check more files → space wasted on stale versions
- Compaction merges files: read N files sorted by key, keep only most recent version of each key, discard resolved tombstones, write one new SSTable, delete old files
- You're implementing **size-tiered compaction**: when you have N files of similar size, merge them
- This is what Cassandra uses by default — simple and correct

### What to read
- **Database Internals Chapter 10** — "Compaction"
  - Read this before implementing anything
  - Covers size-tiered vs leveled compaction with diagrams
  - Understand leveled even though you're building size-tiered — you need to know what tradeoffs you're accepting
- **RocksDB compaction wiki** — https://github.com/facebook/rocksdb/wiki/Compaction
  - Production perspective on compaction strategies
  - Focus on the size-tiered section and the write amplification analysis
- **Cassandra compaction strategy docs** — datastax.com
  - Second opinion on size-tiered specifically
  - More accessible than the RocksDB docs

### Diagram before coding
Three SSTable files with overlapping keys — draw the merge output for each case:
- "apple" in all three files with different values → **newest version wins**
- "banana" in files 1 and 2 only → **newest version wins**
- "cherry" tombstone in file 3 (newer), value in file 1 (older):
  - No older files outside compaction set → **discard tombstone entirely**
  - Older files exist outside compaction set → **preserve tombstone in output**

Draw the atomic file swap:
- Write merged output to temp file → fsync → rename to final path → delete input files
- rename is atomic on Linux — crash before rename = old state, crash after = new state, never partial

### Build steps
1. Implement merge iterator in complete isolation — takes N SSTable readers, produces merged sorted stream
2. Resolve duplicates by keeping highest sequence number entry
3. Implement tombstone handling in merge iterator — suppress older versions, decide whether to emit or discard tombstone based on whether older files exist outside compaction set
4. Implement compaction trigger — after every flush, check SSTable file count, if over threshold select files and compact
5. Write merged result to temp file first
6. After write is complete and fsync'd, rename to final path
7. Delete input files only after rename succeeds
8. Update in-memory file list to reflect new state

### Break it
- Write 50,000 keys in random order → read all 50,000 back → verify correctness
- SIGKILL mid-compaction (add a deliberate sleep to extend the window) → restart → verify consistent state
- Verify file count stays bounded under continuous writes
- Verify read performance doesn't degrade over time

---

## Phase 6: Integrate with Raft (Week 9)

### What you're building and why
- Replace the HashMap state machine in your Raft Node with the LSM engine
- Interface is minimal: Raft apply calls `set` and `get` on whatever implements the state machine
- LSM is a drop-in replacement from Raft's perspective
- The interesting part: snapshot interaction — force-flush MemTable before recording `snapshot_last_index` so SSTable files fully represent state up to `last_applied`

### What to read
- No new reading — reread all your retention notes from phases 1-5
- Reread the snapshot ordering discussion:
  - Flush → record snapshot_last_index → truncate Raft log (this order is non-negotiable)

### Diagram before coding
- Full write path: `client → gRPC → RaftService → mailbox → Node → apply → LSM set`
- Full read path: `client → gRPC → RaftService → mailbox → Node → LSM get → reply`
- Snapshot path: `log threshold hit → force flush MemTable → record snapshot_last_index → truncate log`

### Build steps
1. Define a `StorageEngine` trait with `get` and `set`
2. Implement it with your LSM engine
3. Make Node generic over StorageEngine same way it is already generic over Persister
4. Replace all HashMap calls with StorageEngine calls
5. Run full existing Raft test suite — every test must pass, LSM is a drop-in
6. Run multi-node cluster manually:
   - Write 1,000 keys → kill leader → verify new leader elected → verify all 1,000 keys readable
   - Kill all nodes simultaneously → restart all → verify all 1,000 keys still readable
   - This is your stop criteria

### Break it
- Write continuously while randomly killing and restarting nodes — verify no data loss
- Lower the log compaction threshold to force snapshot → kill all nodes → restart → verify full recovery from snapshot plus subsequent log entries

---

## Retention System

| When | Action | Time |
|------|--------|------|
| Before reading | Write the question you're trying to answer | 2 min |
| After reading | Close the book, explain it in 2 sentences from memory | 5 min |
| Before coding | Draw the data flow on paper | 15 min |
| After each phase | Write 5 sentences in a notes file — plain English | 10 min |
| Before next phase | Reread all previous notes | 5 min |
| Before integration | Reread all 5 notes end to end | 10 min |

---

## What you're NOT building (and why)

- **Multi-level compaction (LevelDB L0/L1/L2)** — size-tiered is sufficient, concepts are the same
- **Block compression** — adds complexity without new concepts
- **Concurrent reads and writes** — single-threaded through the actor is correct for this scope
- **InstallSnapshot RPC** — infrastructure already in place via `snapshot_last_index`, full implementation deferred as 80/20 decision

---

## Stop Criteria

Raft + LSM is done when:
- Write-heavy benchmark doesn't degrade over time
- Read path correctly handles MemTable + multiple SSTables + tombstones
- Cluster survives restart and recovers state correctly from LSM
- Kill -9 any node at any time → restart → no data loss
