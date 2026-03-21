# distributed-kv

Distributed key-value store in Rust implementing Raft consensus and an LSM storage engine from scratch — no consensus or storage libraries. Built to gain hands-on depth in consensus protocols, crash recovery, and storage engine internals.

## Development Notes

The goal is to understand distributed systems and storage engine internals by implementing them, not to build something production ready.

**Error handling** — `unwrap()`, `expect()`, and similar panic-on-failure calls appear throughout the codebase. These are intentional shortcuts while the focus is on understanding and implementing the core logic correctly. Proper error propagation will be tidied up once the implementation stabilises.

## Status

| Component | Status |
|-----------|--------|
| Raft consensus | ✅ Complete |
| gRPC transport | ✅ Complete |
| Persistent state + crash recovery | ✅ Complete |
| Snapshot boundary tracking | ✅ Complete |
| InstallSnapshot RPC | Planned |
| LSM MemTable + WAL | ✅ Complete |
| SSTable flush | ✅ Complete |
| SSTable read path | ✅ Complete |
| Bloom filters | Planned |
| Size-tiered compaction | Planned |
| Raft ↔ LSM integration | ✅ Complete |
| RESP wire protocol | Planned |

## Architecture

Current state machine: `BTreeMap`-backed LSM tree with a Write-Ahead Log, SSTable flush, and multi-level read path (MemTable → SSTables). Planned: bloom filters and size-tiered compaction.

```
┌─────────────────────────────────────────────────────────┐
│                      Client (gRPC)                       │
│                   GET / SET operations                   │
└────────────────────────────┬────────────────────────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
        ┌──────────┐  ┌──────────┐  ┌──────────┐
        │  Node 1  │  │  Node 2  │  │  Node 3  │
        │ (Leader)  │  │(Follower)│  │(Follower)│
        └────┬─────┘  └────┬─────┘  └────┬─────┘
             │              │              │
        ┌────┴─────┐  ┌────┴─────┐  ┌────┴─────┐
        │   Raft   │  │   Raft   │  │   Raft   │
        │Consensus │◄─►│Consensus │◄─►│Consensus │
        └────┬─────┘  └────┬─────┘  └────┬─────┘
             │              │              │
        ┌────┴─────┐  ┌────┴─────┐  ┌────┴─────┐
        │  State   │  │  State   │  │  State   │
        │ Machine  │  │ Machine  │  │ Machine  │
        └──────────┘  └──────────┘  └──────────┘
```

### Raft Consensus Layer

The cluster runs a 3-node Raft protocol for strong consistency. Every write is replicated to a majority before being acknowledged to the client.

**Leader election** — randomized election timeouts (150–300ms) prevent split votes. Candidates request votes from all peers, requiring a strict majority. Nodes step down to follower on discovering a higher term in any RPC. Stale vote replies from old terms are discarded.

**Log replication** — the leader appends entries to its local log, persists state, then sends AppendEntries RPCs to all peers in parallel. Followers perform consistency checks against `prev_log_index` and `prev_log_term`, rejecting entries that don't match. On rejection, the leader decrements `next_index` for that peer and retries with earlier entries until logs converge. Conflicting entries on followers are truncated before appending new ones.

**Commit and apply** — the leader advances `commit_index` only when a majority of nodes have replicated an entry *and* the entry's term matches the current term (the Raft safety property). Followers learn the commit index via `leader_commit` in AppendEntries RPCs and advance accordingly, taking the minimum of `leader_commit` and their own `last_log_index`. Committed entries are applied to the state machine in order.

**Persistence** — whenever `current_term`, `voted_for`, or log entries change, they're serialized to disk and fsync'd before replying success to RPCs. `commit_index` is also persisted as a startup optimization — it allows the node to immediately apply up to the last known committed index on restart, without waiting for a leader heartbeat to re-establish `leader_commit`. This is not required for Raft safety (`commit_index` is volatile in the paper).

**Snapshot boundary tracking** — `snapshot_last_index` and `snapshot_last_term` support log compaction. Log entries are indexed relative to the snapshot point. AppendEntries consistency checks correctly handle the boundary between snapshotted and live entries. InstallSnapshot RPC is not implemented — lagging followers catch up via log replay only.

**Actor-based concurrency** — each Raft node runs as an async actor with an mpsc mailbox. All state mutations happen in a single `select!` loop — no locks, no shared mutable state. Incoming gRPC requests are translated into mailbox messages with oneshot reply channels. The network sender runs as a separate task, spawning per-RPC tasks with 100ms connection timeouts to prevent slow peers from blocking the actor loop.

**Client interaction** — writes (`SET`) go through Raft consensus: the leader replicates the command, waits for majority acknowledgment, commits, applies, then responds. The leader tracks pending client reply channels keyed by log index. Non-leader nodes reject writes. Reads (`GET`) are served by any node directly from the local state machine; followers may return stale data (see Known Limitations).

### LSM Storage Engine

Replaces the in-memory `HashMap` state machine with a durable, crash-safe storage engine.

- **MemTable** — in-memory sorted map (`BTreeMap<Vec<u8>, MemTableEntry>`) for fast writes, with size tracking to trigger flushes
- **Write-Ahead Log** — binary-encoded append-only file, fsync'd on every write for durability before acknowledgment. Supports rotation for flush and recovery of partial flushes
- **SSTable flush** — when MemTable exceeds size threshold, a background task flushes sorted entries to an immutable SSTable file with a sparse index. Single fsync at end of flush; old WAL deleted only after SSTable fsync succeeds. Retried indefinitely on failure (idempotent via truncate-on-write)
- **SSTable read path** — binary search on the sparse index to find the starting offset, then sequential scan from that point. SSTables are checked newest-to-oldest; first match wins. Min/max key ranges per SSTable skip files that can't contain the target key
- **Multi-level read path** — check MemTable first, then flushing MemTable (if flush in progress), then SSTables newest-to-oldest. Tombstones short-circuit the search
- **Crash recovery** — on startup, replay the current WAL into a fresh MemTable. If a stale flush WAL exists (`wal.log.tmp`), its entries are recovered and the flush is retried, ensuring no data loss even on mid-flush crashes
- **File I/O abstraction** — `FileSystem` and `FileHandle` traits decouple storage code from the OS, enabling in-memory fakes for deterministic unit testing
- **Binary format** — length-prefixed records: `[total_len: 4B][index: 8B][op: 1B][key_len: 4B][key][value_len: 4B][value]`. Same encoding for WAL entries and SSTable records
- **Bloom filters** *(planned)* — per-SSTable probabilistic filter to eliminate unnecessary disk reads for missing keys. Currently using min/max key ranges instead
- **Size-tiered compaction** *(planned)* — merge N similarly-sized SSTables into one, resolving duplicates and expired tombstones

### RESP Wire Protocol *(planned)*

Redis-compatible protocol layer so standard Redis clients (`redis-cli`, `go-redis`, etc.) can talk to the cluster directly, replacing the current gRPC client interface.

- **Supported commands:** `GET`, `SET`, `DEL`, `INCR`
- **RESP2 parsing** for request/response framing
- Command dispatch to Raft layer for writes, direct LSM read for reads

## Design Decisions

### Why Raft
Raft is well-specified enough to implement correctly from a paper, and complex enough to expose real distributed systems challenges: leader election edge cases, log divergence after partitions, and crash recovery ordering. It's a good target for learning because the invariants are clearly defined and testable.

### Why actor-based concurrency
The Raft node owns all its state — term, log, commit index, peer tracking — behind a single `tokio::select!` loop with an mpsc mailbox. No locks, no shared mutable state. gRPC requests become mailbox messages with oneshot reply channels; the network sender is a separate task that spawns per-RPC tasks so a slow peer can't block the main loop. A natural fit for Raft's single-threaded event loop model.

### Why LSM over B-tree
LSM is write-optimized — all writes are sequential appends (WAL then SSTable flush). It's also a good learning target: the WAL, memtable, SSTable, and compaction layers each teach something distinct about storage engine design.

### Why bytes, not strings
The LSM engine stores `Vec<u8>` keys and values with lexicographic byte ordering and never interprets the content. Type handling (strings for SET/GET, integers for INCR) belongs to the RESP layer above. Keeping the storage engine as a plain sorted byte map is a cleaner separation of concerns.

### Crash safety
Whenever `current_term`, `voted_for`, or log entries change, they're serialized to disk and fsync'd before replying success. On restart, committed entries are replayed into the state machine. The LSM layer adds a binary WAL with per-write fsync, SSTable flush with a single fsync at the end, and WAL deletion only after the SSTable is durably on disk.

## Correctness Invariants

- Uncommitted entries are never applied to the state machine
- Log matching property enforced via `prev_log_index`/`prev_log_term` checks on every AppendEntries
- All committed entries survive crash and restart (persisted before acknowledgment, replayed on recovery)
- State machine applies entries strictly in log order
- Node steps down to follower on discovering a higher term in any RPC or reply
- Leader only commits entries from its own term (prevents the ghost commit problem from §5.4.2 of the Raft paper)

Writes are linearizable (Raft quorum commit). Reads are local and potentially stale unless served by the leader with a read-index or lease mechanism (not yet implemented).

## Non-Goals (For Now)

I'm intentionally keeping this to one Raft group so I can go deep on correctness and durability first. These are scope decisions, not oversights:

- **No partitioning** — single Raft group. Partitioning (consistent hashing, range-based sharding, multi-Raft) is understood conceptually but adds breadth, not depth.
- **No MVCC transactions** — single-key operations only. Multi-key transactions require snapshot isolation and timestamp ordering.
- **No read linearizability** — GET is served by any node directly from the local state machine. Followers may return stale data, and a partitioned leader can serve stale reads until it discovers a new term. Read-index or lease-based reads would fix this but are out of scope.
- **No pre-vote protocol** — a partitioned node that rejoins can trigger unnecessary elections by having a higher term. Pre-vote would let it check if an election is needed before incrementing its term.
- **No InstallSnapshot RPC** — snapshot boundary tracking and log indexing are implemented, but lagging followers that fall behind the snapshot point cannot catch up. If this occurs, the follower must be wiped and rejoined manually.
- **Leader hint is empty after restart** — a restarted follower doesn't know the current leader until it receives its first heartbeat.

## Project Structure

```
src/
├── main.rs                   # CLI args, gRPC server setup, actor wiring
├── lib.rs                    # Crate root — concrete type aliases (LsmTreeNode)
├── storage/
│   ├── mod.rs                # Module declarations + re-exports (MemTableEntry)
│   ├── entry.rs              # WAL/SSTable entry type (op, key, value, index)
│   ├── encoder.rs            # Binary codec: encode/decode WAL records
│   ├── wal.rs                # WalStorage trait + file-backed Wal implementation
│   ├── memtable.rs           # MemTable trait + BTreeMap implementation
│   ├── fs.rs                 # FileSystem + FileHandle traits (DI for testability)
│   ├── sstable.rs            # SSTable read/write, sparse index, metadata manager
│   └── lsm_tree.rs           # LSMTree: MemTable + WAL + flush + recovery; StorageEngine impl
└── raft/
    ├── mod.rs                # Module declarations + generated proto inclusion
    ├── raft_types.rs         # Message types, log entries, RPC data structures
    ├── network_sender.rs     # Outbound RPC worker (spawns per-peer tasks)
    ├── network_types.rs      # Outbound message enum (RequestVote, AppendEntries)
    ├── state_machine.rs      # StorageEngine trait + StateMachine wrapper (apply, get)
    ├── state_persister.rs    # Persister trait + file-based JSON implementation
    ├── proto/
    │   └── raft.proto        # gRPC service definition
    └── node/
        ├── mod.rs            # Module declarations + re-exports (Node, State)
        ├── core.rs           # Node<T, SM> struct, actor loop, core handlers, tests
        ├── election.rs       # Leader election (RequestVote send/receive)
        ├── replication.rs    # Log replication (AppendEntries send/receive)
        └── log.rs            # Log helpers (last_log_index, get_log_entry, get_log_term)
```

## Quick Start (Single Node)

A single-node cluster elects itself leader immediately — useful for exploring the API without managing multiple terminals.

```bash
cargo run -- --id node1 --port 5001
```

```bash
# Set a key
grpcurl -plaintext -d '{"key": "foo", "value": "bar"}' 127.0.0.1:5001 proto.Raft/Set

# Get it back
grpcurl -plaintext -d '{"key": "foo"}' 127.0.0.1:5001 proto.Raft/Get
```

## Running a 3-Node Cluster

```bash
# Terminal 1
cargo run -- --id node1 --port 5001 --nodes 127.0.0.1:5002,127.0.0.1:5003

# Terminal 2
cargo run -- --id node2 --port 5002 --nodes 127.0.0.1:5001,127.0.0.1:5003

# Terminal 3
cargo run -- --id node3 --port 5003 --nodes 127.0.0.1:5001,127.0.0.1:5002
```

Nodes elect a leader within a few hundred milliseconds. Writes to the leader are replicated before acknowledgment. Kill any single node and the cluster continues operating. Restart the killed node and it catches up via log replication.

### Client interaction (gRPC)

```bash
# Write a key (must hit the leader node)
grpcurl -plaintext -d '{"key": "foo", "value": "bar"}' 127.0.0.1:5001 proto.Raft/Set

# Read a key (can hit any node; followers may return stale data)
grpcurl -plaintext -d '{"key": "foo"}' 127.0.0.1:5002 proto.Raft/Get
```

Verify the service and method names match your `.proto` package/service definition.

## Testing

```bash
cargo test
```

The Raft tests use mock persisters (TestPersister, LoadedStatePersister, RecordingPersister, FailingLoadPersister) to isolate consensus logic from disk I/O. The storage tests use an in-memory `FileSystem` fake to test SSTable and WAL behaviour without touching the filesystem.

### Raft consensus

**Leader election:** granting votes to up-to-date candidates, rejecting votes when already voted for a different candidate, rejecting candidates with stale terms, rejecting candidates with shorter or older logs, stepping down on higher terms, majority vote counting, and idempotent voting for the same candidate.

**Log replication:** appending entries on matching prev_log_index/term, rejecting on prev_log_index gaps, rejecting on prev_log_term mismatches, truncating conflicting entries before appending, updating commit_index from leader_commit bounded by local log length, and stepping down on AppendEntries from a higher term.

**AppendEntries reply handling:** advancing match_index/next_index on success, decrementing next_index on failure (with floor at 1), stepping down on higher term in reply, and majority-based commit advancement with the current-term safety check.

**Persistence and recovery:** loading persistent state on startup, replaying committed entries into the state machine (including key overwrites), verifying state is persisted after leader writes, handling missing state files gracefully, and propagating persistence errors.

**Snapshot boundary:** accepting AppendEntries when prev_log_index matches snapshot_last_index/term, rejecting when snapshot term doesn't match.

**Safety invariants:** same-term heartbeats don't reset voted_for (preventing double-voting within a term).

### Storage engine

**Encoder:** encode/decode round-trips for set and delete operations, corruption detection on truncated or malformed records, multi-entry encoding with correct offset accumulation.

**MemTable:** byte-accurate size tracking across insertions, overwrites, and deletes; lexicographic key ordering; tombstone handling.

**LSM tree:** get/set/delete through the full write path, WAL persistence and recovery (including recovery after set+delete sequences and recovery followed by new writes), flush simulation with sparse index offset correctness, and Raft index tracking across operations.

**SSTable:** read path with sparse index binary search, range filtering (skip SSTables where key is out of min/max bounds), newer-SSTable-wins ordering, fallback to older SSTables, keys between sparse index entries found via linear scan. Flush path: file creation with incrementing IDs, data round-trips through encoder, sparse index deserialization, min/max key metadata, size tracking, and metadata persistence and recovery across manager instances.

## Built With

- **Rust** — memory safety without garbage collection
- **tokio** — async runtime for the actor loop and concurrent network I/O
- **tonic** — gRPC framework for inter-node RPCs and client interface
- **serde** / **serde_json** — state persistence serialization
- **clap** — CLI argument parsing
