# distributed-kv

A learning project — a distributed key-value store implemented in Rust to understand Raft consensus and LSM storage engines from the inside out. Not intended for production use.

Raft and the LSM engine are built from scratch — no embedded databases, no consensus libraries. The project uses tokio for async scheduling and tonic for gRPC transport. RESP support is planned; today the client API is gRPC.

I built this to learn Raft and LSMs end-to-end by implementing the tricky parts myself — leader election edge cases, crash recovery ordering, WAL/SSTable durability guarantees.

## Development Notes

This is purely a learning project. The goal is to understand distributed systems and storage engine internals by implementing them, not to build something deployable.

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
| SSTable flush + read path | 🔄 In Progress |
| Bloom filters | Planned |
| Size-tiered compaction | Planned |
| Raft ↔ LSM integration | ✅ Complete |
| RESP wire protocol | Planned |

## Architecture

Current state machine: `BTreeMap`-backed LSM tree with a Write-Ahead Log. Planned: SSTable flush, bloom filters, and size-tiered compaction.

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

### LSM Storage Engine *(in progress)*

Replaces the in-memory `HashMap` state machine with a durable, crash-safe storage engine.

- **MemTable** — in-memory sorted map (`BTreeMap<Vec<u8>, Vec<u8>>`) for fast writes
- **Write-Ahead Log** — binary-encoded append-only file, fsync'd on every write for durability before acknowledgment
- **SSTable flush** — when MemTable exceeds size threshold, flush sorted entries to immutable SSTable file with a sparse index. Single fsync at end of flush; old WAL deleted only after SSTable fsync succeeds
- **Multi-level read path** — check MemTable first, then SSTables newest-to-oldest. First result wins. Tombstones short-circuit the search
- **Bloom filters** — per-SSTable probabilistic filter loaded into memory, eliminates unnecessary disk reads for missing keys
- **Size-tiered compaction** — merge N similarly-sized SSTables into one, resolving duplicates and expired tombstones. Old SSTables deleted only after merged output is renamed into place (atomic on Linux)
- **Binary format** — length-prefixed records with big-endian u32 sizes: `[op_type: 1B][key_len: 4B][key][value_len: 4B][value]`. Same encoding for WAL entries and SSTable records

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
- **Heartbeat not per-peer optimized** — the leader sends the same AppendEntries to all peers using its own `last_log_index` rather than each peer's `next_index`, so it may resend entries a follower already has and trigger unnecessary backtracking on rejection. Mainly a performance wart.
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
│   └── lsm_tree.rs           # LSMTree: MemTable (BTreeMap) + WAL; StorageEngine impl
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

The test suite uses mock persisters (TestPersister, LoadedStatePersister, RecordingPersister, FailingLoadPersister) to isolate Raft logic from disk I/O.

**Leader election:** granting votes to up-to-date candidates, rejecting votes when already voted for a different candidate, rejecting candidates with stale terms, rejecting candidates with shorter or older logs, stepping down on higher terms, majority vote counting, and idempotent voting for the same candidate.

**Log replication:** appending entries on matching prev_log_index/term, rejecting on prev_log_index gaps, rejecting on prev_log_term mismatches, truncating conflicting entries before appending, updating commit_index from leader_commit bounded by local log length, and stepping down on AppendEntries from a higher term.

**AppendEntries reply handling:** advancing match_index/next_index on success, decrementing next_index on failure (with floor at 1), stepping down on higher term in reply, and majority-based commit advancement with the current-term safety check.

**Persistence and recovery:** loading persistent state on startup, replaying committed entries into the state machine (including key overwrites), verifying state is persisted after leader writes, handling missing state files gracefully, and propagating persistence errors.

**Snapshot boundary:** accepting AppendEntries when prev_log_index matches snapshot_last_index/term, rejecting when snapshot term doesn't match.

**Safety invariants:** same-term heartbeats don't reset voted_for (preventing double-voting within a term).

## Built With

- **Rust** — memory safety without garbage collection
- **tokio** — async runtime for the actor loop and concurrent network I/O
- **tonic** — gRPC framework for inter-node RPCs and client interface
- **serde** / **serde_json** — state persistence serialization
- **clap** — CLI argument parsing
