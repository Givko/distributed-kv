# Redis-LSM: Distributed Consensus + LSM Storage Engine
## Complete Systems Engineering Capstone

**Author:** Systems Engineer (Career Transformation Plan)  
**Date:** January 2026  
**Duration:** Weeks 25–48 (24 weeks, ~10-15 hours/week)  
**Status:** Pre-Implementation  

---

## 1. Project Overview

Build a **production-grade distributed key-value store** in Rust combining:
- **Raft Consensus** (Leader Election, Log Replication, Persistence)
- **LSM Tree Storage Engine** (MemTable, WAL, SSTables, Compaction)
- **Redis-Compatible Protocol** (RESP)
- **Full Cluster Deployment** (Docker, Kubernetes, Observability)

**Final Deliverable:** A 3-node Raft cluster running on Kubernetes, passing chaos tests (node failures, network partitions, disk corruption), with distributed tracing and SLO dashboards.

**Difficulty:** 9/10 (Ultramarathon-level systems programming)

---

## 2. Strategic Motivation

### Why This Project?

**Instead of:**
- 20+ isolated "drill" projects (Chat App, Load Balancer, etc.)
- Generic system design interviews

**Build:**
- ONE coherent, real database that uses all components you learned

**Resume Signal:**
- "I wrote a distributed consensus engine from first principles"
- "I understand LSM compaction, Raft log matching, and crash recovery"
- "I can deploy to production orchestration (K8s)"

### Learning Arc

| Phase | Component | Difficulty | Learning Goal |
|-------|-----------|------------|----------------|
| **1** | Raft Leader Election | 5/10 | Distributed State, Voting |
| **2** | Raft Log Replication | 7/10 | Consistency, State Machines |
| **3** | Persistence & WAL | 6/10 | Durability, Crash Recovery |
| **4** | TCP Server + RESP Protocol | 3/10 | Networking, Tokio Async |
| **5** | MemTable (In-Memory Buffer) | 5/10 | Concurrent Data Structures |
| **6** | SSTable + Block Cache | 6/10 | Binary Formats, Serialization |
| **7** | LSM Compaction | 9/10 | **The Nightmare** |
| **8** | Production Hardening | 5/10 | Observability, Resilience |

---

## 3. Architecture (High Level)

```
┌─────────────────────────────────────────────────────────────┐
│                    Client (redis-cli)                        │
└──────────────────────────┬──────────────────────────────────┘
                           │ RESP Protocol (TCP)
                           ▼
        ┌──────────────────────────────────────────┐
        │     Node 1 (Leader / Candidate)          │
        │  ┌────────────────────────────────────┐  │
        │  │   Raft Consensus Layer             │  │
        │  │  - Election, Log Replication       │  │
        │  │  - Term Management, Persistence    │  │
        │  └────────────────────────────────────┘  │
        │                   │                        │
        │  ┌────────────────▼────────────────────┐  │
        │  │   State Machine (LSM Engine)       │  │
        │  │  ┌──────────────────────────────┐  │  │
        │  │  │  MemTable (BTreeMap, RwLock) │  │  │
        │  │  └──────────────────────────────┘  │  │
        │  │  ┌──────────────────────────────┐  │  │
        │  │  │  SSTable Files (Disk)        │  │  │
        │  │  │  └─ Compaction Thread        │  │  │
        │  │  └──────────────────────────────┘  │  │
        │  │  ┌──────────────────────────────┐  │  │
        │  │  │  Block Cache (LRU)           │  │  │
        │  │  └──────────────────────────────┘  │  │
        │  └────────────────────────────────────┘  │
        │  ┌────────────────────────────────────┐  │
        │  │   WAL (Write Ahead Log)            │  │
        │  │   ├─ Raft Log File                 │  │
        │  │   └─ Data Log File (Durability)    │  │
        │  └────────────────────────────────────┘  │
        └──────────────────────────────────────────┘
                ▲            │            ▲
                │ Raft RPC   │ Raft RPC   │
                │            │            │
        ┌───────┴────┐   ┌───┴─────┬──────┴─────┐
        │  Node 2    │   │  Node 3 │            │
        │ (Follower) │   │(Follower)            │
        └────────────┘   └─────────┘            │
                                     (Partition/Kill for Chaos Testing)
```

---

## 4. Week-by-Week Execution Plan

### **Phase 1: Raft Consensus (Weeks 25–27)**

#### Week 25: Raft Lab 1 — Leader Election
**Goal:** Deterministic leader election in a 3-node cluster.

**Deliverables:**
- `raft/src/lib.rs`: Core Raft struct
- `RequestVote` RPC implementation
- Randomized election timers (150–300ms)
- Tests: "Single node elected"; "Split vote → retry"; "Stable leader under no failures"

**Key Concepts:**
- Term management
- Voting safety
- Election timeout handling

**Hours:** 13h (Study: 2h, Code: 8h, Test: 2h, Debug: 1h)

---

#### Week 26: Raft Lab 2 — Log Replication
**Goal:** Replicate commands from leader to followers; apply to state machine.

**Deliverables:**
- `AppendEntries` RPC
- Commit index tracking
- State machine trait: `trait StateMachine { fn apply(&mut self, cmd: Command); }`
- Implement dummy KV state machine (HashMap)
- Tests: "Commands replicate in order"; "No lost writes under message delays"

**Key Concepts:**
- Log matching property
- Commit index advancement
- Follower catch-up

**Hours:** 14h

---

#### Week 27: Raft Lab 3 — Persistence & Recovery
**Goal:** Raft survives crashes; recovers state from disk.

**Deliverables:**
- Persist `current_term`, `voted_for`, and `log` entries to disk
- Implement recovery logic: load state on startup
- Tests: "Kill node, restart, verify no split-brain"; "Majority commits, minority crashes, majority continues"

**Key Concepts:**
- Durable state
- fsync strategy
- Recovery protocol

**Milestone:** By end of Week 27, you have a functional **Raft library**. Run:
```bash
./node1 --id 1 --peer 2:localhost:5001 --peer 3:localhost:5002 &
./node2 --id 2 --peer 1:localhost:5000 --peer 3:localhost:5002 &
./node3 --id 3 --peer 1:localhost:5000 --peer 2:localhost:5001 &

# Node 1 is elected leader. Kill it.
kill %1

# Node 2 or 3 takes over. Cluster survives.
```

---

### **Phase 2: Networking & Integration (Weeks 28–32)**

#### Week 28: TCP Server + RESP Protocol
**Original Plan:** System Design Drill (Chat App) → **REPLACE WITH CODECRAFTERS REDIS**

**Deliverables:**
- `server/src/main.rs`: TCP server binding to port 6379
- RESP parser (handles `*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n`)
- In-memory HashMap backend (no Raft yet)
- Commands: `SET key value`, `GET key`, `DEL key`

**Hours:** 12h

**Test:**
```bash
redis-cli -p 6379
> SET foo bar
OK
> GET foo
"bar"
```

---

#### Week 29: Node Discovery & Cluster Formation
**Goal:** Nodes discover each other and form a Raft cluster.

**Deliverables:**
- Static config file (3 nodes, ports 6379–6381)
- Raft RPC layer (gRPC or custom binary protocol over TCP)
- Test: Start 3 nodes, verify leader election

**Hours:** 12h

---

#### Week 30: Consistency Models Deep Dive (Jepsen Testing)
**Goal:** Prove your Raft implementation is linearizable.

**Deliverables:**
- Write a test harness that:
  - Sends concurrent `SET` commands
  - Randomly partitions the network
  - Verifies no lost writes, no stale reads
- Understand: What guarantees does your Raft provide?

**Hours:** 12h

---

#### Week 31: Smart Client / Proxy
**Goal:** Route client requests to the Raft leader.

**Deliverables:**
- Client-side library that caches the leader's identity
- On leader failure, retry with new leader
- Failover detection (connection timeout → ask a follower for leader hint)

**Hours:** 12h

---

#### Week 32: Raft + Network Integration (Consolidation)
**Goal:** Kill a node mid-request; verify the cluster survives.

**Deliverables:**
- Merge `raft/` + `server/` + `client/`
- End-to-end test: 3-node cluster, `redis-cli` sends commands, nodes fail randomly
- Verify: No duplicate executions, no lost writes

**Milestone:**
```bash
redis-cli -p 6379
> SET counter 0
> INCR counter
(integer) 1
> INCR counter
(integer) 2
# Kill Node 1 (leader)
> INCR counter
(integer) 3  # Automatic failover, no lost increments
```

---

### **Phase 3: Storage Engine — LSM Tree (Weeks 33–40)**

#### Week 33: MemTable + Thread-Safe LRU Cache
**Goal:** Implement an in-memory buffer for writes before they hit disk.

**Deliverables:**
- `MemTable`: `Mutex<BTreeMap<String, String>>`
- Property tests: No duplicates, LRU order maintained (optional for MemTable)
- Benchmark: Latency of `SET` operations

**Hours:** 12h

**Why BTreeMap, not SkipList?** Correctness first. Optimize later.

---

#### Week 34: File I/O & Durability
**Goal:** Understand disk I/O performance characteristics.

**Deliverables:**
- Benchmark: Sequential vs random writes with/without `fsync`
- Understand: Why `fsync` is slow; when it's necessary
- Decision: When will your code call `fsync`? (After every write? Every batch?)

**Hours:** 12h

---

#### Week 35: Write-Ahead Log (WAL)
**Goal:** Durably persist writes to disk before applying to MemTable.

**Deliverables:**
- WAL format: `[length: u32][checksum: u32][data]`
- Logic: `SET key value` → Write to WAL → fsync → Update MemTable
- Crash test: Kill the process; restart; verify no lost writes

**Hours:** 12h

**Test:**
```bash
# Session 1
SET foo bar
# Kill process (SIGKILL, don't let it clean up)

# Session 2 (restart)
$ ./node1
# On startup, WAL is replayed
GET foo
"bar"  # Data survives!
```

---

#### Week 36: Block Cache (Buffer Pool)
**Goal:** Cache disk blocks to avoid repeated reads.

**Deliverables:**
- `BlockCache`: LRU cache of file blocks (4KB chunks)
- `get_block(file_id, offset)` → Check cache → If miss, read from disk
- Metrics: Hit rate, miss rate, eviction rate

**Hours:** 12h

---

#### Week 37: SSTable Format + Flushing
**Goal:** Dump the MemTable to disk as a sorted file.

**Deliverables:**
- SSTable binary format:
  ```
  [header: magic=0xDEADBEEF]
  [metadata: num_entries, key_value_pairs_offset]
  [data: key1, value1, key2, value2, ...]
  [sparse_index: key1→offset, key100→offset, key200→offset, ...]
  [footer: index_offset]
  ```
- Flush logic: When MemTable reaches 10MB, write SSTable to disk, clear MemTable
- Test: Insert 100M keys; verify they all end up on disk

**Hours:** 14h

---

#### Week 38: Compaction Strategy (The Nightmare)
**Goal:** Merge old SSTables to keep read performance stable.

**Deliverables:**
- **Leveled Compaction:**
  - Level 0: Up to 4 SSTables (flushed from MemTable)
  - Level 1: 10× larger, single file
  - Level 2+: Exponentially larger
- Background compactor thread:
  - Find two overlapping SSTables
  - Merge them (remove duplicates, keep latest value)
  - Write new SSTable
  - Delete old files
  - Update manifest file (tracks valid SSTables)
- Tests:
  - "Read still works during compaction"
  - "Compaction doesn't lose data"
  - "Manifest file survives crash"

**Key Nightmare:** Concurrent reads + compaction = Use-after-free bugs. Protect with `Arc<Mutex<Manifest>>`.

**Hours:** 18h (Extra time for debugging)

---

#### Week 39: Get Path Optimization (Reading)
**Goal:** Fast lookups across MemTable + SSTables.

**Deliverables:**
- `GET key`:
  1. Check MemTable (fast, in-memory)
  2. If miss, check SSTables (from newest to oldest)
  3. Use Bloom filters to avoid disk reads for non-existent keys
- Metrics: Read latency, Bloom filter false positive rate

**Hours:** 12h

---

#### Week 40: Consolidation
**Goal:** Pull together MemTable + WAL + SSTables + Compaction.

**Deliverables:**
- End-to-end test: Insert 1B keys (simulate); verify no data loss
- Benchmark: Compare with/without caching
- Document: Architecture decision log

**Milestone:** You have a **working LSM storage engine**. It passes crash tests.

---

### **Phase 4: Production Hardening (Weeks 41–48)**

#### Week 41: Circuit Breakers + Timeouts
**Goal:** Don't keep trying to replicate to a dead node.

**Deliverables:**
- Circuit breaker for Raft RPC client:
  - State: Closed (normal) → Open (failing) → Half-Open (probing)
  - Threshold: 5 failures in 10 seconds → Open
  - Recovery: Try 1 request every 5 seconds
- Exponential backoff with jitter for retries

**Hours:** 12h

---

#### Week 42: Rate Limiting (Token Bucket)
**Goal:** Protect the cluster from client storms.

**Deliverables:**
- Per-client rate limiter: 10k requests/sec
- Middleware: Reject requests if rate exceeded
- Test: Send 20k req/sec; verify only 10k processed

**Hours:** 12h

---

#### Week 43: Transactions & 2PC (Optional Advanced)
**Goal:** Understand the cost of distributed transactions (for interview prep).

**Deliverables:**
- Simple 2PC implementation (coordinator + 2 participants)
- Measure latency: 2PC vs independent writes
- Conclusion: "2PC adds 2 RTTs; Raft adds 1 RTT"

**Hours:** 12h (Can skip if time-limited)

---

#### Week 45: Docker + Containerization
**Goal:** Package your cluster as Docker images.

**Deliverables:**
- `Dockerfile`: Multi-stage build
  - Stage 1: Build Rust binary
  - Stage 2: Minimal runtime image (Alpine + binary only)
- `docker-compose.yml`: 3-node cluster
- Test: `docker-compose up`; verify cluster works

**Hours:** 12h

---

#### Week 46: Kubernetes Deployment
**Goal:** Run on production orchestration.

**Deliverables:**
- `StatefulSet` for the 3-node cluster
- `ConfigMap` for tuning parameters
- `Service` for client discovery
- Liveness/readiness probes
- Rolling update test: Upgrade one node, verify zero downtime

**Hours:** 12h

---

#### Week 47: Observability (Tracing + SLOs)
**Goal:** Full visibility into your system.

**Deliverables:**
- Instrument Raft with `tracing` crate
- Export traces to Jaeger
- Define SLOs:
  - p99 latency < 50ms
  - 99.9% requests succeed
  - Error budget: 8.64 seconds downtime per day
- Set up burn-rate alerting

**Hours:** 12h

---

#### Week 48: Final Capstone — Chaos Testing
**Goal:** Prove the system survives everything.

**Deliverables:**
- Chaos test suite:
  - Kill random nodes every 10 seconds
  - Network partition: 50% packet loss on one node
  - Disk full: Fill the disk; verify graceful degradation
  - CPU throttle: Limit to 10% CPU for 30 seconds
- Run for 5 minutes; verify:
  - No lost writes
  - No duplicates
  - Latency degradation is acceptable
- Write "Postmortem": What failed? How did you fix it?

**Deliverables:**
- Blog post: "How I Built a Distributed Database from Scratch"
- GitHub README with architecture diagrams
- Performance benchmarks (throughput, latency, resource usage)

**Hours:** 20h (Extended capstone week)

---

## 5. Repository Structure

```
distributed-redis-rust/
├── Cargo.toml                    # Workspace root
├── raft/                         # Consensus library
│   ├── src/
│   │   ├── lib.rs               # Raft core
│   │   ├── rpc.rs               # RequestVote, AppendEntries
│   │   ├── storage.rs           # WAL persistence
│   │   └── tests/
│   └── Cargo.toml
├── server/                       # TCP + RESP
│   ├── src/
│   │   ├── main.rs              # Server entry point
│   │   ├── protocol.rs          # RESP parser
│   │   ├── handler.rs           # Command execution
│   │   └── tests/
│   └── Cargo.toml
├── storage/                      # LSM engine
│   ├── src/
│   │   ├── lib.rs               # Public API
│   │   ├── memtable.rs          # In-memory buffer
│   │   ├── wal.rs               # Write-ahead log
│   │   ├── sstable.rs           # Sorted string table
│   │   ├── compactor.rs         # Compaction logic
│   │   ├── block_cache.rs       # Block cache
│   │   └── tests/
│   └── Cargo.toml
├── client/                       # Smart client library
│   ├── src/
│   │   ├── lib.rs
│   │   └── discovery.rs         # Leader discovery
│   └── Cargo.toml
├── tests/                        # Integration tests
│   ├── chaos_test.rs            # Chaos engineering
│   ├── linearizability_test.rs  # Jepsen-style
│   └── crash_recovery_test.rs
├── k8s/                          # Kubernetes manifests
│   ├── statefulset.yaml
│   ├── service.yaml
│   └── configmap.yaml
├── docker-compose.yml            # Local 3-node cluster
├── Dockerfile                    # Single image for all nodes
├── benches/                      # Performance benchmarks
│   ├── throughput.rs
│   └── latency.rs
├── README.md
├── ARCHITECTURE.md               # Detailed design doc
└── .github/
    └── workflows/
        └── ci.yml               # Test on every commit
```

---

## 6. Key Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Consensus** | Raft (from scratch) | Learn algorithm deeply |
| **Language** | Rust | Memory safety for concurrent systems |
| **Storage** | LSM Tree | Write-optimized; used by real DBs (RocksDB, etc.) |
| **Network** | Custom binary protocol | Full control; understand serialization |
| **Async Runtime** | Tokio | Battle-tested; production-grade |
| **Concurrency** | RwLock + Arc | Simple; correct. Optimize later if needed. |
| **Testing** | Property-based + Chaos | Prove correctness under randomness |

---

## 7. Difficulty Breakdown & Risk Mitigation

### Hardest Parts (and how to survive them)

| Component | Difficulty | Risk | Mitigation |
|-----------|-----------|------|-----------|
| **Raft Logic** | 7/10 | Non-deterministic bugs (timing-dependent) | Extensive logging; deterministic tests |
| **LSM Compaction** | 9/10 | Use-after-free (logical), race conditions | Use Arc + Mutex; write crash tests first |
| **Concurrent MemTable** | 6/10 | Deadlocks, lock contention | Start with `Mutex<BTreeMap>`; optimize later |
| **Integration Testing** | 7/10 | System behaves differently under load | Use `tokio::test` for determinism |

---

## 8. Success Criteria

You succeed when:

1. ✅ **Correctness:**
   - Raft passes all 3 labs with zero race conditions under deterministic testing
   - Jepsen test verifies linearizability (no lost writes, no stale reads)
   - Chaos test: 5-minute run with random node kills → zero data loss

2. ✅ **Performance:**
   - Single node: 10k SET/sec
   - 3-node cluster: 5k SET/sec (Raft overhead)
   - p99 latency < 50ms

3. ✅ **Durability:**
   - Crash test: Kill process after `SET` → restart → data recovers
   - No data loss across any failure scenario

4. ✅ **Deployment:**
   - `docker-compose up` → 3-node cluster works
   - `kubectl apply -f k8s/` → Runs on Kubernetes
   - Rolling update → Zero downtime

5. ✅ **Observability:**
   - Traces visible in Jaeger
   - SLO dashboard shows p99 latency, success rate
   - Alerts fire on burn-rate

---

## 9. Timeline Summary

| Phase | Weeks | Focus | Output |
|-------|-------|-------|--------|
| **1** | 25-27 | Raft Consensus | Working 3-node Raft cluster (in-process) |
| **2** | 28-32 | Network Integration | TCP + RESP; Raft over network |
| **3** | 33-40 | Storage Engine | Full LSM tree with compaction |
| **4** | 41-48 | Production | Docker, K8s, Observability, Chaos testing |

**Total:** 24 weeks, ~10-15 hours/week = ~300 hours of focused engineering.

---

## 10. Portfolio Impact

When done, you can show:
- **GitHub:** Full source code with 100+ commits
- **Blog post:** "How I built a distributed database"
- **Demo video:** 3-node cluster handling chaos
- **Architecture doc:** Detailed design decisions

**Interview Signal:**
- "I understand Raft consensus deeply"
- "I know how real databases (RocksDB, etc.) work"
- "I can debug non-deterministic failures"
- "I can operate systems in production (K8s, observability)"

This project **alone** is enough to get you job offers from:
- Infrastructure companies (Databricks, Stripe, Figma)
- Distributed systems teams (AWS, Cloudflare, Google)
- Startups building platform infrastructure

---

## 11. Getting Started

**Week 1 (Week 25):**
1. Read Raft paper (§5.2)
2. Scaffold `raft/` crate with `cargo new`
3. Implement `RequestVote` RPC
4. Write first test: "Single node wins election"

**Commit 1:** "Initial Raft scaffold with leader election (in-process simulation)"

---

**Final Note:** This is hard. Expect bugs. Expect 2-3 week delays on LSM compaction. Respect the complexity. But when you finish, you will be a different engineer—one who understands how real systems are built.

Good luck. 🚀
