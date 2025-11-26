# The MAANG Systems Engineer Study Plan
**48 Weeks → Senior/Staff Engineer Readiness**

**Target**: Well-rounded systems generalist competitive for Google L5/L6, Meta E5/E6, Amazon L6/L7  
**Philosophy**: Brick-by-brick fundamentals → no niche specialization → production-ready skills  
**Time Budget**: 10-15h/week (sustainable for full-time employed)

---

## Weekly Rhythm & Quality Gates

### Time Structure (12h default)
- **Monday (2h)**: Read core material, write learning objectives
- **Tuesday-Thursday (7h)**: Code + test (aim for Bronze by Wed night)
- **Friday (3h)**: Profile/bench, docs, reflection

### Bronze/Silver/Gold Framework
**Bronze** (minimum to advance):
- Core implementation works
- Unit tests pass (happy path + 2 edge cases)
- Can explain what/why in simple terms

**Silver** (target for most weeks):
- Bronze + basic benchmarks/profiling
- Failure modes identified
- Trade-offs articulated

**Gold** (only if time permits):
- Advanced optimization
- Property tests
- Extended soak testing

### Three Shipping Artifacts (every week)
1. **Invariants Card** (≤10 lines): Core properties that must hold
2. **Failure Modes** (≤10 lines): What breaks and why
3. **Perf Note** (≤10 lines): Complexity/bottlenecks/optimization opportunities

### Safety Nets
- **Stuck? 2 Pomodoros max** → change approach, stub it, or ask for help
- **Behind schedule?** → Ship Bronze and move on
- **Burned out?** → Take consolidation week early

---

## Phase 1: Foundations (Weeks 1-10)
*Build perfect fundamentals before distributed systems*

### Week 1: Rust Foundations
**Goal**: Get comfortable with Rust before combining with complex topics

**Learn**:
- Rust Book Ch. 1-4 (ownership, borrowing, lifetimes)
- Cargo, rustfmt, clippy workflow

**Build**:
- CLI guessing game (rand crate)
- Temperature converter
- FizzBuzz with tests

**Stop when**: Borrow checker errors make sense; clean `cargo clippy` runs

**Study**: Rust Book, official docs  
**Hours**: R3 C6 T2 D1 = 12h

---

### Week 2: Asymptotic Analysis + Benchmarking
**Goal**: Develop intuition for complexity and measurement

**Learn**:
- Big-O/Θ/Ω notation
- Criterion basics
- Why benchmarks lie (warmup, variance, outliers)

**Build**:
- Implement O(1), O(n), O(n²), O(n log n) toy functions
- Criterion benchmarks showing expected scaling
- Plot results, explain variance

**Stop when**: Can classify algorithms on sight; understand benchmark noise

**Study**: Skiena Ch. 2, Criterion docs  
**Hours**: R3 C5 T1 B2 D1 = 12h

---

### Week 3: Singly Linked List (Safe Rust)
**Goal**: Master ownership through the hardest beginner problem

**Learn**:
- `Box<T>`, `Option<T>`
- Ownership transfer in data structures
- Why borrow checker prevents UAF/double-free

**Build**:
- `List<T>` with push/pop/peek/iter
- Edge case tests (empty, single element, iteration during modification)

**Stop when**: Can explain why certain designs don't compile

**Study**: "Learning Rust With Entirely Too Many Linked Lists" (first 2 chapters)  
**Hours**: R2 C7 T2 D1 = 12h

**Invariants**: Non-empty list has head; no cycles; drop is O(n)  
**Failures**: Stack overflow on long lists (recursive drop)  
**Perf**: Cache-unfriendly; pointer chasing

---

### Week 4: Stack & Queue (Vec-based)
**Goal**: Understand amortized analysis and cache effects

**Learn**:
- Amortized O(1) for dynamic arrays
- Why `Vec` beats linked lists in practice
- False sharing basics

**Build**:
- `Stack<T>` and `Queue<T>` using `Vec`
- Micro-benchmarks vs linked list versions
- Underflow error handling

**Stop when**: Can explain when to use each; prove amortized bounds

**Study**: CLRS Ch. 17 (Amortized Analysis), Rust std docs  
**Hours**: R2 C6 T2 B1 D1 = 12h

**Invariants**: Capacity ≥ len; resize on push when full  
**Failures**: OOM on unbounded growth  
**Perf**: Amortized O(1) push; resize copies O(n)

---

### Week 5: Binary Search Tree
**Goal**: Recursion + structural ownership

**Learn**:
- BST invariants
- Tree ownership patterns in Rust
- Recursive vs iterative trade-offs

**Build**:
- `insert`, `search`, `in_order_traversal`
- Optional: `delete` (complex, can skip)
- Property test: traversal yields sorted sequence

**Stop when**: Insertion/search work; complexity clear

**Study**: CLRS Ch. 12  
**Hours**: R2 C7 T2 D1 = 13h

**Invariants**: Left < parent < right at every node  
**Failures**: Degenerates to O(n) on sorted input  
**Perf**: O(h) operations; O(log n) only if balanced

---

### Week 6: Hash Map from Scratch
**Goal**: Understand hashing, collisions, load factors

**Learn**:
- Hash functions (don't write crypto, use std)
- Separate chaining vs open addressing
- Load factor and resize policy

**Build**:
- `HashMap<K,V>` with chaining
- `get`, `insert`, `remove`
- Tests for collisions, probe length, resize

**Stop when**: Works correctly; can explain when each collision strategy wins

**Study**: CLRS Ch. 11, Rust `HashMap` internals blog posts  
**Hours**: R2 C7 T2 B0 D1 = 12h

**Invariants**: Capacity = 2^n; resize at load > 0.75; no lost keys  
**Failures**: Poor hash function → all keys in one bucket  
**Perf**: O(1) expected; O(n) worst case

---

### Week 7: Binary Heap & Priority Queue
**Goal**: Learn heap operations for Dijkstra next week

**Learn**:
- Min-heap invariants
- Sift-up/sift-down operations
- Why array-based heaps win

**Build**:
- `MinHeap<T>` with `insert`, `extract_min`, `peek`
- Property test: heap invariant maintained
- Heapsort as validation

**Stop when**: Operations work; complexity proven

**Study**: CLRS Ch. 6  
**Hours**: R2 C6 T2 B1 D1 = 12h

**Invariants**: Parent ≤ children; complete binary tree  
**Failures**: None if invariant held  
**Perf**: Insert/extract O(log n); heapify O(n)

---

### Week 8: Graph Algorithms (BFS, Dijkstra)
**Goal**: Apply heap to real algorithm; understand graph representations

**Learn**:
- Adjacency list vs matrix trade-offs
- BFS for shortest path (unweighted)
- Dijkstra with priority queue

**Build**:
- `Graph` with adjacency list
- BFS and Dijkstra implementations
- Small test graphs with known answers

**Stop when**: Correct paths; can explain O(E log V) for Dijkstra

**Study**: CLRS Ch. 22, 24  
**Hours**: R2 C7 T2 B0 D1 = 12h

**Invariants**: Dijkstra: distances never increase; visited set grows  
**Failures**: Negative weights break Dijkstra  
**Perf**: BFS O(V+E); Dijkstra O(E log V) with heap

---

### Week 9: Threads, Channels, Mutex
**Goal**: Concurrency fundamentals before networking

**Learn**:
- OS thread model (OSTEP Ch. 26)
- `std::thread`, `mpsc`, `Mutex<T>`, `Arc<T>`
- Context switching cost

**Build**:
- Parallel sum with work chunking
- Message-passing pipeline (producer → processor → consumer)
- Benchmark 1, 2, 4, 8 threads

**Stop when**: Can explain when threads help/hurt; no data races

**Study**: OSTEP Ch. 26, Rust Atomics Ch. 1  
**Hours**: R3 C6 T2 B0 D1 = 12h

**Invariants**: No data races; work partitioning correct  
**Failures**: Contention on shared mutex; false sharing  
**Perf**: Amdahl's Law limits; context switch overhead

---

### Week 10: Consolidation & Portfolio Setup
**Goal**: Polish everything; establish reusable patterns

**Tasks**:
- Unified bench harness (criterion + CSV export)
- README template (problem → invariants → API → trade-offs)
- Fix clippy warnings across all projects
- GitHub portfolio setup (one repo per week, clear structure)

**Stop when**: All Phase 1 code is production-quality; easy to showcase

**Hours**: R1 C4 T2 B2 D1 = 10h

---

## Phase 2: Networking & Async I/O (Weeks 11-20)
*From bare TCP to production async servers*

### Week 11: TCP Fundamentals
**Goal**: Understand what's below your abstractions

**Learn**:
- 3-way handshake, teardown (FIN/ACK)
- Nagle's algorithm, delayed ACK
- Flow control (window), congestion control basics
- tcpdump/Wireshark basics

**Build**:
- Single-threaded TCP echo server (`TcpListener`)
- Handle multiple sequential clients
- Capture with tcpdump, annotate handshake/teardown

**Stop when**: Can explain trace screenshots; server stable

**Study**: Stevens "TCP/IP Illustrated Vol. 1" Ch. 17-18 (skim), Beej's Guide  
**Hours**: R3 C5 T2 B1 D1 = 12h

**Invariants**: Each conn: SYN → established → FIN  
**Failures**: Half-open conns; RST on errors; keepalive needed  
**Perf**: Handshake latency (1 RTT); Nagle delays small writes

---

### Week 12: Event-Driven I/O (epoll/mio)
**Goal**: Understand non-blocking I/O before async/await

**Learn**:
- Readiness vs completion models
- Edge-triggered vs level-triggered
- Why event loops beat threads at scale

**Build**:
- Multi-client chat server using `mio`
- No busy-waiting; bounded message queues
- Handle partial reads/writes

**Stop when**: Concurrent clients work; CPU usage low when idle

**Study**: "The C10K Problem" paper, mio docs  
**Hours**: R3 C6 T2 B0 D1 = 12h

**Invariants**: All sockets non-blocking; poll timeout reasonable  
**Failures**: Starvation if one conn floods; buffer growth  
**Perf**: O(active conns) not O(total conns)

---

### Week 13: Async/Await with Tokio
**Goal**: Modern async; foundation for all future servers

**Learn**:
- Futures, tasks, executor
- `async`/`await` syntax
- Tokio runtime, channels

**Build**:
- Port echo/chat server to Tokio
- Graceful shutdown (SIGINT → drain tasks)
- Timeout per-request

**Stop when**: Simpler code than mio; clean shutdown; can explain runtime

**Study**: Tokio tutorial, "Async Book"  
**Hours**: R3 C6 T2 B0 D1 = 12h

**Invariants**: Tasks don't block; cancellation-safe  
**Failures**: Unbounded task spawn → OOM  
**Perf**: M:N threading; work-stealing scheduler

---

### Week 14: Capacity Planning & Queueing Theory
**Goal**: Size systems correctly; understand saturation

**Learn**:
- Little's Law: L = λW
- M/M/1 queue basics (utilization vs latency)
- Back-pressure concepts

**Build**:
- Simple queue simulator
- Plot latency vs utilization (hockey stick)
- Apply to echo server (measure throughput, queue depth)

**Stop when**: Can estimate capacity from target latency; see queuing in practice

**Study**: "Performance Modeling" (Raj Jain), SRE Book Ch. 6  
**Hours**: R4 C4 T1 B2 D1 = 12h

**Invariants**: Response time → ∞ as utilization → 1  
**Failures**: No admission control → cascading overload  
**Perf**: Every system has a "knee" in latency curve

---

### Week 15: Back-Pressure & Overload Protection
**Goal**: Build servers that degrade gracefully

**Learn**:
- Admission control patterns
- Load shedding, circuit breaking preview
- Connection/request limits

**Build**:
- Add to Tokio server:
  - Max concurrent connections
  - Bounded request queues
  - `/healthz` endpoint (reports queue depth)
  - Reject requests at limit (503 response)

**Stop when**: Server survives overload; metrics show drops

**Study**: SRE Book Ch. 21 (Cascading Failures)  
**Hours**: R2 C7 T2 B0 D1 = 12h

**Invariants**: Queue depth ≤ limit; rejects counted  
**Failures**: No limits → OOM; limits too low → underutilized  
**Perf**: Small queues = lower latency; large = higher throughput

---

### Week 16: TLS 1.3 Essentials
**Goal**: Secure all future servers; understand handshake cost

**Learn**:
- TLS 1.3 handshake (1-RTT, 0-RTT)
- Certificate chains, trust stores
- Session resumption

**Build**:
- Wrap Tokio server with `rustls`
- Self-signed certs for dev
- Measure handshake latency overhead

**Stop when**: Clients connect over TLS; can explain handshake steps

**Study**: "High Performance Browser Networking" Ch. 4, rustls docs  
**Hours**: R3 C5 T2 B1 D1 = 12h

**Invariants**: Cert valid; private key protected  
**Failures**: Cert expiry; revocation not checked; cipher suite mismatch  
**Perf**: Handshake adds ~1-2 RTT; resumption helps

---

### Week 17: Observability - Metrics & Logging
**Goal**: Instrument everything; foundation for debugging

**Learn**:
- RED method (Rate, Errors, Duration)
- Structured logging (`tracing` crate)
- Histogram vs summary metrics

**Build**:
- Add to server:
  - `/metrics` endpoint (Prometheus format)
  - Request latency histogram (p50/p95/p99)
  - Error rate counter
  - Tracing spans with request IDs

**Stop when**: Dashboards show traffic patterns; can trace requests

**Study**: SRE Book Ch. 6, Prometheus docs  
**Hours**: R2 C6 T2 B1 D1 = 12h

**Invariants**: Every request logged/metered; spans properly nested  
**Failures**: Too much logging → I/O bound; cardinality explosion  
**Perf**: Async logging; sampling for high volumes

---

### Week 18: Load Testing & Profiling
**Goal**: Find bottlenecks scientifically

**Learn**:
- wrk/k6 basics
- Coordinated omission problem
- Flamegraph interpretation

**Build**:
- Reproducible load test scripts
- Benchmark suite (vary concurrency, payload size)
- Generate flamegraphs under load
- Identify top 3 CPU hotspots

**Stop when**: Capacity curve plotted; bottlenecks explained

**Study**: "How NOT to Measure Latency" (Gil Tene), Brendan Gregg's blog  
**Hours**: R2 C4 T1 B4 D1 = 12h

**Invariants**: Tests run long enough for steady state  
**Failures**: Short tests miss warmup; single-client tests misleading  
**Perf**: Usually bottleneck is: (1) locks, (2) syscalls, (3) allocations

---

### Week 19: System Design Drill 1 - URL Shortener
**Goal**: Practice interview-style design (no code)

**Learn**:
- Requirements gathering
- Back-of-envelope estimation
- Presenting trade-offs verbally

**Build**:
- 45-min timed design (alone or with partner)
- Write-up: requirements → capacity → API → data model → architecture
- Compare your solution to reference designs

**Stop when**: Can defend all choices; identify weaknesses in your design

**Study**: "System Design Interview Vol. 1" Ch. 8  
**Hours**: R3 C0 T0 B0 D9 = 12h (mostly design doc)

---

### Week 20: Consolidation
**Goal**: Harden async servers; prepare for distributed systems

**Tasks**:
- Add TLS + metrics to all Phase 2 servers
- Run 10-min soak tests; fix memory leaks
- Write "lessons learned" doc
- Practice explaining your servers out loud (Feynman test)

**Hours**: R1 C5 T2 B1 D1 = 10h

---

## Phase 3: Distributed Systems Fundamentals (Weeks 21-32)
*Consensus, replication, partitioning - the hard stuff*

### Week 21: Distributed Systems Mental Models
**Goal**: Build intuition before diving into algorithms

**Learn**:
- Fallacies of distributed computing
- Partial failures, network partitions
- Time and ordering challenges
- CAP theorem (understand limits)

**Build**:
- Nothing (pure theory week)
- Write a "failure modes catalog" (network, disk, clock)
- Draw sequence diagrams for common failures

**Stop when**: Paranoid mindset established; expect failure

**Study**: DDIA Ch. 1, 8; "Distributed Systems for Fun and Profit"  
**Hours**: R8 C0 T0 B0 D4 = 12h

---

### Week 22: Partitioning Strategies
**Goal**: Understand how to split data

**Learn**:
- Range-based vs hash-based partitioning
- Consistent hashing with virtual nodes
- Hot shard problem
- Rebalancing cost

**Build**:
- Simulator: keys → nodes (both strategies)
- Add/remove nodes; measure key moves
- Visualize distribution (histogram)

**Stop when**: Can choose strategy for given workload; see hot-key risks

**Study**: DDIA Ch. 6, Dynamo paper (partitioning section)  
**Hours**: R3 C6 T1 B1 D1 = 12h

**Invariants**: All keys assigned; moves minimized on rebalance  
**Failures**: Hash-based can't do range queries; poor hash → hot shards  
**Perf**: Rebalance transfers O(keys/nodes) ideally

---

### Week 23: Replication Models
**Goal**: Understand sync vs async, quorums

**Learn**:
- Primary-secondary (leader-follower)
- Quorum reads/writes (R + W > N)
- Synchronous vs asynchronous replication trade-offs
- Read-after-write consistency

**Build**:
- Simulation: 1 primary + 2 replicas
- Model: async replication → potential loss
- Quorum: demonstrate how R=2, W=2, N=3 provides linearizability

**Stop when**: Can explain when each model fits; latency vs durability trade-off clear

**Study**: DDIA Ch. 5, Dynamo paper  
**Hours**: R3 C6 T1 B1 D1 = 12h

**Invariants**: Quorum: R+W > N guarantees overlap  
**Failures**: Async: data loss on crash; sync: high latency  
**Perf**: Async fast but unsafe; sync safe but slow

---

### Week 24: Consensus Foundations (Raft Theory)
**Goal**: Understand distributed agreement before implementing

**Learn**:
- Why consensus is hard (FLP impossibility)
- Raft roles: follower, candidate, leader
- Terms, elections, heartbeats
- Safety vs liveness

**Build**:
- State machine diagrams
- Election timeout calculation (why randomized?)
- Failure scenario catalog (split vote, network partition)

**Stop when**: Can explain leader election verbally; draw state transitions

**Study**: Raft paper (§1-5), DDIA Ch. 9  
**Hours**: R6 C2 T0 B0 D4 = 12h

---

### Week 25: Raft Lab 1 - Leader Election
**Goal**: First working consensus component

**Learn**:
- `RequestVote` RPC
- Election timeouts (150-300ms typical)
- Term updates

**Build**:
- 3-node cluster (in-process, simulated network)
- Randomized timeouts → stable leader
- Leader crash → re-election
- Split-brain prevention (term checks)

**Stop when**: Deterministic tests pass; leader stable under no failures

**Study**: Raft paper §5.2, MIT 6.824 Lab 2A guide  
**Hours**: R2 C8 T2 B0 D1 = 13h

**Invariants**: At most one leader per term; majority votes needed  
**Failures**: Split vote → no leader (retry); clock skew affects timeouts  
**Perf**: Election takes ~2× timeout in practice

---

### Week 26: Raft Lab 2 - Log Replication
**Goal**: Achieve linearizable state machine

**Learn**:
- `AppendEntries` RPC
- Commit index vs apply index
- Log matching property

**Build**:
- Leader appends commands to log
- Replicate to followers
- Commit when majority has entry
- Apply to KV state machine (GET/SET/DEL)

**Stop when**: Commands execute in order; no lost writes under message delays

**Study**: Raft paper §5.3-5.4, MIT 6.824 Lab 2B  
**Hours**: R2 C9 T2 B0 D1 = 14h

**Invariants**: Committed entries never lost; applied in log order  
**Failures**: Follower falls behind → catch-up via log repair  
**Perf**: Commit requires 1 RTT to majority

---

### Week 27: Raft Lab 3 - Persistence & Recovery
**Goal**: Survive crashes

**Learn**:
- What must be durable (current term, voted for, log)
- Recovery protocol
- When to fsync

**Build**:
- Persist state to disk (use `serde` + file I/O)
- Restart node → recover state
- Test: kill leader, restart, verify no data loss

**Stop when**: Cluster survives rolling restarts; linearizability maintained

**Study**: Raft paper §5.5, MIT 6.824 Lab 2C  
**Hours**: R2 C8 T2 B0 D2 = 14h

**Invariants**: Durable state written before RPC response  
**Failures**: Lost fsync → split-brain on restart  
**Perf**: Each write requires disk sync (slow); batch helps

---

### Week 28: System Design Drill 2 - Chat Application
**Goal**: Practice design with back-pressure considerations

**Learn**:
- Message ordering guarantees
- Read vs write path optimization
- Fan-out strategies

**Build**:
- 45-min design (WhatsApp-scale)
- Handle: delivery, ordering, presence, media
- Identify bottlenecks; propose mitigations

**Stop when**: Can explain partition strategy, replication model, failure recovery

**Study**: System Design Interview Vol. 2 Ch. 12  
**Hours**: R2 C0 T0 B0 D10 = 12h

---

### Week 29: Service Discovery & Health Checks
**Goal**: Dynamic membership management

**Learn**:
- Heartbeat-based vs lease-based discovery
- Active vs passive health checks
- TTL management

**Build**:
- Simple registry service
- Clients register with TTL
- Expire stale entries
- Health check endpoint

**Stop when**: Services discover each other; failures detected within TTL

**Study**: Consul architecture docs, "Building Microservices" Ch. 11  
**Hours**: R2 C7 T2 B0 D1 = 12h

**Invariants**: Entry exists → service reachable (eventually)  
**Failures**: Split-brain if multiple registries; TTL too short → flapping  
**Perf**: TTL trade-off: short = fast detection, high load

---

### Week 30: Consistency Models Deep Dive
**Goal**: Understand what guarantees systems actually provide

**Learn**:
- Linearizability, sequential consistency, causal consistency
- Read-your-writes, monotonic reads
- Session guarantees
- Eventual consistency anomalies

**Build**:
- Simulator showing anomalies:
  - Read skew
  - Write skew  
  - Lost updates
- Show how different consistency levels prevent each

**Stop when**: Can name model for any system; explain trade-offs

**Study**: DDIA Ch. 9, Jepsen blog posts  
**Hours**: R4 C5 T1 B1 D1 = 12h

**Invariants**: Stronger models compose; weaker models don't  
**Failures**: Eventual consistency → application must handle stale reads  
**Perf**: Linearizability requires coordination (slow)

---

### Week 31: Layer 4 Load Balancer
**Goal**: Build production traffic distribution

**Learn**:
- Round-robin, least-connections, weighted
- Health checking + outlier detection
- Connection draining

**Build**:
- TCP load balancer (reverse proxy)
- 3 backend servers
- Health checks → automatic failover
- Graceful backend removal (drain)

**Stop when**: Distributes load; survives backend failures; no dropped requests

**Study**: Envoy docs (concepts), HAProxy manual  
**Hours**: R2 C7 T2 B0 D1 = 12h

**Invariants**: No requests to unhealthy backends; connections balanced  
**Failures**: All backends down → 503; health check false positives  
**Perf**: Least-connections better than RR under variance

---

### Week 32: Consolidation & Raft Polish
**Goal**: Production-ready Raft implementation

**Tasks**:
- Add metrics to Raft (leader elections, log size, commit lag)
- Implement snapshot/compaction (truncate log)
- Write operational runbook (how to add/remove nodes)
- Soak test: 3-node cluster, 1000 req/sec, random kills

**Stop when**: Cluster stable under chaos; ready to showcase

**Hours**: R1 C6 T2 B2 D2 = 13h

---

## Phase 4: Storage & Caching (Weeks 33-40)
*From memory to disk; understanding storage engines*

### Week 33: Thread-Safe LRU Cache
**Goal**: Master concurrent data structures

**Learn**:
- `Arc<RwLock>` vs message-passing
- False sharing, cache line padding
- Lock granularity

**Build**:
- `LruCache<K,V>` using `HashMap` + doubly-linked list
- Thread-safe via `RwLock`
- Property tests: capacity never exceeded, LRU order maintained

**Stop when**: No deadlocks; performance acceptable under concurrent load

**Study**: Rust Atomics Ch. 1-2, "The Art of Multiprocessor Programming" Ch. 9  
**Hours**: R2 C7 T2 B0 D1 = 12h

**Invariants**: Size ≤ capacity; MRU at head, LRU at tail  
**Failures**: Lock contention → poor scaling; deadlock if improper lock order  
**Perf**: Read lock shared; write lock exclusive → batching helps

---

### Week 34: File I/O & Durability
**Goal**: Understand what "persistent" means

**Learn**:
- Page cache behavior
- `fsync` vs `fdatasync`
- `O_DIRECT`, `O_SYNC` flags
- Write amplification basics

**Build**:
- Benchmark: sequential vs random reads/writes
- Measure with/without fsync
- Show how OS caching hides true disk latency

**Stop when**: Can explain when fsync is needed; understand caching layers

**Study**: OSTEP Ch. 39-40, "What every programmer should know about SSDs"  
**Hours**: R3 C5 T1 B2 D1 = 12h

**Invariants**: Fsync → data survives crash  
**Failures**: No fsync → data loss; fsync every write → 100× slower  
**Perf**: Sequential I/O ~500 MB/s; random ~10k IOPS (SSD)

---

### Week 35: Write-Ahead Log (WAL)
**Goal**: Recover from crashes correctly

**Learn**:
- WAL protocol (write log → fsync → apply)
- Checkpointing
- Replay on recovery

**Build**:
- Simple KV store with WAL
- Operations: SET(key, val) → append to log → fsync → update memory
- Crash & recover: replay log to rebuild state

**Stop when**: Survives kill -9; no lost writes

**Study**: DDIA Ch. 3, Postgres WAL docs  
**Hours**: R2 C7 T2 B0 D1 = 12h

**Invariants**: Log is append-only; checkpoint marks consistent state  
**Failures**: Corrupted log → partial recovery; fsync too frequent → slow  
**Perf**: Batching log writes amortizes fsync cost

---

### Week 36: Buffer Pool Manager
**Goal**: Understand how DBs cache pages

**Learn**:
- Page-based storage (4KB pages)
- Buffer pool with LRU eviction
- Pin/unpin protocol
- Dirty page tracking

**Build**:
- `BufferPool` managing fixed # of pages
- `get_page(page_id)` → fetch from disk if not cached
- Track hits/misses
- Flush dirty pages on eviction

**Stop when**: Cache working; hit rate measurable

**Study**: "Database Internals" Ch. 1-2, CMU 15-445 lectures  
**Hours**: R3 C7 T1 B0 D1 = 12h

**Invariants**: # pinned pages ≤ pool size; dirty pages eventually flushed  
**Failures**: All pages pinned → eviction impossible; no free pages  
**Perf**: Hit rate = 1 - (disk reads / total reads)

---

### Week 37: B+ Tree Basics
**Goal**: Understand index structure

**Learn**:
- B+ tree vs B-tree (data in leaves only)
- Search, insert, split
- Why databases love B+ trees (range scans)

**Build**:
- Simple B+ tree (order = 4 for testing)
- Insert → split when full
- Range scan (iterate leaves)
- Don't worry about concurrency yet

**Stop when**: Can insert/search; splits work; in-order traversal correct

**Study**: CLRS Ch. 18, "Database Internals" Ch. 2  
**Hours**: R3 C8 T2 B0 D1 = 14h

**Invariants**: All leaves at same depth; parent has M-1 keys for M children  
**Failures**: None if invariants maintained (structure self-balancing)  
**Perf**: O(log N) ops; cache-friendly (fewer levels than BST)

---

### Week 38: System Design Drill 3 - Key-Value Store
**Goal**: Design your own Redis/DynamoDB

**Learn**:
- Partitioning strategy (consistent hashing)
- Replication (Raft or quorum)
- Persistence (WAL + snapshots)

**Build**:
- Design doc (45 min + writeup)
- API: GET/SET/DEL, TTL optional
- Handle: node failures, rebalancing, backups
- Choose: CP or AP? Why?

**Stop when**: Can defend all choices; know where design breaks

**Study**: Redis architecture, Dynamo paper  
**Hours**: R3 C0 T0 B0 D9 = 12h

---

### Week 39: Cache Policies Comparison
**Goal**: Choose the right eviction algorithm

**Learn**:
- LRU, LFU, ARC, CLOCK
- When each wins (workload patterns)
- Hit rate vs implementation complexity

**Build**:
- Implement LRU and LFU
- Simulator with different access patterns:
  - Temporal locality (LRU wins)
  - Frequency-based (LFU wins)
  - Mixed
- Measure hit rates, CPU overhead

**Stop when**: Can recommend policy for any workload

**Study**: ARC paper, memcached eviction docs  
**Hours**: R2 C6 T1 B2 D1 = 12h

**Invariants**: LRU: recency order; LFU: frequency order  
**Failures**: LRU: cache pollution from scans; LFU: old hot items never evicted  
**Perf**: LRU O(1); LFU O(log n) typically

---

### Week 40: Consolidation
**Goal**: Polish storage projects; prepare for reliability phase

**Tasks**:
- Integrate buffer pool + B+ tree + WAL into single KV store
- Add metrics (cache hit rate, write amplification, latency)
- Benchmark: compare with/without caching
- Document trade-offs in your implementations

**Stop when**: Storage stack works end-to-end; bottlenecks understood

**Hours**: R1 C6 T2 B2 D1 = 12h

---

## Phase 5: Reliability & Production Readiness (Weeks 41-48)
*Circuit breakers, rate limiting, deployment*

### Week 41: Circuit Breakers & Timeouts
**Goal**: Fail fast; prevent cascading failures

**Learn**:
- States: closed, open, half-open
- Threshold calculation (error rate vs count)
- Exponential backoff with jitter

**Build**:
- Circuit breaker middleware for HTTP client
- Track: success/failure counts, state transitions
- Probe in half-open (limited requests)
- Metrics: state duration, open → closed transitions

**Stop when**: Fails fast when backend down; recovers when healthy

**Study**: "Release It!" Ch. 5, Netflix Hystrix design  
**Hours**: R2 C7 T2 B0 D1 = 12h

**Invariants**: Open → no requests sent; half-open → limited probing  
**Failures**: Threshold too low → false positives; too high → slow detection  
**Perf**: Adds negligible latency when closed

---

### Week 42: Rate Limiting (Token Bucket)
**Goal**: Protect services from overload

**Learn**:
- Token bucket vs leaky bucket vs fixed window
- Distributed rate limiting challenges
- Where to place limiters (gateway, service, client)

**Build**:
- Token bucket limiter (refill rate, burst capacity)
- Middleware for async server
- Test accuracy over 60s window (±5%)
- Measure fairness under concurrent clients

**Stop when**: Enforces limits accurately; minimal overhead

**Study**: "System Design Interview" rate limiter chapter, Stripe rate limiting  
**Hours**: R2 C7 T2 B0 D1 = 12h

**Invariants**: Tokens ≤ capacity; refill at constant rate  
**Failures**: Clock skew breaks refill; no distributed coordination → per-node limits  
**Perf**: O(1) per request; atomic ops for concurrency

---

### Week 43: Transactions & 2PC
**Goal**: Understand coordination cost

**Learn**:
- Two-phase commit protocol
- Coordinator failure handling
- Why most systems avoid distributed transactions
- Sagas as alternative

**Build**:
- Simple 2PC implementation (coordinator + 2 participants)
- Handle: coordinator crash, participant timeout
- Compare latency: 2PC vs independent writes

**Stop when**: Can explain when to use (rarely); know alternatives

**Study**: DDIA Ch. 9, "Designing Data-Intensive Applications"  
**Hours**: R3 C6 T1 B1 D1 = 12h

**Invariants**: All commit or all abort; blocking on coordinator failure  
**Failures**: Coordinator crash in phase 2 → participants blocked  
**Perf**: 2 RTTs minimum; high latency kills it for most use cases

---

### Week 44: System Design Drill 4 - Newsfeed/Timeline
**Goal**: Design read-heavy, write-distribution system

**Learn**:
- Fan-out on write vs read
- Celebrity problem (high fan-out)
- Ranking/sorting at scale

**Build**:
- 45-min design (Twitter/Instagram scale)
- Handle: posting, timeline generation, pagination
- Optimize: hot users, real-time updates

**Stop when**: Can justify fan-out strategy; handle edge cases

**Study**: System Design Interview Vol. 2  
**Hours**: R2 C0 T0 B0 D10 = 12h

---

### Week 45: Docker & Containerization
**Goal**: Package your services

**Learn**:
- Image layers, caching strategy
- Namespaces, cgroups basics
- Resource limits (CPU, memory)
- Multi-stage builds

**Build**:
- Dockerfile for your KV store + Raft
- Optimize layer caching
- Set resource limits
- Run locally with docker-compose (3-node cluster)

**Stop when**: Container works; can explain isolation; size optimized

**Study**: "Docker Deep Dive", best practices docs  
**Hours**: R3 C6 T1 B0 D2 = 12h

**Invariants**: Reproducible builds; minimal attack surface  
**Failures**: Shared kernel vulnerabilities; resource limits too low  
**Perf**: Minimal overhead vs bare metal; startup time matters

---

### Week 46: Kubernetes Basics
**Goal**: Deploy to production orchestration

**Learn**:
- Pods, Services, Deployments
- ConfigMaps, Secrets
- Rolling updates, rollbacks
- Health checks (liveness, readiness)

**Build**:
- Deploy KV cluster to local K8s (minikube/kind)
- Service for discovery
- ConfigMap for tuning params
- Test: rolling update with zero downtime

**Stop when**: App runs in K8s; can scale replicas; updates work

**Study**: "Kubernetes in Action" Ch. 1-4, official docs  
**Hours**: R4 C5 T1 B0 D2 = 12h

**Invariants**: Desired state = actual state (eventually)  
**Failures**: Readiness probe fails → no traffic; liveness probe fails → restart  
**Perf**: Scheduler overhead; inter-pod latency

---

### Week 47: Observability Deep Dive
**Goal**: Full-stack tracing and SLOs

**Learn**:
- Distributed tracing (OpenTelemetry)
- Trace context propagation
- SLIs, SLOs, error budgets
- Burn rate alerts

**Build**:
- Instrument KV cluster with tracing
- Jaeger for visualization
- Define SLOs (e.g., p99 < 50ms, 99.9% success)
- Set up burn-rate alert (error budget consumption)

**Stop when**: Can trace requests across services; SLO dashboard working

**Study**: SRE Book Ch. 4, OpenTelemetry docs  
**Hours**: R2 C5 T1 B3 D1 = 12h

**Invariants**: Every request has trace ID; spans form tree  
**Failures**: High cardinality → storage explosion; sampling loses rare errors  
**Perf**: Sampling reduces overhead (1-10% typical)

---

### Week 48: Final Capstone - Production KV Store
**Goal**: Pull everything together

**Build**:
- Full-featured KV store with:
  - Raft consensus (from W25-27)
  - TLS (W16)
  - Rate limiting (W42)
  - Circuit breakers (W41)
  - Metrics + tracing (W17, W47)
  - Kubernetes deployment (W46)
- Load test: 10k req/sec, 5-min soak
- Chaos: kill nodes, partition network, CPU throttle
- Document: architecture, runbook, postmortem

**Stop when**: Stable under chaos; passes all tests; ready for portfolio

**Study**: Review all previous material  
**Hours**: R2 C10 T3 B3 D2 = 20h (extended capstone)

---

## Post-Plan: Interview Prep & Specialization

### Interview Readiness Checklist
After completing the 48 weeks, you should:

**Systems Knowledge**:
- ✅ Explain Raft, 2PC, consistency models without notes
- ✅ Design distributed cache, KV store, rate limiter from scratch
- ✅ Debug production issues with strace, tcpdump, flamegraphs
- ✅ Size systems using Little's Law and queuing theory

**Coding**:
- ✅ Implement common data structures in 30 min (heap, LRU, trie)
- ✅ Solve medium/hard Leetcode in 45 min
- ✅ Write production-quality Rust (idiomatic, safe, tested)

**System Design**:
- ✅ Complete 10+ full designs (use "System Design Interview" books)
- ✅ Practice presenting designs verbally (record yourself)
- ✅ Defend trade-offs under questioning

**Portfolio**:
- ✅ GitHub with 15+ polished projects
- ✅ One "capstone" project (the KV store) with full docs
- ✅ Blog posts explaining 3-5 projects (demonstrate communication)

### Recommended Interview Prep (Weeks 49-52+)
1. **Week 49-50**: Leetcode grind (2 problems/day, focus on hard)
2. **Week 51**: Mock system design interviews (Pramp, interviewing.io)
3. **Week 52**: Mock coding interviews + behavioral prep (STAR method)

### Specialization Paths (After Landing the Job)
Once you have the fundamentals, specialize based on team needs:

**Storage Systems**:
- LSM tree implementation (leveled compaction, bloom filters)
- Distributed transactions (Spanner, Calvin)
- Query optimization

**Infrastructure/Platform**:
- Kubernetes operators, custom controllers
- Service mesh (Envoy, Linkerd)
- Multi-tenancy, resource isolation

**Data Engineering**:
- Stream processing (Kafka, Flink)
- Data pipelines, ETL
- Column stores, OLAP

**Reliability/SRE**:
- Chaos engineering
- Incident response, postmortems
- Capacity planning, forecasting

---

## Study Resources

### Core Books (Must Read)
1. **"Designing Data-Intensive Applications"** (Martin Kleppmann) - Chapters 5-9 essential
2. **"Operating Systems: Three Easy Pieces"** (Arpaci-Dusseau) - Free online
3. **"Computer Systems: A Programmer's Perspective"** (Bryant/O'Hallaron) - Ch. 6, 8, 10-12
4. **"The Rust Programming Language"** (Klabnik/Nichols) - Official book, free online
5. **"System Design Interview Vol. 1 & 2"** (Alex Xu) - Interview-focused

### Supplementary
- **"Introduction to Algorithms"** (CLRS) - Reference for DS&A
- **"Site Reliability Engineering"** (Google SRE Book) - Free online
- **"High Performance Browser Networking"** (Ilya Grigorik) - Free online
- **"Database Internals"** (Alex Petrov) - Deep storage engine coverage
- **"Rust Atomics and Locks"** (Mara Bos) - Concurrency deep dive

### Papers
- Raft consensus (In Search of an Understandable Consensus Algorithm)
- Dynamo (Amazon's Highly Available Key-value Store)
- Bigtable, Spanner, F1 (Google storage systems)
- Kafka: a Distributed Messaging System for Log Processing

### Online Resources
- MIT 6.824 Distributed Systems (videos + labs)
- CMU 15-445/645 Database Systems (videos + projects)
- Brendan Gregg's Blog (performance, profiling)
- Jepsen analyses (consistency testing)
- Rust by Example (hands-on Rust learning)

---

## Weekly Template

### Monday (2h)
- [ ] Read assigned material (book chapters, papers)
- [ ] Write learning objectives (what I need to understand by Friday)
- [ ] Draft Invariants card (what must be true)
- [ ] Draft Failure Modes card (how can it break)

### Tuesday-Thursday (7h)
- [ ] Set up project structure (Bronze goal defined)
- [ ] Implement core functionality
- [ ] Write unit tests (happy path + 2 edge cases)
- [ ] Reach Bronze by Wednesday EOD
- [ ] Add benchmarks/profiling (Silver)
- [ ] Run integration tests

### Friday (3h)
- [ ] Complete Perf Note (complexity, bottlenecks, optimization ideas)
- [ ] Write README (problem, approach, trade-offs, usage)
- [ ] Clean up code (fmt, clippy, dead code)
- [ ] Commit and push to GitHub
- [ ] Weekly retrospective (what went well, what to improve)

---

## Success Metrics

### Monthly Checkpoints
**End of Month 1 (Week 4)**:
- Can implement any basic data structure from memory
- Comfortable with Rust ownership model
- Portfolio: 4 projects on GitHub

**End of Month 3 (Week 12)**:
- Built async server handling 1000 concurrent connections
- Can explain TCP behavior from packet traces
- Completed 3 system design drills

**End of Month 6 (Week 24)**:
- Working Raft implementation
- Can design distributed systems with proper trade-offs
- Started receiving positive feedback on mock interviews

**End of Month 9 (Week 36)**:
- Complete storage engine (WAL, buffer pool, B+ tree)
- Can debug production issues with profiling tools
- Portfolio showcases 20+ projects

**End of Month 12 (Week 48)**:
- Production-ready KV store deployed on Kubernetes
- Passing mock interviews consistently
- Ready to apply to MAANG companies

### When You're Ready
You're ready to interview when you can:
1. **Design a distributed cache in 45 minutes** and defend all choices
2. **Implement LRU cache in 30 minutes** with proper concurrency
3. **Explain your Raft implementation** to a senior engineer for 1 hour
4. **Debug a production latency issue** using the right tools
5. **Write production Rust** that passes code review

---

## Common Pitfalls & How to Avoid Them

### Pitfall 1: Perfectionism
**Symptom**: Spending 20+ hours on Week 3 linked list  
**Fix**: Ship Bronze, move on. You can always revisit.

### Pitfall 2: Tutorial Hell
**Symptom**: Watching videos without building anything  
**Fix**: Code-first learning. Read docs while implementing.

### Pitfall 3: Skipping Fundamentals
**Symptom**: Jumping to Kubernetes without understanding processes/networks  
**Fix**: Trust the sequence. Each week builds on previous.

### Pitfall 4: No System Design Practice
**Symptom**: Great at coding, can't design systems under time pressure  
**Fix**: Bi-weekly design drills starting Week 19. Time yourself.

### Pitfall 5: Isolated Learning
**Symptom**: Can't explain your projects to others  
**Fix**: Feynman technique. Explain out loud weekly.

### Pitfall 6: Ignoring Health
**Symptom**: Burnout by Month 6  
**Fix**: 12h/week max. Take consolidation weeks. Sleep matters.

---

## Motivation & Mindset

### The Long Game
This is a **marathon, not a sprint**. 48 weeks seems long, but:
- A CS degree is 4 years (192 weeks)
- You're compressing it into 1 year of focused learning
- Most bootcamp grads lack systems depth
- This plan gives you 5+ years of experience in theory

### When You Feel Behind
Everyone hits walls. Common stuck points:
- **Week 3**: Rust borrow checker (it clicks by Week 5)
- **Week 12**: Event loops (draw diagrams)
- **Week 25-27**: Raft (hardest part; take 4 weeks if needed)
- **Week 37**: B+ trees (implementation is tricky)

**Remember**: Getting stuck means you're learning. Shipping Bronze means you're progressing.

### The Compound Effect
Each week builds:
- Week 1: CLI app
- Week 12: Async server
- Week 27: Consensus system
- Week 48: Production distributed database

You won't notice day-to-day, but look back every 12 weeks. The growth is exponential.

---

## Final Notes

This plan is **aggressive but achievable**. It assumes:
- 12 hours/week sustained (realistic for full-time employed)
- Strong motivation (you want Staff+ at MAANG)
- Willingness to struggle (learning is uncomfortable)
- Long-term commitment (1 year is significant)

**Adjust as needed**. If a week takes 15h, that's fine. If you need to repeat Raft, do it. The goal isn't to finish in exactly 48 weeks—it's to build **unshakeable fundamentals** that make you competitive at the highest level.

You'll know you're ready when you can:
1. Confidently walk into a Google interview
2. Design systems that scale to millions of users
3. Debug production issues that stump others
4. Build engines, not just use them

**Now go build.**

---

## Quick Reference

### Progression Summary
- **Phase 1 (W1-10)**: Language + DS&A fundamentals
- **Phase 2 (W11-20)**: Networking + Async I/O
- **Phase 3 (W21-32)**: Distributed systems + Consensus
- **Phase 4 (W33-40)**: Storage engines + Caching
- **Phase 5 (W41-48)**: Reliability + Production deployment

### Key Milestones
- Week 10: Portfolio foundation set
- Week 20: Production async servers mastered
- Week 27: Raft consensus working
- Week 40: Storage stack complete
- Week 48: Full distributed system deployed

### Time Investment
- Regular weeks: 12h
- Consolidation weeks: 10h  
- Capstone weeks: 14-20h
- **Total: ~550-600 hours over 48 weeks**

### Expected Outcome
After 48 weeks, you will be a **systems engineer** capable of:
- Building distributed systems from first principles
- Designing scalable architectures for MAANG-level scale
- Debugging complex production issues
- Interviewing confidently for Senior/Staff roles

**The journey of 1000 miles begins with a single commit. Start with Week 1 today.**
