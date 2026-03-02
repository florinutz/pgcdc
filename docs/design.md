# Systems Design: A Problem-First Training Guide

*A field manual organized by the failures every engineer eventually ships — not by the solutions.*

---

Each chapter is a failure mode. You have either experienced it already or you will. The structure is the same throughout: you see what the failure looks like, what the obvious fix is (and why it breaks), and then the real solution. Where pgcdc is the worked example, you get code anchors. Three chapters — storage engines, caching, and distributed consensus — stand alone and use real-world systems as illustrations. They don't require pgcdc to be meaningful.

## Table of Contents

- [Chapter 1: "A write that wasn't fsynced didn't happen."](#chapter-1-a-write-that-wasnt-fsynced-didnt-happen)
- [Chapter 2: "Every replication slot you forget is a disk bomb with no timer."](#chapter-2-every-replication-slot-you-forget-is-a-disk-bomb-with-no-timer)
- [Chapter 3: "A poison pill is patient. It will wait forever for you to stop retrying it."](#chapter-3-a-poison-pill-is-patient-it-will-wait-forever-for-you-to-stop-retrying-it)
- [Chapter 4: "A circuit breaker isn't a retry strategy — it's permission to stop trying."](#chapter-4-a-circuit-breaker-isnt-a-retry-strategy--its-permission-to-stop-trying)
- [Chapter 5: "Reliability is always at the cost of the source."](#chapter-5-reliability-is-always-at-the-cost-of-the-source)
- [Chapter 6: "You can restart quickly, or you can restart correctly. Pick one."](#chapter-6-you-can-restart-quickly-or-you-can-restart-correctly-pick-one)
- [Chapter 7: "Arrival time is not event time. Confusing them means your windows are lying."](#chapter-7-arrival-time-is-not-event-time-confusing-them-means-your-windows-are-lying)
- [Chapter 8: "Your checkpoint is only as advanced as your slowest adapter."](#chapter-8-your-checkpoint-is-only-as-advanced-as-your-slowest-adapter)
- [Chapter 9: "Cache invalidation is a distributed consistency problem with a friendly name."](#chapter-9-cache-invalidation-is-a-distributed-consistency-problem-with-a-friendly-name)
- [Chapter 10: "Consensus is the price you pay for having more than one machine."](#chapter-10-consensus-is-the-price-you-pay-for-having-more-than-one-machine)
- [Chapter 11: "Lock-free is not the same as wait-free. Both beat the mutex, but not equally."](#chapter-11-lock-free-is-not-the-same-as-wait-free-both-beat-the-mutex-but-not-equally)
- [Chapter 12: "A schema change is a contract change. Treat it like one."](#chapter-12-a-schema-change-is-a-contract-change-treat-it-like-one)
- [Appendix](#appendix)
  - [pgcdc Pipeline Architecture](#pgcdc-pipeline-architecture)
  - [The 12 Principles](#the-12-principles)
  - [Concept → File Quick Reference](#concept--file-quick-reference)
  - [Delivery Guarantee Decision Tree](#delivery-guarantee-decision-tree)
  - [The Full Middleware Stack](#the-full-middleware-stack)
  - [CDC Method Comparison](#cdc-method-comparison)
  - [Storage Engine Trade-offs](#storage-engine-trade-offs)
  - [Concurrency Primitive Quick Reference](#concurrency-primitive-quick-reference)
  - [Window Type Comparison](#window-type-comparison)

---

**How to use this guide.** Read it in order once to build the mental map. The chapters are designed to be self-contained after that first pass, so you can return to any one in isolation. The appendix has reference tables and decision trees for when you need a fast answer during a design review. The interview traps scattered throughout are not trivia — they mark the boundary between knowing a concept and understanding it well enough to use it correctly under the pressures of production.

---

## Chapter 1: "A write that wasn't fsynced didn't happen."

**The failure.** Your service reports a successful write. The user sees a confirmation. Then the server reboots — power loss, OOM kill, whatever — and the record is gone. You check the disk. The file is there, but it has the old content. The write was buffered in the OS page cache and never made it to the physical medium.

**What everyone tries first.** Add a confirmation step in the application layer: write the record, read it back, confirm it's there, then return success. This doesn't work. The read-back also comes from the page cache — you're confirming the existence of your own buffer, not persisted data. The underlying problem is that the OS deliberately lies to you about durability to give you faster write throughput.

**The real solution.** You need to understand three distinct storage engine approaches and why each one chose a different answer to the durability problem.

**B-tree engines** (PostgreSQL, MySQL InnoDB) organize data in 8 KB pages sorted like a tree. Random reads are fast because you navigate the tree. Random writes are expensive because inserting a value into the middle of a sorted structure may require splitting a page — moving data, updating parent pointers, potentially cascading splits up the tree. A page split touches multiple locations on disk non-atomically. If you crash mid-split, you've corrupted the tree.

The Write-Ahead Log (WAL) exists to solve this. Before touching any page, the database records the *intent* in a sequential append-only log. The WAL entry is written first and fsynced. Then the page modification happens. On recovery, the database replays WAL entries to complete interrupted operations or rolls back partial ones. The WAL transforms random writes with corruption risk into sequential writes with a recovery path.

**LSM-tree engines** (RocksDB, Cassandra, LevelDB) invert the trade-off. All writes go to an in-memory structure (the memtable) plus a WAL. When the memtable reaches a size threshold, it's flushed to disk as an immutable sorted file (SSTable). Reads require merging across multiple levels of SSTables plus the current memtable — more complex than a B-tree read. A background compaction process periodically merges levels to reduce read amplification. LSM trees are designed for write-heavy workloads where you're willing to pay for reads.

**fsync** is the barrier that matters in both cases. The OS page cache is DRAM, not storage. `write(fd, buf, n)` copies data into the page cache and returns immediately. The data is vulnerable until `fsync(fd)` is called, which blocks until the OS has confirmed that the data is on a persistent medium (or in a power-backed write cache that counts as one). Databases call `fsync` on the WAL file after writing each transaction's log entry. This is why database writes feel slower than filesystem writes — you're paying for an actual durability guarantee, not a promise about DRAM.

**MVCC** (Multi-Version Concurrency Control) is how PostgreSQL allows concurrent readers and writers without blocking each other. When a row is updated, PostgreSQL does not overwrite it in place. It writes a new version of the row with updated transaction visibility metadata (`xmin`, `xmax`) and leaves the old version intact for in-progress readers who started before the update. Dead row versions accumulate in the heap. VACUUM reclaims them. Understanding MVCC explains why table bloat exists, why VACUUM is not optional, and why `DELETE FROM large_table WHERE ...` does not immediately return disk space.

**Write amplification** is the hidden cost that every storage engine design negotiates. A single user-visible write may result in many physical writes to disk. In a B-tree engine, a single insert can trigger a page split that writes two new pages plus a parent page update plus a WAL entry — four physical I/Os for one logical write. In an LSM-tree engine, data is written once to the WAL, once to the memtable flush, and then rewritten multiple times by compaction as it moves from L0 → L1 → L2. Each compaction level in RocksDB has 10× more capacity than the previous; data may be rewritten 10–30 times by the time it reaches the lowest level. The write amplification factor in RocksDB defaults to approximately 30× in the worst case. Understanding write amplification explains why SSDs wear out faster under database workloads than sequential file copies, and why configuring compaction parameters matters for both throughput and hardware lifespan.

**Checksum validation** is the silent corruption detector most engineers don't think about until they need it. PostgreSQL 12+ enables page-level checksums (`initdb --data-checksums`). Every 8 KB page has a checksum computed at write time. On read, the checksum is verified. Silent corruption — a storage controller that flips a bit, a firmware bug that writes garbage to a block — is detected immediately on the next read rather than years later when the corrupted data surfaces in an application error. Without checksums, you may run on corrupted storage indefinitely, producing wrong query results with no error indicators.

> **Key insight.** The page cache is a performance optimization maintained by the OS that makes writes look instant. `fsync` is the syscall that converts that illusion into a durability guarantee. A database that doesn't call `fsync` after a commit is not actually committing — it's buffering and hoping the power stays on.

> **The Principle.** *"A write that wasn't fsynced didn't happen."*

> **Interview trap.** "Isn't mmap faster than read/write?" It can be, for read-heavy workloads. But mmap with `msync(MS_ASYNC)` is the worst of both worlds for write durability — you've made fsync semantics implicit and easy to forget. Most production storage engines use explicit read/write + fsync for this reason.

> **Interview trap.** "Why does PostgreSQL need VACUUM if it has MVCC?" MVCC requires keeping old row versions alive for concurrent readers. When those readers finish, the old versions become dead tuples — they're invisible to all transactions but still occupy space. VACUUM finds and marks them as free space. Without it, tables grow without bound.

---

## Chapter 2: "Every replication slot you forget is a disk bomb with no timer."

**The failure.** It's 2 AM. PagerDuty fires. The on-call engineer opens their laptop to find that the primary PostgreSQL server's data volume is full. The culprit: a replication slot created three weeks ago by a CDC pipeline that was decommissioned. The pipeline is gone. The slot is not. PostgreSQL has been retaining WAL since the slot's last confirmed flush LSN, which hasn't moved in three weeks.

**What everyone tries first.** Add a monitoring alert on disk space. This tells you there's a problem after it's already happening. The more useful alert is on `pg_replication_slots` — specifically on slots where `active = false` and `confirmed_flush_lsn` is far behind the current WAL position. But most teams add the disk alert and not the slot alert, so the disk fills before anyone looks at slot state.

**The real solution.** Understanding CDC methods and their trade-offs lets you pick the right one for each use case — and understand what you're signing up for operationally.

**LISTEN/NOTIFY** is PostgreSQL's pub/sub built on the connection layer. Your application calls `NOTIFY channel, payload` and any session with `LISTEN channel` receives it. Sub-millisecond latency. Zero infrastructure beyond a PG connection. The catch: if the listener is disconnected when the NOTIFY fires, the message is lost. There's no buffering, no ordering, no replay. For fire-and-forget notifications (cache invalidation signals, lightweight presence updates) it's excellent. For reliable event delivery, it's the wrong tool.

**WAL logical replication** reads the database's write-ahead log through a logical decoding plugin (`pgoutput`, `wal2json`). A replication slot persists the consumer's read position so PostgreSQL won't discard WAL pages that haven't been consumed. This gives you: zero application coupling (the app doesn't know CDC exists), strong per-transaction ordering via LSN, schema change visibility, and the foundation for exactly-once delivery. The operational cost is the slot itself: an abandoned slot accumulates WAL indefinitely. Monitor it. Drop slots you're not using.

**The outbox pattern** solves a specific problem: writing to the database and notifying a broker in the same operation atomically. Without it, you write to the DB and then to Kafka — but if the Kafka write fails after the DB commit, you've made a change with no notification. The outbox pattern writes an event row to an `outbox` table in the same transaction as the business operation. A separate process (your CDC pipeline) polls the outbox table and publishes the events. Atomicity comes from the database transaction, not from any distributed protocol.

`SELECT ... FOR UPDATE SKIP LOCKED` is the concurrency primitive that makes multi-instance outbox polling safe. Multiple pipeline instances polling the same outbox table each atomically claim a different batch of rows. Rows being processed by one instance are invisible to others until the lock is released.

**MySQL binlog** and **MongoDB Change Streams** provide similar capabilities at the database layer for their respective engines. MySQL GTID (Global Transaction ID) provides ordering semantics analogous to PostgreSQL's LSN. MongoDB Change Streams use a resume token for position tracking. Neither provides the exactly-once potential of outbox + transactional writes, but both are zero-coupling and suitable for most CDC workloads.

> **Key insight.** WAL replication is the only CDC method with zero application coupling, strong ordering, and exactly-once delivery potential simultaneously. Every other method sacrifices at least one of these properties. When choosing between the outbox pattern and WAL replication, the default answer is WAL — unless you can't grant replication permissions or your organization can't tolerate the operational risk of replication slots.

**How pgcdc does it.** `detector/walreplication/walreplication.go` handles the WAL connection and logical decoding. Slot management and LSN reporting back to PostgreSQL are handled there. `detector/outbox/outbox.go` implements the `SKIP LOCKED` polling pattern. The detector interface does not close the events channel — the bus owns that lifecycle, which prevents the slot from being released prematurely.

**TOAST** is PostgreSQL's mechanism for storing large column values (>8 KB) out-of-line in a secondary heap. On `UPDATE`, unchanged TOAST columns appear in WAL as "unchanged" — they're not re-emitted, only a reference is. Without special handling, a downstream consumer would see `null` or a sentinel for those columns on updates. pgcdc's TOAST cache (`detector/walreplication/toastcache/`) is an LRU cache keyed on row identity that stores the last known full value for each TOAST column, so downstream consumers always see the complete row.

> **The Principle.** *"Every replication slot you forget is a disk bomb with no timer."*

> **Interview trap.** "Can't you just set a higher `wal_keep_size`?" That's a retention buffer for physical replication, not logical. Replication slots bypass `wal_keep_size` — they pin WAL at the slot's LSN regardless of that setting. The slot is the authoritative retention mechanism.

---

## Chapter 3: "A poison pill is patient. It will wait forever for you to stop retrying it."

**The failure.** An event arrives with a malformed JSON payload. Your webhook adapter tries to send it. The downstream API returns 400. You retry. It returns 400 again. You retry with exponential backoff. Three hours later, you're still retrying the same malformed event. Every event that arrived after it is queued behind it, waiting. Your pipeline has effectively stopped.

**What everyone tries first.** Add a max retry count. After N retries, log the failure and drop the event. This works for transient failures, but it silently loses data for permanent failures. In a payment processing or audit log context, a dropped event is a compliance violation, not a recoverable situation.

**The real solution.** A Dead Letter Queue separates "this event cannot be delivered right now" from "this event can never be delivered." The DLQ is not the trash — it's a quarantine with a recovery path.

After `MaxRetries` attempts fail, the middleware writes the event to the DLQ backend (stderr for development, a PostgreSQL table for production, or a custom Wasm plugin backend) and returns `nil` to the pipeline. This is the key move: swallowing the error. The pipeline continues processing subsequent events. The failed event is not lost — it's stored in the DLQ with its error metadata — but it is no longer blocking.

The recovery path: `pgcdc dlq list` inspects quarantined events. `pgcdc dlq replay` re-injects them into the pipeline after you've fixed the underlying problem (corrected a schema, fixed the downstream API, patched the transform). `pgcdc dlq purge` discards events you've determined are genuinely unrecoverable. The DLQ stores the original event with its error history — delivery attempt count, last error message, timestamp of first failure. This metadata is what distinguishes a DLQ from a drop: you have the information needed to investigate and decide whether replay is appropriate.

**CEL filter as a pre-DLQ gate:** before events reach the retry/DLQ layer, a CEL (Common Expression Language) expression can filter them. `--filter-cel 'event.table == "audit_log" && event.operation == "INSERT"'` passes only matching events to the adapter. Events that don't match are dropped silently — they don't count as failures, don't increment error metrics, and don't trigger the circuit breaker. This separates "events I don't care about" (filter) from "events I care about but can't deliver" (DLQ). The distinction matters: a CEL filter that drops 90% of events should not generate 90% DLQ entries.

**The sliding nack window** addresses a different failure mode: the adapter is partially degraded, not completely broken. Events aren't permanently failing (not DLQ-worthy individually) but are failing faster than retries can keep up. You need to detect "this adapter is systematically unhealthy" before the DLQ fills up.

A ring buffer of the last N delivery outcomes (success/nack) gives you a rolling failure rate with O(1) update. `nackCount` is maintained incrementally: when the oldest entry exits the window, subtract 1 if it was a nack; when a new entry enters, add 1 if it's a nack. `Exceeded()` compares `nackCount` to a threshold — no full-window scan. When the failure rate exceeds the threshold, the adapter can be paused or flagged for intervention.

> **Key insight.** The DLQ middleware swallows the error after recording the event. This is intentional and correct. A DLQ is not a failure indicator — it's a safety valve that keeps the pipeline moving while quarantining the undeliverable events for human review. If you treat DLQ writes as errors (alerting on them, failing the pipeline), you've negated the entire value of having a DLQ.

**How pgcdc does it.** `dlq/dlq.go` defines the `DLQ` interface. `adapter/middleware/dlq.go` catches errors from the inner retry layer, calls `dlq.Write()`, and returns `nil`. The `PGTableDLQ` backend stores events with their error metadata in a PostgreSQL table. `dlq/window.go` implements the ring buffer nack window.

> **The Principle.** *"A poison pill is patient. It will wait forever for you to stop retrying it."*

> **Interview trap.** "Exactly-once delivery means using a DLQ, right?" No. DLQ is compatible with at-least-once delivery. For a DLQ event that gets replayed, the consumer may see it twice — once when it was originally attempted (if partial delivery occurred) and once on replay. If your workload needs exactly-once semantics, your consumers must be idempotent, and DLQ replay must preserve the original event ID so consumers can detect duplicates.

> **Interview trap.** "If DLQ is safe, why not use it for everything?" A DLQ is only safe for workloads where events are independent and delivery is idempotent. If event B depends on event A, and A is in the DLQ while B is being delivered, you've delivered a dependent event out of order. For ordered, dependent events, the right answer is to stop the pipeline at the failure point, not route around it.

---

## Chapter 4: "A circuit breaker isn't a retry strategy — it's permission to stop trying."

**The failure.** Your webhook adapter's retry backoff is set to a maximum of 60 seconds. A downstream service goes down. Every one of your 20 pipeline instances starts retrying with exponential backoff. After a few minutes, they're all firing synchronized retries every 60 seconds, hitting the recovering service with 20× the normal request rate right when it's trying to come back up. The service crashes again. This is a retry storm.

**What everyone tries first.** Add jitter to the retry backoff. This helps — desynchronized retries don't all arrive at once. But it's not enough. You're still retrying against a service that is down. Every retry consumes a thread or goroutine, holds a connection open, and generates log noise. The fundamental problem is that once a downstream service is clearly unhealthy, individual adapter instances have no mechanism to stop trying and give the service time to recover.

**The real solution.** The circuit breaker is a three-state finite state machine that lives in the caller and makes failure-rate-based decisions about whether to allow requests through.

```
Closed ──(N failures in window)──> Open ──(resetTimeout)──> Half-Open
  ^                                                               |
  └──────────────────(success)───────────────────────────────────┘
  Open <──────────────────────────(failure)──────────────────────┘
```

**Closed** is normal operation. Requests pass through. Failures are counted. When failures exceed a threshold within a window, the breaker **opens**.

**Open** is the protection state. Requests are rejected immediately with `ErrCircuitBreakerOpen` — no attempt is made to contact the downstream service. After `resetTimeout`, the breaker transitions to **half-open**.

**Half-open** is the probe state. Exactly one request is allowed through. If it succeeds, the breaker closes (full traffic resumes). If it fails, the breaker opens again with a fresh `resetTimeout`. This converts recovery from a timer into a feedback loop — you're not guessing that the service has recovered, you're verifying it.

**Retry with full jitter** is a separate mechanism that should be composed with the circuit breaker, not used in its place. Full jitter means each retry waits a random duration drawn uniformly from `[0, min(cap, base × 2^n)]`. The 2015 AWS "Exponential Backoff and Jitter" paper demonstrated empirically that full jitter distributes retry load uniformly across the window, while additive jitter (base interval ± random) only spreads retries around a central point and produces herding at scale.

> **Key insight.** The circuit breaker lives in the caller, not the callee. You cannot put a circuit breaker on a downstream service — you put it in every client that calls it. The service has no knowledge that it's being protected. This means in a microservices system, every service that calls service X must maintain its own CB state for X independently.

**How pgcdc does it.** `internal/circuitbreaker/circuitbreaker.go` implements the three-state FSM. `adapter/middleware/retry.go` implements exponential backoff with configurable base and cap. Both are wired into the middleware chain for all `Deliverer` adapters via `adapter/middleware/middleware.go`. They compose: the retry layer is inside the circuit breaker layer, so the CB counts the outcome of the entire retry sequence, not individual retry attempts.

> **The Principle.** *"A circuit breaker isn't a retry strategy — it's permission to stop trying."*

> **Interview trap.** "Exponential backoff without jitter — why is that worse than fixed backoff?" Because it synchronizes clients. Imagine 100 clients that all failed at T=0. With `base=1s`, they all retry at T=1, T=2, T=4, T=8. Each retry wave contains 100 clients hitting the service simultaneously. With fixed backoff at 2s, they also all retry at T=2, T=4, T=6 — also synchronized, but at least the inter-retry spacing doesn't grow. With full jitter, the clients spread themselves across the entire interval, distributing load rather than concentrating it.

> **Interview trap.** "Where does the reset timeout come from?" It should be empirically derived from the p95 or p99 recovery time of the downstream service, not a round number like "30 seconds." A `resetTimeout` shorter than the typical recovery time means the breaker half-opens before the service is ready, fails the probe, and opens again — extending the outage. A `resetTimeout` much longer than recovery time means you leave load off the service longer than necessary.

---

## Chapter 5: "Reliability is always at the cost of the source."

**The failure.** You have a Kafka adapter that occasionally takes 5 seconds to acknowledge a batch. During those 5 seconds, the bus dispatch goroutine is blocked trying to send to the Kafka adapter's channel. The channel is full. The bus stops dispatching. The detector stops reading from WAL. WAL lag starts growing. The primary PostgreSQL server's replication slot retains more WAL. Your DBA calls at 3 PM asking why WAL retention has spiked.

**What everyone tries first.** Increase the channel buffer size. This helps for short bursts but doesn't solve the fundamental problem: a slow consumer can always fill a finite buffer. The real question is what happens when the buffer is full. Drop the event? Block? Panic? This is not a configuration question — it's an architectural decision with explicit trade-offs.

**The real solution.** Pub/sub fan-out has two modes and you must choose one.

**Fast mode (drop on full)** sends to each subscriber channel with a non-blocking select. If the channel is full, the event is dropped for that subscriber. The bus dispatch goroutine never blocks. The detector keeps running at full speed. Slow adapters lose events. Fast adapters see no impact from slow neighbors. This is the right choice when downstream consumers are independently scalable and event loss is tolerable (metrics aggregation, logging, caching updates).

**Reliable mode (block on full)** sends to each subscriber with a blocking select. If any subscriber's channel is full, the dispatch goroutine blocks until there's space. The bus slows down. The ingest channel backs up. The detector slows. Eventually, if all buffers are full, WAL reading pauses — exerting backpressure back to the source. This guarantees no drops, but it means the slowest consumer controls the pace of the entire pipeline.

Neither mode is wrong. They encode different contracts. The trap is treating mode selection as a performance tweak rather than a fundamental reliability decision.

**Proportional backpressure** addresses the oscillation problem with binary pause/resume. If you pause the detector when WAL lag crosses a threshold and resume when it drains, you get a sawtooth: lag spikes → pause → lag drains → resume → lag spikes. The system thrashes at the threshold because the response is binary.

Proportional throttle works like a pressure valve. Three zones:
- **Green**: lag below warn threshold. Full speed.
- **Yellow**: lag between warn and critical. Throttle by inserting a sleep proportional to how deep in the yellow zone you are. `throttle = maxThrottle × (lag - warn) / (critical - warn)`. At the warn boundary, sleep is zero; at the critical boundary, sleep is maxThrottle.
- **Red**: lag above critical. Pause entirely and shed non-essential load (drop from fast-mode subscribers).

Hysteresis prevents thrashing: you exit Red only when lag drops below the warn threshold (not the critical threshold). This gives the system headroom before re-entering Yellow.

> **Key insight.** Reliability is always at the cost of the source. In reliable mode, a slow consumer's full channel blocks the dispatch goroutine, which stops reading from the ingest channel, which backs up into the detector. "Reliable" means no drops — it doesn't mean fast. There is no configuration that gives you both.

**How pgcdc does it.** `bus/bus.go`: `ModeFast` uses `select { case sub.ch <- ev: default: /* drop */ }`. `ModeReliable` tries non-blocking first, then falls back to a blocking send. `backpressure/backpressure.go` implements the zone model: `computeZone()` applies hysteresis, `computeThrottle()` computes the proportional sleep. Fan-out uses COW (copy-on-write) subscriber lists — `Subscribe()` builds a new slice, stores it atomically via `atomic.Pointer`. The dispatch hot path reads the pointer with no lock.

> **The Principle.** *"Reliability is always at the cost of the source."*

> **Interview trap.** "Can't you use a separate goroutine per subscriber to decouple them?" Yes, and that's what pgcdc does — each subscriber gets a wrapper goroutine. But the goroutines must communicate through bounded channels. If the channel is full, the wrapper goroutine blocks. At that point you're back to the same choice: drop (fast) or block (reliable). The goroutine doesn't change the trade-off; it just isolates the blocking to the per-subscriber goroutine rather than the dispatch goroutine.

---

## Chapter 6: "You can restart quickly, or you can restart correctly. Pick one."

**The failure.** You push a new version of a transform configuration. The deployment process stops the old pipeline and starts the new one. During the 8 seconds of downtime, 12 WAL events arrive that no consumer receives. When the new pipeline starts, it resumes from the last confirmed checkpoint LSN — which was 8 seconds ago. It re-delivers some events and misses others depending on exactly when the last checkpoint was confirmed before shutdown. Your downstream Kafka topic now has a 12-event gap and some duplicates from the re-delivery window.

**What everyone tries first.** Make the restart faster. Reduce the shutdown timeout. Script a faster startup. This doesn't solve the problem — it reduces the gap duration but doesn't eliminate it. The events that arrive during any restart window are still vulnerable.

**The real solution.** Two separable problems: avoiding downtime during configuration changes, and ensuring in-flight work isn't lost during process termination.

**Hot reload via atomic pointer swap** eliminates the restart for configuration changes that don't touch adapters, detectors, or CLI flags. The key data structure is a `map[string]*atomic.Pointer[wrapperConfig]` — a map from adapter name to a pointer to that adapter's configuration. The map itself is immutable after the pipeline starts. On SIGHUP, a reload function reads the new configuration from the YAML file, builds new `wrapperConfig` structs (transforms + routes), and atomically stores them via `ptr.Store(newConfig)`. Each wrapper goroutine calls `cfgPtr.Load()` on every event — lock-free, always sees the current config. There is no window where an event is processed with a partially-updated config.

**What can reload vs what cannot:** transforms and routes can reload (per-adapter wrapper config). Adapters, detectors, bus mode, and CLI flags cannot — they're wired into the pipeline at startup. This is a deliberate trade-off: making more things reloadable requires more complex state management, and the remaining immutables are rarely changed in practice. Transforms and routes are pure configuration — swapping them is a well-defined, bounded operation. Adding a new adapter at runtime would require initializing new goroutines, channels, health registrations, and metric labels while events are flowing, which creates ordering problems. The scope of reload matches the scope of what benefits from it without introducing disproportionate complexity.

**Graceful shutdown** ensures in-flight work completes before the process exits. The shutdown sequence:
1. Signal received (SIGTERM/SIGINT).
2. Root context cancelled — all goroutines receive the cancellation signal through their `ctx.Done()` channel.
3. `errgroup.Wait()` blocks until all goroutines return.
4. `drainAdapters()` calls `Drain(ctx)` on every adapter that implements `adapter.Drainer` — these are adapters with internal buffers (DuckDB, S3/Iceberg, batched Kafka writes) that need to flush before exit.
5. The drain context has a bounded `shutdownTimeout` — if flushing takes longer than the timeout, you accept some data loss and exit.

**Panic recovery** is the third piece. A nil pointer dereference in a buggy transform or Wasm plugin panics the goroutine. Without recovery, this propagates to `errgroup` as a Go runtime panic that crashes the entire process. With `safeGo` — a wrapper that `defer recover()`s over every `errgroup.Go` call — the panic is converted to an error, the pipeline cancels cleanly, and the stack trace is logged with full context rather than crashing silently.

> **Key insight.** Context cancellation is the only teardown mechanism you need. `errgroup.WithContext` creates a context that cancels the moment any goroutine returns a non-nil error. Every blocking operation — channel reads, HTTP requests, database queries — should respect `ctx.Done()`. If they do, cancellation propagates instantly to every part of the pipeline. If they don't, graceful shutdown hangs indefinitely.

**How pgcdc does it.** `pgcdc.go` `wrapperConfigs map[string]*atomic.Pointer[wrapperConfig]`. `Reload()` iterates and stores. `safeGo()` wraps all goroutines. `drainAdapters()` runs post-`Wait()` with a timeout-bounded context. Hot reload is triggered by `cmd/listen.go`'s SIGHUP handler calling `pipeline.Reload(newConfig)`.

> **The Principle.** *"You can restart quickly, or you can restart correctly. Pick one."*

> **Interview trap.** "What's the difference between `sync.Mutex` and `atomic.Pointer` for config swaps?" A mutex serializes access — readers wait while a writer holds the lock. `atomic.Pointer.Store()` and `Load()` are lock-free — readers never wait. For a hot path that reads config on every event, even a lightly-contended mutex creates measurable latency under load. The atomic pointer gives you the same memory safety guarantee without the contention.

> **Interview trap.** "Does SIGHUP reload break in-flight deliveries?" No, because route filtering happens in the per-adapter wrapper goroutine, not in the bus dispatch loop. The dispatch loop distributes events to all subscriber channels before any routing decisions. The wrapper goroutine makes the routing decision on each event using `cfgPtr.Load()`. An in-flight event sees the old config or the new config — both are valid complete configs, never a partially-swapped state.

---

## Chapter 7: "Arrival time is not event time. Confusing them means your windows are lying."

**The failure.** You're aggregating CDC events to compute "orders placed per 5-minute window." A mobile app generates events while offline. When it reconnects at 14:35, it uploads 40 events with timestamps from 14:00–14:20. Your streaming pipeline processes them at 14:35 — processing time. Your "14:00–14:05 window" closed at 14:05 and emitted. The late events go into the "14:35–14:40 window." Your analytics dashboard shows 40 extra orders in a window where they don't belong and a 40-order deficit in the correct window. Nobody notices for 3 days.

**What everyone tries first.** Increase the window size to "give events more time to arrive." This doesn't fix the underlying problem — it just reduces the frequency of the error. Events that arrive later than your larger window still land in the wrong bucket. The root cause is a category confusion between two fundamentally different notions of time.

**The real solution.** Distinguish event time from processing time and choose the right one for each use case.

**Processing time** is when the event arrived at the pipeline. It's always available, always monotonically increasing, and never requires watermarks or late-event handling. Use it for operational metrics (pipeline throughput, delivery latency) and for workloads where the event timestamp is unreliable or irrelevant.

**Event time** is the timestamp embedded in the event payload, representing when the event actually occurred. Use it for business analytics (orders, user activity, revenue) where "when did this happen" is the semantically meaningful question. Event time is the right choice when events can arrive out of order or late.

**The three window types** answer different analytical questions:

**Tumbling windows** divide time into non-overlapping equal-width buckets. Every event belongs to exactly one bucket. "Orders per 5-minute window" is a tumbling window. Memory usage is O(window size). Output is produced once per window when the window closes.

**Sliding windows** maintain a window of width W that advances every S seconds. An event contributes to `ceil(W/S)` windows simultaneously. "Rolling 5-minute average latency, updated every 30 seconds" is a sliding window with W=5m, S=30s. Memory usage is O(W/S × window state). Sliding windows are expensive for large W/S ratios.

**Session windows** have variable length. A new window opens on the first event and extends by a gap duration every time a new event arrives for the same key. It closes after `gap` seconds of silence. "User session length" is a session window — a session is active as long as the user keeps generating events, and it ends after 30 minutes of inactivity. Memory usage is O(active sessions).

Merging STDDEV across sliding window sub-periods requires **Welford's online algorithm** in its parallel variant, which maintains a sum-of-squares component that can be combined across independently-computed sub-windows. The naive approach — computing STDDEV as `sqrt(sum_sq/n - mean^2)` across concatenated partial aggregates — produces wrong answers. This is a common bug in streaming analytics implementations.

> **Key insight.** Watermarks are the mechanism that makes event-time windows possible without waiting forever. A watermark is a monotonically-increasing assertion: "I believe all events with event time earlier than T have already arrived." `watermark = max(observed_event_time) - allowed_lateness`. When the watermark advances past a window's end time, the window closes and emits. Late events that arrive before `allowed_lateness` is exhausted re-trigger the window with updated results. Events beyond the lateness bound are dropped.

**How pgcdc does it.** `view/window.go` (tumbling), `view/sliding_window.go` (sliding + Welford parallel STDDEV), `view/session_window.go` (session). Event-time watermarks: `view/engine.go` per-`viewInstance` `highWatermark time.Time` + mutex. On each event: `wm = eventTime - allowedLateness; if wm.After(highWatermark) { highWatermark = wm }`. Ticker loop calls `window.FlushUpTo(wm)`. Interval joins: `view/join.go` with `leftBuf`/`rightBuf` maps pruned on each new event arrival.

> **The Principle.** *"Arrival time is not event time. Confusing them means your windows are lying."*

> **Interview trap.** "Do sliding windows store N copies of the data?" No. A sliding window with W=10m, S=1m stores `ceil(10/1) = 10` partial aggregates — one per slide period. Each partial aggregate maintains the sufficient statistics for that period (count, sum, sum-of-squares for STDDEV). Merging 10 partial aggregates to produce the full-window result is cheap. Storing 10 full copies of the raw data would be prohibitive.

> **Interview trap.** "What happens to events that arrive after the watermark has passed their window?" If they arrive within `allowed_lateness`, the window is re-emitted with the updated result. If they arrive after `allowed_lateness` is exhausted, they're dropped. Setting `allowed_lateness = 0` means windows close immediately when the watermark passes — maximum throughput, no late-event tolerance.

---

## Chapter 8: "Your checkpoint is only as advanced as your slowest adapter."

**The failure.** You have a pipeline with three adapters: stdout (fast), webhook (medium), and S3 Iceberg (slow — batches 1000 events before flushing). The stdout and webhook adapters have processed up to LSN 5,000,000. The S3 adapter has only flushed up to LSN 4,800,000 — it's waiting for the current batch to fill. The pipeline crashes. On restart, it resumes from the last confirmed checkpoint LSN — which is 4,800,000, the minimum across all adapters. The stdout and webhook adapters re-deliver events 4,800,001 through 5,000,000. Your webhook endpoint receives 200,000 duplicate events.

**What everyone tries first.** Track checkpoint per adapter and restart each adapter from its own position. This sounds right but requires the pipeline to maintain separate read positions on the WAL stream per adapter — effectively running N independent WAL streams, which defeats the purpose of fan-out and multiplies the load on the source database.

**The real solution.** Cooperative checkpointing with min-LSN tracking. Every adapter that implements `adapter.Acknowledger` calls `AckFunc(name, lsn)` after successfully delivering each event (including DLQ writes — DLQ is a form of delivery). The checkpoint tracker maintains a position per adapter and exposes `MinAckedLSN()` — the minimum confirmed position across all acknowledging adapters. The pipeline reports this min-LSN to PostgreSQL as the confirmed flush position. PostgreSQL retains WAL back to this position.

This is the convoy model: the fleet speed is the speed of the slowest truck. The S3 adapter's batching behavior means the fleet (WAL retention boundary) moves in large jumps rather than continuously. After each S3 flush, the min-LSN jumps forward significantly.

**Practical implication for batching adapters:** if you have a slow-flushing adapter (DuckDB, S3/Iceberg, batched Kafka), the WAL retention window on the source database grows proportionally to the flush interval. A 5-minute flush interval means PostgreSQL retains at least 5 minutes of WAL continuously, regardless of the pipeline's throughput. Size your source database's WAL storage accordingly, and consider using a smaller flush interval or a separate pipeline for the slow adapter if WAL retention is a concern.

**The delivery guarantee spectrum** is a consequence of how you configure acknowledgment:

**At-most-once:** No ack, no persistent slot. The pipeline starts from current WAL on startup. Events during downtime are missed. Use for observability pipelines where gaps are tolerable.

**At-least-once:** Persistent slot, restart from last checkpoint LSN. Events between last checkpoint and crash will re-deliver. Consumers must be idempotent (tolerate duplicates by deduplicating on event ID). Default for most production pipelines.

**Exactly-once:** Cooperative checkpoint + persistent slot + all adapters implement `Acknowledger`. The checkpoint advances only as fast as the slowest adapter. No event is missed or double-delivered — *provided consumers are idempotent for the re-delivery window at the exact crash boundary.* True exactly-once is extremely difficult end-to-end; "exactly-once within the pipeline" is achievable, but the consumer's idempotency closes the final gap.

> **Key insight.** The `Ack()` hot path must be lock-free. Every adapter calls it after every event delivery. At high throughput (hundreds of thousands of events per second), a mutex on the ack path becomes a bottleneck. The tracker uses `sync.Map` for storage (optimized for read-heavy, many-keys maps) with `atomic.CompareAndSwap` per adapter position — a spin loop that completes in nanoseconds under no contention.

**How pgcdc does it.** `ack/tracker.go`: `adapters sync.Map` of `*adapterPos` structs with `atomic.Uint64` LSN fields. `MinAckedLSN()` iterates all adapters (called every ~10s, not on hot path). `Register`/`Deregister` take a mutex (called at startup/shutdown only).

> **The Principle.** *"Your checkpoint is only as advanced as your slowest adapter."*

> **Interview trap.** "In Kafka, exactly-once means what exactly?" Exactly-once in Kafka means within a topic partition, using the idempotent producer + transactional API. For a Kafka producer sending to partition 3, exactly-once means each message appears in the partition log exactly once even if the producer retries. For your HTTP webhook endpoint receiving events from Kafka, exactly-once is not Kafka's problem — it's your endpoint's problem, solved by idempotency keys on the consumer side. These are different guarantees at different layers.

---

## Chapter 9: "Cache invalidation is a distributed consistency problem with a friendly name."

**The failure.** You cached a user's billing address when they loaded the checkout page. Three days later, they updated their address in their profile. The cache TTL is 24 hours. They place an order. The order goes to the old address. The cache was stale and nobody noticed — the stale-read rate metric only fires when a stale read causes an observable error, and a stale address isn't an observable error until the package arrives at the wrong place.

**What everyone tries first.** Reduce the TTL. This reduces the window during which staleness can occur, but it doesn't eliminate it — and it increases database load proportionally (every cache miss hits the database). "Reduce TTL" is "accept more database load in exchange for a shorter consistency gap." It's a knob, not a solution.

**The real solution.** Understand the four caching patterns and what each one trades away.

**Cache-aside (lazy loading)** is the most common pattern. The application checks the cache on read. On a miss, it queries the database, populates the cache, and returns the result. Writes go directly to the database; the cache is not updated, so it becomes stale until the TTL expires or an explicit invalidation occurs.

Pros: simple, cache only contains data that's actually been requested. Cons: thundering herd on cold start (many simultaneous misses all hit the DB before anyone can populate the cache), stale reads between a write and the TTL expiry.

**Write-through** writes to both the cache and the database in the same operation. Reads always hit the cache (assuming a warm cache). The cache is always consistent with the database.

Pros: no staleness on the write path. Cons: every write doubles in latency (cache write + DB write). Cache may contain data that's never read again (write to cache on every update, read from cache infrequently = wasted memory and write amplification).

**Write-behind (write-back)** writes to the cache immediately and asynchronously flushes to the database. Reads always hit the cache. Writes are fast — the cache acknowledges before the DB is updated.

Pros: fastest writes. Cons: data loss if the cache node dies before flushing. This is the strategy your CPU's L1 cache uses — it's acceptable when "cache" and "database" are components of the same physical machine with no power-loss risk. For distributed systems, it requires careful consideration of failure modes.

**Read-through** delegates the cache-miss handling to the caching layer itself. The application queries the cache; on a miss, the cache queries the database and populates itself. Application code doesn't know or care whether data came from cache or database.

Pros: cleaner application code. Cons: same thundering herd problem as cache-aside (the caching layer is now the one with the thundering herd, rather than the application).

**The hard invalidation problem.** When a row changes in the database, who tells the cache? Options:

- **TTL**: live with staleness of up to N seconds. Simple, wrong for time-sensitive data.
- **Explicit invalidation**: the write path deletes or updates the cache entry. Works at small scale. Race condition: write to DB, crash before cache invalidation → stale cache until TTL. Also, the write path now has knowledge of all the cache keys that might reference this data, which is a coupling nightmare as the application grows.
- **CDC-driven invalidation**: pgcdc tails the database WAL, publishes every row change, and a Redis adapter (or custom consumer) uses the change event to update or invalidate the relevant cache keys. This eliminates the race condition (the invalidation is derived from the committed change, not from a second step in the write path), doesn't couple the write path to cache logic, and scales to any number of cache consumers.

**Redis data structure selection** has a larger effect on consistency behavior than most engineers expect. Using Redis `SET user:123:address "..."` with a TTL is the most common pattern, but it's also the most fragile: you can only store one version of the value, and concurrent writes can produce last-write-wins behavior where a stale write races with a fresh one. Redis `SETNX` (set-if-not-exists) is the primitive for cache-aside population — populate only on a cache miss, never overwrite an existing entry. For complex objects, `HSET user:123 address "..." name "..." email "..."` lets you atomically update individual fields with `HSET user:123 address "new address"` without touching unrelated fields — which matters when multiple writers update different fields of the same object.

**Pipeline commands** (sending multiple Redis commands in a single round trip) and **Lua scripts** (`EVAL`) are the tools for atomic multi-key operations. A cache invalidation that must atomically delete cache key A and update cache key B cannot be done safely with two separate `DEL` and `SET` commands — a reader can see the intermediate state. A Lua script runs atomically on the Redis server, with no intermediate states visible to other clients.

**Cluster-mode Redis** introduces a constraint that catches teams by surprise: all keys touched by a multi-key command or transaction must be in the same hash slot. By default, `user:123:address` and `user:123:name` are in different slots (hash is computed on the full key). Hash tags (`{user:123}:address`, `{user:123}:name`) force both keys into the same slot by making Redis hash only the content inside `{}`. Without this, multi-key operations on related keys will fail in cluster mode.

> **Key insight.** Cache invalidation is not a caching problem. It's a distributed consistency problem: you have two systems (database and cache) that must agree on the current value of a record, and writes to one must propagate to the other reliably and in order. The friendly name obscures the complexity. CDC-driven invalidation is the correct architecture at scale — it decouples the write path from the invalidation mechanism and makes the propagation observable.

> **The Principle.** *"Cache invalidation is a distributed consistency problem with a friendly name."*

> **Interview trap.** "Cache-aside vs read-through — what's the difference?" Both check the cache, fall back to the DB on a miss, and return the result. The difference is who populates the cache on a miss: the application (cache-aside) or the caching layer itself (read-through). That difference matters for two reasons: thundering herd (the caching layer can implement request coalescing on a miss, the application layer typically cannot) and coupling (read-through requires the caching layer to know how to query the database, which couples two layers that are usually kept separate).

> **Interview trap.** "What is a cache stampede and how do you prevent it?" Cache stampede (thundering herd) occurs when a popular cache key expires and many concurrent requests all miss simultaneously, all hitting the database at once. Prevention: probabilistic early expiration (re-fetch the value before it expires, with probability proportional to proximity to expiry — `P(refetch) ∝ 1/ttl_remaining`). This trades occasional unnecessary refetches for dramatic reduction in simultaneous miss spikes.

---

## Chapter 10: "Consensus is the price you pay for having more than one machine."

**The failure.** You have a two-node cluster. One node is the leader — it processes writes. The other is the follower — it replicates. The network between them partitions. Both nodes are still running. Both can receive requests. The leader keeps processing writes. The follower, having lost contact with the leader and seeing itself as potentially isolated, runs a leader election. It wins — it's the only voter it can reach. Now you have two leaders. Both are accepting writes. The cluster is in split-brain. When the partition heals, you have two diverged histories to reconcile.

**What everyone tries first.** Use a heartbeat and a timeout. If the leader doesn't hear from the follower for T seconds, it steps down. If the follower doesn't hear from the leader for T seconds, it calls an election. This sounds reasonable and is subtly wrong. During a slow network (not a partition), both nodes step down simultaneously, leaving no leader. And the timeout T is a guess — a partition that lasts T-1 seconds causes nothing; a partition that lasts T+1 seconds triggers a double-step-down. The timeout is an attempt to solve a binary problem (partitioned or not) with a continuous variable (time).

**The real solution.** Consensus algorithms formalize what it means for a distributed system to agree on a single value in the presence of failures, without relying on timing assumptions.

**Raft** is the most teachable consensus algorithm (intentionally designed to be more understandable than Paxos). It decomposes the consensus problem into three sub-problems: leader election, log replication, and safety.

**Leader election:** servers start as followers. If a follower doesn't receive a heartbeat from the leader within an election timeout (randomized per-server to reduce simultaneous elections), it becomes a candidate, increments its term number, and sends `RequestVote` RPCs to all other servers. A server grants its vote to the first candidate it hears from in a given term, provided the candidate's log is at least as up-to-date as the voter's. A candidate becomes leader when it receives votes from a majority of servers.

**Log replication:** the leader accepts client requests, appends them to its log, and sends `AppendEntries` RPCs to all followers. A log entry is committed when a majority of servers have written it to their logs. Only committed entries are applied to the state machine. The leader never overwrites committed entries.

**Safety:** a server can only grant a vote to a candidate whose log is at least as up-to-date as its own. This prevents a server with a stale log from becoming leader and overwriting committed entries.

**Term numbers** are Raft's solution to stale leaders. Every RPC includes the sender's current term. If a server receives a message with a higher term, it immediately updates its term and reverts to follower. If it receives a message with a lower term, it rejects it. A partitioned leader that missed an election will immediately step down when the partition heals and it receives messages from the new leader.

**Quorum math:** a cluster of N servers can tolerate `floor((N-1)/2)` failures. 3 servers → 1 failure tolerated. 5 servers → 2 failures tolerated. This is why 2-node clusters are fundamentally unsafe — `floor((2-1)/2) = 0` failure tolerance. Any single failure makes the cluster unable to form a majority.

**CAP theorem** is often stated as "you can only pick 2 of consistency, availability, and partition tolerance." The more useful formulation: "In the presence of a network partition, you must choose between consistency (return an error when uncertain) and availability (return possibly stale data, risk divergence). You cannot have both." Partition tolerance is not optional — partitions happen. The choice is what you do when they happen.

**Distributed locks** are almost always a mistake. A distributed lock protecting a non-idempotent operation is a distributed transaction in disguise — and distributed transactions have all the complexity of consensus plus the latency of round trips. The better pattern is idempotency keys: the operation is designed so that executing it twice with the same key has the same effect as executing it once. etcd's distributed lock (via lease-based TTL + compare-and-swap) is correct and well-implemented, but the need for one usually indicates the protected operation isn't idempotent — fix that first.

> **Key insight.** A consensus system requires a majority of nodes to be alive and able to communicate. "More replicas" improves failure tolerance but doesn't improve availability — a 7-node cluster requires 4 nodes up, which is a higher absolute number to keep healthy than a 3-node cluster requiring 2. More replicas also increases write latency (the leader must wait for a majority acknowledgment). There is a real trade-off between fault tolerance and write throughput that more nodes cannot resolve.

**Paxos vs Raft.** Raft was explicitly designed to be more understandable than Paxos, the original consensus algorithm (Lamport, 1989). Paxos specifies consensus for a single value (Single-Decree Paxos). Multi-Paxos, which extends it to a replicated log, requires significant gaps to be filled by the implementer — gaps that different teams fill differently, leading to incompatible implementations. Raft specifies the full replicated log protocol, including leader election, log replication, cluster membership changes, and snapshotting. The Raft paper's explicit goal was "understandability" — it includes a user study comparing comprehension of Raft vs Paxos. This is why most new distributed systems (etcd, TiKV, CockroachDB) use Raft rather than Paxos.

**Cluster membership changes** are the least-discussed complexity in consensus systems. Adding or removing a node requires consensus about the new membership configuration, which creates a window where two different majorities could coexist (the old configuration and the new one). Raft's joint consensus approach handles this by running both configurations simultaneously during the transition. Getting membership changes wrong is how you produce split-brain even in a correctly-implemented consensus system.

**Lease-based reads** (linearizable reads without a round trip): in Raft, reads that go through the leader are serializable but not necessarily linearizable — the leader might be a stale leader that lost the election but hasn't received the new leader's heartbeat yet. Linearizable reads require the leader to confirm it's still the leader (either by a round trip to a majority, or by using a lease — a time-bounded guarantee that no new leader can be elected for duration T, allowing the current leader to serve reads without a round trip). Leases require synchronized clocks to be safe; their duration must be less than the minimum election timeout.

**Real systems:** etcd (Kubernetes state store — uses Raft), ZooKeeper (Kafka metadata — uses ZAB, a Paxos derivative), Consul (service discovery — uses Raft), CockroachDB (distributed SQL — uses Raft per range), TiKV (distributed KV — uses Raft per region). All are consensus systems at their core. The Raft paper by Ongaro and Ousterhout (2014) is the most readable introduction to these concepts.

> **The Principle.** *"Consensus is the price you pay for having more than one machine."*

> **Interview trap.** "With 5 nodes, can you tolerate 3 failures?" No. `floor((5-1)/2) = 2`. You need 3 nodes to form a majority in a 5-node cluster. If 3 nodes fail, the remaining 2 cannot form a majority and the cluster becomes unavailable. This is a hard mathematical limit, not a configuration issue.

> **Interview trap.** "Can you use a distributed lock to coordinate idempotency?" You can, but you shouldn't. A distributed lock has the same failure modes as any distributed system: the lock holder can die after acquiring the lock and before releasing it (the lease TTL eventually expires, but now you've had unavailability). If you design your operation to be idempotent by default (check-then-act with an idempotency key stored in the database in the same transaction as the operation), you don't need the lock at all.

---

## Chapter 11: "Lock-free is not the same as wait-free. Both beat the mutex, but not equally."

**The failure.** You add a fifth adapter to a pipeline that was handling four adapters comfortably. The p99 event latency doubles. You add a sixth adapter and it doubles again. You look at profiling data and find that `sync.Mutex` contention in the bus subscriber list accounts for 40% of CPU time on the dispatch hot path. Every event dispatch acquires the mutex to read the subscriber list, even though the list almost never changes.

**What everyone tries first.** `sync.RWMutex`. Multiple concurrent readers, exclusive writers. This is better than a full mutex for read-heavy workloads, but it still serializes all readers against any writer. During a subscriber change (rare but blocking for write), all event dispatches pause. And even without a writer, `RLock()` is not free — it involves an atomic operation to increment a reader count, plus cache coherence traffic across CPUs.

**The real solution.** Distinguish between the hot path (every event) and the cold path (adapter registration, which happens once at startup). Optimize the hot path for zero contention; tolerate some cost on the cold path.

**Copy-on-write (COW) subscriber lists** eliminate locking on the dispatch hot path entirely. The subscriber list is stored in an `atomic.Pointer[subscriberList]`. On the hot path: `subs := ptr.Load()` — one atomic load, no lock. On the cold path (subscribe/unsubscribe): acquire a mutex, copy the existing slice, modify the copy, release the mutex, then `ptr.Store(newSlice)` atomically. In-flight dispatches see the old list (missing the new subscriber for at most one event cycle) or the new list — both are valid states. The only invariant is that each subscriber channel is either present or absent, never partially initialized.

**Lock-free vs wait-free:** these terms are often confused.
- **Lock-free** means at least one thread makes progress at any time. It doesn't guarantee any specific thread makes progress. A CAS (compare-and-swap) retry loop is lock-free — under contention, some thread's CAS succeeds and makes progress even if yours fails. But your specific operation might retry many times.
- **Wait-free** means every thread completes its operation in a bounded number of steps, regardless of what other threads are doing. Wait-free is strictly stronger than lock-free and significantly harder to implement.

`atomic.Pointer.Load()` is wait-free — it completes in O(1) regardless of concurrency. `atomic.CompareAndSwap` in a retry loop (the standard CAS loop pattern) is lock-free but not wait-free. Both beat a mutex for the read-heavy case.

**Token bucket rate limiting** is the mechanism for preventing a burst of events from overwhelming a downstream API. A bucket holds tokens (up to the burst capacity). Each request consumes one token. Tokens refill at the rate limit. If the bucket is empty, requests wait.

Token bucket allows bursts: if traffic was quiet, tokens accumulate, and a burst can proceed immediately until the bucket empties. After that, throughput is limited to the refill rate. This differs from leaky bucket, which smooths output at a constant rate with no burst tolerance. Token bucket is the right model when your downstream can handle occasional bursts but not sustained high load.

> **Key insight.** The dispatch hot path must never acquire a lock. Even a lightly-contended mutex adds microseconds of latency per event under load and creates false sharing between CPU cores. For a fanout system with millions of events per second, this is the difference between throughput scaling linearly with adapter count and throughput collapsing under contention.

**How pgcdc does it.** `bus/bus.go`: `subsPtr atomic.Pointer[subscriberList]`. `Subscribe()` uses a mutex to build the new slice safely, then `Store()`s it atomically. The dispatch loop (inside `Start()`) does `subs := b.subsPtr.Load()` once per event cycle, then iterates with no lock. `internal/ratelimit/ratelimit.go` wraps `golang.org/x/time/rate` (token bucket). The rate limiter is wired into the middleware chain for `Deliverer` adapters.

> **The Principle.** *"Lock-free is not the same as wait-free. Both beat the mutex, but not equally."*

> **Interview trap.** "Is `sync.Map` always better than `map` + `sync.Mutex`?" No. `sync.Map` is optimized for two specific access patterns: (1) many goroutines reading the same stable set of keys, or (2) each goroutine reads/writes a disjoint set of keys. For a map with frequent writes to the same keys across goroutines, `sync.Map` can be slower than `map` + `RWMutex` due to the internal locking it uses for the "dirty" map. Read the documentation before reaching for it.

> **Interview trap.** "Rate limiting with a blocking call backs up the pipeline, right?" Yes. If the token bucket is empty, `Wait(ctx)` blocks until a token is available or the context cancels. That blocking propagates: the middleware goroutine blocks, the per-adapter channel fills, the bus dispatch goroutine blocks (reliable mode) or drops events (fast mode). Rate limiting is not free — it converts excess throughput into either latency (reliable) or loss (fast). Choose based on which you prefer.

---

## Chapter 12: "A schema change is a contract change. Treat it like one."

**The failure.** A backend team renames `user_id` to `userId` in their PostgreSQL schema. They update their application. WAL events start flowing with the new field name. Your downstream consumers — a Kafka consumer, a Redis adapter, a webhook endpoint — were written expecting `user_id`. They break silently: the field is missing, so they use a default value or null, and nobody notices until a monitoring alert fires 40 minutes later when a downstream aggregate goes to zero.

**What everyone tries first.** Coordinate the rename with all consumers. Have everyone deploy simultaneously. This works at small scale with a small number of consumers. At large scale, coordinated deploys are operationally expensive and create deployment windows that all consumers must be available during. One consumer with a slow deploy pipeline becomes a dependency that delays the schema change for everyone.

**The real solution.** Make schema changes backward-compatible by default, enforce schema contracts at the infrastructure layer, and use envelope standards so consumers can decode the schema version they were written for.

**Schema evolution** in Avro works through a registry of numbered schema versions and a compatibility mode. The Confluent Schema Registry stores every schema version and validates that new versions are compatible (backward-compatible, forward-compatible, or fully compatible) with existing versions. The Confluent wire format is: `[magic 0x00][4-byte schema ID][binary payload]`. Consumers decode the schema ID, look up the schema in the registry, and deserialize. A consumer written for schema v3 can read a message encoded with schema v4 (backward compatibility) if the new field has a default value.

This is the value of Avro — not the binary encoding (which saves maybe 30% vs JSON in practice), but the enforced schema contract and the managed evolution path.

**Envelope standards** shift the compatibility problem from field names to envelope structure:

**CloudEvents** (CNCF standard): `specversion`, `id`, `type`, `source`, `subject`, `time`, `datacontenttype`, `data`. The `type` field is the schema discriminator — consumers that understand `type = "com.example.user.updated.v2"` are unaffected by a rename in `type = "com.example.user.updated.v3"`. The envelope structure itself doesn't change.

**Debezium** wraps every row change in `before`/`after`/`op`/`source`/`ts_ms`. Consumers understand that `after.user_id` is the new field value. A field rename in the payload is still a breaking change, but the envelope contract (the existence and meaning of `before`, `after`, `op`) is stable.

Both formats are transform-layer concerns — the same raw CDC event can be wrapped in either format by adding a transform in YAML. The envelope choice is a deployment decision, not a detector or adapter decision.

**The middleware chain as composition pattern** is how cross-cutting concerns (metrics, tracing, circuit breaker, rate limiting, retry, DLQ, acknowledgment) get applied uniformly to every adapter without duplicating code in each adapter.

Middleware is composed by wrapping: each layer wraps the next. `Stack(metrics, tracing, CB, RL, retry, DLQ, ack)` iterates from the last element backward — `metrics(tracing(CB(RL(retry(DLQ(ack(deliver)))))))`. Metrics is outermost because it measures the outcome of everything inside it, including retries and DLQ writes. A metric that only counts the initial delivery attempt (not retries) is misleading. A metric inside the retry layer sees every attempt. The right place for aggregate delivery metrics is outermost.

Adding a new cross-cutting concern means adding one middleware implementation and inserting it in the stack. Every adapter gets it automatically.

**The self-registering registry** (Go's `init()` pattern) keeps adapter/detector registration decoupled from the CLI orchestration. `cmd/register_stdout.go` contains only an `init()` function that calls `registry.RegisterAdapter(...)`. `init()` runs when the package is imported. The `cmd/` package includes this file by listing it in the same package — it runs unconditionally when the binary starts. This is how build tags (`no_kafka`, `no_grpc`, etc.) work for slim builds: excluding the file from the build means its `init()` never runs, so that adapter is never registered.

> **Key insight.** Schema changes are contract changes. A field rename is not a refactor — it's a breaking API change, the same as renaming a function in a public library. The tools that make schema changes non-breaking (Avro schema evolution, backward-compatible CloudEvents types, field addition with defaults) all require treating the schema as a versioned contract with explicit compatibility rules from the start.

**How pgcdc does it.** `transform/cloudevents.go`, `transform/debezium.go` (envelope wrapping as transforms). `encoding/avro.go`, `encoding/registry/` (Confluent Schema Registry client). `adapter/middleware/middleware.go` `Stack()` function (compose in reverse). `registry/registry.go` + `cmd/register_*.go` (self-registration via `init()`). `registry/spec.go` `ParamSpec` + `ConnectorSpec` for `pgcdc describe` introspection — each adapter and detector can describe its own parameters.

> **The Principle.** *"A schema change is a contract change. Treat it like one."*

> **Interview trap.** "Why does middleware composition iterate in reverse?" Because wrapping creates nesting. If you iterate `[A, B, C]` forward, you get `C(B(A(deliver)))` — C is outermost, A is closest to delivery. If you want A outermost (A sees the final outcome, including retries in B and C), you need to wrap in reverse. The end-to-end order — from first call to actual delivery — is the order of the original list. Reverse iteration means the first element in your list is the first to be called and the last to return.

> **Interview trap.** "Can Go `init()` order be controlled?" Within a single package, `init()` functions in multiple files run in the order the files are passed to the compiler (alphabetical by filename is the convention, but not guaranteed). Across packages, `init()` runs in import order — a package's `init()` runs after all its dependencies' `init()` functions. You cannot control `init()` order between sibling packages in the same binary. This is why the registry pattern works: each adapter's `init()` calls `registry.RegisterAdapter()`, and the registry is a simple append-to-slice operation that doesn't depend on the order registrations arrive.

---

## Appendix

### pgcdc Pipeline Architecture

```
Signal (SIGINT/SIGTERM)
  |
  v
Context ──> Pipeline (pgcdc.go orchestrates everything)
              |
  Detector ──> Bus (fan-out + routing) ──> Adapter (stdout)
  (listennotify     |                   ──> Adapter (webhook)     ──> DLQ
   walreplication   |                   ──> Adapter (file/exec)
   outbox           |                   ──> Adapter (pg_table)
   mysql            |                   ──> Adapter (SSE/WS)      ──> HTTP server
   mongodb          |                   ──> Adapter (nats/kafka/search/redis/grpc/s3)
   webhookgw        |                   ──> Adapter (embedding)   ──> DLQ
   or sqlite)       |                   ──> Adapter (graphql)     ──> HTTP server
                    |                   ──> Adapter (arrow)       ──> gRPC Flight server
                    |                   ──> Adapter (duckdb)      ──> HTTP query endpoint
              ingest chan               ──> Adapter (view)        ──> re-inject to bus
                                subscriber chans (one per adapter, filtered by route)
                                      |
                              Health Checker + Prometheus Metrics
```

**Concurrency model:** `errgroup` + `safeGo` (panic-recovering wrapper). One context cancellation tears everything down. Every goroutine — detector, bus, backpressure controller, each adapter wrapper — runs inside `g.Go(safeGo(...))`.

**Event routing:** `--route adapter=channel1,channel2`. Route filtering happens per-adapter in the wrapper goroutine (not bus-level), which is why SIGHUP hot reload works without event loss — in-flight events see either old or new route config, both complete.

**Adapter lifecycle:** optional `Validator` (`Validate(ctx) error`) for pre-flight checks at startup; optional `Drainer` (`Drain(ctx) error`) for graceful shutdown flush bounded by `shutdown_timeout`; optional `Acknowledger` (`SetAckFunc`) for cooperative checkpointing.

**Middleware wiring:** adapters implementing `Deliverer` (`Deliver(ctx, event) error`) get the full middleware chain (circuit breaker, rate limit, retry, DLQ, tracing, metrics, ack) automatically via `adapter/middleware.Build()`. Adapters implementing only `Adapter` (`Start(ctx, <-chan event.Event) error`) manage their own delivery logic.

---

### The 12 Principles

| # | Principle |
|---|---|
| 1 | *"A write that wasn't fsynced didn't happen."* |
| 2 | *"Every replication slot you forget is a disk bomb with no timer."* |
| 3 | *"A poison pill is patient. It will wait forever for you to stop retrying it."* |
| 4 | *"A circuit breaker isn't a retry strategy — it's permission to stop trying."* |
| 5 | *"Reliability is always at the cost of the source."* |
| 6 | *"You can restart quickly, or you can restart correctly. Pick one."* |
| 7 | *"Arrival time is not event time. Confusing them means your windows are lying."* |
| 8 | *"Your checkpoint is only as advanced as your slowest adapter."* |
| 9 | *"Cache invalidation is a distributed consistency problem with a friendly name."* |
| 10 | *"Consensus is the price you pay for having more than one machine."* |
| 11 | *"Lock-free is not the same as wait-free. Both beat the mutex, but not equally."* |
| 12 | *"A schema change is a contract change. Treat it like one."* |

---

### Concept → File Quick Reference

| Concept | Primary file |
|---|---|
| Pipeline orchestration | `pgcdc.go` |
| Structured concurrency | `pgcdc.go` (`safeGo`, `errgroup`) |
| Graceful shutdown / Drainer | `pgcdc.go` (`drainAdapters`), `adapter/adapter.go` |
| Hot reload (atomic pointer swap) | `pgcdc.go` (`Reload`, `wrapperConfigs`), `cmd/reload.go` |
| Event bus fan-out (COW) | `bus/bus.go` |
| Backpressure zones (proportional) | `backpressure/backpressure.go` |
| Circuit breaker (3-state FSM) | `internal/circuitbreaker/circuitbreaker.go` |
| Retry + full jitter | `adapter/middleware/retry.go` |
| Token bucket rate limiting | `internal/ratelimit/ratelimit.go` |
| Dead letter queue | `dlq/dlq.go`, `adapter/middleware/dlq.go` |
| Sliding nack window | `dlq/window.go` |
| Middleware chain (composition) | `adapter/middleware/middleware.go` |
| Transform chain + JSON cache | `transform/transform.go` |
| WAL logical replication | `detector/walreplication/walreplication.go` |
| TOAST LRU cache | `detector/walreplication/toastcache/` |
| Outbox (SKIP LOCKED) | `detector/outbox/outbox.go` |
| Cooperative checkpointing | `ack/tracker.go` |
| Tumbling windows | `view/window.go` |
| Sliding windows (Welford STDDEV) | `view/sliding_window.go` |
| Session windows | `view/session_window.go` |
| Event-time watermarks | `view/engine.go` |
| Interval joins | `view/join.go` |
| CloudEvents / Debezium transforms | `transform/cloudevents.go`, `transform/debezium.go` |
| Avro + Schema Registry | `encoding/avro.go`, `encoding/registry/` |
| CEL filter | `cel/`, `transform/transform.go` |
| Wasm plugins (Extism) | `plugin/wasm/runtime.go` |
| Ring buffer inspector | `inspect/` |
| Self-registering registry | `registry/registry.go`, `cmd/register_*.go` |
| Connector specs (describe) | `registry/spec.go` |
| Multi-pipeline server | `server/manager.go` |
| Schema store | `schema/` |

---

### Delivery Guarantee Decision Tree

```
Do you need to survive restarts without missing events?
├── No → at-most-once: no ack, no persistent slot
│         (use for: metrics aggregation, non-critical logging)
└── Yes → Do all adapters need exactly-once?
    ├── No → at-least-once: persistent slot, restart from last checkpoint LSN
    │         Consumers must tolerate duplicates (idempotency by event ID)
    └── Yes → exactly-once: cooperative checkpoint + persistent slot
               All adapters must implement adapter.Acknowledger
               Checkpoint advances at the pace of the slowest adapter
               Consumers must still be idempotent for the exact crash boundary
```

---

### The Full Middleware Stack

```
Inbound event
      │
      ▼
┌─────────────┐  outermost: sees all outcomes including retries + DLQ writes
│   Metrics   │
└──────┬──────┘
       │
┌──────▼──────┐  creates CONSUMER span; propagates from event header
│   Tracing   │
└──────┬──────┘
       │
┌──────▼────────────┐  rejects immediately if open; probes in half-open
│  Circuit Breaker  │
└──────┬────────────┘
       │
┌──────▼──────┐  blocks until token available (or drops if fast mode)
│ Rate Limit  │
└──────┬──────┘
       │
┌──────▼──────┐  exponential backoff with full jitter
│    Retry    │
└──────┬──────┘
       │
┌──────▼──────┐  catches terminal failures; writes to DLQ; swallows error
│     DLQ     │
└──────┬──────┘
       │
┌──────▼──────┐  calls AckFunc(name, lsn) after successful Deliver
│     Ack     │
└──────┬──────┘
       │
┌──────▼──────┐
│   Deliver   │  adapter's actual delivery function
└─────────────┘
```

---

### CDC Method Comparison

| | LISTEN/NOTIFY | WAL Replication | Outbox | MySQL Binlog | MongoDB Change Streams | SQLite Change Table |
|---|---|---|---|---|---|---|
| **Ordering** | None | Strong (LSN) | Per-row insert order | Strong (GTID) | Per-document | Poll order |
| **Latency** | Sub-ms | Sub-100ms | Seconds (poll) | Sub-100ms | Sub-100ms | Seconds (poll) |
| **App coupling** | High | None | Medium | None | None | Medium |
| **Schema changes** | No | Yes (DDL events) | No | Yes | No | No |
| **Replay from offset** | No | Yes (LSN) | Yes (row ID) | Yes (GTID) | Yes (resume token) | No |
| **Exactly-once possible** | No | Yes | Yes (transactional) | Yes | No | No |
| **Infrastructure needed** | None | Replication slot | Outbox table | Binlog enabled | Change Streams | Change table |
| **Primary risk** | Missed under load | Slot fills disk | Table growth | Binary log size | Oplog window | Poll lag |

---

### Storage Engine Trade-offs

| | B-tree (PostgreSQL, InnoDB) | LSM-tree (RocksDB, Cassandra) |
|---|---|---|
| **Write performance** | Slower (page splits, random I/O) | Faster (sequential append to memtable + WAL) |
| **Read performance** | Faster (sorted pages, direct lookup) | Slower (merge multiple levels + memtable) |
| **Write amplification** | Lower | Higher (compaction rewrites data multiple times) |
| **Read amplification** | Lower | Higher (must check multiple SSTable levels) |
| **Best for** | Read-heavy, mixed, OLTP | Write-heavy, time-series, append-dominant |
| **WAL role** | Crash recovery for in-place page mutations | Durability for memtable before flush |

---

### Concurrency Primitive Quick Reference

| Primitive | Use case | Trade-offs |
|---|---|---|
| `sync.Mutex` | Protect shared state with short critical sections | Simple, but serializes all access; avoid on hot paths |
| `sync.RWMutex` | Read-heavy shared state, rare writes | Better than Mutex for reads; writes still block all readers |
| `atomic.Pointer[T]` | COW data structures (subscriber lists, configs) | Lock-free reads; write path needs a mutex for copy construction |
| `atomic.Uint64` | Single numeric field updated concurrently (LSN tracking) | Wait-free load/store; CAS for read-modify-write |
| `sync.Map` | Many goroutines, stable key set, read-dominant | Faster than map+mutex for that pattern; slower for write-heavy |
| `chan T` (buffered) | Bounded async message passing between goroutines | Backpressure via full channel; blocks or drops depending on send style |
| `errgroup.WithContext` | Structured concurrency — cancel all on first error | Automatically propagates context cancellation to all group members |
| `sync.Once` | One-time initialization | Zero cost after first call; avoids double-initialization race |

---

### Window Type Comparison

| | Tumbling | Sliding | Session |
|---|---|---|---|
| **Overlap** | None | Yes (`ceil(W/S)` copies) | None |
| **Window length** | Fixed | Fixed | Variable (gap-bounded) |
| **Memory** | O(window) | O(W/S × window) | O(active sessions) |
| **Use case** | Hourly aggregates | Rolling averages | User session tracking |
| **STDDEV across sub-windows** | N/A | Requires Welford parallel variant | N/A |
| **Event belongs to** | Exactly 1 window | `ceil(W/S)` windows | Exactly 1 window |
