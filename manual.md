# The pgcdc Technical Manual

> **pgcdc** — PostgreSQL, MySQL, and MongoDB change data capture streaming to everywhere.

---

## Table of Contents

- [Preamble: How to Read This Manual](#preamble-how-to-read-this-manual)
- [Chapter 1: The Event](#chapter-1-the-event)
- [Chapter 2: Where Events Come From — Detectors](#chapter-2-where-events-come-from--detectors)
  - [2.1 LISTEN/NOTIFY](#21-listennotify)
  - [2.2 WAL Replication](#22-wal-replication)
  - [2.3 The Outbox Pattern](#23-the-outbox-pattern)
  - [2.4 MySQL Binlog](#24-mysql-binlog)
  - [2.5 MongoDB Change Streams](#25-mongodb-change-streams)
  - [2.6 Feature Availability by Detector](#26-feature-availability-by-detector)
  - [2.7 Multi-Detector Composition](#27-multi-detector-composition)
- [Chapter 3: The Bus — Fan-Out and Backpressure](#chapter-3-the-bus--fan-out-and-backpressure)
  - [3.1 Fan-Out](#31-fan-out)
  - [3.2 Fast vs Reliable Mode](#32-fast-vs-reliable-mode)
  - [3.3 Source-Aware Backpressure](#33-source-aware-backpressure)
- [Chapter 4: Transforms — Reshaping Events in Flight](#chapter-4-transforms--reshaping-events-in-flight)
  - [4.1 TransformFunc and Chain](#41-transformfunc-and-chain)
  - [4.2 Built-in Transforms](#42-built-in-transforms)
  - [4.3 Hot Reload](#43-hot-reload)
  - [4.4 CEL Filter](#44-cel-filter)
- [Chapter 5: Simple Adapters — stdout, file, exec, pg_table](#chapter-5-simple-adapters--stdout-file-exec-pg_table)
  - [5.1 stdout — The Canonical Adapter](#51-stdout--the-canonical-adapter)
  - [5.2 file — Size-Based Rotation](#52-file--size-based-rotation)
  - [5.3 exec — Subprocess Piping](#53-exec--subprocess-piping)
  - [5.4 pg_table — INSERT with Sanitization](#54-pg_table--insert-with-sanitization)
  - [5.5 The Adapter Lifecycle](#55-the-adapter-lifecycle)
- [Chapter 6: HTTP Adapters — webhook, SSE, WebSocket, gRPC](#chapter-6-http-adapters--webhook-sse-websocket-grpc)
  - [6.1 Webhook](#61-webhook)
  - [6.2 SSE](#62-sse)
  - [6.3 WebSocket](#63-websocket)
  - [6.4 gRPC Streaming](#64-grpc-streaming)
- [Chapter 7: Message Brokers — NATS, Kafka, and Building Kafka from Scratch](#chapter-7-message-brokers--nats-kafka-and-building-kafka-from-scratch)
  - [7.1 NATS JetStream](#71-nats-jetstream)
  - [7.2 Kafka Adapter](#72-kafka-adapter)
  - [7.3 The Kafka Protocol Server](#73-the-kafka-protocol-server)
- [Chapter 8: Storage and Data Adapters — S3, Search, Redis, Embedding](#chapter-8-storage-and-data-adapters--s3-search-redis-embedding)
  - [8.1 S3](#81-s3)
  - [8.2 Search — Typesense and Meilisearch](#82-search--typesense-and-meilisearch)
  - [8.3 Redis](#83-redis)
  - [8.4 Embedding — pgvector](#84-embedding--pgvector)
- [Chapter 9: Stream Processing — The View Engine](#chapter-9-stream-processing--the-view-engine)
  - [9.1 What is a Streaming SQL View?](#91-what-is-a-streaming-sql-view)
  - [9.2 The SQL Parser](#92-the-sql-parser)
  - [9.3 Three Window Types](#93-three-window-types)
  - [9.4 Aggregators](#94-aggregators)
  - [9.5 Loop Prevention](#95-loop-prevention)
- [Chapter 10: Checkpointing and Exactly-Once](#chapter-10-checkpointing-and-exactly-once)
  - [10.1 Checkpoint Store](#101-checkpoint-store)
  - [10.2 Cooperative Checkpointing](#102-cooperative-checkpointing)
  - [10.3 Snapshots](#103-snapshots)
- [Chapter 11: When Things Go Wrong — DLQ and Errors](#chapter-11-when-things-go-wrong--dlq-and-errors)
  - [11.1 Typed Errors](#111-typed-errors)
  - [11.2 DLQ Interface](#112-dlq-interface)
  - [11.3 DLQ Management CLI](#113-dlq-management-cli)
  - [11.4 Nack Window — Adaptive DLQ](#114-nack-window--adaptive-dlq)
- [Chapter 12: Encoding, Schema Evolution, and TOAST](#chapter-12-encoding-schema-evolution-and-toast)
  - [12.1 Encoding](#121-encoding)
  - [12.2 Schema Evolution](#122-schema-evolution)
  - [12.3 TOAST Columns](#123-toast-columns)
- [Chapter 13: Observability](#chapter-13-observability)
  - [13.1 Prometheus Metrics](#131-prometheus-metrics)
  - [13.2 Health Checks](#132-health-checks)
  - [13.3 OpenTelemetry Tracing](#133-opentelemetry-tracing)
  - [13.4 Event Inspector](#134-event-inspector)
- [Chapter 14: The Plugin System — Wasm Extensions](#chapter-14-the-plugin-system--wasm-extensions)
  - [14.1 Why Wasm?](#141-why-wasm)
  - [14.2 Four Extension Points](#142-four-extension-points)
  - [14.3 The LSN Boundary](#143-the-lsn-boundary)
- [Chapter 14c: Connector Specs and Introspection](#chapter-14c-connector-specs-and-introspection)
- [Chapter 15: The Pipeline — How It All Fits Together](#chapter-15-the-pipeline--how-it-all-fits-together)
  - [15.1 Construction](#151-construction)
  - [15.2 The Run Method](#152-the-run-method)
  - [15.3 Graceful Shutdown](#153-graceful-shutdown)
- [Chapter 16: Operations — Config, Migrations, CLI](#chapter-16-operations--config-migrations-cli)
  - [16.1 Configuration](#161-configuration)
  - [16.2 Schema Migrations](#162-schema-migrations)
  - [16.3 CLI Commands](#163-cli-commands)
  - [16.4 Flag Interdependencies](#164-flag-interdependencies)
  - [16.5 Build System and Slim Binary](#165-build-system-and-slim-binary)
- [Chapter 17: Operational Runbook](#chapter-17-operational-runbook)
  - [17.1 Webhook Endpoint Down](#171-webhook-endpoint-down)
  - [17.2 WAL Disk Filling Up](#172-wal-disk-filling-up)
  - [17.3 Adding a New Adapter to a Running System](#173-adding-a-new-adapter-to-a-running-system)
  - [17.4 Changing Transforms Without Restart](#174-changing-transforms-without-restart)
  - [17.5 Debugging Event Flow](#175-debugging-event-flow)
  - [17.6 Upgrading pgcdc](#176-upgrading-pgcdc)
- [Chapter 18: Multi-Pipeline Server](#chapter-18-multi-pipeline-server)
  - [18.1 Multi-Pipeline Server](#181-multi-pipeline-server)
- [Epilogue: The Shape of the System](#epilogue-the-shape-of-the-system)

---

## Preamble: How to Read This Manual

This manual traces a single CDC event from its birth in a PostgreSQL database through every layer of pgcdc until it reaches its destination — a webhook, a Kafka consumer, an S3 bucket, a search index, or one of a dozen other places. Concepts are introduced when you encounter them naturally, not before.

**Reading order.** Chapters 1–3 form the backbone: the Event, the Detectors, and the Bus. Read them sequentially. After that, any chapter can be read independently — the transforms chapter (4) is useful before the adapter chapters (5–8), but not strictly required. Chapter 15 (The Pipeline) ties everything together and is best saved for after you've seen the individual components.

**Architecture at a glance:**

```
Signal (SIGINT/SIGTERM)
  |
  v
Context ──> Pipeline (pgcdc.go orchestrates everything)
              |
  Detector ──> Bus (fan-out + routing) ──> Adapter (stdout)
  (listennotify    |                   ──> Adapter (webhook)     ──> DLQ
   walreplication  |                   ──> Adapter (file)
   outbox          |                   ──> Adapter (s3)
   mysql           |
   or mongodb)     |                   ──> Adapter (kafkaserver) ──> TCP :9092
                   |                   ──> Adapter (view)        ──> re-inject
             ingest chan               ──> Adapter (...)
                                      subscriber chans
```

One detector produces events. One bus fans them out. N adapters consume them. One context cancellation tears everything down. That's the whole system. The rest is detail — and the detail is where the interesting engineering lives.

**Who this manual is for.** You know Go. You know PostgreSQL. You've used message queues and HTTP APIs. What you might not know is how PostgreSQL's write-ahead log works at the protocol level, how Kafka's wire protocol encodes records, how backpressure can prevent your database from filling its disk, or how to build a streaming SQL engine that computes rolling averages over a CDC event stream. This manual explains all of those things through the lens of one system — pgcdc — with real code excerpts from the actual codebase.

**Notation.** File paths are relative to the repository root. When we refer to `bus/bus.go:99`, that means line 99 of the file `bus/bus.go`. Code excerpts are taken verbatim from the codebase with at most light trimming for length — no pseudocode. Every excerpt includes a file reference so you can read the surrounding context. ASCII diagrams are used for architecture, data flow, and state machines.

---

## Chapter 1: The Event

Everything in pgcdc is an event. A row inserted into a PostgreSQL table, a MongoDB document updated, a MySQL binlog entry — they all become the same struct before the bus ever sees them. This is the atom of the system.

**File: `event/event.go`**

```go
type Event struct {
    ID          string            `json:"id"`
    Channel     string            `json:"channel"`
    Operation   string            `json:"operation"`
    Payload     json.RawMessage   `json:"payload"`
    Source      string            `json:"source"`
    CreatedAt   time.Time         `json:"created_at"`
    Transaction *TransactionInfo  `json:"transaction,omitempty"`
    LSN         uint64            `json:"-"`
    SpanContext trace.SpanContext `json:"-"`
}
```

Let's examine every field and understand why it exists.

**ID** is a UUIDv7, not v4. This matters. UUIDv7 encodes the current timestamp in its most significant bits, making IDs monotonically increasing and lexicographically sortable by creation time. When events land in a database or a log, you can sort by ID to get chronological order without needing a separate timestamp index. The `uuid.NewV7()` call in the constructor handles this:

```go
func New(channel, operation string, payload json.RawMessage, source string) (Event, error) {
    id, err := uuid.NewV7()
    if err != nil {
        return Event{}, fmt.Errorf("generate event id: %w", err)
    }
    return Event{
        ID:        id.String(),
        Channel:   channel,
        Operation: operation,
        Payload:   payload,
        Source:    source,
        CreatedAt: time.Now().UTC(),
    }, nil
}
```

**Channel** is the routing key. For WAL events it's derived from the table name: `pgcdc:orders`, `pgcdc:inventory.products`. For LISTEN/NOTIFY it's the PostgreSQL channel name directly. For MySQL: `pgcdc:mydb.orders`. For MongoDB: `pgcdc:mydb.users`. The bus uses this field for route-based filtering — when you configure `--route webhook=pgcdc:orders,pgcdc:payments`, only events on those channels reach the webhook adapter.

**Operation** is one of `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `NOTIFY`, `SNAPSHOT`, `SNAPSHOT_STARTED`, `SNAPSHOT_COMPLETED`, `SCHEMA_CHANGE`, `BEGIN`, `COMMIT`, or `VIEW_RESULT`. Transforms use this to filter events (`--filter-operations INSERT,UPDATE`), and the Debezium envelope maps it to Debezium's single-character codes (`c`, `u`, `d`, `r`).

**Payload** is `json.RawMessage` — a raw JSON byte slice, not a parsed `map[string]any`. This is deliberate. Most adapters (stdout, file, webhook, Kafka, NATS) forward the payload without ever touching its contents. Parsing JSON only to re-marshal it is pure waste. By keeping the payload raw, the hot path for most adapters is a zero-copy forward. Only adapters that need to inspect the payload (embedding, search, view engine) unmarshal it.

**LSN** has the tag `json:"-"` — it never appears in serialized output. The Log Sequence Number is a WAL-internal concept: a monotonically increasing 64-bit position in PostgreSQL's write-ahead log. It exists on the event struct solely for cooperative checkpointing (Chapter 10). When the system needs to tell PostgreSQL "I've safely processed everything up to position X," it reads this field. LISTEN/NOTIFY events and snapshot events set it to zero — they have no WAL position.

**SpanContext** also has `json:"-"`. It carries OpenTelemetry trace context for distributed tracing (Chapter 13). When tracing is disabled (the default), this is a zero-value struct with no overhead.

**Transaction** is only populated by the WAL detector when `--tx-metadata` is enabled. It carries the PostgreSQL transaction ID (xid), commit timestamp, and sequence number within the transaction. This lets downstream consumers correlate events that were part of the same database transaction.

Here's what a real event looks like serialized:

```json
{
  "id": "019505a3-8c7e-7d44-b2c8-1a4f5e6d7890",
  "channel": "pgcdc:orders",
  "operation": "INSERT",
  "payload": {
    "op": "INSERT",
    "table": "orders",
    "row": {
      "id": "42",
      "customer_id": "7",
      "total": "99.50",
      "created_at": "2026-02-26T10:30:00Z"
    },
    "old": null
  },
  "source": "wal_replication",
  "created_at": "2026-02-26T10:30:00.123456Z",
  "transaction": {
    "xid": 48291,
    "commit_time": "2026-02-26T10:30:00.120000Z",
    "seq": 1
  }
}
```

Notice that the payload's `row` values are all strings. PostgreSQL's logical replication protocol transmits all column values as text representations (DataType `'t'` in the WAL protocol). pgcdc doesn't parse them into native Go types — that would require schema awareness and risk precision loss for numeric types. The raw string values flow through to the consumer, which knows its own schema.

The Event struct has two important properties that the rest of the system depends on: it is cheap to copy (no pointers except the optional Transaction and the json.RawMessage slice header), and it carries all the metadata needed for any adapter to do its job without reaching back to the source.

**Why not use a generic map or interface{}?** A typed struct provides compile-time guarantees. Every component that touches an event knows exactly what fields are available. The ID is always a string, the payload is always JSON, the LSN is always a uint64. There's no runtime type assertion, no "check if field exists" pattern, no defensive nil checks on optional fields that should be required. The optional fields (`Transaction`, `LSN`, `SpanContext`) use zero-value semantics: if not applicable, they're empty, and consumers check before using them.

**Why json.RawMessage for Payload?** Three reasons: (1) Zero-copy forwarding — most adapters pass the payload through without modification. (2) Schema independence — pgcdc doesn't need to know the structure of the table being replicated. (3) Late binding — transforms and views can unmarshal the payload when they need to inspect it, and marshal back only if they modify it.

The Event flows through the pipeline as a value type (not a pointer). This means each goroutine (bus, wrapper, adapter) gets its own copy. There are no data races on events — a transform can modify its copy without affecting other adapters that received the same event from the bus. The cost is a few allocations per event (the json.RawMessage slice header and Transaction pointer are shared via the underlying array/struct until modified), which is negligible compared to the I/O cost of delivering to an adapter.

---

## Chapter 2: Where Events Come From — Detectors

A detector watches a database for changes and pushes events into a channel. The interface is minimal:

**File: `detector/detector.go`**

```go
type Detector interface {
    Start(ctx context.Context, events chan<- event.Event) error
    Name() string
}
```

Two methods. `Start` blocks until the context is cancelled or a fatal error occurs. The `events` channel is send-only — the detector pushes, the bus pulls. The detector **must not** close this channel; the bus owns its lifecycle.

pgcdc ships five detectors. We'll spend the most time on WAL replication, because understanding it means understanding PostgreSQL's change data capture mechanism at the protocol level.

### 2.1 LISTEN/NOTIFY

**File: `detector/listennotify/listennotify.go`**

LISTEN/NOTIFY is PostgreSQL's built-in pub/sub mechanism. You create a trigger on your table that calls `pg_notify('channel', payload)`, and any client listening on that channel receives the message. It's the simplest path to CDC — no logical replication, no slots, no WAL configuration.

The tradeoffs are real: the payload is limited to ~8KB (the maximum NOTIFY payload size in PostgreSQL), you need to write and maintain triggers, and you get no transaction metadata or LSN positions. For many use cases — especially when you control the application layer and can craft the trigger payload — this is perfectly sufficient.

Here's a typical trigger that feeds pgcdc:

```sql
CREATE OR REPLACE FUNCTION notify_orders() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('pgcdc:orders', json_build_object(
    'op', TG_OP,
    'table', TG_TABLE_NAME,
    'row', row_to_json(NEW),
    'old', CASE WHEN TG_OP = 'UPDATE' THEN row_to_json(OLD) ELSE NULL END
  )::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_notify
  AFTER INSERT OR UPDATE OR DELETE ON orders
  FOR EACH ROW EXECUTE FUNCTION notify_orders();
```

The detector maintains a single dedicated connection (not a pool) and listens on all configured channels:

```go
func (d *Detector) run(ctx context.Context, events chan<- event.Event) error {
    conn, err := pgx.Connect(ctx, d.dbURL)
    if err != nil {
        return fmt.Errorf("connect: %w", err)
    }
    defer func() {
        closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        _ = conn.Close(closeCtx)
    }()

    for _, ch := range d.channels {
        safe := pgx.Identifier{ch}.Sanitize()
        if _, err := conn.Exec(ctx, "LISTEN "+safe); err != nil {
            return fmt.Errorf("listen %s: %w", safe, err)
        }
    }

    for {
        notification, err := conn.WaitForNotification(ctx)
        if err != nil {
            return fmt.Errorf("wait: %w", err)
        }
        op, payload := parsePayload(notification.Payload)
        ev, err := event.New(notification.Channel, op, payload, source)
        // ... send to events channel
    }
}
```

Notice the use of `pgx.Connect` — a single connection, not `pgxpool.New`. This is a hard requirement: `LISTEN` state is per-connection. If you used a pool, your `LISTEN` command might execute on connection A, but the next call to `WaitForNotification` might get connection B, which knows nothing about your subscriptions. The detector uses `pgx.Identifier{ch}.Sanitize()` for the channel name, preventing SQL injection through channel names.

The `parsePayload` function handles both JSON and non-JSON payloads. If the payload parses as JSON and contains an `op` field, the operation is extracted from the payload (so your trigger can signal INSERT/UPDATE/DELETE). If the payload isn't valid JSON, it's wrapped as a raw string and the operation defaults to `NOTIFY`.

The outer `Start` method wraps `run` in a reconnect loop with exponential backoff and jitter:

```go
func (d *Detector) Start(ctx context.Context, events chan<- event.Event) error {
    var attempt int
    for {
        runErr := d.run(ctx, events)
        if ctx.Err() != nil {
            return ctx.Err()
        }
        delay := backoff.Jitter(attempt, d.backoffBase, d.backoffCap)
        // ... log, sleep, retry
        attempt++
    }
}
```

The backoff uses "full jitter" — `rand(0, min(cap, base * 2^attempt))` — which is the optimal strategy for reducing contention when multiple clients reconnect simultaneously (as shown in the AWS Architecture Blog's analysis of exponential backoff). The implementation in `internal/backoff/backoff.go` computes `min(cap, base * 2^attempt)` and then draws a uniform random duration from `[0, ceiling)`:

```go
func Jitter(attempt int, base, cap time.Duration) time.Duration {
    const minDelay = 100 * time.Millisecond
    exp := float64(base) * math.Pow(2, float64(attempt))
    if exp > float64(cap) || exp <= 0 { // overflow guard
        exp = float64(cap)
    }
    jitter := time.Duration(rand.Int64N(int64(exp)))
    if jitter < minDelay {
        jitter = minDelay
    }
    return jitter
}
```

Full jitter (rather than equal jitter or decorrelated jitter) produces the best spread across concurrent reconnecting clients, minimizing thundering herd effects. The `exp <= 0` guard catches overflow when `attempt` is very large. The 100ms minimum delay prevents sub-millisecond retries that would hammer a recovering server.

**When to use LISTEN/NOTIFY vs WAL:**

| | LISTEN/NOTIFY | WAL Replication |
|---|---|---|
| Setup | Triggers on each table | Publication + replication slot |
| Payload control | Full (you write the trigger) | Raw row data from WAL |
| Size limit | ~8KB per notification | Unlimited |
| Transaction metadata | None | xid, commit_time, sequence |
| Crash recovery | No (messages are fire-and-forget) | Yes (persistent slots + checkpoints) |
| Performance impact | Trigger overhead per row | Reads existing WAL (no extra writes) |
| Multiple consumers | Each gets all messages | Slot-based (one consumer per slot) |

### 2.2 WAL Replication

**File: `detector/walreplication/walreplication.go`**

This is the core educational section. WAL replication is how pgcdc captures every change from PostgreSQL without triggers, without polling, and without missing anything — even across crashes and restarts.

#### What is WAL?

PostgreSQL writes every change to a write-ahead log (WAL) before writing it to the actual data files. The WAL is an append-only, sequential journal stored in `pg_wal/`. Its primary purpose is crash recovery: if the server crashes, PostgreSQL replays the WAL from the last checkpoint to reconstruct any changes that were written to the WAL but not yet flushed to the data files. The "write-ahead" part is the key insight: the WAL entry is written and fsync'd *before* the data page is modified, guaranteeing that every committed change can be recovered.

WAL is structured as a series of fixed-size segment files (default 16MB each), named sequentially: `000000010000000000000001`, `000000010000000000000002`, etc. PostgreSQL recycles old segments once all consumers (standby replicas, replication slots, archive commands) have confirmed they no longer need them.

But WAL has a second life: it's the foundation for replication. A standby server can stream WAL records from the primary and apply them, staying in sync. That's **physical replication** — the standby applies the same low-level page modifications, producing a byte-for-byte copy of the primary's data files.

**Logical replication** is different. Instead of raw page modifications, PostgreSQL's logical decoding layer interprets the WAL and produces a stream of high-level change events: "row X was inserted into table Y with these column values." The `pgoutput` plugin (built into PostgreSQL since version 10) is the standard logical decoding output plugin that speaks this format.

The key requirement is `wal_level = logical` in `postgresql.conf`. This tells PostgreSQL to include additional information in WAL records (like the old row values for updates and the full tuple data) that's needed for logical decoding. Without this setting, logical replication is impossible — WAL records only contain page-level modifications.

```
wal_level = logical    # required for pgcdc's WAL detector
max_wal_senders = 10   # max number of replication connections
max_replication_slots = 10  # max number of replication slots
```

#### Replication Slots

A replication slot is a server-side bookmark. When you create a replication slot, PostgreSQL guarantees that it will not recycle (delete) any WAL segments that the slot's consumer hasn't yet processed. This ensures you never miss a change, even if your consumer goes offline for hours.

The danger is the flip side: if your consumer stops advancing the slot, PostgreSQL keeps accumulating WAL segments forever. On a busy database, this can fill the disk in hours. This is why pgcdc has heartbeats (`--heartbeat-interval`), slot lag monitoring (`pgcdc_slot_lag_bytes` metric, `--slot-lag-warn`), and a full backpressure system (Section 3.3) — all designed to prevent the "consumer fell behind, disk filled up, database crashed" scenario.

#### The Replication Protocol

To stream WAL changes, pgcdc doesn't use regular SQL queries. It uses PostgreSQL's streaming replication protocol, which requires a special connection parameter:

```go
func ensureReplicationParam(connStr string) string {
    u, err := url.Parse(connStr)
    // ... add replication=database if not present
}
```

The `replication=database` parameter tells PostgreSQL this isn't a normal connection — it's a replication connection that speaks a different protocol. The sequence is:

1. **Connect** with `replication=database`
2. **Create a replication slot** (temporary or persistent)
3. **Start replication** from the slot's consistent point
4. **Enter the message loop**: receive WAL messages, parse them, emit events

Here's the slot creation and replication start:

```go
result, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput",
    pglogrepl.CreateReplicationSlotOptions{Temporary: true})
// ...
err = pglogrepl.StartReplication(ctx, conn, slotName, consistentPoint,
    pglogrepl.StartReplicationOptions{
        PluginArgs: []string{
            "proto_version '1'",
            fmt.Sprintf("publication_names '%s'", d.publication),
        },
    })
```

The `pgoutput` plugin requires a publication — a PostgreSQL object that defines which tables to replicate. Publications control which tables produce change events:

```sql
-- Specific tables
CREATE PUBLICATION my_pub FOR TABLE orders, payments;

-- All tables in a schema
CREATE PUBLICATION my_pub FOR ALL TABLES;
```

pgcdc's `--all-tables` flag auto-creates a `FOR ALL TABLES` publication (named `all` by default, override with `--publication`). This is the zero-config path: `pgcdc listen --db postgres://... --detector wal --all-tables` needs no prior SQL setup — pgcdc creates the publication, the replication slot, and starts streaming. The publication creation is idempotent; on restart, pgcdc reuses the existing publication.

For production deployments with many tables, you'll want to create a publication explicitly for just the tables you care about. This reduces the volume of WAL data the detector processes and limits the replication slot's WAL retention to only the tables that matter.

#### The Message Loop

The heart of the WAL detector is a loop that receives messages from PostgreSQL and dispatches them by type. Here's the critical section:

```go
for {
    // Periodically send standby status updates
    if time.Now().After(nextStatusDeadline) {
        reportLSN := clientXLogPos
        if d.cooperativeLSNFn != nil {
            coopLSN := pglogrepl.LSN(d.cooperativeLSNFn())
            if coopLSN > 0 && coopLSN <= clientXLogPos {
                reportLSN = coopLSN
            }
        }
        err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
            pglogrepl.StandbyStatusUpdate{WALWritePosition: reportLSN})
        // ... checkpoint
    }

    // Backpressure: pause/throttle if WAL lag is too high
    if d.backpressureCtrl != nil && d.backpressureCtrl.IsPaused() {
        d.backpressureCtrl.WaitResume(ctx)
    }

    receiveCtx, cancel := context.WithDeadline(ctx, nextStatusDeadline)
    rawMsg, err := conn.ReceiveMessage(receiveCtx)
    cancel()
    // ...

    switch msg.Data[0] {
    case pglogrepl.PrimaryKeepaliveMessageByteID:
        // PostgreSQL heartbeat — may request immediate status reply
    case pglogrepl.XLogDataByteID:
        xld, _ := pglogrepl.ParseXLogData(msg.Data[1:])
        logicalMsg, _ := pglogrepl.Parse(xld.WALData)

        switch m := logicalMsg.(type) {
        case *pglogrepl.RelationMessage:
            // Table metadata (column names, types) — cached
        case *pglogrepl.BeginMessage:
            // Transaction start
        case *pglogrepl.InsertMessage:
            d.emitEvent(ctx, events, relations, m.RelationID,
                "INSERT", m.Tuple, nil, currentTx, clientXLogPos, tc)
        case *pglogrepl.UpdateMessage:
            d.emitEvent(ctx, events, relations, m.RelationID,
                "UPDATE", m.NewTuple, m.OldTuple, currentTx, clientXLogPos, tc)
        case *pglogrepl.DeleteMessage:
            d.emitEvent(ctx, events, relations, m.RelationID,
                "DELETE", nil, m.OldTuple, currentTx, clientXLogPos, tc)
        case *pglogrepl.TruncateMessage:
            // Emits TRUNCATE for each affected relation
        case *pglogrepl.CommitMessage:
            // Transaction end
        }
    }
}
```

The message types arrive in a specific order within each transaction:

```
RelationMessage(table="orders", columns=[id, customer_id, total])
BeginMessage(xid=48291, commitTime=..., finalLSN=...)
  InsertMessage(relationID=16384, tuple=[42, 7, 99.50])
  InsertMessage(relationID=16384, tuple=[43, 3, 150.00])
CommitMessage(commitLSN=..., transactionEndLSN=...)
```

The `RelationMessage` carries table metadata (column names, types, key flags) and is sent once per table per session (or when the table schema changes). The detector caches it in a `map[uint32]*pglogrepl.RelationMessage` so subsequent change messages can look up their columns by `RelationID`. This is how the detector knows that column 0 is `id`, column 1 is `customer_id`, etc. — the WAL protocol sends column values by position, not by name.

The `BeginMessage` marks the start of a transaction and carries the transaction ID (`xid`), commit timestamp, and final LSN. When `--tx-metadata` is enabled, these are stored on each event's `Transaction` field. When `--tx-markers` is enabled, synthetic `BEGIN` and `COMMIT` events are emitted on the `pgcdc:_txn` channel, allowing downstream consumers to reconstruct transaction boundaries.

**LSN** (Log Sequence Number) is the 64-bit position in the WAL where each message was written. It increases monotonically — each new WAL record has a higher LSN than the previous one. You can think of it as a byte offset into the WAL stream: LSN `0/1A2B3C4D` means byte position `0x1A2B3C4D` in the WAL.

The detector tracks `clientXLogPos` — the highest LSN it has received — and periodically reports this back to PostgreSQL via `SendStandbyStatusUpdate`. This is how PostgreSQL knows it's safe to recycle old WAL segments. The reporting interval is tied to `StandbyMessageTimeout` (default 10s) — if the detector doesn't report its position within this window, PostgreSQL may terminate the replication connection.

#### tupleToMap — Decoding WAL Column Values

When a row is inserted, updated, or deleted, the WAL protocol sends the column values as a `TupleData` structure. Each column has a DataType byte:

```go
func tupleToMap(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (map[string]any, []string) {
    m := make(map[string]any, len(tuple.Columns))
    var unchanged []string
    for i, col := range tuple.Columns {
        colName := rel.Columns[i].Name
        switch col.DataType {
        case 'n': // null
            m[colName] = nil
        case 'u': // unchanged TOAST
            m[colName] = nil
            unchanged = append(unchanged, colName)
        case 't': // text
            m[colName] = string(col.Data)
        case 'b': // binary
            m[colName] = string(col.Data)
        }
    }
    return m, unchanged
}
```

The `'u'` DataType is crucial — it means "this column's value hasn't changed, and it's stored in TOAST." TOAST (The Oversized-Attribute Storage Technique) is PostgreSQL's mechanism for handling large column values. When a row is updated but a large text column isn't modified, PostgreSQL doesn't include the value in the WAL — it sends DataType `'u'` instead. We'll cover the TOAST cache solution in Chapter 12.

#### emitEvent — From WAL Tuple to Event

The `emitEvent` function is where WAL protocol messages become pgcdc Events. It's the bridge between the replication protocol and the rest of the system:

```go
func (d *Detector) emitEvent(
    ctx context.Context,
    events chan<- event.Event,
    relations map[uint32]*pglogrepl.RelationMessage,
    relationID uint32,
    operation string,
    newTuple, oldTuple *pglogrepl.TupleData,
    tx *currentTx,
    currentLSN pglogrepl.LSN,
    tc *toastcache.Cache,
) {
    rel, ok := relations[relationID]
    if !ok {
        d.logger.Warn("unknown relation", "relation_id", relationID)
        return
    }

    table := rel.RelationName

    var row map[string]any
    var unchangedCols []string
    if newTuple != nil {
        row, unchangedCols = tupleToMap(rel, newTuple)
    }

    var old map[string]any
    if oldTuple != nil {
        old, _ = tupleToMap(rel, oldTuple)
    }

    // For DELETE, "row" is the old row (matches LISTEN/NOTIFY trigger format).
    if operation == "DELETE" && row == nil && old != nil {
        row = old
        old = nil
        unchangedCols = nil
    }

    // TOAST cache: backfill unchanged columns from cache, then update cache.
    if tc != nil && row != nil {
        pk := toastcache.BuildPK(rel, row)

        switch operation {
        case "INSERT":
            if pk != "" {
                tc.Put(relationID, pk, copyMap(row))
            }
            unchangedCols = nil // INSERT never has unchanged TOAST.
        case "UPDATE":
            if len(unchangedCols) > 0 && pk != "" {
                if cached, ok := tc.Get(relationID, pk); ok {
                    for _, col := range unchangedCols {
                        row[col] = cached[col]
                    }
                    unchangedCols = nil
                    metrics.ToastCacheHits.Inc()
                } else {
                    metrics.ToastCacheMisses.Inc()
                }
            }
            if pk != "" {
                tc.Put(relationID, pk, copyMap(row))
            }
        case "DELETE":
            if pk != "" {
                tc.Delete(relationID, pk)
            }
        }
    } // end TOAST cache

    // Build payload JSON.
    payload := map[string]any{
        "op":    operation,
        "table": table,
        "row":   row,
        "old":   old,
    }
    if len(unchangedCols) > 0 {
        payload["_unchanged_toast_columns"] = unchangedCols
    }
    // ... marshal, create event, set tx metadata, emit to channel
}
```

The flow is: look up the relation (table metadata) by ID → decode the tuple into a `map[string]any` → swap row/old for DELETE (matching the trigger-based format where `row` always has the data) → resolve TOAST columns from the cache → build the payload JSON → create an Event → send it to the events channel. Each step is straightforward, but together they handle the full complexity of PostgreSQL's change data capture output.

Notice the TOAST cache integration: INSERT populates the cache (and clears `unchangedCols` — INSERTs always have full data), UPDATE reads from it to backfill unchanged columns (then updates the cache with the merged result), and DELETE evicts. The `toastcache.BuildPK` extracts the primary key from the relation's column flags and the row data. This is a write-through cache — every change updates it, so it always reflects the latest known state of each row.

#### Standby Status and Cooperative LSN Reporting

The detector must periodically tell PostgreSQL how far it has processed the WAL. This "standby status update" is what advances the replication slot and allows PostgreSQL to recycle WAL segments.

The subtle part is cooperative checkpointing. When `--cooperative-checkpoint` is enabled, the detector doesn't report its own position — it reports the minimum position across all adapters:

```go
reportLSN := clientXLogPos
if d.cooperativeLSNFn != nil {
    coopLSN := pglogrepl.LSN(d.cooperativeLSNFn())
    if coopLSN > 0 && coopLSN <= clientXLogPos {
        reportLSN = coopLSN
    }
}
err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
    pglogrepl.StandbyStatusUpdate{WALWritePosition: reportLSN})
```

The `cooperativeLSNFn` is injected by the Pipeline (it's `ackTracker.MinAckedLSN`). The guard `coopLSN <= clientXLogPos` prevents reporting a position the detector hasn't reached yet — a defensive check against clock skew or race conditions.

If the cooperative LSN is zero (no adapters have acknowledged anything yet), `reportLSN` stays at the detector's position. The detector also saves this position to the checkpoint store (if configured) so it can resume from the right point after a restart.

#### Heartbeat and Slot Lag

Two maintenance concerns on idle databases:

**Heartbeat** (`--heartbeat-interval 30s`): When a database has no writes, the WAL position doesn't advance, and the replication slot stays at the same position. Some PostgreSQL configurations and monitoring tools flag stale slots. The heartbeat goroutine periodically writes a timestamp to the `pgcdc_heartbeat` table, generating a WAL write that advances the slot.

**Slot lag monitoring**: The detector periodically queries `pg_replication_slots` to get the slot's `confirmed_flush_lsn` and compares it to the database's current WAL position. The difference (in bytes) is published as the `pgcdc_slot_lag_bytes` Prometheus gauge. When lag exceeds `--slot-lag-warn` (default 100MB), a warning is logged. This gives operators early warning before the disk-full scenario develops.

#### Temporary vs Persistent Slots

By default, the detector creates a temporary replication slot that exists only for the lifetime of the connection. When the connection drops, the slot vanishes, and the next connection starts fresh. This is the simplest mode but offers no crash recovery — on restart, you get events from the current WAL position forward, potentially missing events that occurred during the downtime.

With `--persistent-slot`, the detector creates a named slot that survives disconnections. On reconnect, it loads the last checkpointed LSN from the `pgcdc_checkpoints` table and resumes from there. This enables exactly-once semantics (no duplicate events) at the cost of the WAL-retention risk described above.

The tradeoff matrix:

| Mode | Crash recovery | WAL retention risk | Use case |
|---|---|---|---|
| Temporary slot | No | None (slot vanishes) | Dev, ephemeral streaming |
| Persistent slot, no checkpoint | No | High (slot remembers, but no LSN saved) | Rarely useful |
| Persistent slot + checkpoint | Yes | Moderate (manageable with backpressure) | Production CDC |
| Persistent slot + checkpoint + backpressure | Yes | Low (auto-throttle prevents bloat) | Production CDC on busy databases |

### 2.3 The Outbox Pattern

**File: `detector/outbox/outbox.go`**

The outbox pattern is the application-layer alternative to WAL replication. Instead of capturing changes from the database's internal log, you write events explicitly to an outbox table as part of your business transaction:

```sql
BEGIN;
INSERT INTO orders (id, total) VALUES (42, 99.50);
INSERT INTO pgcdc_outbox (channel, operation, payload)
  VALUES ('pgcdc:orders', 'INSERT', '{"id": 42, "total": 99.50}');
COMMIT;
```

Because both the business write and the outbox write happen in the same transaction, they're atomically consistent. Either both succeed or neither does. This is the outbox pattern's key guarantee: you can't have a business write without the corresponding event, and you can't have a phantom event without the business write.

The outbox detector polls this table using `SELECT ... FOR UPDATE SKIP LOCKED`:

```go
func (d *Detector) poll(ctx context.Context, conn *pgx.Conn, safeTable string, events chan<- event.Event) (int, error) {
    tx, err := conn.Begin(ctx)
    if err != nil {
        return 0, fmt.Errorf("begin tx: %w", err)
    }
    defer func() { _ = tx.Rollback(ctx) }()

    query := fmt.Sprintf(
        "SELECT id, channel, operation, payload FROM %s ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED",
        safeTable,
    )
    rows, err := tx.Query(ctx, query, d.batchSize)
    // ... scan rows, emit events ...

    // Clean up processed rows.
    if d.keepProcessed {
        _, err = tx.Exec(ctx,
            fmt.Sprintf("UPDATE %s SET processed_at = now() WHERE id = ANY($1)", safeTable), ids)
    } else {
        _, err = tx.Exec(ctx,
            fmt.Sprintf("DELETE FROM %s WHERE id = ANY($1)", safeTable), ids)
    }
    return len(processed), tx.Commit(ctx)
}
```

The `FOR UPDATE SKIP LOCKED` clause is what makes this concurrency-safe — multiple pgcdc instances can poll the same outbox table without processing the same row twice. Rows locked by one instance are silently skipped by others. After processing, rows are either deleted (default) or marked with a `processed_at` timestamp (`--outbox-keep-processed`).

The detector polls at a configurable interval (`--outbox-poll-interval`, default 500ms) in batches (`--outbox-batch-size`, default 100). All rows in a batch are selected, emitted as events, and cleaned up within a single transaction — ensuring atomicity of the poll-process-cleanup cycle.

Why use outbox over WAL? When you need the event payload to be under application control (not a raw row representation). When you're on a managed PostgreSQL that restricts logical replication (e.g., some cloud providers don't allow `wal_level = logical`). When you want CDC without configuring publications and slots. When you need guaranteed transactional consistency between the business write and the event. The tradeoff is polling latency — the outbox detector checks at a configurable interval rather than receiving changes in real-time.

### 2.4 MySQL Binlog

**File: `detector/mysql/mysql.go`**

MySQL's equivalent of WAL is the binary log (binlog). When configured with `binlog_format = ROW`, every row change is recorded as a `RowsEvent` containing the actual row data (before and after). The MySQL detector connects as a replication slave using `go-mysql-org/go-mysql`'s `BinlogSyncer`:

```go
cfg := replication.BinlogSyncerConfig{
    ServerID: d.serverID,
    Flavor:   d.flavor,  // "mysql" or "mariadb"
    Host:     host,
    Port:     port,
    User:     d.user,
    Password: d.password,
}
syncer := replication.NewBinlogSyncer(cfg)
```

The `serverID` must be unique across all replication slaves connected to the same MySQL master. This is a MySQL requirement — the master uses it to identify replication clients.

The detector processes three types of binlog events:

- **RowsEvent** with `WRITE_ROWS_EVENTv2`: An INSERT. The event contains one or more rows with their column values.
- **RowsEvent** with `UPDATE_ROWS_EVENTv2`: An UPDATE. The event contains pairs of (before, after) rows.
- **RowsEvent** with `DELETE_ROWS_EVENTv2`: A DELETE. The event contains the deleted rows.

Events are emitted on channels following the pattern `pgcdc:<schema>.<table>`.

**Position encoding**: MySQL identifies binlog positions with a filename and an offset within that file (e.g., `mysql-bin.000003` at position 456). pgcdc's checkpoint system uses a single `uint64`, so the detector encodes both values into one number:

```go
// encodePosition: uint64 = (file_sequence_number << 32) | uint64(offset)
// Example: mysql-bin.000003 offset 4562 -> (3 << 32) | 4562
func encodePosition(pos mysqldriver.Position) (uint64, error) {
    seq, err := parseFileSequence(pos.Name)
    if err != nil {
        return 0, err
    }
    return (uint64(seq) << 32) | uint64(pos.Pos), nil
}

func decodePosition(encoded uint64, prefix string) mysqldriver.Position {
    seq := uint32(encoded >> 32)
    offset := uint32(encoded & 0xFFFFFFFF)
    return mysqldriver.Position{
        Name: fmt.Sprintf("%s.%06d", prefix, seq),
        Pos:  offset,
    }
}
```

The file sequence number occupies the upper 32 bits, the offset the lower 32 bits. This preserves ordering (within a single binlog file, offsets increase monotonically; when the file rotates, the sequence number increases) and fits the existing checkpoint store interface without any changes.

**Column name resolution**: MySQL's `RowsEvent` contains column values but not column names — those come from a preceding `TableMapEvent`. In MySQL 8.0.1+, the `TableMapEvent` includes column names in its metadata. For older versions, the detector falls back to querying `information_schema.columns` at startup and caching the results. If even that fails (e.g., missing permissions), it uses synthetic names like `col_0`, `col_1`.

The detector supports GTID (Global Transaction ID) mode via `--mysql-gtid`, which provides a more robust resume mechanism than file-based positions. In GTID mode, the position is encoded as the GTID set string, and the syncer starts from the last committed GTID rather than a file position.

### 2.5 MongoDB Change Streams

**File: `detector/mongodb/mongodb.go`**

MongoDB's change stream API is the simplest detector to implement because MongoDB exposes changes at the same abstraction level pgcdc needs — document-level operations with full document payloads.

The detector calls `collection.Watch()` (or `database.Watch()` or `client.Watch()` depending on `--mongodb-scope`) and iterates the change stream cursor:

```go
// Scope determines what we watch.
switch d.scope {
case "collection":
    cs, err = coll.Watch(ctx, pipeline, watchOpts)
case "database":
    cs, err = db.Watch(ctx, pipeline, watchOpts)
case "cluster":
    cs, err = client.Watch(ctx, pipeline, watchOpts)
}

// Iterate the cursor.
for cs.Next(ctx) {
    var change changeStreamDoc
    if err := cs.Decode(&change); err != nil { ... }

    ev, err := d.changeToEvent(change)
    events <- ev

    // Save resume token after each event.
    d.saveResumeToken(ctx, tokenColl, cs.ResumeToken())
}
```

**Resume tokens**: MongoDB change streams produce an opaque BSON resume token with each change event. The detector persists these to a MongoDB metadata collection (`pgcdc_resume_tokens`) so it can resume after a restart. On startup, the detector loads the last saved resume token and passes it as `StartAfter` in the watch options. This makes the detector crash-resumable.

Events are emitted on channels following the pattern `pgcdc:<database>.<collection>`. System events (drop, rename, invalidate) go to `pgcdc:_mongo`.

The `--mongodb-full-document updateLookup` option tells MongoDB to include the full document (not just the changed fields) in update events. Without it, updates contain only a `updateDescription` with the changed fields — useful for some use cases but not for adapters that need the complete document.

**Key limitation**: MongoDB change streams have no LSN equivalent. There's no monotonic position number that pgcdc can use for cooperative checkpointing or backpressure. The detector works with pgcdc's pipeline, but the advanced WAL-aware features (cooperative checkpoint, source-aware backpressure) are not available. Resume tokens are binary and non-comparable — you can resume from a token, but you can't compute "how far behind" a consumer is in bytes.

The operational model for MongoDB is simpler but less powerful: events flow through, adapters consume them, but there's no way to coordinate multiple adapters' progress or throttle based on lag. If you need these features with MongoDB, the outbox pattern (writing to a PostgreSQL outbox table from your application) provides a WAL-based alternative.

### 2.6 Feature Availability by Detector

Not every detector supports every feature. The WAL detector is the only one that produces an LSN — a monotonically increasing position in PostgreSQL's write-ahead log — and most advanced features depend on it. This table shows what works where:

| Feature                    | WAL  | LISTEN/NOTIFY | Outbox | MySQL | MongoDB |
|----------------------------|------|---------------|--------|-------|---------|
| Persistent slot            | Yes  | —             | —      | —     | —       |
| Cooperative checkpoint     | Yes  | —             | —      | —     | —       |
| Source-aware backpressure  | Yes  | —             | —      | —     | —       |
| Snapshot-first             | Yes  | —             | —      | —     | —       |
| Incremental snapshots      | Yes  | —             | —      | —     | —       |
| Heartbeat / slot lag       | Yes  | —             | —      | —     | —       |
| TOAST cache                | Yes  | —             | —      | —     | —       |
| Transaction metadata       | Yes  | —             | —      | —     | —       |
| Schema events              | Yes  | —             | —      | —     | —       |
| TRUNCATE events            | Yes  | —             | —      | —     | —       |
| Crash-resume               | Yes¹ | —             | —      | Yes²  | Yes³    |
| Event routing              | Yes  | Yes           | Yes    | Yes   | Yes     |
| Transforms                 | Yes  | Yes           | Yes    | Yes   | Yes     |
| All adapters               | Yes  | Yes           | Yes    | Yes   | Yes     |

¹ Via persistent replication slot + LSN checkpoint (`pgcdc_checkpoints` table).
² Via MySQL binlog position encoded as `(file_seq << 32) | offset` stored in `pgcdc_checkpoints`.
³ Via MongoDB resume tokens persisted to `pgcdc_resume_tokens` collection — binary, non-comparable.

If you need the full feature stack (cooperative checkpointing, backpressure, snapshots, heartbeat), you need WAL. The other detectors are simpler — fewer flags, less infrastructure — but lack the WAL-aware features that enable production-grade exactly-once delivery and disk-safety guarantees.

### 2.7 Multi-Detector Composition

**File: `detector/multi/multi.go`**

Sometimes one detector isn't enough. You want a snapshot followed by live streaming. Or you want a primary and standby database, failing over automatically. Or you want two detectors running in parallel, merging their event streams. The multi-detector composes multiple detectors into a single `detector.Detector`, dispatching to one of three modes.

```go
type Detector struct {
    detectors   []ChildDetector
    mode        Mode
    logger      *slog.Logger
    DedupWindow time.Duration // when > 0 in parallel mode, dedup events by ID within this window
}

type ChildDetector struct {
    Name     string
    Detector detector.Detector
}

type Mode string

const (
    Sequential Mode = "sequential"
    Parallel   Mode = "parallel"
    Failover   Mode = "failover"
)
```

The `Start` method dispatches based on mode:

```go
func (d *Detector) Start(ctx context.Context, events chan<- event.Event) error {
    if len(d.detectors) == 0 {
        return fmt.Errorf("multi-detector: no child detectors configured")
    }
    switch d.mode {
    case Parallel:
        return d.startParallel(ctx, events)
    case Failover:
        return d.startFailover(ctx, events)
    default:
        return d.startSequential(ctx, events)
    }
}
```

**Sequential mode** (default) runs detectors one after another. The first detector runs until its `Start` method returns, then the second starts. If a detector returns an error (and the context isn't cancelled), the error propagates immediately — no fallthrough. This is the natural fit for "snapshot first, then live streaming": the snapshot detector runs, exports all rows, returns nil, and the WAL detector takes over from the slot's consistent point.

```go
func (d *Detector) startSequential(ctx context.Context, events chan<- event.Event) error {
    for i, child := range d.detectors {
        if err := child.Detector.Start(ctx, events); err != nil {
            if ctx.Err() != nil {
                return ctx.Err()
            }
            return fmt.Errorf("detector %s: %w", child.Detector.Name(), err)
        }
    }
    return nil
}
```

**Parallel mode** runs all detectors concurrently via goroutines. All events merge into the same channel — the ordering between detectors is interleaved, but ordering within each detector is preserved. If any detector fails, the entire group is cancelled.

The interesting part is deduplication. When `DedupWindow` is set (e.g., `5s`), a `dedupTracker` goroutine sits between the detectors and the real events channel. It maintains a time-expiring map of event IDs:

```go
type dedupTracker struct {
    mu     sync.Mutex
    seen   map[string]time.Time
    window time.Duration
    stopCh chan struct{}
}

func (dt *dedupTracker) isDuplicate(id string) bool {
    dt.mu.Lock()
    defer dt.mu.Unlock()
    if _, exists := dt.seen[id]; exists {
        return true
    }
    dt.seen[id] = time.Now()
    return false
}
```

A reap goroutine periodically cleans expired entries (every `window/2`), so the map doesn't grow unbounded. The proxy channel pattern is clean: detectors write to a proxy channel, the dedup forwarder reads from the proxy, checks `isDuplicate`, and forwards only unseen events to the real events channel. Duplicate events are logged at debug level and silently dropped.

Why would parallel detectors produce duplicates? Consider two WAL detectors pointed at a primary and a read replica with streaming replication — the same change appears in both WAL streams. The dedup window handles this: as long as the replica lag is shorter than the window duration, duplicates are caught.

**Failover mode** tries detectors in order. Each runs until it errors. On error, the current detector's context is cancelled and the next one starts:

```go
func (d *Detector) startFailover(ctx context.Context, events chan<- event.Event) error {
    var lastErr error
    for i, child := range d.detectors {
        if ctx.Err() != nil {
            return ctx.Err()
        }
        childCtx, childCancel := context.WithCancel(ctx)
        err := child.Detector.Start(childCtx, events)
        childCancel()

        if err == nil {
            return nil
        }
        if ctx.Err() != nil {
            return ctx.Err()
        }
        lastErr = fmt.Errorf("detector %s: %w", child.Detector.Name(), err)
    }
    return fmt.Errorf("all failover detectors exhausted: %w", lastErr)
}
```

If a detector returns nil (meaning it completed successfully without error), failover returns immediately — there's nothing to fail over from. If all detectors fail, the error message wraps the last error: `"all failover detectors exhausted: detector wal: connect: ..."`. This is useful for primary/standby database configurations where you want automatic failover without external tooling.

Each child detector gets its own cancellable context (`childCtx`). When a detector fails, `childCancel()` ensures it cleans up before the next detector starts. If the parent context is cancelled (e.g., SIGTERM), the failover exits immediately without trying the remaining detectors.

CLI: `--detector-mode sequential|parallel|failover` (default `sequential`). The mode is wired in `cmd/listen.go` when multiple detector configurations are present.

---

## Chapter 3: The Bus — Fan-Out and Backpressure

The bus is the junction between one producer (the detector) and N consumers (the adapters). Every event that enters the ingest channel is delivered to every subscriber channel. The bus is the only component that touches every event.

### 3.1 Fan-Out

**File: `bus/bus.go`**

The Bus struct is straightforward:

```go
type Bus struct {
    ingest      chan event.Event
    subscribers []subscriber
    bufferSize  int
    mode        BusMode
    closed      bool
    mu          sync.RWMutex
    logger      *slog.Logger
}
```

One ingest channel, a slice of subscribers, a mutex to protect the subscriber list. The `Start` method is a simple loop that reads from ingest and writes to every subscriber:

```go
func (b *Bus) Start(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            b.closeSubscribers()
            return ctx.Err()
        case ev := <-b.ingest:
            metrics.EventsReceived.Inc()
            b.mu.RLock()
            // ... fan out to subscribers
            b.mu.RUnlock()
        }
    }
}
```

The fan-out is synchronous — the bus processes one event at a time, delivering it to all subscribers before reading the next event. This is intentional. It means event ordering is preserved across all adapters: if event A enters the bus before event B, every adapter receives A before B. Parallel fan-out would break this guarantee.

Subscribers can have a `FilterFunc` that determines whether an event should be delivered. This is used for bus-level route filtering — if an adapter only cares about `pgcdc:orders` events, the filter skips everything else, saving the channel write and context switch.

Why is fan-out synchronous rather than using one goroutine per subscriber? Two reasons. First, ordering: if the bus sent to subscribers concurrently, events could arrive at different subscribers in different orders depending on scheduling. Second, simplicity: the single-goroutine approach is easier to reason about and has lower overhead (no goroutine creation per event). The bus is not a bottleneck — the limiting factor is always the detector's ingestion rate or the slowest adapter's delivery rate, not the bus's fan-out speed. A single fan-out goroutine can distribute millions of events per second to dozens of subscribers.

### 3.2 Fast vs Reliable Mode

The bus has two modes that represent fundamentally different reliability guarantees. They're implemented as two different `select` patterns inside the same loop.

**Fast mode** (default): Non-blocking sends. If a subscriber channel is full, the event is dropped with a warning log and a `pgcdc_events_dropped_total` metric increment:

```go
// Fast mode
for _, sub := range b.subscribers {
    if sub.filter != nil && !sub.filter(ev) {
        continue
    }
    select {
    case sub.ch <- ev:
    default:
        metrics.EventsDropped.WithLabelValues(sub.name).Inc()
        b.logger.Warn("subscriber channel full, dropping event",
            slog.String("event_id", ev.ID),
            slog.String("adapter", sub.name),
        )
    }
}
```

**Reliable mode** (`--bus-mode reliable`): Blocking sends. If a subscriber channel is full, the bus blocks until space is available (or the context is cancelled). No events are ever dropped, but a slow consumer blocks the entire pipeline:

```go
// Reliable mode
for _, sub := range b.subscribers {
    if sub.filter != nil && !sub.filter(ev) {
        continue
    }
    select {
    case sub.ch <- ev:
    default:
        metrics.BusBackpressure.WithLabelValues(sub.name).Inc()
        select {
        case sub.ch <- ev:
        case <-ctx.Done():
            b.mu.RUnlock()
            b.closeSubscribers()
            return ctx.Err()
        }
    }
}
```

The analogy is TCP vs UDP. Fast mode is like UDP — fire-and-forget, maximum throughput, accept some loss. Reliable mode is like TCP — guaranteed delivery, accept the latency cost of flow control. The two-stage `select` in reliable mode is an optimization: first try a non-blocking send (the common case when the channel has space), and only if that fails, do we count the backpressure metric and enter a blocking send.

Most production deployments use fast mode with large channel buffers (`--bus-buffer 8192` or higher). If you need zero loss, reliable mode is the answer — but you should also configure backpressure (next section) to prevent a slow adapter from causing WAL growth.

### 3.3 Source-Aware Backpressure

**File: `backpressure/backpressure.go`**

Here's the nightmare scenario: you're using WAL replication with a persistent slot. Your webhook endpoint goes down. The webhook adapter queues up, the bus channel fills, and in reliable mode, the bus stops accepting events from the detector. The detector stops reading from the WAL connection. PostgreSQL keeps accumulating WAL segments because the slot hasn't advanced. After a few hours, the disk fills up and your database crashes.

Source-aware backpressure prevents this. The controller monitors WAL lag — the difference between PostgreSQL's current WAL position and the slot's confirmed position — and takes progressively aggressive action as lag increases.

**Three zones:**

```
  WAL lag (bytes)
  |
  |  0 ─────── warn ─────── critical ─────── ∞
  |  |  GREEN   |   YELLOW    |    RED      |
  |  | full     |  throttle + |  pause +    |
  |  | speed    |  shed       |  shed more  |
```

The zone computation includes hysteresis — the system doesn't oscillate between zones when lag hovers near a threshold:

```go
func (c *Controller) computeZone(lag int64, current Zone) Zone {
    if lag >= c.criticalThreshold {
        return ZoneRed
    }
    // Hysteresis: red exits only when lag drops below warn.
    if current == ZoneRed {
        if lag < c.warnThreshold {
            return ZoneGreen
        }
        return ZoneRed
    }
    if lag >= c.warnThreshold {
        return ZoneYellow
    }
    return ZoneGreen
}
```

In the yellow zone, the controller throttles the detector with a proportional sleep — the further into the yellow band, the longer the sleep, up to `--bp-max-throttle` (default 500ms). It also sheds "best-effort" adapters: events are auto-acked and skipped for those adapters, reducing the checkpoint lag.

```go
func (c *Controller) computeThrottle(lag int64) time.Duration {
    band := c.criticalThreshold - c.warnThreshold
    if band <= 0 {
        return c.maxThrottle
    }
    position := lag - c.warnThreshold
    ratio := float64(position) / float64(band)
    if ratio > 1.0 {
        ratio = 1.0
    }
    return time.Duration(float64(c.maxThrottle) * ratio)
}
```

In the red zone, the controller pauses the detector entirely. The WAL detector blocks on `WaitResume`:

```go
func (c *Controller) WaitResume(ctx context.Context) error {
    for {
        c.mu.Lock()
        ch := c.pauseCh
        paused := c.paused.Load()
        c.mu.Unlock()
        if !paused {
            return nil
        }
        select {
        case <-ch:
            // Re-check — a new red cycle may have started
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}
```

The `pauseCh` is a channel that's closed when the controller exits the red zone. The detector selects on this channel and wakes up when lag decreases. The loop re-checks `paused` after waking because a rapid red→green→red transition could close a stale channel.

Adapters are assigned priorities via `--adapter-priority name=critical|normal|best-effort`:

| Priority | Yellow zone | Red zone |
|---|---|---|
| `critical` | Normal delivery | Normal delivery |
| `normal` | Normal delivery | Shed (auto-ack, skip delivery) |
| `best-effort` | Shed | Shed |

Shedding means the wrapper goroutine auto-acks events for that adapter without delivering them — the cooperative checkpoint can still advance. The event is counted in `pgcdc_backpressure_load_shed_total{adapter}` but not delivered. When the zone transitions back to green, delivery resumes.

The typical priority assignment: your critical business adapter (Kafka) is `critical`, your analytics adapters (S3, search) are `normal`, and your monitoring adapters (stdout, webhook for alerts) are `best-effort`. Under extreme load, the system sacrifices analytics delivery to keep the business pipeline flowing and the WAL from growing.

The backpressure controller polls WAL lag at `--bp-poll-interval` (default 10s) by querying:

```sql
SELECT pg_current_wal_lsn() - confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = $1
```

This single query returns the number of WAL bytes the slot is holding. The controller compares this against `--bp-warn-threshold` (default 500MB) and `--bp-critical-threshold` (default 2GB) to determine the zone.

---

## Chapter 4: Transforms — Reshaping Events in Flight

Between the bus and each adapter sits a transform pipeline. Transforms mutate, filter, or reshape events before they reach the adapter. They run per-adapter: each adapter can have its own transform chain, plus there are global transforms that run for all adapters.

### 4.1 TransformFunc and Chain

**File: `transform/transform.go`**

```go
type TransformFunc func(event.Event) (event.Event, error)

var ErrDropEvent = errors.New("drop event")

func Chain(fns ...TransformFunc) TransformFunc {
    if len(fns) == 0 {
        return nil
    }
    if len(fns) == 1 {
        return fns[0]
    }
    return func(ev event.Event) (event.Event, error) {
        var err error
        for _, fn := range fns {
            ev, err = fn(ev)
            if err != nil {
                return ev, err
            }
        }
        return ev, nil
    }
}
```

This is classic functional composition. Each `TransformFunc` takes an event and returns a (possibly modified) event. `Chain` composes them left-to-right, short-circuiting on any error.

The `ErrDropEvent` sentinel is the filter mechanism. A transform that wants to silently discard an event returns `ErrDropEvent`. The wrapper goroutine recognizes this specific error, increments `pgcdc_transform_dropped_total`, and moves on without logging an error. Any other error increments `pgcdc_transform_errors_total` and logs a warning.

### 4.2 Built-in Transforms

pgcdc ships seven built-in transforms:

**drop_columns** removes specified columns from the payload's `row` and `old` fields. Used via `--drop-columns password,ssn` or YAML config.

**rename_fields** renames payload keys. Useful when your database column names don't match your API contract.

**mask** has three modes: `zero` (replace with zero value), `hash` (SHA-256 hash), and `redact` (replace with `"***REDACTED***"`). Used for PII compliance — mask email addresses in the CDC stream without modifying the source database.

**filter** filters events by operation type or field value. `--filter-operations INSERT,UPDATE` drops DELETE and TRUNCATE events. Field-value filters let you route only specific events: "only orders where total > 100."

**debezium** rewrites the payload into a Debezium-compatible envelope. Debezium is the dominant open-source CDC tool, and many downstream systems (Kafka Connect sinks, Flink, Spark) expect the Debezium JSON format. The transform maps pgcdc's payload structure into Debezium's `before`/`after`/`op`/`source`/`transaction` envelope:

```go
func Debezium(opts ...DebeziumOption) TransformFunc {
    // ...
    return func(ev event.Event) (event.Event, error) {
        // Parse payload, map before/after based on operation
        envelope := map[string]any{
            "before": before,
            "after":  after,
            "op":     debeziumOp(ev.Operation), // INSERT→"c", UPDATE→"u", DELETE→"d"
            "source": source,
        }
        if ev.Transaction != nil {
            envelope["transaction"] = map[string]any{
                "id":                    fmt.Sprintf("%d:%d", ev.Transaction.Xid, ev.LSN),
                "total_order":           ev.Transaction.Seq,
                "data_collection_order": ev.Transaction.Seq,
            }
        }
        raw, _ := json.Marshal(envelope)
        ev.Payload = raw
        return ev, nil
    }
}
```

**cloudevents** rewrites the payload into CloudEvents v1.0 structured-mode JSON. CloudEvents is a CNCF specification for describing event data in a common format. Like Debezium, this is about interoperability — systems that consume CloudEvents can process pgcdc events without custom parsing.

**cel_filter** evaluates a CEL (Common Expression Language) expression against each event. Events where the expression evaluates to `false` are dropped via `ErrDropEvent`. Events that cause evaluation errors pass through unchanged — a deliberate choice: a broken expression shouldn't silently drop your data. Configured via `--filter-cel 'expression'` CLI flag or `type: cel_filter` with `expression:` in YAML config.

The CEL environment exposes seven variables derived from the event: `channel` (string), `operation` (string), `source` (string), `id` (string), `has_record` (bool), `table` (string, from record metadata), and `schema_name` (string, from record metadata). The environment is compiled once at startup and the resulting `Program` is reused for every event — no per-event compilation overhead:

```go
// File: cel/cel.go
func Compile(expr string) (*Program, error) {
    env, err := cel.NewEnv(
        cel.Variable("channel", cel.StringType),
        cel.Variable("operation", cel.StringType),
        cel.Variable("source", cel.StringType),
        cel.Variable("id", cel.StringType),
        cel.Variable("has_record", cel.BoolType),
        cel.Variable("table", cel.StringType),
        cel.Variable("schema_name", cel.StringType),
    )
    // ... compile, verify output type is bool, build Program
}

func (p *Program) Eval(ev event.Event) (bool, error) {
    vars := map[string]any{
        "channel":     ev.Channel,
        "operation":   ev.Operation,
        "source":      ev.Source,
        "id":          ev.ID,
        "has_record":  ev.HasRecord(),
        "table":       "",
        "schema_name": "",
    }
    // Extract table/schema from Record metadata if available
    out, _, err := p.program.Eval(vars)
    // ... return bool result
}
```

The transform wrapper (`transform/cel_filter.go`) is minimal — it calls `Compile` once, then `Eval` per event:

```go
func FilterCEL(expr string) (TransformFunc, error) {
    prg, err := pgcel.Compile(expr)
    if err != nil {
        return nil, fmt.Errorf("compile filter expression: %w", err)
    }
    return func(ev event.Event) (event.Event, error) {
        ok, err := prg.Eval(ev)
        if err != nil {
            return ev, nil // pass through on error
        }
        if !ok {
            return ev, ErrDropEvent
        }
        return ev, nil
    }, nil
}
```

Example usage:

```sh
# Only INSERT events on the orders channel
pgcdc listen --db postgres://... --detector wal --all-tables \
  --filter-cel 'operation == "INSERT" && channel == "pgcdc:orders"'

# Only events from the inventory schema
pgcdc listen --db postgres://... --detector wal --all-tables \
  --filter-cel 'schema_name == "inventory"'
```

CEL expressions support the full CEL language — boolean operators (`&&`, `||`, `!`), string operations (`contains`, `startsWith`, `endsWith`), comparisons, and ternary expressions. The `google/cel-go` library handles parsing, type-checking, and evaluation. The compile step validates the expression and verifies the return type is `bool` — a non-boolean expression fails at startup, not at runtime.

### 4.3 Hot Reload

**File: `pgcdc.go` — wrapSubscription**

Transforms and routes can be hot-reloaded via `SIGHUP` without restarting the pipeline or losing events. The mechanism uses `sync/atomic.Pointer[wrapperConfig]` — one per adapter:

```go
type wrapperConfig struct {
    transformFn transform.TransformFunc
    routeFilter bus.FilterFunc
}
```

The wrapper goroutine loads the current config atomically on every event:

```go
func (p *Pipeline) wrapSubscription(name string, inCh <-chan event.Event,
    cfgPtr *atomic.Pointer[wrapperConfig], autoAck bool) <-chan event.Event {
    out := make(chan event.Event, cap(inCh))
    go func() {
        defer close(out)
        for ev := range inCh {
            wcfg := cfgPtr.Load() // atomic load — no lock

            origLSN := ev.LSN

            // Route filter
            if wcfg.routeFilter != nil && !wcfg.routeFilter(ev) {
                // auto-ack filtered events
                continue
            }

            // Transform chain
            if wcfg.transformFn != nil {
                result, err := wcfg.transformFn(ev)
                if err != nil {
                    // ErrDropEvent or transform error — auto-ack
                    continue
                }
                ev = result
            }

            out <- ev
        }
    }()
    return out
}
```

Built-in transforms are registered via `registry.RegisterTransform()` in `cmd/register_transforms.go` (called from `init()`), one entry per transform type. The `registry.SpecToTransform(spec)` function looks up the factory by `spec.Type` and calls it — no hardcoded switch. Adding a new transform type means adding one `registry.RegisterTransform` call; `specToTransform` in `cmd/reload.go` does not need to change.

When `SIGHUP` arrives, the `Pipeline.Reload` method builds new `wrapperConfig` values and stores them atomically:

```go
func (p *Pipeline) Reload(cfg ReloadConfig) error {
    for name, cfgPtr := range p.wrapperConfigs {
        fns := make([]transform.TransformFunc, 0, len(cfg.Transforms)+len(cfg.AdapterTransforms[name]))
        fns = append(fns, cfg.Transforms...)
        fns = append(fns, cfg.AdapterTransforms[name]...)

        wcfg := &wrapperConfig{
            transformFn: transform.Chain(fns...),
            routeFilter: channelFilter(cfg.Routes[name]),
        }
        cfgPtr.Store(wcfg) // atomic store
    }
    metrics.ConfigReloads.Inc()
    return nil
}
```

There's no mutex on the hot path. The wrapper goroutine does an atomic pointer load on every event, and the reload handler does an atomic pointer store. The new config takes effect on the next event processed — zero event loss, no pause, no restart.

Why `atomic.Pointer` instead of a mutex? Because the hot path (every event, every adapter) would contend on the mutex with the reload path (once per SIGHUP). Even though the reload is infrequent, a mutex creates a potential priority inversion: if the reload holds the write lock while rebuilding transform chains, all wrapper goroutines block until the rebuild completes. With atomic.Pointer, the wrapper goroutine reads the current config in a single atomic operation — no blocking, no contention, no priority inversion. The reload builds the entire new config, then swaps it in with a single store.

Why `atomic.Pointer[wrapperConfig]` instead of two separate atomic values (one for transforms, one for routes)? Because transforms and routes need to change atomically together. If they were separate, there's a window where the new transforms are active but the old routes are still in place (or vice versa). By storing both in a single struct behind one atomic pointer, the swap is all-or-nothing.

The SIGHUP handler in `cmd/listen.go` re-reads the YAML config file, extracts the `transforms:` and `routes:` sections, builds new `TransformFunc` chains and route filters, and calls `Pipeline.Reload`:

```go
// Signal handler
signal.Notify(sigCh, syscall.SIGHUP)
go func() {
    for range sigCh {
        cfg, err := reload.BuildReloadConfig(configFile, cliTransforms, cliRoutes)
        if err != nil {
            logger.Error("reload failed", "error", err)
            metrics.ConfigReloadErrors.Inc()
            continue
        }
        if err := pipeline.Reload(cfg); err != nil {
            logger.Error("reload failed", "error", err)
            metrics.ConfigReloadErrors.Inc()
            continue
        }
        logger.Info("config reloaded successfully")
    }
}()
```

CLI flags always take precedence: if `--route webhook=orders` was set on the command line, the YAML `routes.webhook` section is ignored for that adapter (CLI wins on name conflict).

**What's reloadable and what isn't.** The SIGHUP boundary is strict:

**RELOADABLE** (takes effect on next event):
- `transforms.global` and `transforms.adapter.<name>` YAML sections
- `routes:` YAML section (CLI `--route` wins per adapter name)
- View definitions from `views:` YAML section

**IMMUTABLE** (requires restart):
- Adapters (can't add/remove adapters at runtime)
- Detectors (type, database URL, publication)
- Bus mode (fast/reliable)
- CLI flags (`--drop-columns`, `--filter-operations`, `--debezium-envelope`, etc.)
- Plugin transforms (`--plugin-transform`)
- All connection parameters (webhook URL, Kafka brokers, S3 bucket, etc.)

**Transform ordering.** When the reload builds the per-adapter transform chain, transforms are applied in this order:

1. CLI transforms (`--drop-columns`, `--filter-operations`, `--debezium-envelope`, `--cloudevents-envelope`)
2. Plugin transforms (`--plugin-transform`)
3. YAML `transforms.global` (in array order)
4. YAML `transforms.adapter.<name>` (in array order)

There is no deduplication between layers. If you specify `--drop-columns password` on the CLI and also list `drop_columns: [password]` in YAML global transforms, the drop runs twice (harmlessly, but wastefully). The ordering logic lives in `cmd/reload.go:buildTransformOpts`.

### 4.4 CEL Filter

The CEL filter transform is registered in the component registry like every other transform:

```go
registry.RegisterTransform(registry.TransformEntry{
    Name:        "cel_filter",
    Description: "Filter events using a CEL expression",
    Create: func(spec config.TransformSpec) transform.TransformFunc {
        if spec.Expression == "" {
            return nil
        }
        fn, err := transform.FilterCEL(spec.Expression)
        if err != nil {
            slog.Warn("invalid cel_filter expression, skipping", "error", err)
            return nil
        }
        return fn
    },
})
```

This means CEL filter works everywhere the registry does: in YAML `transforms:` config, in `SIGHUP` hot-reload (the expression is recompiled from the new YAML), and in `pgcdc describe cel_filter` for parameter introspection. Invalid expressions produce a warning at startup (or reload) and are silently skipped rather than aborting — a defensive choice that lets the rest of the pipeline run even if one filter expression has a typo.

YAML configuration example:

```yaml
transforms:
  global:
    - type: cel_filter
      expression: 'operation != "TRUNCATE"'
  adapter:
    webhook:
      - type: cel_filter
        expression: 'table == "orders" && operation == "INSERT"'
```

The CEL filter composes naturally with other transforms in the chain. Since it returns `ErrDropEvent` to signal filtering, it short-circuits the chain — transforms after the CEL filter in the chain never see dropped events. Place it early in the chain to avoid unnecessary work on events that will be discarded.

---

## Chapter 5: Simple Adapters — stdout, file, exec, pg_table

### 5.1 stdout — The Canonical Adapter

**File: `adapter/stdout/stdout.go`**

The stdout adapter is 77 lines of code. It establishes the pattern that every other adapter follows. Read it in full:

```go
type Adapter struct {
    w      io.Writer
    logger *slog.Logger
    ackFn  adapter.AckFunc
}

func New(w io.Writer, logger *slog.Logger) *Adapter {
    if w == nil { w = os.Stdout }
    if logger == nil { logger = slog.Default() }
    return &Adapter{w: w, logger: logger}
}

func (a *Adapter) SetAckFunc(fn adapter.AckFunc) { a.ackFn = fn }

func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
    a.logger.Info("stdout adapter started")
    enc := json.NewEncoder(a.w)
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case ev, ok := <-events:
            if !ok {
                return nil
            }
            if err := enc.Encode(ev); err != nil {
                return fmt.Errorf("stdout adapter: encode event %s: %w", ev.ID, err)
            }
            metrics.EventsDelivered.WithLabelValues("stdout").Inc()
            if a.ackFn != nil && ev.LSN > 0 {
                a.ackFn(ev.LSN)
            }
        }
    }
}

func (a *Adapter) Name() string { return "stdout" }
```

The pattern: `Start` blocks in a select loop, reading from the events channel. When the context is cancelled, it returns `ctx.Err()`. When the channel is closed (bus shutdown), it returns nil. After processing each event, it acknowledges the LSN for cooperative checkpointing.

Every adapter in the system follows this shape. The differences are in what happens between receiving the event and acknowledging it.

### 5.2 file — Size-Based Rotation

**File: `adapter/file/file.go`**

The file adapter writes JSON-lines to a file and rotates when the file exceeds a configurable size. The `Start` loop follows the same pattern as stdout, with two additions: `f.Sync()` after each write (ensuring durability), and a rotation check:

```go
case ev, ok := <-events:
    if !ok {
        return nil
    }
    if err := a.enc.Encode(ev); err != nil {
        return fmt.Errorf("file adapter write: %w", err)
    }
    if err := a.f.Sync(); err != nil {
        a.logger.Warn("failed to sync file", "error", err)
    }
    if err := a.maybeRotate(); err != nil {
        return fmt.Errorf("file adapter rotation: %w", err)
    }
```

Rotation uses a numbered suffix scheme similar to `logrotate`. When the file exceeds `--file-max-size` (default 100MB):

```go
func (a *Adapter) rotate() error {
    a.close()

    // Remove the oldest rotated file.
    oldest := fmt.Sprintf("%s.%d", a.path, a.maxFiles)
    _ = os.Remove(oldest)

    // Shift existing rotated files: .{n} -> .{n+1}
    for n := a.maxFiles - 1; n >= 1; n-- {
        src := fmt.Sprintf("%s.%d", a.path, n)
        dst := fmt.Sprintf("%s.%d", a.path, n+1)
        _ = os.Rename(src, dst)
    }

    // Rename current file to .1
    if err := os.Rename(a.path, a.path+".1"); err != nil {
        return fmt.Errorf("rename to .1: %w", err)
    }
    return a.open()
}
```

The file at `events.jsonl` becomes `events.jsonl.1`, the previous `.1` becomes `.2`, and so on up to `--file-max-files` (default 5). The oldest file is deleted. This keeps a bounded window of historical events on disk without unbounded growth.

### 5.3 exec — Subprocess Piping

**File: `adapter/exec/exec.go`**

The exec adapter spawns a subprocess (configured via `--exec-command`) and pipes events as JSON-lines to its stdin. The command is run via `sh -c`, so you can use shell features like pipes and redirects.

The interesting engineering is in failure handling. When the subprocess exits (crash, pipe break, or normal exit), the adapter captures the event that was being written at the time and stores it as a "pending" event:

```go
func (a *Adapter) run(ctx context.Context, events <-chan event.Event, pending **event.Event) error {
    cmd := exec.CommandContext(ctx, "sh", "-c", a.command)
    stdin, err := cmd.StdinPipe()
    // ...
    if err := cmd.Start(); err != nil { ... }
    enc := json.NewEncoder(stdin)

    // Re-send pending event from previous process crash.
    if *pending != nil {
        if err := enc.Encode(*pending); err != nil { ... }
        *pending = nil
    }

    // Wait for process exit in the background.
    waitCh := make(chan error, 1)
    go func() { waitCh <- cmd.Wait() }()

    for {
        select {
        case <-ctx.Done():
            _ = stdin.Close()
            <-waitCh
            return ctx.Err()
        case waitErr := <-waitCh:
            return &pgcdcerr.ExecProcessError{Command: a.command, Err: fmt.Errorf("exited: %w", waitErr)}
        case ev, ok := <-events:
            if !ok { ... }
            if err := enc.Encode(ev); err != nil {
                *pending = &ev  // save for re-delivery
                _ = stdin.Close()
                <-waitCh
                return &pgcdcerr.ExecProcessError{Command: a.command, Err: fmt.Errorf("write: %w", err)}
            }
        }
    }
}
```

Note the three-way `select`: it simultaneously waits for context cancellation (shutdown), process exit (crash), and new events. If the write to stdin fails (broken pipe), the failed event is stored in `pending` and re-delivered when the next process starts.

The `Start` method wraps `run` in the standard reconnect-with-backoff loop. Each restart increments `pgcdc_exec_restarts_total`.

This adapter is surprisingly useful: it turns any command-line tool into a CDC consumer. `pgcdc listen --adapter exec --exec-command "jq .payload | my-processor"` gives you a CDC pipeline with a custom processor in any language.

### 5.4 pg_table — INSERT with Sanitization

**File: `adapter/pgtable/pgtable.go`**

The pg_table adapter INSERTs each event into a PostgreSQL table. It maintains its own database connection (separate from the detector) with reconnect-on-failure:

```go
func (a *Adapter) run(ctx context.Context, events <-chan event.Event) error {
    conn, err := pgx.Connect(ctx, a.dbURL)
    // ...
    safeTable := pgx.Identifier{a.table}.Sanitize()
    insertSQL := fmt.Sprintf(
        "INSERT INTO %s (id, channel, operation, payload, source, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
        safeTable,
    )
    // ... event loop with conn.Exec(ctx, insertSQL, ...)
}
```

The table name is sanitized using `pgx.Identifier{tableName}.Sanitize()`, which double-quotes the identifier and escapes any embedded quotes. This prevents SQL injection through table names configured via CLI flags or YAML.

The adapter distinguishes between connection errors (worth reconnecting) and data errors (constraint violations, type mismatches). Connection errors trigger the reconnect loop. Data errors are logged and the event is skipped with an ack — the adapter doesn't let a single bad event block the entire stream:

```go
if conn.IsClosed() {
    return fmt.Errorf("insert (connection lost): %w", err)
}
// Non-connection error: log and skip.
a.logger.Warn("insert failed, skipping event", "event_id", ev.ID, "error", err)
if a.ackFn != nil && ev.LSN > 0 {
    a.ackFn(ev.LSN)
}
continue
```

This is a common pattern in pgcdc adapters: separate connection-level failures (retry) from event-level failures (skip and ack).

### 5.5 The Adapter Lifecycle

**File: `adapter/adapter.go`**

Beyond the basic `Adapter` interface (`Start` + `Name`), adapters can opt into additional capabilities through interface implementation. There are now three distinct **execution paths** the pipeline dispatches through, plus several opt-in capability interfaces.

**Three execution paths** (selected via type assertion in `startAdapter`):

1. **`adapter.Deliverer`** — implements `Deliver(ctx context.Context, ev event.Event) error`. The pipeline calls `middleware.Build()` and runs a `StartLoop` event loop. All of retry, circuit-breaking, rate-limiting, DLQ recording, tracing, and ack are provided by the `adapter/middleware` stack. The adapter implements only the delivery logic. Webhook and embedding use this path.

2. **`adapter.Batcher`** — implements `Flush(ctx context.Context, batch []event.Event) FlushResult` and `BatchConfig() BatchConfig`. The pipeline runs a `batch.Runner` that buffers events and flushes them in bulk on a timer or size trigger, with DLQ and ack managed by the runner.

3. **Legacy `Start` path** — implements only `Start(ctx, <-chan event.Event) error`. The pipeline calls `Start` directly. The adapter manages its own event loop. Used by broker-style adapters (SSE, WebSocket, gRPC, kafkaserver, view).

**Opt-in capability interfaces** (detected via type assertion at construction time):

```go
type Acknowledger interface {
    SetAckFunc(fn AckFunc)
}

type Validator interface {
    Validate(ctx context.Context) error
}

type Drainer interface {
    Drain(ctx context.Context) error
}

type Reinjector interface {
    SetIngestChan(ch chan<- event.Event)
}
```

For Deliverer and Batcher adapters, DLQ and ack are managed by the middleware/batch runner and are **not** injected via `SetDLQ`/`SetAckFunc`. For legacy adapters, the pipeline injects these at construction:

```go
// Only for legacy (non-Deliverer, non-Batcher) adapters:
if !isMiddlewareManaged {
    if da, ok := a.(DLQAware); ok && p.dlq != nil {
        da.SetDLQ(p.dlq)
    }
}
```

**Validator** runs pre-flight checks before the pipeline starts. The webhook adapter validates DNS resolution. The S3 adapter calls HeadBucket. If any validator fails, startup is aborted (unless `--skip-validation`).

**Acknowledger** enables cooperative checkpointing. For legacy adapters, the pipeline injects an ack function. For Deliverer adapters, the middleware's Ack layer calls it after `Deliver` returns.

**Drainer** flushes in-flight work during graceful shutdown. The webhook adapter's Drain waits for its `sync.WaitGroup` (tracking in-flight HTTP requests). The S3 adapter's Drain flushes its buffer.

**Reinjector** is for adapters that produce events back into the bus — specifically the view adapter, which emits `VIEW_RESULT` events that other adapters can consume.

---

## Chapter 6: HTTP Adapters — webhook, SSE, WebSocket, gRPC

### 6.1 Webhook

**File: `adapter/webhook/webhook.go`**

The webhook adapter is the most complex simple adapter. It delivers events as HTTP POST requests and handles all the things that can go wrong in network communication: retries, backoff, signing, circuit breaking, rate limiting, DLQ, and distributed tracing.

The delivery function implements retry with exponential backoff and full jitter:

```go
func (a *Adapter) deliver(ctx context.Context, ev event.Event) error {
    body, err := json.Marshal(ev)
    if err != nil {
        return fmt.Errorf("marshal event %s: %w", ev.ID, err)
    }

    for attempt := range a.maxRetries {
        if attempt > 0 {
            metrics.WebhookRetries.Inc()
            wait := backoff.Jitter(attempt, a.backoffBase, a.backoffCap)
            select {
            case <-ctx.Done():
                return ctx.Err()
            case <-time.After(wait):
            }
        }

        req, _ := http.NewRequestWithContext(ctx, http.MethodPost, a.url, bytes.NewReader(body))
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("X-PGCDC-Event-ID", ev.ID)
        req.Header.Set("X-PGCDC-Channel", ev.Channel)

        // HMAC-SHA256 signature
        if a.signingKey != "" {
            mac := hmac.New(sha256.New, []byte(a.signingKey))
            mac.Write(body)
            sig := hex.EncodeToString(mac.Sum(nil))
            req.Header.Set("X-PGCDC-Signature", "sha256="+sig)
        }

        resp, err := a.client.Do(req)
        if err != nil {
            lastErr = err
            continue // network error — retryable
        }

        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            return nil // success
        }
        if resp.StatusCode == 429 || resp.StatusCode >= 500 {
            continue // retryable
        }
        // 4xx (except 429) — non-retryable, skip
        return nil
    }

    return &pgcdcerr.WebhookDeliveryError{...}
}
```

The retry classification is standard: 2xx is success, 5xx and 429 (Too Many Requests) are retryable, other 4xx responses are non-retryable (the request is wrong, retrying won't help). Network errors (connection refused, timeout) are always retryable.

**HMAC signing** (`--webhook-signing-key`): Each request includes an `X-PGCDC-Signature` header containing `sha256=<hex-encoded-hmac>`. The HMAC is computed over the raw JSON body using the signing key. The receiver can verify the signature to ensure the request came from pgcdc and wasn't tampered with:

```python
# Receiver-side verification (Python example)
import hmac, hashlib
expected = hmac.new(signing_key.encode(), request.body, hashlib.sha256).hexdigest()
received = request.headers['X-PGCDC-Signature'].removeprefix('sha256=')
if not hmac.compare_digest(expected, received):
    return 403  # Forged request
```

The signature covers the serialized event body — if any field is modified in transit, the signature won't match. The `sha256=` prefix follows the same convention as GitHub's webhook signatures, making it familiar to developers who've implemented webhook verification before.

**W3C Trace Context**: When OpenTelemetry tracing is enabled, the webhook adapter injects `traceparent` and `tracestate` headers into each request. This lets the webhook receiver extract the trace context and correlate the webhook delivery with the upstream CDC pipeline — all the way back to the database change that triggered the event.

**Custom headers** (`--webhook-headers`): Arbitrary headers can be added for authentication (`Authorization: Bearer <token>`), routing (`X-Target-Service: orders`), or metadata (`X-Environment: production`).

The webhook adapter implements `adapter.Deliverer` — its `Deliver(ctx, ev)` method handles only the HTTP logic (signing, headers, trace context injection, per-delivery retry with backoff). The pipeline's `startAdapter` dispatches webhook through the `adapter/middleware` chain, which provides the circuit breaker, rate limiter, DLQ recording, tracing span, and ack — none of that lives inside the webhook package itself.

The middleware chain for webhook looks like:

```
Metrics → Tracing → CircuitBreaker → RateLimit → Retry → DLQ → Ack → webhook.Deliver
```

`middleware.Build(name, cfg, opts...)` constructs this chain, and `middleware.StartLoop(ctx, events, chain, logger)` is the event loop that calls the chain for each event. The entire resilience stack is configured via `registry.AdapterResult.MiddlewareConfig` (set in `cmd/register_core.go` for webhook):

```go
mwCfg := webhookMiddlewareConfig(cfg)
return registry.AdapterResult{
    Adapter:          a,
    MiddlewareConfig: &mwCfg,
}, nil
```

**Circuit breaker** (`internal/circuitbreaker/circuitbreaker.go`): Three states — Closed (normal operation), Open (all requests rejected), HalfOpen (one trial request allowed). After `--webhook-cb-failures` consecutive failures, the breaker opens. After `--webhook-cb-reset` duration, it transitions to half-open. One success in half-open closes the breaker; one failure re-opens it.

```go
func (cb *CircuitBreaker) Allow() bool {
    switch cb.state {
    case StateClosed:
        return true
    case StateOpen:
        if time.Since(cb.lastFailure) >= cb.resetTimeout {
            cb.state = StateHalfOpen
            return true
        }
        return false
    case StateHalfOpen:
        return true
    }
}
```

**Rate limiter** (`internal/ratelimit/ratelimit.go`): Token-bucket algorithm wrapping `golang.org/x/time/rate`. Configured via `--webhook-rate-limit` (events per second) and `--webhook-rate-burst` (burst capacity). The `Wait` method blocks until a token is available, with Prometheus metrics tracking wait counts and durations.

**Resilience stack across adapters.** The webhook adapter has the most complete resilience story, but not every adapter needs all of these features. Here's what's wired in today:

| Adapter       | Retries | Circuit Breaker | Rate Limiter | DLQ   | Reconnect |
|---------------|---------|-----------------|--------------|-------|-----------|
| webhook       | Yes     | Yes             | Yes          | Yes   | —         |
| embedding     | Yes     | Yes             | Yes          | Yes   | —         |
| kafka         | —¹      | —               | —            | Yes   | Yes       |
| nats          | —       | —               | —            | —     | Yes       |
| redis         | —       | —               | —            | —     | Yes       |
| pg_table      | —       | —               | —            | —     | Yes       |
| search        | —       | —               | —            | —     | —         |
| s3            | —       | —               | —            | —     | —         |
| stdout        | —       | —               | —            | —     | —         |
| file          | —       | —               | —            | —     | —         |
| exec          | —       | —               | —            | —     | —         |
| SSE           | —²      | —               | —            | —     | —         |
| WebSocket     | —²      | —               | —            | —     | —         |
| gRPC          | —²      | —               | —            | —     | —         |
| kafkaserver   | —³      | —               | —            | —     | —         |
| view          | —       | —               | —            | —     | —         |

¹ franz-go handles retries internally; terminal errors (auth, serialization) go to DLQ.
² Broker pattern: events are broadcast to connected clients via non-blocking channel sends. Slow clients are dropped, not retried.
³ In-memory ring buffer per partition. Consumers that fall behind receive `OFFSET_OUT_OF_RANGE` — no DLQ, the data is gone.

Three "network adapters" — webhook, embedding, and kafka — have the full delivery guarantee stack. The remaining adapters are either local (stdout, file, exec), broker-based (SSE, WebSocket, gRPC — the client is responsible for reconnecting), or connection-oriented with reconnect logic but no per-event retry.

### 6.2 SSE

**File: `adapter/sse/sse.go`**

The SSE (Server-Sent Events) adapter uses a broker pattern: the adapter goroutine receives events from the bus and broadcasts them to all connected client channels. Client connections come and go dynamically.

The broker maintains a map of client channels with optional channel filters:

```go
type Broker struct {
    clients    map[chan event.Event]string // channel -> optional channel filter
    mu         sync.RWMutex
    bufferSize int
    heartbeat  time.Duration
    logger     *slog.Logger
}
```

When an HTTP client connects, the `ServeHTTP` handler registers a buffered channel, sets the SSE headers, and enters a select loop:

```go
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    flusher, ok := w.(http.Flusher)
    if !ok {
        http.Error(w, "streaming unsupported", http.StatusInternalServerError)
        return
    }

    ch, unsubscribe := b.Subscribe(channelFilter)
    defer unsubscribe()

    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")
    w.Header().Set("X-Accel-Buffering", "no")  // disable nginx buffering
    flusher.Flush()

    heartbeatTicker := time.NewTicker(b.heartbeat)
    defer heartbeatTicker.Stop()

    for {
        select {
        case <-r.Context().Done():
            return
        case ev, ok := <-ch:
            if !ok { return }
            writeSSE(w, ev) // "id: {id}\nevent: {channel}\ndata: {json}\n\n"
            flusher.Flush()
        case <-heartbeatTicker.C:
            fmt.Fprint(w, ": heartbeat\n\n")
            flusher.Flush()
        }
    }
}
```

The `X-Accel-Buffering: no` header is specifically for nginx — without it, nginx buffers the response and the client never sees events in real-time.

The broadcast is non-blocking: if a client's channel is full, the event is dropped for that client (same fast-mode philosophy as the bus). This prevents a slow client from affecting other clients or the pipeline:

```go
func (b *Broker) broadcast(ev event.Event) {
    b.mu.RLock()
    defer b.mu.RUnlock()
    for ch, filter := range b.clients {
        if filter != "" && filter != ev.Channel {
            continue
        }
        select {
        case ch <- ev:
            metrics.EventsDelivered.WithLabelValues("sse").Inc()
        default:
            metrics.EventsDropped.WithLabelValues("sse").Inc()
        }
    }
}
```

The SSE wire format uses the event ID as the SSE `id:` field. This enables the standard SSE reconnection mechanism: if a client disconnects and reconnects with a `Last-Event-ID` header, it knows which events it missed. (pgcdc doesn't replay missed events — that would require buffering — but the client can use the ID for its own tracking.)

### 6.3 WebSocket

**File: `adapter/ws/ws.go`**

The WebSocket adapter follows the same broker pattern as SSE, using `coder/websocket` for the upgrade. The key difference is bidirectional communication — the adapter spawns a read goroutine per client to detect disconnections:

```go
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
        InsecureSkipVerify: true, // CORS handled by middleware
    })
    // ...
    ch, unsubscribe := b.Subscribe(channelFilter)
    defer unsubscribe()

    // Read goroutine: detect client disconnect.
    readDone := make(chan struct{})
    go func() {
        defer close(readDone)
        for {
            _, _, err := c.Read(ctx)
            if err != nil { return }
        }
    }()

    pingTicker := time.NewTicker(b.pingInterval)
    defer pingTicker.Stop()

    for {
        select {
        case <-ctx.Done():
            c.Close(websocket.StatusNormalClosure, "server shutting down")
            return
        case <-readDone:
            return  // client disconnected
        case ev, ok := <-ch:
            // ... write JSON to websocket
        case <-pingTicker.C:
            c.Ping(ctx)  // keep-alive
        }
    }
}
```

The read goroutine is essential: without it, the WebSocket library can't process incoming control frames (close, pong), and the server won't detect that a client has disconnected. The goroutine sits in a `Read` loop that blocks on incoming messages. When the client disconnects (sending a close frame or dropping the TCP connection), `Read` returns an error, `readDone` is closed, and the write loop exits.

The ping ticker sends WebSocket ping frames at `--ws-ping-interval` (default 15s). The client responds with a pong, which the read goroutine consumes. If the pong doesn't arrive before the next ping timeout, the connection is considered dead.

### 6.4 gRPC Streaming

**File: `adapter/grpc/grpc.go`**

The gRPC adapter starts a gRPC server with a single streaming RPC:

```protobuf
service EventStream {
  rpc Subscribe(SubscribeRequest) returns (stream Event);
}

message SubscribeRequest {
  repeated string channels = 1;  // empty = all channels
}
```

The server-side implementation follows the same broker pattern as SSE and WebSocket:

```go
func (s *grpcServer) Subscribe(req *pb.SubscribeRequest, stream grpc.ServerStreamingServer[pb.Event]) error {
    id, c := s.adapter.addClient(req.GetChannels())
    defer s.adapter.removeClient(id)

    for {
        select {
        case <-stream.Context().Done():
            return nil
        case ev, ok := <-c.ch:
            if !ok {
                return status.Error(codes.Unavailable, "server shutting down")
            }
            if err := stream.Send(ev); err != nil {
                return err
            }
        }
    }
}
```

Clients call `Subscribe` with an optional channel filter. The server-side stream runs until the client disconnects or the context is cancelled. HTTP/2 flow control handles backpressure between server and client — if the client can't keep up, the server's `Send` call blocks until the HTTP/2 flow control window opens. This is a significant advantage over SSE and WebSocket, where slow clients get dropped events.

The adapter converts pgcdc events to protobuf messages in the broadcast function, including the `traceparent` field for distributed tracing continuity. Each gRPC client gets its own 256-element buffered channel; events that would overflow are silently dropped (non-blocking send), matching the SSE/WS pattern.

---

## Chapter 7: Message Brokers — NATS, Kafka, and Building Kafka from Scratch

### 7.1 NATS JetStream

**File: `adapter/nats/nats.go`**

The NATS JetStream adapter publishes events to NATS subjects with a naming convention that maps pgcdc channels to NATS subjects: `pgcdc:orders` → `pgcdc.orders` (colons replaced with dots, matching NATS subject conventions).

**Auto-stream creation**: On startup, the adapter creates or updates a JetStream stream covering the `pgcdc.>` subject wildcard:

```go
streamCfg := jetstream.StreamConfig{
    Name:     a.streamName,          // default: "pgcdc"
    Subjects: []string{a.subjectPrefix + ".>"},  // "pgcdc.>"
    MaxAge:   a.maxAge,              // default: 24h
}
_, err = js.CreateOrUpdateStream(ctx, streamCfg)
```

This means zero NATS configuration — just point pgcdc at a NATS server and events start flowing. `CreateOrUpdateStream` is idempotent; if the stream already exists with matching config, it's a no-op.

**Deduplication**: Each message includes a `Nats-Msg-Id` header set to the event's UUIDv7 ID:

```go
msg := &natsclient.Msg{
    Subject: subject,
    Data:    data,
    Header:  natsclient.Header{},
}
msg.Header.Set("Nats-Msg-Id", ev.ID)
```

NATS JetStream uses this for server-side deduplication — if the adapter retries a publish (due to network timeout), NATS recognizes the duplicate by message ID and discards it. The dedup window is configurable on the NATS server (default 2 minutes).

**Trace context propagation**: Because NATS headers are `http.Header` (same type), the adapter uses OpenTelemetry's `propagation.HeaderCarrier` directly — no custom carrier needed:

```go
otel.GetTextMapPropagator().Inject(pubCtx, propagation.HeaderCarrier(msg.Header))
```

This injects W3C `traceparent` and `tracestate` headers into the NATS message, so downstream consumers can continue the distributed trace.

**Reconnection**: The adapter uses the standard reconnect-with-backoff pattern. NATS's built-in reconnection (`MaxReconnects: -1` for infinite retries) handles transient network issues, while pgcdc's outer reconnect loop handles cases where the entire NATS connection is lost.

### 7.2 Kafka Adapter

**File: `adapter/kafka/kafka.go`**

The Kafka adapter uses `twmb/franz-go` to publish events to Kafka topics. Channel-to-topic mapping follows the same convention: `pgcdc:orders` → `pgcdc.orders`. Each event becomes a Kafka record with:

- **Key**: The event ID (used for partition assignment)
- **Value**: The full serialized event (JSON by default, or Avro/Protobuf with encoding)
- **Headers**: `pgcdc-channel`, `pgcdc-operation`, `pgcdc-event-id`, `content-type`

The adapter uses `RequiredAcks: AllISRAcks()` — all in-sync replicas must acknowledge the write before it's considered successful. This provides the strongest durability guarantee at the cost of slightly higher latency.

**Exactly-once delivery** is available via `--kafka-transactional-id`. When set, each event is produced inside its own Kafka transaction:

```go
func (a *Adapter) produceTransactional(ctx context.Context, client *kgo.Client, record *kgo.Record) error {
    if err := client.BeginTransaction(); err != nil {
        return fmt.Errorf("begin transaction: %w", err)
    }
    if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
        abortCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        _ = client.EndTransaction(abortCtx, kgo.TryAbort)
        metrics.KafkaTransactionErrors.Add(1)
        return err
    }
    if err := client.EndTransaction(ctx, kgo.TryCommit); err != nil {
        metrics.KafkaTransactionErrors.Add(1)
        return fmt.Errorf("commit transaction: %w", err)
    }
    return nil
}
```

The abort path uses `context.Background()` with a timeout because the original context may already be cancelled — aborting a transaction after cancellation is still important to avoid dangling transactions on the Kafka broker.

**Error classification**: The adapter distinguishes terminal errors from transient ones:

```go
func isTerminalError(err error) bool {
    var ke *kerr.Error
    if errors.As(err, &ke) {
        return !ke.Retriable
    }
    return false
}
```

Terminal errors (authorization denied, message too large, topic doesn't exist) go to the DLQ — retrying won't help. Transient errors (leader not available, not enough replicas) cause the adapter to close the client and reconnect with backoff. This distinction prevents DLQ-ing events that would succeed after a brief broker hiccup.

**Encoding and Schema Registry**: The adapter supports JSON (default), Avro, and Protobuf encoding via `--kafka-encoding`. With Avro or Protobuf, the adapter can optionally register schemas with a Confluent Schema Registry (`--schema-registry-url`). The wire format follows the Confluent convention: magic byte (0x00) + 4-byte schema ID + encoded payload.

**SASL authentication** supports PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512:

```go
switch saslMechanism {
case "plain":
    opts = append(opts, kgo.SASL(plain.Auth{User: saslUser, Pass: saslPass}.AsMechanism()))
case "scram-sha-256":
    opts = append(opts, kgo.SASL(scram.Auth{User: saslUser, Pass: saslPass}.AsSha256Mechanism()))
case "scram-sha-512":
    opts = append(opts, kgo.SASL(scram.Auth{User: saslUser, Pass: saslPass}.AsSha512Mechanism()))
}
```

TLS is enabled via `--kafka-tls` with optional CA file (`--kafka-tls-ca-file`). The TLS configuration is passed via `kgo.DialTLSConfig`.

### 7.3 The Kafka Protocol Server

**Files: `adapter/kafkaserver/server.go`, `protocol.go`, `handler.go`, `broker.go`, `partition.go`, `group.go`**

This is the showpiece of the system. Instead of publishing to an external Kafka cluster, pgcdc *becomes* a Kafka cluster. Any Kafka consumer library — librdkafka, franz-go, sarama, the Java client, confluent-kafka-python — can connect to pgcdc on port 9092 and consume CDC events using the standard Kafka protocol. No Kafka cluster needed.

#### 7.3.1 Why Build a Kafka Server?

Because Kafka is everywhere. Every data team has Kafka consumers. Every streaming framework speaks the Kafka protocol. By implementing the server side, pgcdc becomes a drop-in data source for the entire Kafka ecosystem without requiring users to operate a Kafka cluster.

The implementation covers the minimum viable subset: 11 API keys that handle consumer group management, metadata discovery, offset tracking, and record fetching. It's not a full Kafka broker (no producer API, no replication, no log compaction), but it's enough for any consumer library to connect, join a group, and fetch records.

#### 7.3.2 Wire Protocol

Every Kafka request starts with a 4-byte length prefix (big-endian int32), followed by a header containing the API key, API version, correlation ID, and client ID:

```go
func readRequest(r io.Reader) (requestHeader, []byte, error) {
    var length int32
    if err := binary.Read(r, binary.BigEndian, &length); err != nil {
        return requestHeader{}, nil, fmt.Errorf("read length: %w", err)
    }
    if length <= 0 || length > 100*1024*1024 {
        return requestHeader{}, nil, fmt.Errorf("invalid request length: %d", length)
    }

    buf := make([]byte, length)
    if _, err := io.ReadFull(r, buf); err != nil {
        return requestHeader{}, nil, fmt.Errorf("read body: %w", err)
    }

    var hdr requestHeader
    hdr.APIKey = int16(binary.BigEndian.Uint16(buf[0:2]))
    hdr.APIVersion = int16(binary.BigEndian.Uint16(buf[2:4]))
    hdr.CorrelationID = int32(binary.BigEndian.Uint32(buf[4:8]))
    // ... parse client ID (standard or flexible encoding)
    return hdr, buf[off:], nil
}
```

Responses follow the same length-prefixed framing, with the correlation ID echoed back so clients can match responses to requests:

```go
func writeResponse(w io.Writer, correlationID int32, body []byte) error {
    length := int32(4 + len(body))
    binary.Write(w, binary.BigEndian, length)
    binary.Write(w, binary.BigEndian, correlationID)
    w.Write(body)
    return nil
}
```

Starting with ApiVersions v3, Kafka uses "flexible encoding" — a breaking change in the wire format that affects how arrays, strings, and headers are encoded:

| Element | Non-flexible | Flexible |
|---|---|---|
| Array length | int32 | unsigned varint (compact) |
| String length | int16 | unsigned varint (compact) |
| Nullable string | int16 (-1 = null) | unsigned varint (0 = null) |
| Tagged fields | Not present | After each struct |

Tagged fields are key-value pairs appended after each structure. They enable forward compatibility — a server can include new fields that old clients silently skip, and vice versa. pgcdc writes zero tagged fields (empty tag buffer = single `0x00` byte) but parses and ignores any tagged fields it receives from clients.

The handler detects the version and switches encoding accordingly. This is one of the more tedious parts of the implementation — every response builder has two code paths, one for flexible and one for non-flexible encoding.

#### 7.3.3 RecordBatch v2

Kafka consumers expect records in RecordBatch v2 format — the binary encoding used in Fetch responses. A batch contains:

```
baseOffset        int64
batchLength       int32
partitionLeaderEpoch  int32
magic             int8 (= 2)
crc               uint32 (CRC32-C of everything after this field)
attributes        int16
lastOffsetDelta   int32
baseTimestamp     int64
maxTimestamp       int64
producerID        int64
producerEpoch     int16
baseSequence      int32
recordCount       int32
records           []Record
```

Each record within the batch uses zigzag-encoded signed varints for compact representation:

```go
func putVarint(buf []byte, v int64) int {
    uv := uint64((v << 1) ^ (v >> 63)) // zigzag encode
    i := 0
    for uv >= 0x80 {
        buf[i] = byte(uv) | 0x80
        uv >>= 7
        i++
    }
    buf[i] = byte(uv)
    return i + 1
}
```

Zigzag encoding maps signed integers to unsigned integers so that small-magnitude values (both positive and negative) use fewer bytes:

```
 0 → 0
-1 → 1
 1 → 2
-2 → 3
 2 → 4
...
```

The formula `(v << 1) ^ (v >> 63)` performs this transformation in a single operation without branches. The result is then encoded as a variable-length unsigned integer where each byte's MSB indicates whether more bytes follow. Small values (common in record batch deltas — offset deltas, timestamp deltas, length prefixes) encode in 1-2 bytes instead of the fixed 4 or 8 bytes that standard int32/int64 would require.

The CRC uses CRC32-C (Castagnoli), not the more common CRC32-IEEE. CRC32-C was chosen by the Kafka protocol designers because it has better error detection properties and hardware acceleration support (SSE 4.2 on x86). Go's `hash/crc32` package provides `MakeTable(crc32.Castagnoli)` for this. Consumer libraries will reject batches with incorrect CRCs — a single bit flip in the encoding causes the entire batch to be rejected.

The complete encoding of a single-record batch (the most common case in pgcdc, where each event becomes one Kafka record) looks like:

```
┌─ Batch header (61 bytes fixed) ─────────────────────────────┐
│ baseOffset (8)  batchLength (4)  partLeaderEpoch (4)        │
│ magic=2 (1)  crc32c (4)  attributes (2)                     │
│ lastOffsetDelta (4)  baseTimestamp (8)  maxTimestamp (8)     │
│ producerId (8)  producerEpoch (2)  baseSequence (4)         │
│ recordCount (4)                                              │
├─ Record (variable length, zigzag-encoded) ──────────────────┤
│ length (varint)  attributes (1)  timestampDelta (varint)    │
│ offsetDelta (varint)  keyLength (varint)  key (bytes)       │
│ valueLength (varint)  value (bytes)                          │
│ headerCount (varint)  [headers...]                           │
└──────────────────────────────────────────────────────────────┘
```

This is a significant amount of binary encoding to get right. The implementation in `protocol.go` was built by reading the Kafka protocol specification and testing against real consumer libraries (franz-go, librdkafka, sarama) until all of them could successfully decode the batches.

#### 7.3.4 The 11 API Keys

The handler dispatches requests based on API key:

```go
func (h *handler) dispatch(ctx context.Context, hdr requestHeader, body []byte) []byte {
    switch hdr.APIKey {
    case apiApiVersions:    return h.handleApiVersions(hdr)
    case apiMetadata:       return h.handleMetadata(hdr, body)
    case apiFindCoordinator: return h.handleFindCoordinator(hdr, body)
    case apiJoinGroup:      return h.handleJoinGroup(hdr, body)
    case apiSyncGroup:      return h.handleSyncGroup(hdr, body)
    case apiHeartbeat:      return h.handleHeartbeat(hdr, body)
    case apiLeaveGroup:     return h.handleLeaveGroup(hdr, body)
    case apiListOffsets:    return h.handleListOffsets(hdr, body)
    case apiFetch:          return h.handleFetch(ctx, hdr, body)
    case apiOffsetCommit:   return h.handleOffsetCommit(ctx, hdr, body)
    case apiOffsetFetch:    return h.handleOffsetFetch(ctx, hdr, body)
    }
}
```

Here's what each API key does and why the server needs it:

**ApiVersions** (key 18): The very first request any Kafka client sends. The server responds with the list of API keys it supports and their version ranges. Starting with ApiVersions v3, Kafka uses "flexible encoding" — compact arrays with unsigned varint lengths and tagged fields. The handler detects the version and switches encoding accordingly. If the server doesn't support this API, no Kafka client can connect.

**Metadata** (key 3): Returns the cluster topology — broker addresses, topic names, partition counts, and partition leaders. Since pgcdc is a single-node "cluster," every partition has the same leader (this node). Topics are discovered dynamically from the broker's topic registry — each pgcdc channel that has received at least one event becomes a topic.

**FindCoordinator** (key 10): For consumer groups, the client needs to find the "group coordinator" — the broker responsible for managing a particular consumer group. In a real Kafka cluster, groups are assigned to specific brokers. In pgcdc, every group is coordinated by this node.

**JoinGroup** (key 11) / **SyncGroup** (key 14): The group membership protocol. JoinGroup is how a consumer announces itself to the group. SyncGroup is how the group leader distributes partition assignments to all members. These two requests implement Kafka's consumer group rebalance protocol.

**Heartbeat** (key 12): Members send heartbeats periodically. If a heartbeat isn't received within `sessionTimeout`, the member is reaped and the group rebalances.

**LeaveGroup** (key 13): Clean shutdown — the member voluntarily leaves and the group rebalances immediately.

**ListOffsets** (key 2): Returns the earliest and latest offsets for partitions. Consumers use this to seek to the beginning or end of a topic.

**Fetch** (key 1): The core data path — returns records from partitions. This is where the actual event data flows to consumers. The handler implements long-polling:

```go
// Wait for new data (long-poll)
waiterCh := t.partitions[partIdx].waiter()
select {
case <-waiterCh:
    // New data available — retry read
case <-time.After(time.Duration(maxWaitMs) * time.Millisecond):
    // Timeout — return empty
case <-ctx.Done():
    return
}
```

Long-polling is important for efficiency: without it, the consumer would spin in a tight loop sending Fetch requests and getting empty responses. With long-polling, the consumer blocks until new data arrives (or the timeout expires), reducing network traffic and CPU usage on both sides.

**OffsetCommit** (key 8) / **OffsetFetch** (key 9): Consumer groups track their progress by committing offsets. The server persists these via the checkpoint store with keys like `kafka:{group}:{topic}:{partition}`. This means consumer group offsets survive pgcdc restarts — a consumer group can pick up exactly where it left off.

#### 7.3.5 Consumer Groups

**File: `adapter/kafkaserver/group.go`**

Consumer groups implement Kafka's cooperative partition assignment protocol. The state machine has four states:

```
Empty ──> PreparingRebalance ──> CompletingRebalance ──> Stable
  ^                                                        |
  |                                                        |
  +──────────────── (member leaves/expires) <──────────────+
```

```go
const (
    groupEmpty               groupState = iota
    groupPreparingRebalance
    groupCompletingRebalance
    groupStable
)
```

The state transitions in detail:

**Empty → PreparingRebalance**: The first JoinGroup request for a group creates it and immediately transitions to PreparingRebalance. The coordinator assigns a randomly generated member ID (returned in the JoinGroup response) and records the member's protocol metadata (typically the list of topics it wants to subscribe to).

**PreparingRebalance → CompletingRebalance**: Once all expected members have joined (in pgcdc's simplified implementation, this happens immediately — there's no delay waiting for additional members), the coordinator picks a leader (the first member to join), assigns a generation ID (monotonically increasing), and transitions to CompletingRebalance. The JoinGroup response to the leader includes the full member list; non-leaders receive only their own member ID.

**CompletingRebalance → Stable**: The leader sends a SyncGroup request containing partition assignments for all members. The coordinator stores these assignments and transitions to Stable. When non-leader members send their SyncGroup requests, they receive their assigned partitions from the stored assignments. At this point, each consumer knows which partitions to fetch from.

**Stable → PreparingRebalance**: If a member's heartbeat doesn't arrive within `sessionTimeout` (default 30s), the session reaper removes it and triggers a rebalance. The reaper runs on a 1-second ticker:

```go
func (g *group) reapExpiredMembers() bool {
    now := time.Now()
    var reaped bool
    for id, m := range g.members {
        if now.Sub(m.lastHeartbeat) > g.sessionTimeout {
            delete(g.members, id)
            reaped = true
        }
    }
    if reaped {
        g.state = groupPreparingRebalance
        g.generation++
    }
    return reaped
}
```

**LeaveGroup**: Clean shutdown. The member is removed immediately and remaining members rebalance. This is faster than waiting for the session timeout — the consumer can signal a planned departure.

The simplified single-node model means there's no cross-broker coordination, no partition migration, and no leadership election. But from the consumer's perspective, the protocol is identical to a real Kafka cluster.

#### 7.3.6 Ring Buffer and Long-Poll Fetch

**File: `adapter/kafkaserver/partition.go`**

Each partition is a fixed-size ring buffer:

```go
type partition struct {
    mu         sync.Mutex
    ring       []record
    capacity   int
    nextOffset int64
    size       int
    waiters    []chan struct{}
}
```

Records are appended at `nextOffset % capacity`, overwriting the oldest record when the buffer is full. Consumers that fall behind the oldest available offset receive `OFFSET_OUT_OF_RANGE` — standard Kafka behavior for consumers that can't keep up.

The `waiters` mechanism enables efficient long-poll Fetch. When a consumer requests records at an offset that equals `nextOffset` (no new data), it registers a waiter channel. When new data arrives via `append`, all waiter channels are closed, waking up the waiting Fetch goroutines:

```go
func (p *partition) append(rec record) int64 {
    p.mu.Lock()
    offset := p.nextOffset
    rec.Offset = offset
    p.ring[int(offset)%p.capacity] = rec
    p.nextOffset++
    waiters := p.waiters
    p.waiters = nil
    p.mu.Unlock()
    for _, ch := range waiters {
        close(ch)
    }
    return offset
}
```

Events from the bus flow through the broker's `ingest` method, which extracts a key from the JSON payload (defaulting to the `id` field), hashes it with FNV-1a, and routes to the appropriate partition:

```go
func (b *broker) ingest(ev event.Event) {
    topic := b.channelToTopic(ev.Channel)
    t := b.getOrCreateTopic(topic)

    key := extractKey(ev.Payload, b.keyColumn)
    partIdx := fnv1aHash(key) % uint32(len(t.partitions))

    rec := record{
        Key:       []byte(key),
        Value:     ev.Payload,
        Timestamp: ev.CreatedAt,
        Headers:   eventHeaders(ev),
    }
    t.partitions[partIdx].append(rec)
}
```

The `channelToTopic` mapping replaces colons with dots: `pgcdc:orders` becomes `pgcdc.orders`. Topics are created lazily — the first event on a new channel creates the topic with its N partitions (`--kafkaserver-partitions`, default 8).

FNV-1a hashing ensures that events with the same key always land in the same partition — preserving per-key ordering for consumers. This is the same guarantee that real Kafka provides with its DefaultPartitioner. For a CDC use case, this means all changes to a single row (identified by primary key) are consumed in order, which is essential for maintaining consistency.

---

## Chapter 8: Storage and Data Adapters — S3, Search, Redis, Embedding

### 8.1 S3

**File: `adapter/s3/s3.go`**

The S3 adapter buffers events in memory and periodically flushes them to any S3-compatible object store (AWS S3, MinIO, Cloudflare R2, etc.). Two triggers cause a flush: a time interval (`--s3-flush-interval`, default 1 minute) and a count threshold (`--s3-flush-size`, default 10,000 events).

Objects are written with Hive-style partitioned keys:

```
{prefix}/channel={channel}/year={yyyy}/month={mm}/day={dd}/{uuid}.jsonl
```

This partitioning scheme is designed for query engines like Athena, Trino, and Spark that can use Hive-style partition pruning to skip irrelevant files.

The flush operation uses atomic buffer swap — the current buffer is swapped for a fresh empty one under the mutex, and the old buffer is uploaded without holding the lock:

```go
func (a *Adapter) flush(ctx context.Context) error {
    a.mu.Lock()
    if len(a.buffer) == 0 {
        a.mu.Unlock()
        return nil
    }
    batch := a.buffer
    a.buffer = make([]event.Event, 0, a.flushSize)
    a.mu.Unlock()

    // Upload batch (no lock held)
    // ...
}
```

This means the adapter can continue buffering new events while a flush is in progress.

The adapter supports two output formats:

**JSON Lines** (default, `--s3-format jsonl`): One JSON object per line. Human-readable, easy to process with standard tools (`jq`, `grep`), and universally supported. Each line is a complete serialized event.

**Parquet** (`--s3-format parquet`): Columnar format with Snappy compression, using `parquet-go`. Parquet is dramatically more efficient for analytical queries — columnar storage means queries that only need a few columns skip the rest, and Snappy compression typically achieves 3-5x compression on JSON-like data. A 10MB JSON Lines file becomes a 2-3MB Parquet file that queries 10x faster in Athena or Spark.

The Parquet writer converts events into a row-group with typed columns: `id` (string), `channel` (string), `operation` (string), `payload` (string/JSON), `source` (string), `created_at` (timestamp). The payload is stored as a string column containing JSON — full columnar decomposition of the payload would require schema awareness that varies per table.

The Drain method calls flush — on graceful shutdown, any buffered events are written to S3 before the adapter exits. This is critical: without Drain, the last batch of events (up to `flush_size` events) would be lost on every restart.

**Validation** calls `HeadBucket` to verify the bucket exists and the credentials are valid, failing fast at startup rather than on the first flush. This catches misconfigurations like wrong bucket names, invalid credentials, or unreachable endpoints immediately.

### 8.2 Search — Typesense and Meilisearch

**File: `adapter/search/search.go`**

The search adapter syncs CDC events to a search engine. INSERT and UPDATE operations trigger document upserts; DELETE operations trigger document deletions. The adapter batches upserts for efficiency — accumulating documents and flushing them in a single API call:

```go
for {
    select {
    case <-ticker.C:
        flush()  // time-based flush
    case ev, ok := <-events:
        doc, id, isDel, partial := a.extractPayload(ev)
        switch {
        case isDel:
            deletes = append(deletes, id)
        case partial:
            // TOAST: partial update (PATCH) to avoid overwriting existing fields
            a.updateDocument(ctx, id, doc)
        default:
            batch = append(batch, doc)
            if len(batch) >= a.batchSize {
                flush()  // size-based flush
            }
        }
    }
}
```

The adapter supports both Typesense and Meilisearch with engine-specific API calls. Typesense uses `POST /collections/{collection}/documents/import?action=upsert` with JSONL format (one JSON object per line). Meilisearch uses `POST /indexes/{index}/documents` with a JSON array. Authentication differs too: Typesense uses `X-TYPESENSE-API-KEY` header, Meilisearch uses `Authorization: Bearer`.

**TOAST-aware partial updates**: When an UPDATE event has `_unchanged_toast_columns`, the adapter strips those columns from the document and routes it to a partial update path. For Typesense, this uses HTTP PATCH on the individual document endpoint. For Meilisearch, the standard document endpoint merges by default (missing fields are retained). This prevents overwriting existing search-indexed content with null values — a subtle correctness issue that affects any adapter consuming WAL events.

### 8.3 Redis

**File: `adapter/redis/redis.go`**

Two modes, each with distinct operational semantics:

**Invalidate mode** (`--redis-mode invalidate`): Every change triggers a `DEL` on the corresponding Redis key, regardless of operation type (INSERT, UPDATE, or DELETE all result in DEL). This is cache invalidation — your application populates the cache on read, and pgcdc clears it whenever the source data changes. Simple, effective, and the most common use case. The application's next read sees a cache miss and fetches fresh data from the database.

**Sync mode** (`--redis-mode sync`): INSERT and UPDATE trigger `SET` with the full document as JSON. DELETE triggers `DEL`. This keeps Redis in sync with the source database as a read-through cache — reads can be served from Redis without ever touching PostgreSQL.

Sync mode has a TOAST subtlety: when an UPDATE event has unchanged TOAST columns, the adapter can't just SET the partial row — that would overwrite existing data with nulls. Instead, it does GET-merge-SET:

```go
if len(unchangedToast) > 0 {
    existing, getErr := rdb.Get(ctx, key).Result()
    if getErr == nil {
        var existingRow map[string]interface{}
        if json.Unmarshal([]byte(existing), &existingRow) == nil {
            toastSet := make(map[string]struct{}, len(unchangedToast))
            for _, col := range unchangedToast {
                toastSet[col] = struct{}{}
            }
            for k, v := range row {
                if _, skip := toastSet[k]; !skip {
                    existingRow[k] = v
                }
            }
            finalRow = existingRow
        }
    }
}
```

It reads the current value from Redis, overlays the changed columns (skipping TOAST-unchanged ones), and writes back the merged result. If the GET fails (key doesn't exist), it SETs as-is — best effort.

Keys follow the pattern `{prefix}{pk}`, where the primary key is extracted from the event payload's `id` field (configurable via `--redis-id-column`). The adapter distinguishes connection errors (trigger reconnect) from command errors (log and skip), just like the pg_table adapter.

**Validation** pings Redis at startup to verify connectivity, failing fast if the Redis URL is misconfigured.

### 8.4 Embedding — pgvector

**File: `adapter/embedding/embedding.go`**

The embedding adapter turns CDC events into vector embeddings for semantic search. The pipeline per event is:

1. **Extract** text from configured columns (`--embedding-columns title,description`) by concatenating their values
2. **Skip-unchanged check**: Compare embedding columns between `row` and `old` — if nothing changed, skip the API call entirely
3. **Call** an OpenAI-compatible embedding API (`--embedding-api-url`) with retry and backoff
4. **UPSERT** the vector into a pgvector table using `::vector` cast

DELETE events delete the corresponding vector row. The skip-unchanged optimization is significant for high-traffic tables where most updates don't touch the text columns — it avoids unnecessary (and expensive) API calls:

```go
func (a *Adapter) embeddingColumnsChanged(p payload) bool {
    if !a.skipUnchanged { return true }
    if p.Op != "UPDATE" { return true }
    if p.Old == nil { return true }
    for _, col := range a.columns {
        newVal := fmt.Sprintf("%v", p.Row[col])
        oldVal := fmt.Sprintf("%v", p.Old[col])
        if newVal != oldVal { return true }
    }
    return false  // all embedding columns unchanged
}
```

The vector is stored as a string with a `::vector` PostgreSQL cast — no Go vector type needed, no pgvector Go library needed:

```go
upsertSQL := fmt.Sprintf(`
    INSERT INTO %s (source_id, source_table, content, embedding, event_id, updated_at)
    VALUES ($1, $2, $3, $4::vector, $5, NOW())
    ON CONFLICT (source_id) DO UPDATE SET
      content = EXCLUDED.content,
      embedding = EXCLUDED.embedding,
      event_id = EXCLUDED.event_id,
      updated_at = NOW()`, safeTable)
```

The `::vector` cast is handled by pgvector's PostgreSQL extension. The vector itself is formatted as a bracket-enclosed comma-separated string: `[0.123,0.456,...]`. This approach means pgcdc has zero pgvector Go dependencies — the extension handles parsing on the server side.

The adapter has the full resilience stack: circuit breaker (`--embedding-cb-failures`, `--embedding-cb-reset`), rate limiter (`--embedding-rate-limit`, `--embedding-rate-burst`), DLQ for failed events, and OpenTelemetry tracing. The embedding API retry logic follows the same 2xx/429/5xx/4xx classification as the webhook adapter.

The adapter maintains two connections: one HTTP client for the embedding API, and one pgx connection to the pgvector database. The pgx connection reconnects with backoff on connection loss, while the embedding API has per-event retry.

---

## Chapter 9: Stream Processing — The View Engine

### 9.1 What is a Streaming SQL View?

A streaming SQL view is a continuously running query over a stream of events. Instead of querying static data, the view processes events as they arrive and periodically emits aggregate results.

```sql
SELECT
    payload.customer_id,
    COUNT(*) as order_count,
    SUM(payload.total) as total_revenue
FROM pgcdc_events
WHERE operation = 'INSERT' AND channel = 'pgcdc:orders'
GROUP BY payload.customer_id
TUMBLING WINDOW 5m
```

This view counts orders and sums revenue per customer in 5-minute windows. Every 5 minutes, it emits one `VIEW_RESULT` event per customer that had orders in that window. These results are injected back into the bus on `pgcdc:_view:<name>` channels, where other adapters can consume them.

### 9.2 The SQL Parser

**File: `view/parser.go`**

The parser needs to handle standard SQL (SELECT, WHERE, GROUP BY, HAVING) plus non-standard window clauses (TUMBLING WINDOW, SLIDING WINDOW, SESSION WINDOW). The approach: extract the window clause with regex, then parse the remaining standard SQL with the TiDB SQL parser.

```go
var (
    tumblingRe = regexp.MustCompile(`(?i)\s+TUMBLING\s+WINDOW\s+(\S+)\s*$`)
    slidingRe  = regexp.MustCompile(`(?i)\s+SLIDING\s+WINDOW\s+(\S+)\s+SLIDE\s+(\S+)\s*$`)
    sessionRe  = regexp.MustCompile(`(?i)\s+SESSION\s+WINDOW\s+(\S+)\s*$`)
    latenessRe = regexp.MustCompile(`(?i)\s+ALLOWED\s+LATENESS\s+(\S+)`)
)
```

The regex strips `TUMBLING WINDOW 5m` from the end of the query, leaving valid SQL that TiDB's parser can handle. The TiDB parser produces an AST that the parser walks to extract SELECT items, aggregate functions, GROUP BY fields, WHERE predicates, and HAVING clauses.

The result is a `ViewDef` — a fully parsed view definition:

```go
type ViewDef struct {
    Name            string
    Query           string
    Emit            EmitMode
    MaxGroups       int
    SelectItems     []SelectItem
    FromTable       string     // must be "pgcdc_events"
    Where           Predicate
    GroupBy         []string
    Having          Predicate
    WindowSize      time.Duration
    WindowType      WindowType
    SlideSize       time.Duration
    SessionGap      time.Duration
    AllowedLateness time.Duration
}
```

### 9.3 Three Window Types

**Tumbling windows** (`view/window.go`): Fixed-size, non-overlapping windows. A 5-minute tumbling window covers 00:00-05:00, 05:00-10:00, etc. When the window closes, all aggregated groups are emitted and the window resets.

```
time ─────────────────────────────────────────────>
|  window 1  |  window 2  |  window 3  |
| 00:00-05:00| 05:00-10:00| 10:00-15:00|
| ●  ●    ●  | ●  ●  ●    |   ●  ●     |  (events)
| emit 3     | emit 3     | emit 2     |  (results)
```

The `emitGroups` method collects all groups in the current window, builds aggregate rows, runs HAVING filters, and emits results:

```go
func (tw *TumblingWindow) emitGroups(groups map[string]*groupState, windowStart, windowEnd time.Time) []event.Event {
    var rows []map[string]any
    for _, gs := range groups {
        row := tw.buildRow(gs)

        // Apply HAVING filter.
        if tw.def.Having != nil {
            if !tw.def.Having(EventMeta{}, row) {
                continue
            }
        }

        rows = append(rows, row)
    }
    // ... build result events from rows with window metadata
}
```

**Late event handling**: With `ALLOWED LATENESS 30s`, the window keeps recently closed windows for 30 seconds in a `closedWindows` map. If a late event arrives that belongs to a closed window, the window is found in the map, updated with the new value, and re-emitted. This handles events that arrive slightly out of order due to network latency or processing delays. After the lateness period expires, the closed window is evicted from memory.

**Sliding windows** (`view/sliding_window.go`): Overlapping windows. A `SLIDING WINDOW 10m SLIDE 2m` produces results every 2 minutes, each covering the last 10 minutes:

```
time ─────────────────────────────────────────────>
|<────── 10 min window ──────>|
      |<────── 10 min window ──────>|
            |<────── 10 min window ──────>|
|──2m──|──2m──|──2m──|──2m──|──2m──|──2m──|  (slide)
  emit   emit   emit   emit   emit   emit  (every 2 min)
```

Implemented as a series of sub-windows (one per slide interval) that are combined at emission time. Each event is added to the currently active sub-window. At each slide tick, the oldest sub-window is evicted, and all remaining sub-windows are merged to produce the result.

The interesting algorithmic challenge is cross-window aggregation for STDDEV. You can't simply sum standard deviations across sub-windows — standard deviation doesn't compose linearly. The implementation uses Welford's online algorithm, which maintains running mean and M2 (sum of squared differences from the running mean) that can be combined across sub-windows using the parallel algorithm:

```
combined_count = n1 + n2
combined_mean = (n1 * mean1 + n2 * mean2) / combined_count
delta = mean1 - mean2
combined_m2 = m2_1 + m2_2 + delta^2 * n1 * n2 / combined_count
combined_stddev = sqrt(combined_m2 / combined_count)
```

This formula is numerically stable because it works directly with means and M2 values rather than trying to reconstruct individual data points. The COUNT, SUM, AVG, MIN, and MAX aggregators compose trivially across sub-windows.

**Session windows** (`view/session_window.go`): Gap-based windows that close after a period of inactivity. A `SESSION WINDOW 30s` stays open as long as events keep arriving within 30 seconds of each other. When 30 seconds pass with no events for a group, the session window closes and emits results.

```
time ─────────────────────────────────────────────>
group A: ●  ● ●    ●  [30s gap]  ●  ●  [30s gap]
          session 1              session 2
          emit (4 events)        emit (2 events)

group B: ●     ●     ●     ●     ●     ●
          ────── one long session (no 30s gaps) ──────
```

Session windows are per-group: different groups can have different window boundaries depending on their activity patterns. A high-traffic customer might have one continuous session, while a low-traffic customer might generate many short sessions. The session reaper runs on a ticker and checks each group's last-event timestamp against the gap duration.

### 9.4 Aggregators

**File: `view/aggregate.go`**

Seven aggregate functions, each implementing the same interface:

```go
type Aggregator interface {
    Add(v any) bool
    Result() any
    Reset()
}
```

- **COUNT**: Counts non-nil values. For `COUNT(*)`, the caller passes a non-nil sentinel.
- **SUM**: Sums numeric values (strings are parsed as float64).
- **AVG**: Running average (sum/count).
- **MIN/MAX**: Track minimum/maximum values.
- **COUNT(DISTINCT field)**: Uses a `map[string]struct{}` to track unique values.
- **STDDEV**: Welford's online algorithm — numerically stable single-pass standard deviation computation.

Welford's algorithm is worth highlighting because the naive approach (compute mean, then compute variance) requires two passes over the data. Welford's computes both in a single pass:

```
For each new value x:
  count++
  delta = x - mean
  mean += delta / count
  delta2 = x - mean
  m2 += delta * delta2

stddev = sqrt(m2 / count)
```

This is essential for a streaming system where you see each value exactly once.

### 9.5 Loop Prevention

The view adapter emits results back into the bus as `VIEW_RESULT` events on channels prefixed with `pgcdc:_view:`. Without loop prevention, a view could consume its own output, creating an infinite loop.

The engine handles this with a simple prefix check:

```go
func (e *Engine) Process(ev event.Event) {
    if strings.HasPrefix(ev.Channel, "pgcdc:_view:") {
        return
    }
    // ... process event
}
```

Events from view channels are silently skipped. This is checked before any parsing or window processing, making it zero-cost for view-originated events.

**View configuration**: Views can be defined in three ways:

1. **YAML config file** (`views:` section) — best for complex, stable views
2. **CLI flag** (`--view-query 'name:SELECT ...'`) — best for quick experiments (repeatable, merged with YAML)
3. **Auto-created by the view adapter** — configured by the pipeline when it encounters view adapter configuration

CLI definitions win on name conflict with YAML definitions. Multiple `--view-query` flags can be specified for multiple views:

```sh
pgcdc listen --db postgres://... --detector wal --all-tables \
  --adapter stdout --adapter view \
  --view-query 'orders_per_min:SELECT COUNT(*) as cnt FROM pgcdc_events WHERE channel = '"'"'pgcdc:orders'"'"' TUMBLING WINDOW 1m' \
  --view-query 'revenue_5m:SELECT SUM(payload.total) as total FROM pgcdc_events WHERE channel = '"'"'pgcdc:orders'"'"' AND operation = '"'"'INSERT'"'"' TUMBLING WINDOW 5m'
```

**Emit modes**: Two modes control how results are emitted:

- `row` (default): One event per group per window. A view with 100 active groups emits 100 events per window flush.
- `batch`: One event per window flush containing all groups in a `rows` array. More efficient for high-cardinality GROUP BY.

**Max groups** (`max_groups`, default 10,000): Caps the number of distinct groups per window. When a new group arrives that would exceed the limit, it's silently dropped and counted in `pgcdc_view_groups_overflow_total`. This prevents a high-cardinality GROUP BY (e.g., GROUP BY user_id on a table with millions of users) from consuming unbounded memory.

---

## Chapter 10: Checkpointing and Exactly-Once

### 10.1 Checkpoint Store

**File: `checkpoint/checkpoint.go`**

The checkpoint store is a simple key-value interface:

```go
type Store interface {
    Load(ctx context.Context, slotName string) (lsn uint64, err error)
    Save(ctx context.Context, slotName string, lsn uint64) error
    Close() error
}
```

The default implementation uses a PostgreSQL table:

```sql
CREATE TABLE IF NOT EXISTS pgcdc_checkpoints (
    slot_name  TEXT PRIMARY KEY,
    lsn        BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
```

`Save` uses an upsert pattern: `INSERT ... ON CONFLICT (slot_name) DO UPDATE SET lsn = EXCLUDED.lsn`. This is idempotent — calling Save with the same slot name and LSN multiple times is safe.

The checkpoint store is also used by the Kafka protocol server's offset store, where keys take the form `kafka:{group}:{topic}:{partition}` and values are Kafka offsets (not WAL LSNs).

### 10.2 Cooperative Checkpointing

**File: `ack/tracker.go`**

Here's the problem: you have one WAL stream and three adapters (webhook, Kafka, S3). The WAL detector reads events and needs to tell PostgreSQL which position it has processed. But "processed" means different things for each adapter:

- The webhook adapter delivered event at LSN 100 but is still retrying LSN 95
- The Kafka adapter has published up to LSN 100
- The S3 adapter has buffered up to LSN 100 but hasn't flushed yet

If the detector reports LSN 100, PostgreSQL will recycle WAL up to that point. But the webhook adapter hasn't finished with LSN 95 yet. If pgcdc crashes and restarts, LSN 95 is gone — the webhook will never deliver it.

The solution is cooperative checkpointing: the detector only reports the **minimum** acknowledged LSN across all adapters.

```go
type Tracker struct {
    mu       sync.Mutex
    adapters map[string]uint64 // name -> highest acked LSN
}

func (t *Tracker) Ack(name string, lsn uint64) {
    t.mu.Lock()
    defer t.mu.Unlock()
    if lsn > t.adapters[name] {
        t.adapters[name] = lsn
    }
}

func (t *Tracker) MinAckedLSN() uint64 {
    t.mu.Lock()
    defer t.mu.Unlock()
    var min uint64
    first := true
    for _, lsn := range t.adapters {
        if first || lsn < min {
            min = lsn
            first = false
        }
    }
    return min
}
```

The WAL detector calls `MinAckedLSN()` when constructing its standby status update:

```go
if d.cooperativeLSNFn != nil {
    coopLSN := pglogrepl.LSN(d.cooperativeLSNFn())
    if coopLSN > 0 && coopLSN <= clientXLogPos {
        reportLSN = coopLSN
    }
}
```

Adapters that implement `adapter.Acknowledger` get an ack function injected:

```go
ackable.SetAckFunc(func(lsn uint64) {
    p.ackTracker.Ack(name, lsn)
})
```

Adapters that don't implement `Acknowledger` are auto-acked when the event is written to their subscriber channel. This means the checkpoint advances at the speed of the slowest acknowledging adapter, which is the correct behavior for exactly-once semantics.

Here's a concrete example showing why MinAckedLSN matters:

```
Timeline:
  Event A (LSN=100) → all 3 adapters receive it
  Event B (LSN=200) → all 3 adapters receive it
  Event C (LSN=300) → all 3 adapters receive it

  Adapter ack positions:
    webhook:  LSN=300 (delivered all three)
    kafka:    LSN=300 (published all three)
    s3:       LSN=100 (flushed A, B and C still buffered)

  MinAckedLSN() = 100
  → Detector reports LSN=100 to PostgreSQL
  → PostgreSQL can recycle WAL up to position 100
```

If pgcdc crashes now and restarts, it resumes from LSN=100. The webhook and Kafka adapters will re-process events B and C (which they already delivered — duplicates at the receiver), but the S3 adapter will correctly receive B and C again (which it lost when the buffer was destroyed by the crash). The system trades potential duplicates at fast adapters for guaranteed delivery at slow ones.

The auto-ack for non-Acknowledger adapters prevents simple adapters (stdout, file) from holding back the checkpoint. Since these adapters process events synchronously in their select loop, by the time the event is in their channel, it will be processed imminently — close enough to "delivered" for checkpoint purposes.

### 10.3 Snapshots

**Snapshot-first** (`--snapshot-first`): When starting with a new replication slot, the detector runs a table snapshot before beginning live WAL streaming. The snapshot uses the slot's exported snapshot name (`SET TRANSACTION SNAPSHOT 'xxx'`) to see exactly the data at the slot's consistent point. This ensures zero-gap delivery: the snapshot captures every row as of the slot's creation, and the WAL stream captures every change after.

**Incremental snapshots** (`--incremental-snapshot`): Signal-table triggered chunk-based snapshots that run alongside live WAL streaming. This is for backfilling existing data into adapters that were added after the initial deployment.

The workflow:

1. Insert a signal: `INSERT INTO pgcdc_signals (signal, payload) VALUES ('execute-snapshot', '{"table": "orders"}')`
2. The WAL detector picks up the signal via normal WAL replication
3. A background goroutine starts reading the table in chunks:

```sql
SELECT * FROM orders WHERE id > $1 ORDER BY id LIMIT 1000
```

4. Each chunk emits events: `SNAPSHOT_STARTED` → N × `SNAPSHOT` → `SNAPSHOT_COMPLETED`
5. Progress is persisted to `pgcdc_snapshot_progress` so the snapshot can resume after a crash
6. The live WAL stream continues simultaneously — no interruption

Events are emitted as `SNAPSHOT` operations on the `pgcdc:_snapshot` channel. The `SNAPSHOT` operation type lets adapters distinguish snapshot events from live CDC events — some adapters may want to handle them differently (e.g., batch inserts for snapshot vs incremental updates for live).

The chunk-based approach (`--snapshot-chunk-size`, default 1000) with optional delay between chunks (`--snapshot-chunk-delay`) prevents the snapshot from overwhelming the database or the pipeline. On a table with 10 million rows, reading everything in a single query would be catastrophic; reading 1000 rows at a time with a 100ms delay spreads the load over approximately 17 minutes.

---

## Chapter 11: When Things Go Wrong — DLQ and Errors

### 11.1 Typed Errors

**File: `pgcdcerr/errors.go`**

pgcdc defines typed errors for every failure mode that callers might need to branch on:

```go
var ErrBusClosed = errors.New("bus: already stopped")

type WebhookDeliveryError struct {
    EventID    string
    URL        string
    StatusCode int
    Retries    int
    Err        error
}

type DetectorDisconnectedError struct {
    Source string
    Err    error
}
```

Each error type includes context about what failed (event ID, URL, status code, retry count) and implements `Unwrap() error` for `errors.Is`/`errors.As` chaining. The convention is verb-prefixed wrapping: `fmt.Errorf("connect: %w", err)`, where the verb describes the failed action.

The full error type inventory:

```go
// Sentinel errors (use errors.Is)
var ErrBusClosed = errors.New("bus: already stopped")

// Structured errors (use errors.As to extract context)
WebhookDeliveryError       // EventID, URL, StatusCode, Retries, Err
DetectorDisconnectedError  // Source, Err
ExecProcessError           // Command, Err
EmbeddingDeliveryError     // EventID, Model, StatusCode, Retries, Err
NatsPublishError           // Subject, EventID, Err
OutboxProcessError         // Table, Err
S3UploadError              // Bucket, Key, Err
IncrementalSnapshotError   // Table, LastPK, Err
PluginError                // Plugin, Function, Err
MongoDBChangeStreamError   // Operation, Err
MySQLReplicationError      // Operation, Position, Err
SchemaRegistryError        // Subject, SchemaID, StatusCode, Err
KafkaServerError           // Operation, ClientID, Err
ViewError                  // ViewName, Err
ValidationError            // Adapter, Err
CircuitBreakerOpenError    // Adapter
RateLimitExceededError     // Adapter
```

This matters for three reasons. First, callers can use `errors.As` to extract details:

```go
var webhookErr *pgcdcerr.WebhookDeliveryError
if errors.As(err, &webhookErr) {
    log.Error("webhook failed", "url", webhookErr.URL, "status", webhookErr.StatusCode)
}
```

Second, the DLQ can record structured error information alongside the failed event — the error message in the `pgcdc_dead_letters` table includes all the context fields.

Third, the `Error()` string is human-readable. Each type formats a descriptive message — often with branching to include the most useful context (network error vs HTTP status code):

```go
func (e *WebhookDeliveryError) Error() string {
    if e.Err != nil {
        return fmt.Sprintf("webhook delivery failed for event %s to %s after %d retries: %v",
            e.EventID, e.URL, e.Retries, e.Err)
    }
    return fmt.Sprintf("webhook delivery failed for event %s to %s after %d retries (last status %d)",
        e.EventID, e.URL, e.Retries, e.StatusCode)
}
```

The `Unwrap()` method on each type returns the inner `Err`, enabling `errors.Is` to match wrapped errors. This means `errors.Is(err, context.Canceled)` works through the entire error chain — important for distinguishing "delivery failed" from "context was cancelled during delivery."

### 11.2 DLQ Interface

**File: `dlq/dlq.go`**

The dead letter queue catches events that failed delivery after all retries were exhausted:

```go
type DLQ interface {
    Record(ctx context.Context, ev event.Event, adapter string, err error) error
    Close() error
}
```

Three implementations:

**StderrDLQ** writes failed events as JSON lines to stderr. Simple, always available, easily piped to a file or log aggregator.

**PGTableDLQ** writes to a `pgcdc_dead_letters` table with columns for event ID, adapter name, error message, full event payload (JSONB), timestamp, and replay timestamp. The table is auto-created on first use.

```sql
CREATE TABLE IF NOT EXISTS pgcdc_dead_letters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id TEXT NOT NULL,
    adapter TEXT NOT NULL,
    error TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replayed_at TIMESTAMPTZ
)
```

**NopDLQ** discards everything (`--dlq none`).

Adapters with DLQ support (webhook, embedding, kafka) record failed events via `a.dlq.Record(ctx, ev, "webhook", err)`. The event is acked after DLQ recording — from the cooperative checkpoint's perspective, a DLQ'd event is "handled."

**Events that bypass the DLQ.** The DLQ catches delivery failures *after retries are exhausted*, but several failure paths skip the DLQ entirely. Understanding these is critical for production operation:

1. **Webhook 4xx (non-429) responses.** When the endpoint returns 400, 403, 404, etc., the `deliver()` method returns `nil` — success from the adapter's perspective (`webhook.go:328-333`). The event is acked normally. The response is logged at Error level ("non-retryable response, skipping event"), so it's visible in logs, but no DLQ record is created and no specific metric tracks 4xx skips. If your endpoint returns 4xx due to a bug, events silently vanish.

2. **Circuit breaker open + DLQ write failure.** When the circuit breaker is open, the webhook adapter attempts to DLQ the event but ignores the DLQ write result (`webhook.go:128-132`): the `_ = a.dlq.Record(...)` discards the error, then the event is acked regardless. If the DLQ backend (e.g., the PostgreSQL table) is also down, the event is lost with no trace.

3. **Non-Acknowledger auto-ack on channel send.** Adapters that don't implement the `Acknowledger` interface (stdout, file, exec, pg_table, SSE, WebSocket, gRPC, search, redis, S3, view) are auto-acked the moment the event is written to the wrapper's output channel (`pgcdc.go:wrapSubscription`, line 594). The event hasn't been *delivered* yet — it's sitting in a buffered channel waiting for the adapter to read it. If the adapter crashes or the process is killed between the auto-ack and actual delivery, the checkpoint has already advanced past that event. On restart, it won't be re-delivered.

4. **Cooperative checkpoint stall.** If an `Acknowledger` adapter (webhook, embedding, kafka) stops acking — perhaps it's stuck in an infinite retry loop or waiting on an unresponsive endpoint — `MinAckedLSN()` in the ack tracker freezes at that adapter's last acked position. There is no timeout on this. The checkpoint never advances, WAL accumulates on the PostgreSQL server, and eventually either backpressure kicks in (if enabled) or the disk fills. Monitor `pgcdc_ack_position{adapter}` per adapter to detect a stalled acknowledger.

### 11.3 DLQ Management CLI

**File: `cmd/dlq.go`**

The `pgcdc dlq` command provides operational tools:

- `pgcdc dlq list` — shows dead letter records, filterable by adapter and time range
- `pgcdc dlq replay` — replays dead letter records to their original adapter (or an override destination). Supports `--dry-run` for verification before committing
- `pgcdc dlq purge` — deletes dead letter records by age, adapter, or ID

Replay marks the `replayed_at` timestamp on each record, so you can distinguish replayed records from original failures. Adapter-specific overrides let you replay webhook events to a different URL (`--webhook-url`) or Kafka events to different brokers (`--kafka-brokers`).

Here's a typical operational workflow:

```sh
# 1. See what's in the DLQ
pgcdc dlq list --db postgres://localhost/mydb --adapter webhook
# Shows: 15 records, oldest 2h ago, newest 5m ago

# 2. Preview what replay would do
pgcdc dlq replay --db postgres://localhost/mydb --adapter webhook --dry-run
# Shows: would replay 15 records to https://api.example.com/hook

# 3. Replay to a fixed endpoint (maybe the original was down)
pgcdc dlq replay --db postgres://localhost/mydb --adapter webhook \
  --webhook-url https://api.example.com/hook-v2

# 4. Clean up old records
pgcdc dlq purge --db postgres://localhost/mydb --before 24h
```

The replay command reconstructs the original event from the stored JSONB payload, creates a temporary adapter with the specified (or default) configuration, and delivers the event. Each successful replay updates `replayed_at`; failed replays leave the record untouched for the next attempt.

### 11.4 Nack Window — Adaptive DLQ

**File: `dlq/window.go`**

The circuit breaker protects against sustained downstream failures, but it operates at the connection level — it opens after N consecutive failures and blocks all delivery until the reset timeout. The nack window operates at the event level, providing a finer-grained mechanism: it tracks recent delivery outcomes in a sliding window and pauses an adapter when the failure rate exceeds a threshold.

The data structure is a fixed-size ring buffer of boolean outcomes (ack = `true`, nack = `false`):

```go
type NackWindow struct {
    mu        sync.Mutex
    outcomes  []bool // ring buffer: true=ack, false=nack
    size      int
    head      int    // index of oldest entry
    count     int    // total outcomes in buffer (0..size)
    nackCount int    // count of nacks in buffer
    threshold int    // nack count to trigger pause
}
```

The `record` method handles the sliding window mechanics. When the buffer isn't full, outcomes are appended at the tail. When full, the oldest outcome at `head` is evicted — and if it was a nack, `nackCount` is decremented before the new outcome is written:

```go
func (w *NackWindow) record(ack bool) bool {
    w.mu.Lock()
    defer w.mu.Unlock()

    if w.count < w.size {
        idx := (w.head + w.count) % w.size
        w.outcomes[idx] = ack
        w.count++
    } else {
        if !w.outcomes[w.head] {
            w.nackCount--
        }
        w.outcomes[w.head] = ack
        w.head = (w.head + 1) % w.size
    }

    if !ack {
        w.nackCount++
    }

    return w.nackCount >= w.threshold
}
```

Both `RecordAck()` and `RecordNack()` return `true` if the threshold is currently exceeded, so the caller can act immediately. `Exceeded()` provides a read-only check. `Reset()` clears the window (useful when the downstream issue is resolved). `Stats()` returns `(total, nacks, threshold)` for debugging and metrics.

The window self-recovers. As successful deliveries (acks) slide in and failed deliveries (nacks) slide out, the nack count drops below the threshold and `Exceeded()` returns `false` again. No manual intervention needed — unlike the circuit breaker, which requires a successful half-open delivery to close, the nack window naturally recovers as the error burst moves out of the window.

**Wiring.** In `pgcdc.go`'s wrapper subscription, when `nackWindowSize > 0`, a `NackWindow` is created per adapter. After each delivery attempt:
- Success: `window.RecordAck()`
- Failure (after DLQ): `window.RecordNack()`

When `window.Exceeded()` returns `true`, events are skipped with a warning log and the `pgcdc_nack_window_exceeded_total{adapter}` counter increments. The `pgcdc_adapter_paused{adapter}` gauge shows `1` when an adapter is in the paused state, `0` when it recovers.

CLI: `--dlq-window-size 100` (default), `--dlq-window-threshold 50` (default). With these defaults, if 50 out of the last 100 deliveries failed, the adapter is paused. Once enough acks slide in to bring the nack count below 50, delivery resumes automatically.

The relationship between the circuit breaker and the nack window: the circuit breaker protects the downstream system (it stops sending requests when the endpoint is clearly down), while the nack window protects the pipeline (it stops wasting resources on an adapter that's failing intermittently). They complement each other — the circuit breaker handles "endpoint is down," the nack window handles "endpoint is flaky."

---

## Chapter 12: Encoding, Schema Evolution, and TOAST

### 12.1 Encoding

**File: `encoding/encoder.go`, `encoding/registry/client.go`**

By default, events are serialized as JSON. For high-throughput deployments, pgcdc supports Avro and Protobuf encoding for the Kafka and NATS adapters.

The encoding package provides an `Encoder` interface:

```go
type Encoder interface {
    Encode(ev event.Event) ([]byte, error)
    ContentType() string
}
```

**Avro** encoding uses `hamba/avro/v2`. The encoder generates an Avro schema from the event structure and encodes payloads in Avro binary format. Avro's strength is its compact binary representation and schema evolution support — you can add fields to the schema without breaking existing consumers.

**Protobuf** encoding follows the same pattern with Protocol Buffer binary format. Protobuf is even more compact than Avro for most payloads and has excellent multi-language support through generated code.

With Schema Registry integration, the wire format follows the Confluent convention:

```
[0x00] [4-byte schema ID, big-endian] [encoded payload]
```

The magic byte `0x00` identifies this as a Schema Registry-encoded message. The 4-byte schema ID lets the consumer look up the schema from the registry without needing to embed it in every message. This is the standard wire format used by the entire Confluent ecosystem — Kafka Connect, ksqlDB, Flink, and every Schema Registry-aware consumer library.

The Schema Registry client (`encoding/registry/client.go`) is a straightforward HTTP client:

```
POST /subjects/{subject}/versions          — register schema, returns schema ID
GET  /schemas/ids/{id}                     — lookup schema by ID
POST /compatibility/subjects/{subject}/versions/latest  — check compatibility
```

Schema caching ensures each unique schema is registered only once per subject. The cache is keyed by the schema fingerprint — if the pgcdc event schema changes (e.g., due to a column addition detected by the WAL detector), a new schema version is automatically registered.

**JSON** encoding is the default — events are serialized as JSON. This is the most human-readable and debuggable format, and the only one that doesn't require a Schema Registry. The tradeoff is size: JSON events are typically 3-5x larger than Avro or Protobuf equivalents.

### 12.2 Schema Evolution

**File: `detector/walreplication/walreplication.go` — diffRelation**

PostgreSQL sends a `RelationMessage` before each change message, describing the table's column layout. The WAL detector caches these messages and compares them on each new occurrence to detect schema changes:

```go
func diffRelation(oldRel, newRel *pglogrepl.RelationMessage) []schemaChange {
    oldCols := make(map[string]*pglogrepl.RelationMessageColumn, len(oldRel.Columns))
    for _, col := range oldRel.Columns {
        oldCols[col.Name] = col
    }
    newCols := make(map[string]*pglogrepl.RelationMessageColumn, len(newRel.Columns))
    for _, col := range newRel.Columns {
        newCols[col.Name] = col
    }

    // Check for added or type-changed columns
    for name, newCol := range newCols {
        if oldCol, exists := oldCols[name]; !exists {
            changes = append(changes, schemaChange{Type: "column_added", Column: name})
        } else if oldCol.DataType != newCol.DataType {
            changes = append(changes, schemaChange{Type: "column_type_changed", Column: name})
        }
    }
    // Check for removed columns
    for name := range oldCols {
        if _, exists := newCols[name]; !exists {
            changes = append(changes, schemaChange{Type: "column_removed", Column: name})
        }
    }
    return changes
}
```

When `--schema-events` is enabled, detected changes are emitted as `SCHEMA_CHANGE` events on the `pgcdc:_schema` channel.

The OID-to-type-name mapping (`detector/walreplication/oidmap.go`) provides human-readable type names for 40+ common PostgreSQL types:

```go
var oidToTypeName = map[uint32]string{
    16:   "bool",
    20:   "int8",
    23:   "int4",
    25:   "text",
    114:  "json",
    1043: "varchar",
    1114: "timestamp",
    1184: "timestamptz",
    2950: "uuid",
    3802: "jsonb",
    // ... 30 more
}
```

This is a static map — no database round-trip needed for the vast majority of types.

### 12.3 TOAST Columns

**File: `detector/walreplication/toastcache/cache.go`**

TOAST (The Oversized-Attribute Storage Technique) is PostgreSQL's mechanism for storing large column values. When a column value exceeds approximately 2KB, PostgreSQL stores it in a separate TOAST table. The actual row in the main table contains a pointer to the TOAST data.

Here's where this creates a problem for CDC: when a row is updated but a TOASTed column is not modified, PostgreSQL's logical replication protocol sends DataType `'u'` (unchanged) for that column. Without `REPLICA IDENTITY FULL` (which forces PostgreSQL to include all column values in every WAL message), you get nulls for unchanged large columns.

The TOAST cache is an LRU cache that solves this without requiring `REPLICA IDENTITY FULL`:

```go
type Cache struct {
    maxEntries int
    items      map[key]*entry
    relIndex   map[uint32][]*entry
    head       *entry // most recently used
    tail       *entry // least recently used
}
```

The cache stores full rows keyed by `(RelationID, PK)`. The lifecycle follows the WAL event types:

- **INSERT**: The row always has full column values. The cache stores the complete row.
- **UPDATE**: If there are unchanged TOAST columns, the cache looks up the previous row and backfills the missing values. Then it updates the cache with the merged result.
- **DELETE**: The cached entry for this PK is evicted.
- **TRUNCATE / schema change**: All entries for the relation are evicted.

```go
case "UPDATE":
    if len(unchangedCols) > 0 && pk != "" {
        if cached, ok := tc.Get(relationID, pk); ok {
            for _, col := range unchangedCols {
                row[col] = cached[col]
            }
            unchangedCols = nil
            metrics.ToastCacheHits.Inc()
        } else {
            metrics.ToastCacheMisses.Inc()
        }
    }
    if pk != "" {
        tc.Put(relationID, pk, copyMap(row))
    }
```

On cache miss (the row wasn't seen via INSERT or a previous UPDATE), the unchanged columns remain null, and the `_unchanged_toast_columns` array in the payload tells downstream consumers which columns were unresolved. Search and Redis adapters use this metadata to avoid overwriting existing data with nulls.

The LRU eviction strategy is standard doubly-linked list with O(1) access, insert, and eviction. Each cache entry is both in the hash map (for key lookup) and in the doubly-linked list (for LRU ordering). Access moves the entry to the head of the list; eviction removes from the tail.

The `relIndex` secondary index enables efficient bulk eviction on TRUNCATE or schema change. It maps each `RelationID` to the list of entries for that relation:

```go
type Cache struct {
    maxEntries int
    items      map[key]*entry    // primary index: (RelationID, PK) → entry
    relIndex   map[uint32][]*entry  // secondary: RelationID → entries
    head       *entry
    tail       *entry
}
```

Without `relIndex`, evicting all entries for a relation on TRUNCATE would require a full scan of the `items` map — O(n) where n is the total cache size. With it, the operation is O(k) where k is the number of entries for that specific relation.

The default maximum cache size (`--toast-cache-max-entries`, default 100,000) is chosen to balance memory usage against cache hit rate. Each entry stores a `map[string]any` (the full row data), so memory usage depends on row width. For a table with 10 columns averaging 50 bytes each, 100K entries consume roughly 50MB. Metrics (`pgcdc_toast_cache_hits_total`, `pgcdc_toast_cache_misses_total`, `pgcdc_toast_cache_entries`) let you monitor whether the cache is effective and appropriately sized.

---

## Chapter 13: Observability

### 13.1 Prometheus Metrics

**File: `metrics/metrics.go`**

pgcdc uses `promauto` for zero-config metric registration. Every metric follows the `pgcdc_` prefix convention and is registered automatically when the metrics package is imported.

Key metrics by category:

**Pipeline throughput:**
- `pgcdc_events_received_total` — events entering the bus
- `pgcdc_events_delivered_total{adapter}` — events successfully delivered per adapter
- `pgcdc_events_dropped_total{adapter}` — events dropped in fast bus mode

**Adapter health:**
- `pgcdc_webhook_retries_total`, `pgcdc_webhook_duration_seconds`
- `pgcdc_kafka_published_total`, `pgcdc_nats_published_total`
- `pgcdc_s3_flushes_total{status}`, `pgcdc_s3_flush_size`
- `pgcdc_circuit_breaker_state{adapter}` — 0=closed, 1=open, 2=half_open

**Checkpointing:**
- `pgcdc_checkpoint_lsn` — last persisted WAL position
- `pgcdc_ack_position{adapter}` — per-adapter acknowledged position
- `pgcdc_cooperative_checkpoint_lsn` — minimum across all adapters
- `pgcdc_slot_lag_bytes` — WAL bytes retained by the replication slot

**Backpressure:**
- `pgcdc_backpressure_state` — 0=green, 1=yellow, 2=red
- `pgcdc_backpressure_throttle_duration_seconds`
- `pgcdc_backpressure_load_shed_total{adapter}`

**Resilience:**
- `pgcdc_panics_recovered_total{component}`
- `pgcdc_config_reloads_total`, `pgcdc_config_reload_errors_total`
- `pgcdc_transform_dropped_total{adapter}`, `pgcdc_transform_errors_total{adapter}`
- `pgcdc_dlq_records_total{adapter}`

Metrics are exposed at `/metrics` on the main HTTP server, or on a separate server via `--metrics-addr` (useful when you don't want to expose metrics on the same port as SSE/WebSocket endpoints).

### 13.2 Health Checks

**File: `health/health.go`**

The health checker tracks per-component status:

```go
type Checker struct {
    mu         sync.RWMutex
    components map[string]Status // "detector", "bus", "webhook", etc.
}

const (
    StatusUp       Status = "up"
    StatusDown     Status = "down"
    StatusDegraded Status = "degraded"
)
```

It implements `http.Handler` for the `/healthz` endpoint:

```go
func (c *Checker) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
    overall := StatusUp
    for name, status := range c.components {
        if status == StatusDown { overall = StatusDown }
        if status == StatusDegraded && overall == StatusUp { overall = StatusDegraded }
    }
    if overall == StatusDown {
        w.WriteHeader(http.StatusServiceUnavailable)
    }
    json.NewEncoder(w).Encode(response{Status: overall, Components: comps})
}
```

Returns 200 when all components are up, 503 when any component is down. The JSON response includes per-component status for debugging:

```json
{
  "status": "up",
  "components": {
    "detector": "up",
    "bus": "up",
    "webhook": "up",
    "s3": "up",
    "backpressure": "degraded"
  }
}
```

For Kubernetes deployments, a separate `ReadinessChecker` provides the `/readyz` endpoint:

```go
type ReadinessChecker struct {
    mu    sync.RWMutex
    ready bool
}

func (r *ReadinessChecker) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
    r.mu.RLock()
    ready := r.ready
    r.mu.RUnlock()
    if !ready {
        w.WriteHeader(http.StatusServiceUnavailable)
    }
    json.NewEncoder(w).Encode(map[string]bool{"ready": ready})
}
```

The distinction: liveness (`/healthz`) tells Kubernetes whether to restart the pod; readiness (`/readyz`) tells Kubernetes whether to send traffic to the pod. During startup (while the detector is connecting and initial validation is running), the pod is alive but not ready — Kubernetes shouldn't kill it, but shouldn't route traffic to it either. Once the pipeline is fully started, readiness is set to true.

A typical Kubernetes liveness/readiness configuration:

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 15
readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 5
```

### 13.3 OpenTelemetry Tracing

**File: `tracing/tracing.go`**

pgcdc supports distributed tracing via OpenTelemetry. The setup is straightforward:

```go
func Setup(ctx context.Context, cfg Config, logger *slog.Logger) (trace.TracerProvider, func(), error) {
    if cfg.Exporter == "" || cfg.Exporter == "none" {
        return noop.NewTracerProvider(), func() {}, nil
    }

    // Create exporter (stdout or OTLP gRPC)
    // Create tracer provider with batched exporter and ParentBased sampler
    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(exporter),
        sdktrace.WithResource(res),
        sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
    )
    // Set global propagator for W3C TraceContext
    otel.SetTextMapPropagator(propagation.TraceContext{})
    return tp, shutdown, nil
}
```

When tracing is "none" (the default), a noop provider is returned — zero allocations, zero overhead on the hot path.

When enabled, the system creates two types of spans:

**PRODUCER spans** are created by the detector for each event. The span context is stored on the event and propagated to adapters.

**CONSUMER spans** are created by adapters (currently webhook) for each delivery. They link back to the PRODUCER span via `trace.Link`, creating a trace that shows the full journey of an event from database change to final delivery.

For Kafka, the adapter injects trace context into Kafka record headers using a custom carrier (`tracing/carrier.go`):

```go
type KafkaCarrier struct {
    Headers *[]kgo.RecordHeader
}

func (c KafkaCarrier) Set(key, value string) {
    for i, h := range *c.Headers {
        if h.Key == key {
            (*c.Headers)[i].Value = []byte(value)
            return
        }
    }
    *c.Headers = append(*c.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c KafkaCarrier) Get(key string) string {
    for _, h := range *c.Headers {
        if h.Key == key {
            return string(h.Value)
        }
    }
    return ""
}
```

This adapter implements `propagation.TextMapCarrier`, which is the interface that OpenTelemetry's propagator expects. The Kafka adapter injects via `otel.GetTextMapPropagator().Inject(ctx, KafkaCarrier{Headers: &headers})`, and downstream Kafka consumers can extract with the matching Extract call. The trace context appears as `traceparent` and `tracestate` headers in the Kafka record.

The webhook adapter injects W3C `traceparent` headers into HTTP requests:

```go
tracing.InjectHTTP(ctx, req.Header)
```

This is simpler because HTTP headers are already a `TextMapCarrier` (they implement `Get` and `Set`). The webhook receiver can extract the trace context using any OpenTelemetry SDK and see the full pipeline latency in their tracing system — from the WAL detector's event creation through the bus, transform pipeline, and webhook delivery.

The end-to-end trace for a single event typically looks like:

```
[pgcdc.detect] ──link──> [pgcdc.adapter.deliver]
   (detector)                (webhook/kafka/nats)
   PRODUCER span            CONSUMER span
   ~0.1ms                   ~50-200ms (network)
```

The link (rather than parent-child) relationship preserves the fan-out semantics: if one event is delivered to three adapters, each adapter creates its own CONSUMER span linked to the same PRODUCER span. In your tracing UI (Jaeger, Grafana Tempo, Honeycomb), you can follow the event from creation to all its destinations.

### 13.4 Event Inspector

**File: `inspect/inspector.go`, `inspect/ring.go`, `inspect/handler.go`**

Tracing shows you the journey of individual events through distributed systems. Metrics show you aggregate counts and latencies. But sometimes you need to answer a simpler question: "what does the event look like right now, at this point in the pipeline?" The inspector is a ring-buffer event sampler that captures events at three tap points:

```go
type TapPoint string

const (
    TapPostDetector  TapPoint = "post-detector"  // after detector emits, before transforms
    TapPostTransform TapPoint = "post-transform" // after transforms run, before route filtering
    TapPreAdapter    TapPoint = "pre-adapter"     // just before adapter delivery
)
```

Each tap point has its own ring buffer (default 100 entries per point). The ring buffer is a pre-allocated slice with head/count tracking — the same structure as the nack window, applied to events instead of booleans:

```go
type ringBuffer struct {
    mu    sync.Mutex
    items []event.Event
    size  int
    head  int
    count int

    subsMu sync.RWMutex
    subs   map[*subscriber]struct{}
}

func (rb *ringBuffer) add(ev event.Event) {
    rb.mu.Lock()
    idx := (rb.head + rb.count) % rb.size
    if rb.count >= rb.size {
        rb.head = (rb.head + 1) % rb.size
    } else {
        rb.count++
    }
    rb.items[idx] = ev
    rb.mu.Unlock()

    // Notify real-time subscribers (non-blocking).
    rb.subsMu.RLock()
    for sub := range rb.subs {
        select {
        case sub.ch <- ev:
        default: // slow subscriber — drop
        }
    }
    rb.subsMu.RUnlock()
}
```

The `Record` method is a simple append to the ring buffer. When the buffer is full, the oldest event is overwritten — no allocation, no growing. The `Snapshot` method returns the last N events from the buffer, and `Subscribe` returns a channel that receives events in real-time (with a non-blocking send, so slow subscribers don't block the pipeline).

The inspector is enabled via `--inspect-buffer N` (where N is the ring buffer size per tap point). When not enabled, the inspector pointer is nil and all tap points are no-ops — zero overhead on the hot path.

**HTTP endpoints.** When enabled, two endpoints are mounted on the existing HTTP server:

`GET /inspect?point=post-detector&limit=10` returns a JSON snapshot of the last 10 events captured at the `post-detector` tap point:

```json
{
  "point": "post-detector",
  "count": 10,
  "events": [ ... ]
}
```

`GET /inspect/stream?point=post-transform` opens an SSE connection that streams events in real-time as they pass through the `post-transform` tap point. The SSE handler uses the `Subscribe` method to get a channel and forwards events as `data:` frames.

The tap points let you compare events at different pipeline stages. Capture `post-detector` to see the raw event as the detector produced it. Capture `post-transform` to see it after transforms ran (columns dropped, fields renamed, payload reshaped). Capture `pre-adapter` to see the final form before delivery. Comparing the three shows you exactly what each pipeline stage did to the event — invaluable when debugging a transform chain that produces unexpected output.

This is a debug/development tool. The ring buffer is fixed-size and events are not persisted. In production, the buffer size should be small (or the inspector disabled entirely) to minimize memory overhead. The real-time SSE stream is useful for development — connect it while iterating on transform expressions to see immediate results.

---

## Chapter 14: The Plugin System — Wasm Extensions

### 14.1 Why Wasm?

**File: `plugin/wasm/runtime.go`**

pgcdc uses WebAssembly (Wasm) for plugins because of three properties:

**Pure Go**: The Extism Wasm runtime is written in pure Go — no CGo, no C compiler dependency, no cross-compilation complexity. This keeps pgcdc's build simple and portable.

**Sandboxed**: Wasm plugins run in a sandboxed environment. A plugin can't access the filesystem, make network calls, or interact with the host system except through explicitly provided host functions. A buggy or malicious plugin can't compromise the host process.

**Polyglot**: Plugins can be written in any language that compiles to Wasm — Rust, Go, Python, TypeScript, C/C++, Zig. The Extism PDK (Plugin Development Kit) provides the glue for each language.

When no plugins are configured, the Wasm runtime is never initialized — zero overhead.

```go
type Runtime struct {
    mu      sync.Mutex
    modules map[string]*extism.CompiledPlugin
    logger  *slog.Logger
}

func (r *Runtime) LoadModule(ctx context.Context, path string, hostFns []extism.HostFunction) (*extism.CompiledPlugin, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    if compiled, ok := r.modules[path]; ok {
        return compiled, nil
    }
    manifest := extism.Manifest{
        Wasm: []extism.Wasm{extism.WasmFile{Path: path}},
    }
    compiled, err := extism.NewCompiledPlugin(ctx, manifest, extism.PluginConfig{
        EnableWasi: true,
    }, hostFns)
    // ... cache and return
}
```

Modules are compiled once and cached. Instances are created from compiled modules on demand.

### 14.2 Four Extension Points

Plugins can extend pgcdc at four points:

**Transform plugins** (`plugin/wasm/transform.go`): Receive an event as JSON (or Protobuf), return a modified event. Called in the transform chain alongside built-in transforms. Configured via `--plugin-transform name:path.wasm`. The plugin exports a `transform` function that takes the serialized event and returns the modified event (or an empty result to drop).

**Adapter plugins** (`plugin/wasm/adapter.go`): Implement the full adapter interface. The plugin exports a `deliver` function called for each event. The host manages the event loop — the plugin only sees one event at a time. Configured via `--plugin-adapter name:path.wasm`. The adapter plugin also implements `adapter.Acknowledger` — each delivered event is acked automatically.

**DLQ plugins** (`plugin/wasm/dlq.go`): Implement the DLQ interface. The plugin exports a `record` function that receives the failed event, adapter name, and error message. Configured via `--dlq plugin --dlq-plugin path.wasm`. Use this to send dead letters to a custom destination — a Slack webhook, an external queue, a custom database.

**Checkpoint store plugins** (`plugin/wasm/checkpoint.go`): Implement the checkpoint store interface. The plugin exports `load` and `save` functions for LSN persistence. Configured via `--checkpoint-plugin path.wasm`. Use this to store checkpoints in etcd, Redis, DynamoDB, or any other external store.

All four share the same set of host functions — functions that the plugin can call back into the host process:

- `pgcdc_log(level, message)` — write to the host's slog logger at the specified level
- `pgcdc_metric_inc(name, labels)` — increment a Prometheus counter by name, with optional label key-value pairs
- `pgcdc_http_request(method, url, headers, body)` — make an HTTP request from the host and return the response

The `pgcdc_http_request` host function is important because Wasm plugins can't make network calls directly — the sandbox prevents it. By routing HTTP through the host, plugins can call external APIs while the host retains control over network access.

Events are serialized as JSON by default, with an opt-in Protobuf serialization (`plugin/proto/event.proto`) for plugins that need lower overhead. The Protobuf format is more compact and faster to parse but requires the plugin to be compiled with the proto definition. JSON is universal — any language that compiles to Wasm can parse JSON.

Metrics are tracked per-plugin: `pgcdc_plugin_calls_total{plugin,function}`, `pgcdc_plugin_duration_seconds{plugin,function}`, and `pgcdc_plugin_errors_total{plugin,function}`. These help identify slow or failing plugins without inspecting plugin internals.

**Implementation note**: All four Wasm wrapper types embed a shared `basePlugin` struct (`plugin/wasm/base.go`) that holds the common state (`name`, `plugin` instance, `encoding`, `logger`) and implements `call(fnName string, input []byte) ([]byte, error)`. The `call` method records metrics uniformly for all plugin types. Previously this code was duplicated across the four files; now each wrapper type is ~30 lines of type-specific logic on top of `basePlugin`.

### 14.3 The LSN Boundary

There's a subtle correctness issue when events cross the Wasm serialization boundary. The LSN field has `json:"-"` — it's excluded from JSON serialization. If a transform plugin receives a serialized event, modifies it, and returns the result, the LSN is lost.

The Wasm transform wrapper preserves the original LSN across the plugin call:

```go
func (t *WasmTransform) Transform(ev event.Event) (event.Event, error) {
    origLSN := ev.LSN // save before serialization
    // ... serialize, call plugin, deserialize
    result.LSN = origLSN // restore after deserialization
    return result, nil
}
```

This is critical for cooperative checkpointing. Without LSN preservation, the ack tracker would see LSN=0 for plugin-transformed events, and the cooperative checkpoint would never advance.

---

## Chapter 14b: The Registry — Component Self-Registration

**File: `registry/registry.go`**

The registry is the glue between CLI flags, configuration, and component construction. Every adapter, detector, and transform is registered at startup via `init()` functions in `cmd/register_*.go` files. The listen command then calls `registry.CreateAdapter(name, ctx)` or `registry.CreateDetector(name, ctx)` to instantiate them — no hardcoded switch.

**Adapter registration:**

```go
registry.RegisterAdapter(registry.AdapterEntry{
    Name:        "webhook",
    Description: "HTTP POST to a webhook URL",
    Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
        a := webhook.New(ctx.Cfg.Webhook.URL, ...)
        mwCfg := webhookMiddlewareConfig(ctx.Cfg)
        return registry.AdapterResult{
            Adapter:          a,
            MiddlewareConfig: &mwCfg,
        }, nil
    },
    BindFlags: func(f *pflag.FlagSet) {
        f.StringP("url", "u", "", "webhook destination URL")
        // ...
    },
    ViperKeys: [][2]string{
        {"url", "webhook.url"},
        // ...
    },
})
```

`AdapterContext` carries shared resources: `Cfg config.Config`, `Logger *slog.Logger`, `Ctx context.Context`, `KafkaEncoder`, `NatsEncoder`.

`AdapterResult` carries typed outputs: `Adapter`, `MiddlewareConfig *middleware.Config`, `SSEBroker *sse.Broker`, `WSBroker *ws.Broker`. Typed fields replace the old `Extra map[string]any`.

**Detector registration:**

```go
registry.RegisterDetector(registry.DetectorEntry{
    Name:   "wal",
    Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
        det := walreplication.New(...)
        // ... all WAL-specific Set* calls
        return registry.DetectorResult{
            Detector:        det,
            CheckpointStore: cpStore, // non-nil when --persistent-slot
            Cleanup:         cleanup, // deferred (e.g. progress store close)
        }, nil
    },
})
```

`DetectorContext` carries: `Cfg`, `Logger`, `Ctx`, `TracerProvider trace.TracerProvider`, `WasmRT any`.

**Transform registration:**

```go
registry.RegisterTransform(registry.TransformEntry{
    Name: "drop_columns",
    Create: func(spec config.TransformSpec) transform.TransformFunc {
        return transform.DropColumns(spec.Columns...)
    },
})
```

`registry.SpecToTransform(spec)` looks up the factory by `spec.Type` and calls it. Used by both the hot-reload path (`cmd/reload.go`) and initial pipeline construction.

**`pgcdc list`** uses `registry.Adapters()`, `registry.Detectors()`, and `registry.Transforms()` to enumerate what's registered in the current build — slim builds missing `no_kafka` show no kafka entry.

---

## Chapter 14c: Connector Specs and Introspection

**File: `registry/spec.go`**

The registry knows what components exist and how to create them. But it doesn't tell you what a component needs — what flags it requires, what their types are, what values are valid. That's the job of connector specs.

Every adapter and detector registration can include a `Spec` field — a slice of `ParamSpec` structs describing the component's configuration parameters:

```go
type ParamSpec struct {
    Name        string   // "kafka-brokers"
    Type        string   // "string", "int", "duration", "bool", "[]string", "float64"
    Default     any      // default value
    Required    bool     // is this required?
    Description string   // human-readable description
    Validations []string // "min:1", "max:100", "oneof:a,b,c"
}
```

The `Validations` field is a list of human-readable rules (not enforced by the spec system — enforcement happens in `internal/config/validate.go`). They serve as documentation: when `pgcdc describe kafka` shows `Validations: min:1` on the `kafka-brokers` parameter, you know at least one broker is required.

`GetConnectorSpec(name)` searches adapters first, then detectors, returning a `ConnectorSpec` with the component's metadata:

```go
type ConnectorSpec struct {
    Name        string      // connector name
    Type        string      // "adapter" or "detector"
    Description string      // human-readable description
    Params      []ParamSpec // configuration parameters
}

func GetConnectorSpec(name string) *ConnectorSpec {
    if e, ok := GetAdapter(name); ok {
        return &ConnectorSpec{
            Name:        e.Name,
            Type:        "adapter",
            Description: e.Description,
            Params:      e.Spec,
        }
    }
    if e, ok := GetDetector(name); ok {
        return &ConnectorSpec{
            Name:        e.Name,
            Type:        "detector",
            Description: e.Description,
            Params:      e.Spec,
        }
    }
    return nil
}
```

The search order (adapters first, then detectors) means that if an adapter and a detector happen to share a name, the adapter wins. In practice this doesn't happen — adapter and detector names are distinct by convention.

The `pgcdc describe <name>` command (`cmd/describe.go`) uses this to print a formatted table:

```
$ pgcdc describe kafka
kafka (adapter)
Publish events to Kafka topics

NAME                TYPE       DEFAULT    REQUIRED  VALIDATIONS  DESCRIPTION
----                ----       -------    --------  -----------  -----------
kafka-brokers       []string   -          yes       min:1        Kafka broker addresses
kafka-topic         string     -          no        -            Override topic name
kafka-encoding      string     json       no        oneof:...    Event encoding format
...
```

This enables discoverability — you can explore what a connector needs without reading source code or scrolling through `--help` output. The `tabwriter`-based formatting aligns columns cleanly regardless of value lengths. Unknown connector names produce a clear error: `"unknown connector \"foo\": not found in adapters or detectors"`.

The spec system also enables programmatic introspection. The multi-pipeline server (`cmd/serve.go`) could use specs to validate pipeline configurations before starting them, and tooling (IDEs, config generators) can consume specs to provide autocomplete and validation for pgcdc YAML config files.

---

## Chapter 15: The Pipeline — How It All Fits Together

### 15.1 Construction

**File: `pgcdc.go`**

The Pipeline is constructed with functional options:

```go
func NewPipeline(det detector.Detector, opts ...Option) *Pipeline {
    p := &Pipeline{
        detector:        det,
        shutdownTimeout: 5 * time.Second,
    }
    for _, o := range opts {
        o(p)
    }
    // ... defaults, health checker, wiring
}
```

Options include `WithAdapter`, `WithBusBuffer`, `WithBusMode`, `WithLogger`, `WithCheckpointStore`, `WithDLQ`, `WithRoute`, `WithTransform`, `WithCooperativeCheckpoint`, `WithBackpressure`, `WithTracerProvider`, `WithSkipValidation`, `WithShutdownTimeout`, and `WithMiddlewareConfig(adapterName, middleware.Config)`.

`WithMiddlewareConfig` stores per-adapter middleware configuration (retry, circuit breaker, rate limit) in `p.middlewareConfigs`. `startAdapter` reads this map when building the middleware chain for Deliverer adapters.

During construction, the pipeline wires optional interfaces. For Deliverer and Batcher adapters, DLQ and tracer are handled by the middleware/batch runner and are skipped here:

```go
for _, a := range p.adapters {
    p.health.Register(a.Name())
    isMiddlewareManaged := false
    if _, ok := a.(adapter.Deliverer); ok { isMiddlewareManaged = true }
    if _, ok := a.(adapter.Batcher); ok  { isMiddlewareManaged = true }

    if !isMiddlewareManaged {
        if da, ok := a.(DLQAware); ok && p.dlq != nil {
            da.SetDLQ(p.dlq)
        }
    }
}
```

Cooperative checkpointing setup registers each adapter with the ack tracker and injects ack functions:

```go
if p.cooperativeCheckpoint {
    p.ackTracker = ack.New()
    for _, a := range p.adapters {
        p.ackTracker.Register(a.Name(), 0)
        if ackable, ok := a.(adapter.Acknowledger); ok {
            name := a.Name()
            ackable.SetAckFunc(func(lsn uint64) {
                p.ackTracker.Ack(name, lsn)
            })
        } else {
            p.autoAckAdapters[a.Name()] = true
        }
    }
}
```

Wrapper configs are pre-allocated for all adapters so that hot-reload doesn't need to modify the map after construction (preventing a data race between Run and Reload).

### 15.2 The Run Method

The Run method is the orchestrator — it wires everything together and manages the lifecycle of the entire pipeline. Let's walk through it step by step:

```go
func (p *Pipeline) Run(ctx context.Context) error {
    if p.checkpointStore != nil {
        defer func() { _ = p.checkpointStore.Close() }()
    }
```

The checkpoint store close is deferred first, ensuring it's the last thing that runs — even after adapter draining. This is important because adapter Drain methods may still need to ack LSNs during their flush.

```go
    // 1. Startup validation
    if err := p.Validate(ctx); err != nil {
        return fmt.Errorf("startup validation: %w", err)
    }
```

`Validate` iterates all adapters, calling `Validate(ctx)` on those that implement `adapter.Validator`. If any fails, startup is aborted with a clear error. This catches misconfigurations (wrong S3 bucket, unreachable webhook URL, invalid Kafka brokers) before any events flow. The `--skip-validation` flag bypasses this for environments where validation targets may not be reachable during deployment.

```go
    // 2-4. Wire optional features into detector
    if p.cooperativeCheckpoint && p.ackTracker != nil {
        if cc, ok := p.detector.(CooperativeCheckpointer); ok {
            cc.SetCooperativeLSN(p.ackTracker.MinAckedLSN)
        }
    }
    if p.backpressureCtrl != nil {
        if t, ok := p.detector.(Throttleable); ok {
            t.SetBackpressureController(p.backpressureCtrl)
        }
    }
```

The `CooperativeCheckpointer` and `Throttleable` interfaces are defined in `pgcdc.go`, not in the detector package. This avoids a circular import — the WAL detector doesn't need to know about the Pipeline, and the Pipeline doesn't need to know about the WAL detector's internals. The interfaces bridge the gap with minimal coupling.

```go
    g, gCtx := errgroup.WithContext(ctx)
```

This single `errgroup` manages every goroutine in the pipeline. The derived context `gCtx` is cancelled when any goroutine returns an error. This is the cascading shutdown mechanism — one failure brings down everything.

```go
    // 5-7. Start bus, detector, backpressure
    p.safeGo(g, "bus", func() error {
        p.health.SetStatus("bus", health.StatusUp)
        defer p.health.SetStatus("bus", health.StatusDown)
        return p.bus.Start(gCtx)
    })
    p.safeGo(g, p.detector.Name(), func() error {
        p.health.SetStatus("detector", health.StatusUp)
        defer p.health.SetStatus("detector", health.StatusDown)
        return p.detector.Start(gCtx, p.bus.Ingest())
    })
```

Each goroutine updates its health status on entry and exit. This means the `/healthz` endpoint reflects the actual state of each component in real-time.

```go
    // 8-9. Wire reinjectors and start adapters
    for _, a := range p.adapters {
        if r, ok := a.(adapter.Reinjector); ok {
            r.SetIngestChan(p.bus.Ingest())
        }
    }
    for _, a := range p.adapters {
        sub, _ := p.subscribeAdapter(a.Name())
        p.safeGo(g, a.Name(), func() error {
            p.health.SetStatus(a.Name(), health.StatusUp)
            defer p.health.SetStatus(a.Name(), health.StatusDown)
            return p.startAdapter(gCtx, a, sub)
        })
    }
```

Reinjector wiring happens before adapter start. The view adapter needs the ingest channel so it can send `VIEW_RESULT` events back into the bus.

`subscribeAdapter` creates a subscriber channel on the bus, wraps it with the wrapper goroutine (for transforms and route filtering), and returns the output channel. The wrapper goroutine loads its config from the `atomic.Pointer` on every event, enabling hot-reload.

`startAdapter` selects the execution path via type assertion:

```go
// Path 1: Batcher → batch.Runner (timer/size flush, DLQ, ack)
if b, ok := a.(adapter.Batcher); ok { ... }

// Path 2: Deliverer → middleware chain (retry, CB, RL, DLQ, tracing, ack)
if d, ok := a.(adapter.Deliverer); ok {
    chain := middleware.Build(a.Name(), cfg, opts...)
    return middleware.StartLoop(ctx, events, chain, p.logger)
}

// Path 3: Legacy → a.Start() directly
return a.Start(ctx, events)
```

```go
    err := g.Wait()
    p.drainAdapters()

    if err != nil && gCtx.Err() != nil {
        return nil // clean shutdown
    }
    return err
}
```

After `g.Wait()` returns (all goroutines have exited), adapters are drained. The final check `err != nil && gCtx.Err() != nil` distinguishes between a signal-initiated shutdown (return nil) and an unexpected error (return the error).

Everything runs in a single `errgroup`. The errgroup's context is derived from the input context, so cancelling either tears down the entire pipeline. The `safeGo` wrapper adds panic recovery:

```go
func (p *Pipeline) safeGo(g *errgroup.Group, name string, fn func() error) {
    g.Go(func() (err error) {
        defer func() {
            if r := recover(); r != nil {
                metrics.PanicsRecovered.WithLabelValues(name).Inc()
                p.logger.Error("panic recovered",
                    "component", name, "panic", r,
                    "stack", string(debug.Stack()),
                )
                err = fmt.Errorf("panic in %s: %v", name, r)
            }
        }()
        return fn()
    })
}
```

A panic in any goroutine is recovered, logged with a full stack trace, counted in metrics, and returned as an error. The errgroup cancels the context on the first error, which cascades through all other goroutines.

### 15.3 Graceful Shutdown

The shutdown sequence:

1. **Signal** (SIGINT/SIGTERM) cancels the root context
2. **errgroup context** propagates cancellation to all goroutines
3. **Bus** receives context cancellation, closes all subscriber channels, returns
4. **Adapters** receive closed channels (the `ok` in `ev, ok := <-events` becomes false), return nil
5. **Detector** receives context cancellation, returns `ctx.Err()`
6. **errgroup.Wait()** returns when all goroutines have finished
7. **drainAdapters()** calls `Drain(ctx)` on adapters that implement `Drainer`, bounded by `shutdownTimeout`:

```go
func (p *Pipeline) drainAdapters() {
    ctx, cancel := context.WithTimeout(context.Background(), p.shutdownTimeout)
    defer cancel()
    for _, a := range p.adapters {
        d, ok := a.(adapter.Drainer)
        if !ok { continue }
        if err := d.Drain(ctx); err != nil {
            p.logger.Error("adapter drain failed", "adapter", a.Name(), "error", err)
        }
    }
}
```

Note the fresh context with timeout — the original context is already cancelled, but we want to give adapters a bounded window to flush in-flight work. The webhook adapter waits for its WaitGroup (in-flight HTTP requests). The S3 adapter flushes its buffer to S3. After `shutdownTimeout` (default 5s), the drain context is cancelled and any remaining work is abandoned.

The checkpoint store is closed in a deferred call at the top of Run, after drainAdapters returns. This ensures the checkpoint store connection is available for the entire drain period.

---

## Chapter 16: Operations — Config, Migrations, CLI

### 16.1 Configuration

**Files: `internal/config/`**

All configuration flows through a single `Config` struct defined in `config.go`. The config package is split across five files to keep each domain manageable:

- `config.go` — core `Config` struct, shared types (Bus, OTel, DLQ, Transform, Backpressure), `Default()`
- `adapter_config.go` — adapter-specific config structs (WebhookConfig, EmbeddingConfig, KafkaConfig, etc.)
- `detector_config.go` — detector-specific config structs (DetectorConfig, OutboxConfig, MySQLConfig, MongoDBConfig, etc.)
- `plugin_config.go` — plugin config structs (PluginConfig, PluginSpec, etc.)
- `validate.go` — `Validate()` method with all cross-field validation rules

The `Config` struct also has a `Middleware map[string]MiddlewareConfig` field for per-adapter middleware configuration via YAML. Viper handles three layers of configuration with this priority order:

1. **CLI flags** (highest priority)
2. **Environment variables** (prefixed with `PGCDC_`)
3. **YAML config file** (`pgcdc.yaml`)

The YAML config file supports everything that CLI flags do, plus structured sections for transforms, routes, views, and plugins:

```yaml
db: postgres://localhost/mydb
detector: wal
publication: my_pub
bus_mode: reliable

adapters:
  - stdout
  - webhook

webhook_url: https://api.example.com/webhook
webhook_signing_key: secret123

transforms:
  global:
    - type: drop_columns
      columns: [password, ssn]
  adapter:
    webhook:
      - type: mask
        columns: [email]
        mode: hash

routes:
  webhook: [pgcdc:orders, pgcdc:payments]

views:
  - name: order_summary
    query: >
      SELECT payload.status, COUNT(*) as cnt
      FROM pgcdc_events
      WHERE channel = 'pgcdc:orders'
      GROUP BY payload.status
      TUMBLING WINDOW 5m
```

SIGHUP hot-reload only affects the `transforms:`, `routes:`, and `views:` sections. Adapters, detectors, bus mode, and CLI flags are immutable after startup — you can't add a new adapter or change the detector via hot-reload.

This asymmetry is intentional. Transforms and routes are data-plane configuration: they control what happens to events as they flow through. Adding a new filter or changing a route is safe to do at runtime because it doesn't affect the pipeline's structure. Adapters and detectors are control-plane configuration: they define the pipeline's topology. Changing them would require creating/destroying goroutines, channels, and connections — operations that are fundamentally harder to do safely at runtime and are better handled by a restart.

The environment variable prefix `PGCDC_` maps to config keys with dots replaced by underscores: `PGCDC_WEBHOOK_URL` sets `webhook_url`, `PGCDC_BUS_MODE` sets `bus_mode`. This follows the 12-factor app convention of environment-based configuration, making pgcdc container-friendly.

For Kubernetes deployments, a typical pattern is to mount the YAML config file as a ConfigMap and use `SIGHUP` (via a sidecar or the Kubernetes `shareProcessNamespace` feature) to trigger hot-reloads when the ConfigMap changes.

**The database URL cascade.** The `--db` flag sets the default database URL, but many subsystems accept their own override. In production, separating at least the checkpoint database reduces load on the source:

```
--db (primary database URL)
  ├── Detector connection (WAL/LISTEN/outbox)
  ├── Heartbeat writes (pgcdc_heartbeat)
  ├── Schema migrations (pgcdc_migrations)
  ├── DLQ writes (pgcdc_dead_letters)         ← override: --dlq-db (not yet)
  ├── pg_table adapter (INSERT target)        ← override: --pg-table-db (not yet)
  ├── Embedding adapter (pgvector UPSERT)     ← override: --embedding-db (not yet)
  ├── Checkpoint store (pgcdc_checkpoints)    ← override: --checkpoint-db
  ├── Snapshot progress (pgcdc_snapshot_progress) ← override: --snapshot-progress-db
  └── Kafkaserver offsets                     ← override: --kafkaserver-checkpoint-db
```

Today, only `--checkpoint-db`, `--snapshot-progress-db`, and `--kafkaserver-checkpoint-db` have dedicated overrides. Everything else uses `--db`. In production, separate at least `--checkpoint-db` — checkpoint writes are frequent (every ack cycle) and should not contend with the detector's replication connection or the DLQ's writes.

### 16.2 Schema Migrations

**File: `internal/migrate/migrate.go`**

pgcdc needs several tables: `pgcdc_checkpoints`, `pgcdc_dead_letters`, `pgcdc_heartbeat`, plus tables for incremental snapshot progress. These are managed by an embedded SQL migration system.

```go
//go:embed sql/*.sql
var migrations embed.FS
```

Migration files are named `001_initial.sql`, `002_add_column.sql`, etc. The `Run` function:

1. Creates the `pgcdc_migrations` tracking table if it doesn't exist
2. Lists all embedded SQL files, sorted by version number
3. For each migration not yet applied, executes the SQL and records the version

```go
func Run(ctx context.Context, connString string, logger *slog.Logger) error {
    // Create tracking table
    conn.Exec(ctx, migrationsTable)

    for _, mig := range files {
        applied, _ := isApplied(ctx, conn, mig.version)
        if applied { continue }

        sql, _ := migrations.ReadFile(mig.path)
        conn.Exec(ctx, string(sql))
        conn.Exec(ctx, "INSERT INTO pgcdc_migrations (version) VALUES ($1)", mig.version)
    }
    return nil
}
```

Migrations are idempotent (`CREATE TABLE IF NOT EXISTS`) and can be skipped with `--skip-migrations` for environments where you manage schema separately (e.g., when using Flyway, Alembic, or goose alongside pgcdc).

The migration files are embedded at compile time using Go's `//go:embed` directive:

```go
//go:embed sql/*.sql
var migrations embed.FS
```

This means migrations are part of the binary — no external files to deploy. When pgcdc starts, it checks which migrations have been applied (by querying `pgcdc_migrations`), runs any new ones, and records their completion. The whole process is idempotent: running migrations multiple times is safe, and running the same binary version twice doesn't re-apply migrations.

The initial migration (`001_initial.sql`) creates the core tables:

```sql
CREATE TABLE IF NOT EXISTS pgcdc_checkpoints (
    slot_name  TEXT PRIMARY KEY,
    lsn        BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pgcdc_dead_letters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id TEXT NOT NULL,
    adapter TEXT NOT NULL,
    error TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replayed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS pgcdc_heartbeat (
    id INT PRIMARY KEY DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Subsequent migrations add tables for incremental snapshot progress, signal handling, and schema versioning. Each migration runs in its own transaction, so a failure in migration 003 doesn't affect the already-applied 001 and 002.

### 16.3 CLI Commands

pgcdc provides several subcommands:

**`pgcdc listen`** — The main command. Starts the pipeline with the configured detector and adapters. This is where the HTTP server (for SSE, WebSocket, metrics, health) is wired in alongside the Pipeline. The listen command is the most complex — it reads all adapter flags, constructs adapter instances, builds the Pipeline, starts the HTTP server, and sets up the SIGHUP reload handler.

Typical usage:

```sh
# WAL replication to stdout
pgcdc listen --db postgres://localhost/mydb --detector wal --all-tables

# LISTEN/NOTIFY to webhook with signing
pgcdc listen --db postgres://localhost/mydb --channel orders \
  --adapter webhook --webhook-url https://api.example.com/hook \
  --webhook-signing-key secret123

# WAL to Kafka + S3 with cooperative checkpointing
pgcdc listen --db postgres://localhost/mydb --detector wal \
  --all-tables --persistent-slot --cooperative-checkpoint \
  --adapter kafka --kafka-brokers localhost:9092 \
  --adapter s3 --s3-bucket my-cdc-bucket --s3-format parquet
```

**`pgcdc snapshot`** — Runs a one-time table snapshot without live streaming. Uses `REPEATABLE READ` isolation to get a consistent point-in-time view of the table. Useful for backfilling a new adapter with existing data.

**`pgcdc validate`** — Runs the pre-flight validation checks without starting the pipeline. Iterates all configured adapters, calling their `Validate` methods. Reports which validations passed and which failed. Useful in CI/CD to verify configuration before deployment — catch a wrong S3 bucket or unreachable Kafka broker before the container starts.

**`pgcdc slot list|status|drop`** — Manages replication slots. `list` shows all slots on the database with their LSN positions. `status` shows details for a specific slot (confirmed flush LSN, restart LSN, lag in bytes). `drop` removes a persistent slot — necessary when decommissioning a pgcdc instance, as orphaned slots prevent WAL recycling.

**`pgcdc dlq list|replay|purge`** — Dead letter queue management (covered in Chapter 11).

**`pgcdc list [adapters|detectors|transforms]`** — Lists all registered adapters, detectors, or transforms with their descriptions. Because the registry is build-tag-aware, `pgcdc list adapters` on a slim binary shows only the adapters compiled in. Useful for confirming which features are available in the running build.

**`pgcdc playground`** — Starts a Docker-based demo environment with PostgreSQL, auto-generated data, and pgcdc configured to stream to stdout. This is a zero-setup introduction: it pulls a PostgreSQL container, creates tables, inserts random data on a loop, and shows the CDC events flowing in real-time.

**`pgcdc serve --config pipelines.yaml`** — Starts a multi-pipeline server that manages N independent pipelines defined in a YAML config file. Each pipeline has its own detector, adapters, and transforms — completely independent contexts, channels, and goroutines. A shared HTTP server provides Prometheus metrics (`/metrics`), health checks (`/healthz`), and a REST API for runtime pipeline management:

```
GET    /api/v1/pipelines            — list all pipelines with status
GET    /api/v1/pipelines/:id        — get pipeline detail (config + runtime state)
POST   /api/v1/pipelines            — create pipeline from JSON spec
DELETE /api/v1/pipelines/:id        — remove pipeline (must be stopped)
POST   /api/v1/pipelines/:id/start  — start pipeline
POST   /api/v1/pipelines/:id/stop   — stop pipeline
POST   /api/v1/pipelines/:id/pause  — pause a running pipeline
POST   /api/v1/pipelines/:id/resume — resume a paused pipeline
```

The pipeline builder uses the component registry — `registry.CreateDetector`, `registry.CreateAdapter`, and `registry.SpecToTransform` — to construct real pipelines from the YAML specs. Config maps are converted to typed config via JSON round-trip (`json.Marshal` the `map[string]any`, then `json.Unmarshal` into the typed config struct). This is covered in detail in Chapter 18.

**`pgcdc describe <name>`** — Shows the full parameter specification for any registered adapter or detector. Uses `registry.GetConnectorSpec()` to look up the component and prints a formatted table of parameters with name, type, default value, required flag, validation rules, and description. See Chapter 14c for the spec system that powers this command.

Each command follows the same pattern: a cobra `Command` struct with flag definitions in `init()`, a `RunE` function that reads config from viper and executes. Commands register themselves with `rootCmd.AddCommand()` in their `init()` function. This pattern means commands are self-contained — each command file is independently understandable.

### 16.4 Flag Interdependencies

Many pgcdc flags only make sense in combination with others. The validation rules in `cmd/listen.go:524-560` enforce these dependencies at startup, but knowing the dependency graph helps you plan your configuration before running into errors.

**The WAL feature stack.** Most advanced features form a dependency chain:

```
Need crash-resumable streaming?
  └─ --persistent-slot
       │
       ├─ Need multi-adapter exactly-once?
       │    └─ --cooperative-checkpoint (requires --persistent-slot + --detector wal)
       │
       ├─ Need WAL disk safety?
       │    └─ --backpressure (requires --persistent-slot + --detector wal)
       │         └─ Configure: --bp-warn-threshold, --bp-critical-threshold
       │         └─ Per-adapter: --adapter-priority name=critical|normal|best-effort
       │
       └─ Configure: --slot-name, --checkpoint-db
```

**Flag implication chains** (from `cmd/listen.go`):

- `--tx-markers` implies `--tx-metadata` (line 454)
- `--all-tables` forces `--detector wal` if currently `listen_notify` (line 506), sets `--publication all` if empty (line 509)
- `--tx-metadata` and `--tx-markers` require `--detector wal` (line 525)
- `--snapshot-first` requires `--detector wal` + `--snapshot-table` (lines 537, 546)
- `--cooperative-checkpoint` requires `--detector wal` + `--persistent-slot` (lines 540, 543)
- `--backpressure` requires `--detector wal` + `--persistent-slot` (lines 552, 555)
- `--incremental-snapshot` requires `--detector wal` (line 549)
- `--toast-cache` requires `--detector wal` (line 558)
- `--all-tables` is incompatible with `--detector outbox` (line 502)

**The viper binding strategy.** All viper flag bindings are registered in `bindListenFlags`, the `PreRunE` hook on `listenCmd`, not in `init()`. This is deliberate: viper flag bindings are global — if two cobra commands both bind the same flag key in `init()`, they share state. Moving bindings to `PreRunE` means only the active command binds its flags for that invocation, eliminating the collision. Adapter flags are declared in each adapter's `BindFlags` callback in the `registry.AdapterEntry`, then bound to viper keys listed in `ViperKeys [][2]string`. When adding a new flag that is shared between `listen` and `snapshot`, either bind it in both commands' `PreRunE` hooks (with the same key, which is safe since only one runs per invocation), or read it directly from `cmd.Flags()` for flags truly unique to one command.

### 16.5 Build System and Slim Binary

pgcdc ships as two binaries: **full** (all 17 adapters, plugins, view engine) and **slim** (core adapters only). The slim binary excludes heavy dependencies like Kafka, gRPC, NATS, Redis, and the Wasm plugin runtime, cutting binary size significantly.

**Build tags.** The slim binary uses Go build tags to exclude adapter packages at compile time:

| Build Tag        | Excludes                         |
|------------------|----------------------------------|
| `no_kafka`       | Kafka adapter (franz-go)         |
| `no_grpc`        | gRPC adapter (grpc, protobuf)    |
| `no_iceberg`     | Iceberg adapter (parquet-go)     |
| `no_nats`        | NATS adapter (nats.go)           |
| `no_redis`       | Redis adapter (go-redis)         |
| `no_plugins`     | Wasm plugin system (extism)      |
| `no_kafkaserver` | Kafka protocol server            |
| `no_views`       | View engine (TiDB SQL parser)    |
| `no_s3`          | S3 adapter (aws-sdk-go-v2)       |

**Stub file pattern.** Each excludable adapter has two files:

- `cmd/register_kafka.go` — guarded with `//go:build !no_kafka`, calls `registry.RegisterAdapter(...)` with the real factory
- `cmd/register_kafka_stub.go` — guarded with `//go:build no_kafka`, calls `registry.RegisterAdapter(...)` with a factory that returns `fmt.Errorf("kafka adapter not available (built with -tags no_kafka)")`

This means requesting `--adapter kafka` on a slim binary produces a clear runtime error at startup, not a compile-time one. The registry is always populated — the stub's factory just always fails. The user sees exactly which build tag excluded the feature.

Detectors follow the same pattern: `cmd/register_detector_mysql.go` and `cmd/register_detector_mysql_stub.go`.

**Version embedding.** GoReleaser injects the version via ldflags: `-X github.com/florinutz/pgcdc/cmd.Version={{.Version}}`. The slim binary appends `-slim` to the version string, so `pgcdc version` shows whether you're running the full or slim build.

**Release matrix.** GoReleaser (`.goreleaser.yml`) produces:

- Full binary: `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`
- Slim binary: same four targets
- Docker multi-arch manifest: `ghcr.io/florinutz/pgcdc:<version>` (linux/amd64 + arm64)
- Homebrew tap: `florinutz/homebrew-tap` (full binary only)
- `CGO_ENABLED=0` for all builds — pure Go, static binaries, no system dependencies

---

## Chapter 17: Operational Runbook

This chapter covers common operational scenarios: what to check, what to do, and what to watch for. Each scenario is a self-contained recipe.

### 17.1 Webhook Endpoint Down

**Symptoms**: `pgcdc_webhook_delivery_errors_total` rising, DLQ records accumulating, circuit breaker opening (`pgcdc_circuit_breaker_state{adapter="webhook"}` = 1).

**Response**:
1. Check the circuit breaker state: if open, all events are being routed to the DLQ (or dropped if DLQ is `none`)
2. Fix the upstream endpoint
3. The circuit breaker auto-recovers after `--webhook-cb-reset` duration (default 60s) — one successful delivery in half-open state closes it
4. Replay DLQ records: `pgcdc dlq replay --db <url> --adapter webhook`
5. Verify: `pgcdc dlq list --db <url> --adapter webhook` should show zero un-replayed records

**Prevention**: Set `--dlq pg_table` (not `stderr` or `none`) so failed events are recoverable. Monitor `pgcdc_dead_letters_total{adapter="webhook"}`.

### 17.2 WAL Disk Filling Up

**Symptoms**: `pgcdc_slot_lag_bytes` growing, PostgreSQL `pg_wal` directory consuming disk, warnings in pgcdc logs when lag exceeds `--slot-lag-warn` (default 100MB).

**Response**:
1. Check which adapter is behind: query `pgcdc_ack_position{adapter}` — the adapter with the lowest LSN is the bottleneck
2. Check backpressure state: `pgcdc_backpressure_state` (0=green, 1=yellow, 2=red)
3. If an adapter is stuck (e.g., Kafka broker unreachable), fix the downstream issue. The checkpoint will advance once the adapter catches up
4. **Emergency**: If disk is critically low, drop the replication slot: `pgcdc slot drop --db <url> --slot-name <name>`. This releases all retained WAL immediately but loses undelivered events. You'll need to re-snapshot after re-creating the slot

**Prevention**: Enable `--backpressure` with `--persistent-slot`. Set `--adapter-priority` so non-critical adapters (e.g., search sync) are shed first under pressure.

### 17.3 Adding a New Adapter to a Running System

**What to know**: You cannot add an adapter via SIGHUP — adapters are immutable after startup (see section 16.4). Adding an adapter requires a restart.

**Safe procedure with persistent slot**:
1. Stop pgcdc (SIGTERM — graceful drain runs, checkpoint is flushed)
2. Add the new adapter flags to your config
3. Restart pgcdc — the persistent slot picks up from the last checkpoint LSN
4. Zero event loss: WAL between stop and restart is retained by the slot

**Without persistent slot**: Events during the restart window are lost. For LISTEN/NOTIFY, notifications sent while no listener is connected are gone forever (PostgreSQL does not queue them).

### 17.4 Changing Transforms Without Restart

**Procedure**:
1. Edit the `transforms:` section in your YAML config file
2. Send SIGHUP: `kill -HUP $(pgrep pgcdc)`
3. Verify: check `pgcdc_config_reloads_total` incremented. If `pgcdc_config_reload_errors_total` incremented instead, check logs for the parse error
4. New transforms take effect on the next event processed — no events are lost during the swap

**What you can change**: Any transform in `transforms.global` or `transforms.adapter.<name>`, and any route in `routes:`. See section 4.3 for the full reloadable/immutable boundary.

### 17.5 Debugging Event Flow

**Start with validation**: `pgcdc validate --config pgcdc.yaml` runs all adapter pre-flight checks (DNS resolution, bucket existence, API reachability) without starting the pipeline. Fix any validation errors first.

**Add a stdout observer**: Add `--adapter stdout` alongside your real adapters. Every event that passes through the bus will be printed to stdout as JSON, giving you a complete picture of what the pipeline sees. Route it to a specific channel if needed: `--route stdout=pgcdc:orders`.

**Check health**: `curl localhost:8080/healthz` returns per-component status. A 503 means at least one component is down — the response body shows which one.

**Check metrics**: `curl localhost:8080/metrics | grep pgcdc_` shows all pipeline metrics. Key ones to check: `pgcdc_events_ingested_total` (detector producing?), `pgcdc_events_delivered_total{adapter}` (adapters receiving?), `pgcdc_bus_events_dropped_total` (bus dropping in fast mode?).

**Use the inspector**: When `--inspect-buffer` is set, `curl localhost:8080/inspect?point=post-detector&limit=5` shows the last 5 events captured after the detector. This lets you verify events are being produced, check their shape before transforms run, and compare pre-transform vs post-transform payloads. The SSE endpoint `curl localhost:8080/inspect/stream` provides a live feed for real-time debugging — leave it running in a terminal while iterating on transform expressions to see immediate results. See section 13.4 for the full inspector architecture.

### 17.6 Upgrading pgcdc

**Migrations are safe**: The migration system (`internal/migrate`) is idempotent — running a newer binary against an existing database applies only new migrations. Already-applied migrations are skipped. Each migration runs in its own transaction.

**Persistent slots survive restart**: Stop the old version, start the new one. The replication slot is retained by PostgreSQL (it's a named, non-temporary slot). The new pgcdc instance reads the last checkpoint from `pgcdc_checkpoints` and resumes from that LSN. Zero event loss.

**Rolling back**: If the new version has issues, stop it and start the old binary. Schema migrations are forward-only (no down migrations), but they use `CREATE TABLE IF NOT EXISTS` and are additive — a newer schema is backward-compatible with older binaries. The persistent slot and checkpoint are unaffected by the rollback.

**Version check**: Run `pgcdc version` to confirm the running version. The slim binary shows `<version>-slim`.

---

## Chapter 18: Multi-Pipeline Server

### 18.1 Multi-Pipeline Server

**File: `cmd/serve.go`, `server/manager.go`, `server/api.go`**

The `pgcdc listen` command runs one pipeline: one detector, one bus, N adapters. For many deployments, that's sufficient. But when you need multiple independent CDC streams — orders to Kafka, users to search, inventory to webhooks — you'd otherwise need to run multiple pgcdc processes, each with its own config, metrics port, and lifecycle management.

`pgcdc serve` runs N pipelines in a single process. Each pipeline is fully independent: its own detector, bus, adapters, transforms, routes, and context. A shared HTTP server exposes unified metrics, health checks, and a REST API for runtime control.

The config file defines each pipeline declaratively:

```yaml
# pipelines.yaml
addr: ":8080"
pipelines:
  - name: orders-to-kafka
    detector:
      type: wal
      config:
        database_url: postgres://localhost/mydb
    adapters:
      - name: kafka
        type: kafka
        config:
          kafka_brokers: ["localhost:9092"]
    transforms:
      - type: drop_columns
        config:
          columns: ["internal_notes"]
    routes:
      kafka: ["pgcdc:orders", "pgcdc:payments"]

  - name: users-to-search
    detector:
      type: wal
      config:
        database_url: postgres://localhost/mydb
    adapters:
      - name: search
        type: search
        config:
          search_engine: typesense
          search_url: http://localhost:8108
    routes:
      search: ["pgcdc:users"]
```

**The Manager.** The `server.Manager` is the orchestrator. It maintains a map of `ManagedPipeline` structs, each wrapping a `*pgcdc.Pipeline` with lifecycle state:

```go
type ManagedPipeline struct {
    config    PipelineConfig
    pipeline  *pgcdc.Pipeline
    status    PipelineStatus  // stopped | starting | running | paused | error
    startedAt *time.Time
    lastErr   string
    cancel    context.CancelFunc
    done      chan struct{}
}
```

The state machine is simple: `stopped` -> `starting` -> `running` -> (`paused` | `error` | `stopped`). Starting a pipeline builds it from the config (via the `PipelineBuilder` function), creates a cancellable context, and runs `pipeline.Run(ctx)` in a background goroutine. The `done` channel signals when the goroutine exits, which the `Stop` method waits on for clean shutdown.

**Pipeline construction.** The `registryPipelineBuilder` function in `cmd/serve.go` bridges the YAML config to the component registry. The pattern for each component type is the same: take the `map[string]any` config from the YAML spec, JSON round-trip it into the typed config struct, then call the registry factory:

```go
func mapToStruct(m map[string]any, target any) error {
    data, err := json.Marshal(m)
    if err != nil {
        return fmt.Errorf("marshal config map: %w", err)
    }
    return json.Unmarshal(data, target)
}
```

This JSON round-trip is intentionally simple. YAML parsing produces `map[string]any` with string keys and untyped values. JSON marshaling normalizes the types (YAML integers become JSON numbers), and JSON unmarshaling into the typed struct applies Go's standard type coercion. It handles nested structs, slices, and duration strings without any custom mapping code.

Detectors are created via `registry.CreateDetector(type, ctx)`, which returns a `DetectorResult` containing the detector, an optional checkpoint store, and a cleanup function. Adapters are created via `registry.CreateAdapter(type, ctx)`, which returns the adapter and optional middleware config. Transforms are created via `registry.SpecToTransform(spec)`. All three use the same registry that powers `pgcdc listen` — the serve command doesn't have its own component wiring.

**The REST API.** The `server.APIHandler` mounts eight endpoints on a chi router:

```go
func APIHandler(mgr *Manager) http.Handler {
    r := chi.NewRouter()
    r.Get("/api/v1/pipelines", listPipelines(mgr))
    r.Post("/api/v1/pipelines", createPipeline(mgr))
    r.Get("/api/v1/pipelines/{id}", getPipeline(mgr))
    r.Delete("/api/v1/pipelines/{id}", deletePipeline(mgr))
    r.Post("/api/v1/pipelines/{id}/start", startPipeline(mgr))
    r.Post("/api/v1/pipelines/{id}/stop", stopPipeline(mgr))
    r.Post("/api/v1/pipelines/{id}/pause", pausePipeline(mgr))
    r.Post("/api/v1/pipelines/{id}/resume", resumePipeline(mgr))
    return r
}
```

Each handler is a thin wrapper around the corresponding Manager method. Error responses use HTTP status codes that match the semantic: 404 for unknown pipeline, 409 (Conflict) for invalid state transitions (e.g., starting an already-running pipeline, deleting a running pipeline). The `createPipeline` handler accepts a `PipelineConfig` JSON body and adds it to the manager without starting it — you POST to create, then POST to `/:id/start` to run it. This two-step pattern lets you inspect the config before committing to running it.

**Pause and Resume.** Pausing a pipeline cancels its context (which triggers graceful shutdown of the detector, bus, and adapters) but keeps the `ManagedPipeline` entry in the map with `status: "paused"`. Resuming builds a fresh pipeline from the same config and starts it. With a persistent slot, the resumed pipeline picks up from the last checkpoint — zero event loss across pause/resume cycles. Without a persistent slot, events during the pause window are lost (same as a restart).

**Health integration.** Each pipeline registers with the shared health checker as `pipeline:<name>`. When a pipeline is running, its health status is `up`; when stopped, paused, or errored, it's `down`. The `/healthz` endpoint reports the aggregate — if any pipeline is down, the health check returns 503. This integrates with load balancers and orchestrators (Kubernetes readiness probes) that need to know whether the process is healthy.

**Shutdown.** On SIGINT/SIGTERM, the serve command calls `mgr.StopAll()`, which iterates all running/paused pipelines and calls `Stop` on each. Each stop cancels the pipeline's context and waits for the `done` channel, ensuring all adapters drain before the process exits. The HTTP server gets a 5-second graceful shutdown window for in-flight API requests.

---

## Epilogue: The Shape of the System

pgcdc is a single-binary Go application that captures changes from databases and delivers them to everything. The architecture can be summarized in three sentences: detectors produce events, the bus distributes them, adapters consume them. One context cancellation tears it all down. Every optional feature is wired via interface type assertions, adding zero overhead when disabled.

A few design principles that shaped the codebase:

**Follow the event.** Every component in the system is defined by what it does to an event. Detectors create them, the bus distributes them, transforms reshape them, adapters deliver them. The event is the lingua franca. When you want to understand any component, look at what happens to the event as it passes through. This manual follows the same principle — we traced the event from birth to delivery.

**Interfaces at the boundary, concrete types inside.** The Adapter and Detector interfaces are minimal — two methods each. Inside a package, types are concrete and specific. The Pipeline uses type assertions to discover optional capabilities (Acknowledger, Validator, Drainer, Traceable, Reinjector). This keeps the interface surface tiny while allowing rich behavior. A new adapter can implement only `Start` and `Name` and it works. Adding cooperative checkpointing is just implementing `SetAckFunc`. Adding pre-flight validation is just implementing `Validate`. No interface changes, no pipeline changes.

**One goroutine per concern.** The bus fan-out is one goroutine. Each adapter runs in one goroutine. Each wrapper (for transforms and route filtering) is one goroutine. The errgroup manages them all with one context. This makes the concurrency model easy to reason about: each goroutine does one thing, communicates via channels, and exits when the context is cancelled. When something goes wrong, you know exactly which goroutine to look at.

**Channels as connective tissue.** The bus's ingest channel connects the detector to the bus. Subscriber channels connect the bus to wrapper goroutines. Wrapper output channels connect to adapters. The Reinjector's ingest channel connects the view adapter back to the bus. Channels are the only inter-goroutine communication mechanism. There are no shared mutable state issues because all state either lives within a single goroutine or is communicated via channels. The exceptions are atomic.Pointer (for hot-reload), sync.RWMutex (for health checker and bus subscriber list), and sync.WaitGroup (for webhook inflight tracking) — each justified by a specific need where channels would add unnecessary complexity.

**No hidden state.** All configuration flows through the Config struct. All runtime state is on the Pipeline struct. Metrics are package-level variables (the Prometheus convention), but everything else is explicit. When you read the Run method, you see every goroutine that will be started and every wire that will be connected.

**Zero overhead when disabled.** The noop TracerProvider, the nil-checked TOAST cache, the nil-checked backpressure controller, the nil-checked DLQ, the nil-checked inspector, the nil-checked schema store — when a feature is not configured, it doesn't exist at runtime. This means the base case (detector + bus + stdout) is fast and simple, and features are layered on top. The slim binary build (with Go build tags like `no_kafka`, `no_grpc`, `no_plugins`) takes this further — entire adapter packages are excluded from compilation.

**Consistent patterns across adapters.** Adapters converge on three shapes. Deliverer adapters (webhook, embedding) implement only `Deliver(ctx, ev) error` — no event loop, no retry/CB/RL/DLQ logic — the middleware provides all of that. Batcher adapters implement `Flush(batch)` and `BatchConfig()` — the batch runner handles the event loop. Legacy broker adapters (SSE, WebSocket, gRPC, kafkaserver, view) use the Start/select-loop pattern. Within each shape, the pattern is consistent: constructor with sensible defaults, metrics instrumentation, optional interface implementations. Once you understand one adapter of each shape, you can read any of the 17 adapters in the system.

**Fail at boundaries, recover at the loop.** Network errors are expected at the boundary (HTTP request, Kafka produce, Redis command). Each adapter handles these at the appropriate level: per-event retry (webhook, embedding), per-connection reconnect (Kafka, NATS, Redis, pg_table), or per-write failure handling (file, exec). The common pattern is an inner `run` method that fails on connection errors, wrapped in an outer `Start` method that reconnects with backoff. This separates "what to do with an event" from "what to do when the connection dies."

### Where to Go From Here

The file you'll want to read next depends on what you're building:

- **Adding a new adapter**: Start with `adapter/stdout/stdout.go` for the legacy Start-loop pattern. For the modern path, read `adapter/webhook/webhook.go` — it's a pure Deliverer adapter (only `Deliver`, no event loop) with the resilience features (signing, tracing, retry) handled entirely by the `adapter/middleware` chain. Then read `cmd/register_core.go` to see how `MiddlewareConfig` is wired in via the registry. For buffered flush patterns, read `adapter/s3/s3.go`.

- **Adding a new detector**: Start with `detector/listennotify/listennotify.go` for the pattern (connect, loop, reconnect), then study `detector/walreplication/walreplication.go` for protocol-level CDC.

- **Understanding the pipeline**: Read `pgcdc.go` top-to-bottom. The `Run` method is the complete picture.

- **Adding transforms**: Start with `transform/transform.go` (the interface), then pick any built-in transform as a template. `transform/mask.go` is a good starting point.

- **Understanding the Kafka server**: Start with `adapter/kafkaserver/server.go` (accept loop), then `protocol.go` (wire format), then `handler.go` (API dispatch), then `partition.go` (data storage), then `group.go` (consumer coordination).

- **Extending with plugins**: Read `plugin/wasm/runtime.go` for how modules are loaded, then `plugin/wasm/transform.go` for the simplest extension point. The LSN preservation pattern in the transform wrapper is the key correctness concern.

The system is designed to be extended. The patterns are consistent. The interfaces are minimal. The code is the documentation.
