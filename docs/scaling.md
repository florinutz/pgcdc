# pgcdc Scaling Architecture

## Context

pgcdc is designed as a single-binary pipeline: one detector → one bus → N adapters, all in-process. This document explores how pgcdc scales horizontally **without architectural changes**, using existing primitives (NATS/Kafka consumer detectors + adapters) as opt-in infrastructure.

Compared to tools like [GlassFlow](https://github.com/glassflow/clickhouse-etl) which mandate NATS as internal backbone with separate pods per stage, pgcdc scales via **composition of existing CLI flags**.

---

## Single Instance (Default)

```
PostgreSQL WAL ──→ Detector ──→ Bus (in-memory channels) ──→ Adapters
                                     │
                              cooperative checkpoint
                              backpressure controller
```

**Practical limits:**
- ~500K events/s bus throughput (single dispatch goroutine)
- All adapters share one process (slow adapter blocks reliable mode)
- Checkpoint and backpressure are per-instance

For most use cases, a single instance is sufficient. The patterns below are for when you outgrow it.

---

## Bottleneck Analysis

A pipeline has three stages, each with different scaling characteristics:

### Read Side (Detector)

| Detector | Bottleneck | Can parallelize? | Strategy |
|----------|-----------|------------------|----------|
| **PG WAL** | One replication slot = one consumer (PostgreSQL constraint) | No | Scale the source DB (read replicas with separate slots), or fan-out via NATS/Kafka after the single reader |
| **PG LISTEN/NOTIFY** | 8KB payload limit, single connection | No | Architectural limit of LISTEN/NOTIFY — switch to WAL for high throughput |
| **MySQL binlog** | Single binlog stream per server | No | Same as PG WAL — one reader, fan-out after |
| **MongoDB change streams** | One stream per collection/database/cluster | Per-shard | MongoDB sharded clusters produce parallel change streams per shard — use `detector/multi/parallel` |
| **Kafka consumer** | Partition-bound | Yes | Consumer groups scale horizontally — run N instances with same `--kafka-consumer-group` |
| **NATS consumer** | JetStream consumer groups | Yes | Run N instances with same `--nats-consumer-durable` |
| **Webhook gateway** | HTTP server goroutine pool | Vertically | Single instance handles thousands of concurrent HTTP connections; load-balance across instances for more |
| **SQLite** | Poll-based, single file | No | Local-first by design — not a scaling target |

**Key insight**: Database CDC sources (PG WAL, MySQL binlog) are inherently single-reader. The scaling strategy is always: **one reader → fan-out via queue → parallel consumers**. This is Pattern 1 below.

### Bus (Dispatch)

The bus is a single goroutine dispatching to N subscriber channels via atomic pointer reads. At ~500K events/s it's rarely the bottleneck. If it becomes one:

- **Fast mode**: Non-blocking sends, drops events on full channels — throughput limited only by event size and channel buffer
- **Reliable mode**: Blocks on slowest subscriber — throughput limited by slowest adapter

**Mitigation**: Move slow adapters to separate instances (Pattern 1). The bus only needs to feed one fast adapter (NATS/Kafka) which absorbs events at memory speed.

### Write Side (Adapter)

| Adapter | Bottleneck | Strategy |
|---------|-----------|----------|
| **ClickHouse** | Batch INSERT throughput, network | Parallel consumer instances (Pattern 3), async inserts (`--clickhouse-async-insert`), tune `--clickhouse-batch-size` |
| **S3** | Upload latency (~100ms per object) | Larger flush intervals (`--s3-flush-interval`), parallel instances |
| **Kafka producer** | Broker write throughput | Rarely a bottleneck; increase `--kafka-brokers` list |
| **Webhook** | Target server latency | Rate limiting (`middleware.rate_limit`), parallel instances |
| **Search (Typesense/Meili)** | Indexing throughput | Batch size tuning, parallel instances |
| **Embedding** | API rate limits | Rate limiting built in, parallel instances with separate API keys |

**Key insight**: Write-side bottlenecks are solved by Pattern 3 (parallel consumers) or by tuning batch sizes and flush intervals.

---

## Scaling Patterns

### Pattern 1: Fan-Out (One Source, Many Sinks)

Decouple slow sinks so they don't block each other.

```
Instance A (hub):
  PG WAL detector ──→ bus ──→ NATS adapter ──→ NATS JetStream
                                                    │
Instance B (spoke):                                 ├──→ NATS detector ──→ bus ──→ ClickHouse
Instance C (spoke):                                 └──→ NATS detector ──→ bus ──→ S3
```

```bash
# Hub: CDC source → NATS
pgcdc listen --detector wal --publication my_pub --persistent-slot \
  --adapter nats --nats-url nats://hub:4222 --nats-stream cdc-events

# Spoke 1: NATS → ClickHouse (with dedup)
pgcdc listen --detector nats_consumer --nats-url nats://hub:4222 \
  --nats-consumer-stream cdc-events --nats-consumer-durable clickhouse-sink \
  --adapter clickhouse --clickhouse-dsn clickhouse://ch:9000/analytics \
  --dedup-key payload.id --dedup-window 1h

# Spoke 2: NATS → S3 archival
pgcdc listen --detector nats_consumer --nats-url nats://hub:4222 \
  --nats-consumer-stream cdc-events --nats-consumer-durable s3-archive \
  --adapter s3 --s3-bucket cdc-archive --s3-format parquet
```

Each spoke has independent retry, DLQ, and backpressure. Slow ClickHouse flushes don't block S3 writes.

## Pattern 2: Fan-In (Many Sources, One Sink)

Aggregate CDC from multiple databases into one destination.

```
Instance A: PG WAL (db1) ──→ bus ──→ NATS adapter ──→┐
Instance B: PG WAL (db2) ──→ bus ──→ NATS adapter ──→├──→ NATS JetStream
Instance C: MySQL binlog  ──→ bus ──→ NATS adapter ──→┘
                                                         │
Instance D: NATS detector ──→ bus ──→ ClickHouse adapter ←┘
```

Multiple pgcdc instances write to the same NATS stream. One consumer reads all events.

## Pattern 3: Parallel Consumers (Throughput Scaling)

Scale a slow sink horizontally using consumer groups.

```
                                  NATS JetStream (consumer group)
                                         │
Instance A: NATS detector (durable=sink) ──→ bus ──→ ClickHouse
Instance B: NATS detector (durable=sink) ──→ bus ──→ ClickHouse
Instance C: NATS detector (durable=sink) ──→ bus ──→ ClickHouse
```

Set the same `--nats-consumer-durable` on all instances. NATS distributes messages across consumers automatically.

Same pattern works with Kafka:
```bash
pgcdc listen --detector kafka_consumer \
  --kafka-brokers localhost:9092 \
  --kafka-consumer-topics events \
  --kafka-consumer-group clickhouse-sink \
  --adapter clickhouse --clickhouse-dsn clickhouse://ch:9000/analytics
```

Run N instances with the same `--kafka-consumer-group` — Kafka distributes partitions across them.

---

## Full Example: PG WAL → ClickHouse at Scale

Combining patterns to handle a high-throughput CDC pipeline where both read and write sides need optimization:

```
                    ┌── NATS consumer (durable=ch-1) ──→ ClickHouse shard 1
PG WAL ──→ pgcdc ──→ NATS JetStream ──┼── NATS consumer (durable=ch-1) ──→ ClickHouse shard 1
  (1 reader)   (hub)                   ├── NATS consumer (durable=ch-1) ──→ ClickHouse shard 1
                                       └── NATS consumer (durable=s3)   ──→ S3 archival
```

```bash
# 1. Single hub reads WAL, publishes to NATS (read bottleneck: PG slot = 1 reader)
pgcdc listen --detector wal --db "$PG_URL" --publication all --persistent-slot \
  --adapter nats --nats-url nats://nats:4222 --nats-stream cdc

# 2. Three parallel ClickHouse writers (write bottleneck: scale horizontally)
# Run this command on 3 separate machines/containers:
pgcdc listen --detector nats_consumer --nats-url nats://nats:4222 \
  --nats-consumer-stream cdc --nats-consumer-durable ch-sink \
  --adapter clickhouse --clickhouse-dsn clickhouse://ch:9000/analytics \
  --clickhouse-auto-create --clickhouse-async-insert \
  --dedup-key payload.id --dedup-window 1h

# 3. One S3 archiver (independent, doesn't affect ClickHouse throughput)
pgcdc listen --detector nats_consumer --nats-url nats://nats:4222 \
  --nats-consumer-stream cdc --nats-consumer-durable s3-archive \
  --adapter s3 --s3-bucket cdc-archive --s3-format parquet
```

- **Read side**: Can't parallelize (PG slot limit), but one reader saturates a 10Gbps link
- **Bus**: Only feeds NATS adapter — effectively unbounded
- **Write side**: 3 parallel ClickHouse writers, NATS distributes messages, each instance deduplicates independently
- **Crash recovery**: NATS durable consumer resumes from last ack; ClickHouse ReplacingMergeTree handles duplicates on restart

---

## Why This Works Without New Code

| Concern | How it's handled |
|---------|-----------------|
| **Backpressure** | NATS `MaxAckPending` / Kafka consumer lag — external queues handle inter-instance flow control natively |
| **Crash recovery** | NATS durable consumers / Kafka consumer group offsets — each technology tracks its own position |
| **Dedup** | In-memory LRU transform (`--dedup-key`) + ClickHouse ReplacingMergeTree |
| **Ordering** | Kafka partitioning by key, or NATS subject-per-channel. ClickHouse `_version` column for last-write-wins |
| **Checkpointing** | WAL: one slot = one consumer (PG limitation). NATS/Kafka: native offset tracking per consumer |

---

## Comparison: pgcdc vs GlassFlow

| Aspect | GlassFlow | pgcdc |
|--------|-----------|-------|
| Infrastructure | NATS mandatory | NATS/Kafka optional |
| Deployment | Multi-pod, K8s CRD | Multiple `pgcdc listen` processes |
| Stage separation | Separate binaries per role | Same binary, different flags |
| Dedup | BadgerDB pod + batch dedup | In-memory LRU + ReplacingMergeTree |
| Scaling unit | Pod per stage | Process per pipeline segment |
| Complexity | High (5 pods, NATS, orchestrator) | Low (N processes, optional queue) |

---

## Future Additions (Not Yet Needed)

If real bottlenecks emerge:

1. **`pgcdc cluster`** — declarative multi-instance topology from a single YAML
2. **Distributed health** — aggregate metrics from multiple instances
3. **Shared dedup store** — Redis-backed dedup for cross-instance dedup
4. **Bus sharding** — partition subscribers by channel hash for CPU-bound scenarios
