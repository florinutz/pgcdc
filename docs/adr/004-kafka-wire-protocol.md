# ADR-004: Built-in Kafka Wire Protocol Server

## Status
Accepted

## Context
Many organizations have existing Kafka consumer infrastructure (applications, monitoring, dashboards) built on standard Kafka client libraries. To consume CDC events from pgcdc, they would need to either:

1. Run a full Kafka cluster and use the Kafka adapter to publish events (operational overhead, cost, latency).
2. Rewrite consumers to use pgcdc-native protocols (SSE, WebSocket, gRPC).

Both options have significant friction. The Kafka wire protocol is well-documented and client libraries exist for every language.

## Decision
Implement a built-in TCP server (`--adapter kafkaserver`) that speaks the Kafka wire protocol. Any standard Kafka consumer library connects directly to pgcdc -- no Kafka cluster needed.

The server supports 11 API keys: ApiVersions, Metadata, FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ListOffsets, Fetch, OffsetCommit, OffsetFetch.

Key design choices:
- pgcdc channels become Kafka topics (`pgcdc:orders` maps to `pgcdc.orders`)
- Events are hashed across N partitions (FNV-1a on key column or event ID)
- Consumer groups with partition assignment and session reaping
- RecordBatch v2 encoding with headers
- Offset persistence via the existing `checkpoint.Store` interface
- Long-poll Fetch with per-partition waiters for low-latency delivery
- Flexible encoding for ApiVersions v3+ (compact arrays, tagged fields)
- In-memory ring buffer per partition (no disk persistence)

## Consequences

### Positive
- Any Kafka client library (librdkafka, franz-go, sarama, Java client, confluent-kafka-python) connects directly to pgcdc
- No Kafka cluster to operate, monitor, or pay for
- CDC events appear as Kafka topics with familiar semantics (offsets, consumer groups, partitions)
- Existing Kafka consumer code works unchanged -- just point at pgcdc's address
- Much simpler infrastructure for CDC-to-consumer pipelines

### Negative
- In-memory ring buffer means consumers that fall behind lose events (OFFSET_OUT_OF_RANGE) -- no DLQ for this adapter
- Does not implement the full Kafka protocol (no produce, no transactions, no admin APIs)
- Partition count is static at startup (not dynamically adjustable)
- Single-node only -- no multi-broker replication or failover

### Neutral
- Wire protocol compatibility is tested against real Kafka client libraries (franz-go in scenario tests)
- Configuration is minimal: `--kafkaserver-addr`, `--kafkaserver-partitions`, `--kafkaserver-buffer-size`
- Coexists with the Kafka adapter (`--adapter kafka`) which publishes to a real Kafka cluster
