# ADR-002: Fast and Reliable Bus Modes

## Status
Accepted

## Context
CDC pipelines serve two distinct workload profiles:

1. **High-throughput monitoring/analytics**: Event loss is acceptable. Throughput and low latency matter. A slow consumer must not stall the entire pipeline.
2. **Loss-sensitive replication/sync**: Every event must be delivered. Backpressure from slow consumers is preferable to data loss.

A single fan-out strategy cannot serve both profiles well.

## Decision
Implement two bus modes selectable at startup via `--bus-mode`:

- **fast** (default): Non-blocking sends to subscriber channels. If a subscriber's channel buffer is full, the event is dropped for that subscriber. The detector and other subscribers are unaffected.
- **reliable**: Blocking sends to all subscriber channels. If any subscriber is slow, the bus blocks, which backpressures the detector. No events are lost, but throughput is bounded by the slowest consumer.

The bus owns all channel lifecycle (creation, closing). Adapters receive read-only channels. The mode is immutable after pipeline start (not hot-reloadable).

## Consequences

### Positive
- Operators choose the right trade-off for their workload at deploy time
- Default fast mode is safe for most use cases (monitoring, logging, analytics)
- Reliable mode enables loss-free pipelines for data replication and sync
- Clear mental model: fast = drop, reliable = block

### Negative
- Reliable mode can cause WAL bloat if a consumer stalls indefinitely (mitigated by source-aware backpressure controller in ADR-003 and heartbeat mechanism)
- Two code paths in the bus fan-out loop

### Neutral
- Bus buffer size (`--bus-buffer`) is configurable and affects both modes
- Per-adapter channel buffers absorb transient slow-downs in both modes
