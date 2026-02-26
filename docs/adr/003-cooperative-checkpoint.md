# ADR-003: Cooperative Min-Ack Checkpointing

## Status
Accepted

## Context
WAL logical replication requires the consumer to periodically report its position (LSN) to PostgreSQL so the server can recycle old WAL segments. If the reported LSN advances past events that an adapter has not yet processed, those events are lost on crash recovery.

With multiple adapters consuming at different speeds, a naive approach (checkpoint after bus send) would advance the LSN past events that slow adapters have not yet delivered, violating exactly-once or at-least-once guarantees.

## Decision
Implement cooperative min-ack checkpointing (`--cooperative-checkpoint`):

1. Each adapter that implements `adapter.Acknowledger` calls `AckFunc(lsn)` after fully handling an event (successful delivery, DLQ record, or intentional skip).
2. Adapters that do not implement `Acknowledger` are auto-acked on channel send (fire-and-forget semantics).
3. The `ack.Tracker` maintains the acknowledged LSN per adapter.
4. The WAL detector queries `Tracker.MinAckedLSN()` when reporting to PostgreSQL.
5. The checkpoint only advances to `min(all adapter ack positions)`.

This ensures no adapter loses events on crash: the replication slot restarts from the minimum acknowledged position.

## Consequences

### Positive
- No data loss for any adapter on crash recovery
- Adapters with DLQ support can ack after DLQ write, preventing infinite retry loops
- Compatible with backpressure controller (shed events are auto-acked)
- Clear contract: implement `Acknowledger` for at-least-once, skip it for fire-and-forget

### Negative
- The slowest adapter determines the checkpoint pace, which can cause WAL growth if one adapter stalls (mitigated by backpressure controller and slot lag monitoring)
- Adds per-event function call overhead for acknowledging adapters

### Neutral
- Requires `--persistent-slot` and `--detector wal` (validated at startup)
- Auto-ack for non-Acknowledger adapters preserves backward compatibility
- LSN checkpointed to `pgcdc_checkpoints` table for crash recovery
