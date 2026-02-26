# ADR-005: Streaming Window Types for SQL Views

**Status**: Accepted

**Date**: 2026-02-26

## Context

The view adapter runs streaming SQL analytics over CDC events. The initial implementation supported only tumbling windows (fixed-size non-overlapping time buckets). Different CDC analytics use cases require different windowing semantics:

- **Dashboard counters** need fixed periodic snapshots (tumbling).
- **Smooth metric graphs** need overlapping windows to avoid step discontinuities (sliding).
- **User session tracking** needs event-driven windows that close on inactivity (session).

Each window type has fundamentally different memory, latency, and accuracy trade-offs. Users must choose the right type for their use case.

## Decision

Implement three window types selectable via SQL clauses in the view query:

### 1. Tumbling Window (`TUMBLING WINDOW <duration>`)

Fixed-size non-overlapping time buckets. Results emitted after each window closes.

- Lowest memory overhead (one set of aggregation state).
- Optional `ALLOWED LATENESS <duration>` keeps recently closed windows to accept and re-emit corrected results for late-arriving events.
- Best for: periodic counters, batch summaries, alerting thresholds.

### 2. Sliding Window (`SLIDING WINDOW <duration> SLIDE <duration>`)

Overlapping windows of size W with slide interval S. Internally maintains `ceil(W/S)` tumbling sub-windows. On each slide, aggregates are merged across all sub-windows and the oldest sub-window is evicted.

- Higher memory (N sub-windows of aggregation state).
- Requires cross-window merge for all aggregate types. Non-trivial for statistical aggregates: `STDDEV` uses Welford's parallel merge algorithm; `AVG` re-derives from merged sum/count.
- Best for: smooth metric curves, moving averages, trend detection.

### 3. Session Window (`SESSION WINDOW <gap>`)

Event-driven, gap-based windows. A session stays open while events arrive within the gap duration. When no events arrive for the gap timeout, the session closes and results are emitted.

- Memory proportional to active sessions (one aggregation state per group per active session).
- Timer-based flush (not time-aligned like tumbling/sliding).
- Each session tracks start/end time in `_window` metadata.
- Best for: user session analytics, request batching, activity grouping.

All three implement a common `Window` interface (`ProcessEvent`, `Flush`, `WindowInfo`) in `view/`. The `Engine` dispatches to the correct implementation based on `ViewDef.WindowType`.

## Consequences

### Positive

- Covers the full spectrum of CDC analytics: snapshots (tumbling), trends (sliding), sessions (session).
- Window choice is explicit in SQL syntax — self-documenting.
- Each type is optimized for its use case.
- Late event handling via `ALLOWED LATENESS` improves accuracy for tumbling windows with out-of-order events.
- `COUNT(DISTINCT ...)` and `STDDEV` aggregates enable richer analytics.

### Negative

- Memory usage differs significantly: sliding (N sub-windows) > session (per-active-session) > tumbling (single state).
- Sliding window cross-merge adds complexity, especially for `STDDEV` (Welford's parallel algorithm).
- Users must understand semantics to choose correctly.
- Session windows require timer-based flushing and gap tracking per group.
- `ALLOWED LATENESS` only implemented for tumbling windows, not sliding or session.

### Neutral

- All window types emit `VIEW_RESULT` events re-injected into the bus.
- Result rows include `_window` metadata (`start`, `end`) for observability.
- `max_groups` cardinality cap applies to all window types.
- Loop prevention (skip events from `pgcdc:_view:` channels) applies uniformly.
