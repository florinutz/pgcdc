# Architecture

This document describes the internal architecture of pgcdc using Mermaid diagrams.

## Pipeline Data Flow

The core pipeline connects a detector to adapters through a fan-out bus. The `Pipeline` type in `pgcdc.go` orchestrates this:

```mermaid
flowchart LR
    SIG["Signal\nSIGINT/SIGTERM"] --> CTX["Context\ncancel"]

    subgraph DET ["Detectors"]
        direction TB
        LN["listen_notify\nLISTEN/NOTIFY"]
        WAL["wal_replication\nlogical WAL"]
        OB["outbox\npoll table"]
        MY["mysql_binlog\nbinlog replication"]
        MG["mongodb\nchange streams"]
        WH["webhook_gateway\nHTTP ingest"]
        SQ["sqlite\npoll table"]
    end

    subgraph PIP ["Pipeline · pgcdc.go"]
        IC["ingest chan"]
        BUS["Bus\nfast-drop · reliable-block"]
        WRP["Wrapper Goroutine × N\nroute filter → transforms"]
        SC["subscriber chan × N"]
    end

    subgraph ADP ["Adapters"]
        direction TB
        subgraph OUT ["Output"]
            STDOUT["stdout"]
            FILE["file"]
            EXEC["exec"]
            WBH["webhook"]
        end
        subgraph STREAM ["HTTP · Streaming"]
            SSE["sse"]
            WS["websocket"]
            GQL["graphql\ngraphql-transport-ws"]
        end
        subgraph MSG ["Messaging"]
            NATS["nats · JetStream"]
            KFK["kafka"]
            KFS["kafkaserver\nKafka wire protocol"]
        end
        subgraph STORE ["Storage · Analytics"]
            PGT["pg_table"]
            S3["s3 · JSONL · Parquet"]
            ICE["iceberg\nParquet + Avro manifests"]
            DUCK["duckdb\nin-process analytics"]
        end
        subgraph SVC ["External Services"]
            SRCH["search\nTypesense · Meilisearch"]
            RDS["redis\ninvalidate · sync"]
            GRPC["grpc · streaming"]
            EMB["embedding\npgvector upsert"]
        end
        subgraph SPEC ["Special"]
            ARW["arrow\nFlight gRPC"]
            VIEW["view\nstreaming SQL"]
        end
    end

    CTX --> PIP
    DET -->|"events"| IC
    IC --> BUS
    BUS -->|"fan-out"| WRP
    WRP -->|"filtered + transformed"| SC

    SC --> OUT & STREAM & MSG & STORE & SVC & SPEC

    VIEW -->|"VIEW_RESULT\nre-inject"| IC

    WBH & KFK & EMB -.->|"terminal errors"| DLQ["DLQ\nstderr · pg_table · nop"]

    HC["Health Checker\n/healthz"] -. monitors .-> PIP
    PROM["Prometheus\n/metrics"] -. observes .-> PIP
    OTEL["OpenTelemetry\ntracing"] -. traces .-> PIP
```

## Middleware Chain (Deliverer Adapters)

Adapters implementing `Deliver(ctx, event) error` get a middleware chain wired automatically by `adapter/middleware`. The chain wraps the adapter before it's handed to the wrapper goroutine:

```mermaid
flowchart LR
    SC["subscriber chan"] --> WRP

    subgraph WRP ["Wrapper Goroutine"]
        RF["Route Filter"]
        BP["Backpressure\nShed"]
        TC["Transform Chain"]
    end

    TC --> MW

    subgraph MW ["Middleware Chain · adapter/middleware"]
        direction LR
        OTEL2["OTel\nTracing"]
        M["Metrics\nevents_delivered"]
        CB["Circuit Breaker\nclosed · open · half-open"]
        RL["Rate Limiter\ntoken bucket"]
        RT["Retry\nexponential backoff"]
        DLQ2["DLQ\non terminal error"]
        ACK["Ack\n(cooperative checkpoint)"]
    end

    ACK --> ADAPTER["Adapter.Deliver()"]

    OTEL2 --> M --> CB --> RL --> RT --> DLQ2 --> ACK
```

Adapters that implement `adapter.Adapter` directly (not `Deliverer`) skip the middleware chain and consume their subscriber channel in `Start()`.

## Wrapper Goroutine

Each adapter gets a dedicated wrapper goroutine that sits between the bus subscription and the adapter's event channel. The wrapper applies route filtering and transform chains, loading configuration atomically from `sync/atomic.Pointer[wrapperConfig]` for lock-free hot-reload.

```mermaid
flowchart LR
    BusSub["Bus Subscriber Chan"] --> Wrapper

    subgraph Wrapper ["Wrapper Goroutine (per adapter)"]
        LoadConfig["atomic.Pointer.Load()"]
        RouteFilter["Route Filter\n(channel allowlist)"]
        BPShed["Backpressure\nLoad Shedding"]
        TransformChain["Transform Chain\n(global + per-adapter)"]
    end

    LoadConfig --> RouteFilter
    RouteFilter -->|"pass"| BPShed
    RouteFilter -->|"filtered out"| AutoAck1["Auto-Ack\n(if cooperative)"]
    BPShed -->|"pass"| TransformChain
    BPShed -->|"shed"| AutoAck2["Auto-Ack + Metric"]
    TransformChain -->|"pass"| AdapterChan["Adapter Event Chan"]
    TransformChain -->|"ErrDropEvent"| AutoAck3["Auto-Ack + Drop Metric"]
    TransformChain -->|"error"| AutoAck4["Auto-Ack + Error Metric"]
```

## Bus Modes

The bus (`bus/bus.go`) supports two fan-out modes:

```mermaid
flowchart TD
    subgraph Fast ["Fast Mode (default)"]
        FI["Ingest"] --> FF{"subscriber\nchan full?"}
        FF -->|"no"| FS["send"]
        FF -->|"yes"| FD["DROP\n(increment metric,\nlog warning)"]
    end

    subgraph Reliable ["Reliable Mode"]
        RI["Ingest"] --> RF{"subscriber\nchan full?"}
        RF -->|"no"| RS["send"]
        RF -->|"yes"| RB["BLOCK\n(increment backpressure metric,\nthen block until space or ctx cancel)"]
    end
```

**Fast mode** (`--bus-mode fast`): Non-blocking sends. If a subscriber channel is full, the event is dropped for that subscriber. This protects the detector from being stalled by slow adapters.

**Reliable mode** (`--bus-mode reliable`): Blocking sends. The bus blocks on full subscriber channels, which back-pressures all the way to the detector. No event loss at the cost of throughput.

## Cooperative Checkpoint Flow

When `--cooperative-checkpoint` is enabled, the WAL detector only reports LSN positions that all adapters have confirmed:

```mermaid
sequenceDiagram
    participant PG as PostgreSQL
    participant WAL as WAL Detector
    participant Bus
    participant A1 as Adapter A (fast)
    participant A2 as Adapter B (slow)
    participant Tracker as Ack Tracker

    WAL->>Bus: event (LSN=100)
    Bus->>A1: event (LSN=100)
    Bus->>A2: event (LSN=100)

    A1->>Tracker: Ack(A, 100)
    Note over Tracker: A=100, B=0<br/>min=0

    WAL->>Bus: event (LSN=200)
    Bus->>A1: event (LSN=200)
    Bus->>A2: event (LSN=200)

    A1->>Tracker: Ack(A, 200)
    Note over Tracker: A=200, B=0<br/>min=0

    A2->>Tracker: Ack(B, 100)
    Note over Tracker: A=200, B=100<br/>min=100

    WAL->>PG: StandbyStatusUpdate(LSN=100)
    Note over PG: WAL before LSN 100<br/>can be recycled

    A2->>Tracker: Ack(B, 200)
    Note over Tracker: A=200, B=200<br/>min=200

    WAL->>PG: StandbyStatusUpdate(LSN=200)
```

The `ack.Tracker` (`ack/tracker.go`) maintains a map of adapter name to highest acked LSN. `MinAckedLSN()` returns the minimum across all registered adapters. Non-Acknowledger adapters are auto-acked on channel send.

## Backpressure Zones

The backpressure controller (`backpressure/backpressure.go`) monitors WAL lag and transitions between three zones:

```mermaid
stateDiagram-v2
    [*] --> Green

    Green --> Yellow: lag >= warn_threshold
    Yellow --> Green: lag < warn_threshold
    Yellow --> Red: lag >= critical_threshold
    Red --> Green: lag < warn_threshold (hysteresis)

    state Green {
        note right of Green
            Full speed
            Throttle: 0
            Shed: none
        end note
    }

    state Yellow {
        note right of Yellow
            Throttle: proportional to lag position
            Shed: best-effort adapters
            Detector: throttled sleep between events
        end note
    }

    state Red {
        note right of Red
            Throttle: max
            Shed: normal + best-effort adapters
            Detector: PAUSED (WaitResume blocks)
        end note
    }
```

**Hysteresis**: Red exits only when lag drops below `warn_threshold` (not `critical_threshold`), preventing rapid oscillation.

**Proportional throttle**: In the yellow zone, the throttle duration scales linearly from 0 to `max_throttle` (default 500ms) based on the lag's position within the yellow band:

```
throttle = max_throttle * (lag - warn) / (critical - warn)
```

**Load shedding**: Adapters are assigned priorities (`critical`, `normal`, `best-effort`). In yellow zone, best-effort adapters are shed (events auto-acked without delivery). In red zone, both normal and best-effort are shed. Critical adapters are never shed.

## Kafka Wire Protocol Server

The kafkaserver adapter (`adapter/kafkaserver/`) implements a TCP server speaking the Kafka wire protocol:

```mermaid
graph TD
    subgraph KafkaServer ["Kafka Protocol Server"]
        Listener["TCP Listener\n(:9092)"]
        Accept["Accept Loop"]
        Conn1["Connection Handler"]
        Conn2["Connection Handler"]

        subgraph Handler ["Request Handler"]
            Dispatch["API Dispatch"]
            ApiVersions["ApiVersions (18)"]
            Metadata["Metadata (3)"]
            FindCoord["FindCoordinator (10)"]
            JoinGroup["JoinGroup (11)"]
            SyncGroup["SyncGroup (14)"]
            Heartbeat["Heartbeat (12)"]
            LeaveGroup["LeaveGroup (13)"]
            ListOffsets["ListOffsets (2)"]
            Fetch["Fetch (1)\n(long-poll)"]
            OffsetCommit["OffsetCommit (8)"]
            OffsetFetch["OffsetFetch (9)"]
        end

        subgraph Broker ["Topic Registry"]
            Topic1["Topic: pgcdc.orders"]
            Topic2["Topic: pgcdc.users"]
            P0["Partition 0\n(ring buffer)"]
            P1["Partition 1\n(ring buffer)"]
            PN["Partition N\n(ring buffer)"]
        end

        subgraph Groups ["Consumer Groups"]
            CG1["Group: my-app"]
            SM["State Machine\nEmpty → Preparing →\nCompleting → Stable"]
            Reaper["Session Reaper\n(heartbeat timeout)"]
        end

        subgraph Offsets ["Offset Store"]
            CP["checkpoint.Store\nkeys: kafka:{group}:{topic}:{partition}"]
        end
    end

    BusEvents["Bus Events"] --> Ingest["Ingest Goroutine"]
    Ingest -->|"hash key → partition"| Broker
    Listener --> Accept
    Accept --> Conn1
    Accept --> Conn2
    Conn1 --> Dispatch
    Dispatch --> Fetch
    Fetch -->|"long-poll waiter"| P0
```

**Wire format**: Requests are length-prefixed (4-byte big-endian int32). The header contains API key, version, correlation ID, and client ID. ApiVersions v3+ uses flexible encoding (compact arrays, unsigned varint lengths, tagged fields).

**Partition hashing**: Events are hashed using FNV-1a on the key column (default `id`) extracted from the JSON payload, then mapped to a partition index via modulo.

**Long-poll Fetch**: When a partition has no new data, the Fetch handler registers a waiter channel on the partition. The waiter is woken when new records are appended, or when a timeout expires.

## Streaming SQL View Engine

The view engine (`view/`) processes CDC events through SQL-defined windows:

```mermaid
graph TD
    subgraph ViewAdapter ["View Adapter"]
        EventReader["Event Reader\n(from bus subscription)"]
        Engine["View Engine"]
        ResultEmitter["Result Emitter\n(to bus ingest)"]
    end

    subgraph Engine ["View Engine"]
        Process["Process(event)"]
        LoopCheck{"Channel starts with\npgcdc:_view: ?"}
        ParsePayload["Parse JSON Payload"]
        ViewN["View Instance N"]

        subgraph ViewInstance ["View Instance"]
            WherePred["WHERE Predicate"]
            WindowImpl["Window Implementation"]
        end
    end

    subgraph Windows ["Window Types"]
        Tumbling["Tumbling Window\n- Fixed duration\n- Non-overlapping\n- ALLOWED LATENESS"]
        Sliding["Sliding Window\n- Duration W, Slide S\n- ceil(W/S) sub-windows\n- Cross-window merge"]
        Session["Session Window\n- Gap-based\n- Per-group timeout\n- Close on inactivity"]
    end

    subgraph Aggregators ["Aggregators"]
        COUNT["COUNT / COUNT(*)"]
        SUM["SUM"]
        AVG["AVG"]
        MIN["MIN"]
        MAX["MAX"]
        CD["COUNT(DISTINCT)"]
        STDDEV["STDDEV\n(Welford's algorithm)"]
    end

    EventReader --> Process
    Process --> LoopCheck
    LoopCheck -->|"yes (skip)"| Drop["Discard\n(loop prevention)"]
    LoopCheck -->|"no"| ParsePayload
    ParsePayload --> ViewN
    ViewN --> WherePred
    WherePred -->|"pass"| WindowImpl
    WindowImpl --> Tumbling
    WindowImpl --> Sliding
    WindowImpl --> Session

    Tumbling -->|"tick"| Flush["Flush Results"]
    Sliding -->|"slide"| Flush
    Session -->|"gap timeout"| Flush

    Flush -->|"VIEW_RESULT events\non pgcdc:_view:{name}"| ResultEmitter
    ResultEmitter -->|"re-inject"| BusIngest["Bus Ingest Chan"]
```

**SQL parsing**: Queries are parsed with the TiDB SQL parser after extracting custom window clauses (`TUMBLING WINDOW`, `SLIDING WINDOW ... SLIDE`, `SESSION WINDOW`) via regex. The parser validates SELECT fields, FROM (must be `pgcdc_events`), GROUP BY, and HAVING.

**Emit modes**: `row` emits one event per group key; `batch` emits a single event containing all group results as an array.

## Transform Pipeline

Transforms are applied in the wrapper goroutine, loaded atomically from `wrapperConfig`:

```mermaid
flowchart LR
    Event["Input Event"] --> Global["Global Transforms"]
    Global --> PerAdapter["Per-Adapter Transforms"]
    PerAdapter --> Output["Output Event"]

    subgraph Global ["Global Transforms"]
        DropCols["drop_columns"]
        FilterOps["filter (operations)"]
    end

    subgraph PerAdapter ["Per-Adapter Transforms"]
        Rename["rename_fields"]
        Mask["mask (zero/hash/redact)"]
        Debezium["debezium (envelope)"]
        CloudEvents["cloudevents (v1.0)"]
        FilterField["filter (by field)"]
    end
```

Transforms are composed left-to-right via `transform.Chain`. Any transform returning `transform.ErrDropEvent` silently drops the event. Other errors cause the event to be skipped with an error metric.

## SIGHUP Hot-Reload

On `SIGHUP`, the CLI handler re-reads the YAML config file and calls `Pipeline.Reload()`:

```mermaid
sequenceDiagram
    participant OS as SIGHUP Signal
    participant CLI as cmd/listen.go
    participant Pipeline as Pipeline.Reload()
    participant Wrapper as Wrapper Goroutine

    OS->>CLI: kill -HUP <pid>
    CLI->>CLI: Re-read YAML config
    CLI->>CLI: Rebuild transforms + routes
    CLI->>Pipeline: Reload(ReloadConfig)

    loop For each adapter
        Pipeline->>Pipeline: Build transform chain\n(global + per-adapter)
        Pipeline->>Pipeline: Build route filter
        Pipeline->>Pipeline: atomic.Pointer.Store(wrapperConfig)
    end

    Pipeline->>CLI: return nil
    CLI->>CLI: Increment pgcdc_config_reloads_total

    Note over Wrapper: Next event loads\nnew config atomically
    Wrapper->>Wrapper: cfgPtr.Load()
    Note over Wrapper: Zero event loss
```

Immutable on reload: CLI flags, plugin transforms, adapters, detectors, bus mode.
Mutable on reload: `transforms:` and `routes:` YAML sections.

## Ecosystem & Edge Components

The five newer components that extend pgcdc beyond the core pipeline:

```mermaid
flowchart TB
    subgraph WGW ["detector/webhookgw · Webhook Gateway"]
        direction LR
        HTTP_IN["POST /ingest/:source"] --> SigV["Signature Validation\nStripe · GitHub · generic HMAC"]
        SigV --> WGW_OUT["event.Event\nsource=webhook_gateway"]
    end

    subgraph SQ ["detector/sqlite · SQLite CDC"]
        direction LR
        POLL["Poll loop\n100ms default"] --> SQTBL["SELECT FROM pgcdc_changes\nWHERE processed=0"]
        SQTBL --> SQ_OUT["event.Event\nsource=sqlite"]
        SQ_OUT --> SQACT{"keepProcessed?"}
        SQACT -->|"false"| DEL["DELETE row"]
        SQACT -->|"true"| MARK["UPDATE processed=1"]
    end

    subgraph GQL ["adapter/graphql · GraphQL Subscriptions"]
        direction LR
        WS_UP["WebSocket Upgrade\ngraphql-transport-ws"] --> PROTO["Protocol Handler\nconnection_init → ack\nsubscribe → next"]
        PROTO --> FAN["Subscriber Fan-out\n(channel filter)"]
        FAN --> WS_DOWN["WS next message\nJSON event payload"]
    end

    subgraph ARW ["adapter/arrow · Arrow Flight"]
        direction LR
        GRPC_SRV["gRPC Flight Server\n:addr"] --> LF["ListFlights\nper-channel descriptor"]
        LF --> DG["DoGet\nring buffer → RecordBatches"]
    end

    subgraph DUCK ["adapter/duckdb · DuckDB Analytics"]
        direction LR
        BUF["Event Buffer\n(flush interval)"] --> FLUSH["INSERT INTO cdc_events\n(in-process DuckDB)"]
        FLUSH --> QRY["POST /query\nSQL → []map[string]any"]
        FLUSH --> TBL["GET /query/tables\nchannel counts"]
    end
```

| Component | Interface | HTTP/gRPC Mount | Build tag |
|-----------|-----------|-----------------|-----------|
| `detector/webhookgw` | `detector.Detector` + `HTTPMountable` | `MountHTTP(chi.Router)` | — |
| `detector/sqlite` | `detector.Detector` | — | `no_sqlite` |
| `adapter/graphql` | `adapter.Adapter` + `HTTPMountable` | `MountHTTP(chi.Router)` | — |
| `adapter/arrow` | `adapter.Adapter` | gRPC listener (self-managed) | `no_arrow` |
| `adapter/duckdb` | `adapter.Adapter` + `HTTPMountable` + `Drainer` | `MountHTTP(chi.Router)` | `no_duckdb` (CGO) |
