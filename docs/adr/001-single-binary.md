# ADR-001: Single Statically-Linked Binary

## Status
Accepted

## Context
CDC tools often use plugin architectures requiring separate deployments, JVM runtimes, or connector management (Debezium connectors, Kafka Connect). This adds operational complexity: version compatibility, classpaths, plugin discovery, and multi-process orchestration.

pgcdc targets operators who want a single artifact they can deploy anywhere -- bare metal, containers, or serverless -- with zero runtime dependencies.

## Decision
Ship pgcdc as a single statically-linked Go binary with all 17 adapters and 5 detectors compiled in. Use Go build tags (`no_kafka`, `no_grpc`, `no_iceberg`, `no_nats`, `no_redis`, `no_plugins`, `no_kafkaserver`, `no_views`) for slim binary variants that exclude heavy dependencies.

Wasm plugins (Extism-based, pure Go, no CGo) provide extensibility without breaking the single-binary model -- plugins are loaded at runtime from `.wasm` files but the host runtime is statically linked.

## Consequences

### Positive
- Zero-dependency deployment: copy one binary, run it
- No plugin discovery failures, classpath issues, or version mismatches at runtime
- Build tags let operators trade binary size for features (full ~50MB, slim ~15MB)
- Container images are minimal (scratch or distroless base)
- Wasm plugins provide safe extensibility without breaking the single-binary deployment model

### Negative
- Full binary is larger (~50MB) than a minimal core + plugins approach
- Adding a new adapter requires recompiling the binary
- All adapter dependencies are linked even if unused (mitigated by build tags)

### Neutral
- Go's compilation model makes this natural -- static linking is the default
- Cross-compilation produces binaries for all target platforms from a single CI job
