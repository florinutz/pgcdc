package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/florinutz/pgcdc"
	embeddingadapter "github.com/florinutz/pgcdc/adapter/embedding"
	execadapter "github.com/florinutz/pgcdc/adapter/exec"
	fileadapter "github.com/florinutz/pgcdc/adapter/file"
	"github.com/florinutz/pgcdc/adapter/pgtable"
	searchadapter "github.com/florinutz/pgcdc/adapter/search"
	"github.com/florinutz/pgcdc/adapter/sse"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/adapter/webhook"
	"github.com/florinutz/pgcdc/adapter/ws"
	"github.com/florinutz/pgcdc/backpressure"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/detector"
	"github.com/florinutz/pgcdc/detector/listennotify"
	mongodbdetector "github.com/florinutz/pgcdc/detector/mongodb"
	mysqldetector "github.com/florinutz/pgcdc/detector/mysql"
	"github.com/florinutz/pgcdc/detector/outbox"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/encoding"
	"github.com/florinutz/pgcdc/encoding/registry"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/internal/migrate"
	"github.com/florinutz/pgcdc/internal/server"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/snapshot"
	"github.com/florinutz/pgcdc/tracing"
	"github.com/florinutz/pgcdc/transform"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

var listenCmd = &cobra.Command{
	Use:   "listen",
	Short: "Listen for PostgreSQL NOTIFY events and forward them to adapters",
	Long: `Connects to PostgreSQL, subscribes to one or more channels via
LISTEN/NOTIFY, and fans out every notification to the configured adapters
(stdout, webhook, sse).`,
	RunE: runListen,
}

func init() {
	f := listenCmd.Flags()

	f.StringSliceP("channel", "c", nil, "PG channels to listen on (repeatable)")
	f.StringSliceP("adapter", "a", []string{"stdout"}, "adapters: webhook, sse, stdout (repeatable)")
	f.StringP("url", "u", "", "webhook destination URL")
	f.String("sse-addr", ":8080", "SSE server listen address")
	f.String("db", "", "PostgreSQL connection string (env: PGCDC_DATABASE_URL)")
	f.Int("retries", 5, "webhook max retries")
	f.String("signing-key", "", "HMAC signing key for webhook")
	f.String("metrics-addr", "", "standalone metrics/health server address (e.g. :9090)")
	f.String("detector", "listen_notify", "detector type: listen_notify, wal, outbox, mysql, or mongodb")
	f.String("publication", "", "PostgreSQL publication name (required for --detector wal)")
	f.Bool("all-tables", false, "auto-create a FOR ALL TABLES publication and start WAL streaming (zero-config)")
	f.Bool("tx-metadata", false, "include transaction metadata in WAL events (xid, commit_time, seq)")
	f.Bool("tx-markers", false, "emit BEGIN/COMMIT marker events (implies --tx-metadata)")

	// File adapter flags.
	f.String("file-path", "", "file adapter output path")
	f.Int64("file-max-size", 0, "file rotation size in bytes (default 100MB)")
	f.Int("file-max-files", 0, "number of rotated files to keep (default 5)")

	// Exec adapter flags.
	f.String("exec-command", "", "shell command to pipe events to (via stdin)")

	// PG table adapter flags.
	f.String("pg-table-url", "", "PostgreSQL URL for pg_table adapter (default: same as --db)")
	f.String("pg-table-name", "", "destination table name (default: pgcdc_events)")

	// WebSocket adapter flags.
	f.Duration("ws-ping-interval", 0, "WebSocket ping interval (default 15s)")

	// Persistent slot flags.
	f.Bool("persistent-slot", false, "use a named persistent replication slot (survives disconnects)")
	f.String("slot-name", "", "replication slot name (default: pgcdc_<publication>)")
	f.String("checkpoint-db", "", "PostgreSQL URL for checkpoint storage (default: same as --db)")

	// Schema flags.
	f.Bool("include-schema", false, "include column type metadata in WAL events")
	f.Bool("schema-events", false, "emit SCHEMA_CHANGE events when table columns change")

	// Heartbeat flags.
	f.Duration("heartbeat-interval", 30*time.Second, "heartbeat interval for WAL slot advancement (0 to disable)")
	f.String("heartbeat-table", "pgcdc_heartbeat", "heartbeat table name")
	f.Int64("slot-lag-warn", 100*1024*1024, "slot lag warning threshold in bytes (default 100MB)")

	// Snapshot-first flags (no viper bindings — read directly to avoid collision).
	f.Bool("snapshot-first", false, "run a table snapshot before live WAL streaming (requires --detector wal)")
	f.String("snapshot-table", "", "table to snapshot (required with --snapshot-first)")
	f.String("snapshot-where", "", "optional WHERE clause for snapshot")

	// TOAST cache flags (no viper bindings — read directly to avoid collision).
	f.Bool("toast-cache", false, "enable in-memory TOAST column cache to resolve unchanged columns without REPLICA IDENTITY FULL (requires --detector wal)")
	f.Int("toast-cache-max-entries", 100000, "maximum number of rows in the TOAST cache")

	// Incremental snapshot flags (no viper bindings — read directly to avoid collision).
	f.Bool("incremental-snapshot", false, "enable incremental snapshots via signal table (requires --detector wal)")
	f.String("snapshot-signal-table", "pgcdc_signals", "signal table name")
	f.Int("snapshot-chunk-size", 1000, "rows per chunk for incremental snapshots")
	f.Duration("snapshot-chunk-delay", 0, "delay between chunks for incremental snapshots")
	f.String("snapshot-progress-db", "", "PostgreSQL URL for progress storage (default: same as --db)")

	// Embedding adapter flags.
	f.String("embedding-api-url", "", "OpenAI-compatible embedding API URL")
	f.String("embedding-api-key", "", "API key for embedding service")
	f.String("embedding-model", "", "embedding model name (default: text-embedding-3-small)")
	f.StringSlice("embedding-columns", nil, "columns to embed from event payload row (required for embedding adapter)")
	f.String("embedding-id-column", "", "source row ID column for UPSERT/DELETE (default: id)")
	f.String("embedding-table", "", "destination pgvector table (default: pgcdc_embeddings)")
	f.String("embedding-db-url", "", "PostgreSQL URL for pgvector table (default: same as --db)")
	f.Int("embedding-dimension", 0, "vector dimension (default: 1536)")

	// Iceberg adapter flags.
	f.String("iceberg-catalog", "hadoop", "Iceberg catalog type: hadoop, rest, sql")
	f.String("iceberg-catalog-uri", "", "REST catalog URL")
	f.String("iceberg-warehouse", "", "Iceberg warehouse path (s3://... or local path)")
	f.String("iceberg-namespace", "pgcdc", "Iceberg namespace (dot-separated)")
	f.String("iceberg-table", "", "Iceberg table name")
	f.String("iceberg-mode", "append", "Iceberg write mode: append or upsert")
	f.String("iceberg-schema", "raw", "Iceberg schema mode: auto or raw")
	f.StringSlice("iceberg-pk", nil, "primary key columns for upsert mode (repeatable)")
	f.Duration("iceberg-flush-interval", 0, "Iceberg flush interval (default 1m)")
	f.Int("iceberg-flush-size", 0, "Iceberg flush size in events (default 10000)")

	mustBindPFlag("iceberg.catalog_type", f.Lookup("iceberg-catalog"))
	mustBindPFlag("iceberg.catalog_uri", f.Lookup("iceberg-catalog-uri"))
	mustBindPFlag("iceberg.warehouse", f.Lookup("iceberg-warehouse"))
	mustBindPFlag("iceberg.namespace", f.Lookup("iceberg-namespace"))
	mustBindPFlag("iceberg.table", f.Lookup("iceberg-table"))
	mustBindPFlag("iceberg.mode", f.Lookup("iceberg-mode"))
	mustBindPFlag("iceberg.schema_mode", f.Lookup("iceberg-schema"))
	mustBindPFlag("iceberg.primary_keys", f.Lookup("iceberg-pk"))
	mustBindPFlag("iceberg.flush_interval", f.Lookup("iceberg-flush-interval"))
	mustBindPFlag("iceberg.flush_size", f.Lookup("iceberg-flush-size"))

	// S3 adapter flags.
	f.String("s3-bucket", "", "S3 bucket name")
	f.String("s3-prefix", "", "S3 object key prefix")
	f.String("s3-endpoint", "", "S3-compatible endpoint URL (e.g. MinIO, R2)")
	f.String("s3-region", "us-east-1", "S3 region")
	f.String("s3-access-key-id", "", "S3 access key ID (default: AWS default chain)")
	f.String("s3-secret-access-key", "", "S3 secret access key (default: AWS default chain)")
	f.String("s3-format", "jsonl", "S3 output format: jsonl or parquet")
	f.Duration("s3-flush-interval", 0, "S3 flush interval (default 1m)")
	f.Int("s3-flush-size", 0, "S3 flush size in events (default 10000)")
	f.Duration("s3-drain-timeout", 0, "S3 shutdown drain timeout (default 30s)")

	mustBindPFlag("s3.bucket", f.Lookup("s3-bucket"))
	mustBindPFlag("s3.prefix", f.Lookup("s3-prefix"))
	mustBindPFlag("s3.endpoint", f.Lookup("s3-endpoint"))
	mustBindPFlag("s3.region", f.Lookup("s3-region"))
	mustBindPFlag("s3.access_key_id", f.Lookup("s3-access-key-id"))
	mustBindPFlag("s3.secret_access_key", f.Lookup("s3-secret-access-key"))
	mustBindPFlag("s3.format", f.Lookup("s3-format"))
	mustBindPFlag("s3.flush_interval", f.Lookup("s3-flush-interval"))
	mustBindPFlag("s3.flush_size", f.Lookup("s3-flush-size"))
	mustBindPFlag("s3.drain_timeout", f.Lookup("s3-drain-timeout"))

	// NATS adapter flags.
	f.String("nats-url", "nats://localhost:4222", "NATS server URL")
	f.String("nats-subject", "pgcdc", "NATS subject prefix")
	f.String("nats-stream", "pgcdc", "NATS JetStream stream name")
	f.String("nats-cred-file", "", "NATS credentials file")
	f.Duration("nats-max-age", 0, "NATS stream max message age (default 24h)")

	// Kafka adapter flags.
	f.StringSlice("kafka-brokers", []string{"localhost:9092"}, "Kafka broker addresses")
	f.String("kafka-topic", "", "fixed Kafka topic (empty = per-channel, pgcdc:orders→pgcdc.orders)")
	f.String("kafka-sasl-mechanism", "", "SASL mechanism: plain, scram-sha-256, scram-sha-512")
	f.String("kafka-sasl-username", "", "SASL username")
	f.String("kafka-sasl-password", "", "SASL password")
	f.Bool("kafka-tls", false, "enable TLS for Kafka connection")
	f.String("kafka-tls-ca-file", "", "CA certificate file for Kafka TLS")
	f.String("kafka-transactional-id", "", "Kafka transactional.id for exactly-once delivery (empty = idempotent only)")

	// Encoding flags (read directly, not viper-bound).
	f.String("kafka-encoding", "json", "Kafka message encoding: json, avro, or protobuf")
	f.String("nats-encoding", "json", "NATS message encoding: json, avro, or protobuf")
	f.String("schema-registry-url", "", "Confluent Schema Registry URL")
	f.String("schema-registry-username", "", "Schema Registry basic auth username")
	f.String("schema-registry-password", "", "Schema Registry basic auth password")

	// DLQ flags.
	f.String("dlq", "stderr", "dead letter queue backend: stderr, pg_table, or none")
	f.String("dlq-table", "pgcdc_dead_letters", "DLQ table name (for pg_table backend)")
	f.String("dlq-db", "", "PostgreSQL URL for DLQ table (default: same as --db)")

	// Route flags.
	f.StringSlice("route", nil, "route events to adapter: adapter=channel1,channel2 (repeatable)")

	// Search adapter flags.
	f.String("search-engine", "typesense", "search engine: typesense or meilisearch")
	f.String("search-url", "", "search engine URL")
	f.String("search-api-key", "", "search engine API key")
	f.String("search-index", "", "search index name")
	f.String("search-id-column", "id", "row ID column for search documents")
	f.Int("search-batch-size", 100, "search batch size")
	f.Duration("search-batch-interval", time.Second, "search batch flush interval")

	// Redis adapter flags.
	f.String("redis-url", "", "Redis URL (e.g. redis://localhost:6379)")
	f.String("redis-mode", "invalidate", "Redis mode: invalidate or sync")
	f.String("redis-key-prefix", "", "Redis key prefix (e.g. orders:)")
	f.String("redis-id-column", "id", "row ID column for Redis keys")

	// gRPC adapter flags.
	f.String("grpc-addr", ":9090", "gRPC server listen address")

	// Kafka server adapter flags.
	f.String("kafkaserver-addr", ":9092", "Kafka protocol server listen address")
	f.Int("kafkaserver-partitions", 8, "number of partitions per topic")
	f.Int("kafkaserver-buffer-size", 10000, "ring buffer size per partition")
	f.Duration("kafkaserver-session-timeout", 30*time.Second, "consumer group session timeout")
	f.String("kafkaserver-checkpoint-db", "", "PostgreSQL URL for offset storage (default: same as --db)")
	f.String("kafkaserver-key-column", "id", "JSON field used as partition key")

	// Outbox detector flags.
	f.String("outbox-table", "pgcdc_outbox", "outbox table name")
	f.Duration("outbox-poll-interval", 500*time.Millisecond, "outbox polling interval")
	f.Int("outbox-batch-size", 100, "outbox batch size per poll")
	f.Bool("outbox-keep-processed", false, "keep processed outbox rows (set processed_at instead of DELETE)")

	// MySQL detector flags (read directly, not viper-bound — same pattern as snapshot flags).
	f.String("mysql-addr", "", "MySQL address (host:port)")
	f.String("mysql-user", "", "MySQL user")
	f.String("mysql-password", "", "MySQL password")
	f.Uint32("mysql-server-id", 0, "MySQL server ID for replication (must be > 0)")
	f.StringSlice("mysql-tables", nil, "MySQL tables to replicate (schema.table format, repeatable)")
	f.Bool("mysql-gtid", false, "use GTID-based replication instead of file+position")
	f.String("mysql-flavor", "mysql", "MySQL flavor: mysql or mariadb")
	f.String("mysql-binlog-prefix", "mysql-bin", "binlog filename prefix for position decoding")

	mustBindPFlag("mysql.addr", f.Lookup("mysql-addr"))
	mustBindPFlag("mysql.user", f.Lookup("mysql-user"))
	mustBindPFlag("mysql.password", f.Lookup("mysql-password"))
	mustBindPFlag("mysql.server_id", f.Lookup("mysql-server-id"))
	mustBindPFlag("mysql.tables", f.Lookup("mysql-tables"))
	mustBindPFlag("mysql.use_gtid", f.Lookup("mysql-gtid"))
	mustBindPFlag("mysql.flavor", f.Lookup("mysql-flavor"))
	mustBindPFlag("mysql.binlog_prefix", f.Lookup("mysql-binlog-prefix"))

	// MongoDB detector flags.
	f.String("mongodb-uri", "", "MongoDB connection URI")
	f.String("mongodb-scope", "collection", "MongoDB watch scope: collection, database, or cluster")
	f.String("mongodb-database", "", "MongoDB database name")
	f.StringSlice("mongodb-collections", nil, "MongoDB collections to watch (repeatable)")
	f.String("mongodb-full-document", "updateLookup", "MongoDB fullDocument option: updateLookup, default, whenAvailable, required")
	f.String("mongodb-metadata-db", "", "MongoDB database for resume token storage (default: same as --mongodb-database)")
	f.String("mongodb-metadata-coll", "pgcdc_resume_tokens", "MongoDB collection for resume token storage")

	mustBindPFlag("mongodb.uri", f.Lookup("mongodb-uri"))
	mustBindPFlag("mongodb.scope", f.Lookup("mongodb-scope"))
	mustBindPFlag("mongodb.database", f.Lookup("mongodb-database"))
	mustBindPFlag("mongodb.collections", f.Lookup("mongodb-collections"))
	mustBindPFlag("mongodb.full_document", f.Lookup("mongodb-full-document"))
	mustBindPFlag("mongodb.metadata_db", f.Lookup("mongodb-metadata-db"))
	mustBindPFlag("mongodb.metadata_coll", f.Lookup("mongodb-metadata-coll"))

	// Bus mode flag.
	f.String("bus-mode", "fast", "bus fan-out mode: fast (drop on full) or reliable (block on full)")

	// Cooperative checkpoint flag (read directly, not viper-bound).
	f.Bool("cooperative-checkpoint", false, "checkpoint advances only after all adapters acknowledge (requires --persistent-slot and --detector wal)")

	// Transform flags (read directly, not viper-bound — same pattern as --snapshot-first).
	f.StringSlice("drop-columns", nil, "global: drop these columns from event payloads (repeatable)")
	f.StringSlice("filter-operations", nil, "global: only pass events with these operations (e.g. INSERT,UPDATE)")
	f.Bool("debezium-envelope", false, "global: rewrite payloads into Debezium-compatible envelope format")
	f.String("debezium-connector-name", "pgcdc", "Debezium source.name field (requires --debezium-envelope)")
	f.String("debezium-database", "", "Debezium source.db field (requires --debezium-envelope)")
	f.Bool("cloudevents-envelope", false, "global: rewrite payloads into CloudEvents structured-mode JSON")
	f.String("cloudevents-source", "/pgcdc", "CloudEvents source URI-reference (requires --cloudevents-envelope)")
	f.String("cloudevents-type-prefix", "io.pgcdc.change", "CloudEvents type prefix (requires --cloudevents-envelope)")

	// Backpressure flags (read directly, not viper-bound).
	f.Bool("backpressure", false, "enable source-aware backpressure (requires --detector wal and --persistent-slot)")
	f.Int64("bp-warn-threshold", 500*1024*1024, "yellow zone threshold in bytes (default 500MB)")
	f.Int64("bp-critical-threshold", 2*1024*1024*1024, "red zone threshold in bytes (default 2GB)")
	f.Duration("bp-max-throttle", 500*time.Millisecond, "max sleep between WAL reads in yellow zone")
	f.Duration("bp-poll-interval", 10*time.Second, "backpressure lag polling interval")
	f.StringSlice("adapter-priority", nil, "adapter priority: name=critical|normal|best-effort (repeatable)")

	// Plugin flags (read directly, not viper-bound).
	f.StringSlice("plugin-transform", nil, "wasm transform plugin paths (repeatable)")
	f.StringSlice("plugin-transform-config", nil, "JSON config for each plugin-transform (parallel order)")
	f.StringSlice("plugin-adapter", nil, "wasm adapter plugin paths (repeatable)")
	f.StringSlice("plugin-adapter-name", nil, "names for each plugin-adapter (parallel order)")
	f.StringSlice("plugin-adapter-config", nil, "JSON config for each plugin-adapter (parallel order)")
	f.String("dlq-plugin-path", "", "wasm DLQ plugin path (use with --dlq plugin)")
	f.String("dlq-plugin-config", "", "JSON config for DLQ plugin")
	f.String("checkpoint-plugin", "", "wasm checkpoint store plugin path")
	f.String("checkpoint-plugin-config", "", "JSON config for checkpoint plugin")

	// Startup validation flag.
	f.Bool("skip-validation", false, "skip adapter startup validation")
	mustBindPFlag("skip_validation", f.Lookup("skip-validation"))

	// Schema migrations flag.
	f.Bool("skip-migrations", false, "skip internal schema migrations (for read-only DB users)")
	mustBindPFlag("skip_migrations", f.Lookup("skip-migrations"))

	// View query flag (repeatable, name:query format).
	f.StringSlice("view-query", nil, "streaming SQL view query: name:query (repeatable, e.g. --view-query 'counts:SELECT COUNT(*) FROM pgcdc_events GROUP BY channel TUMBLING WINDOW 1m')")

	// Circuit breaker flags for webhook.
	f.Int("webhook-cb-failures", 0, "webhook circuit breaker max failures before open (0 = disabled)")
	f.Duration("webhook-cb-reset", 60*time.Second, "webhook circuit breaker reset timeout")
	mustBindPFlag("webhook.cb_max_failures", f.Lookup("webhook-cb-failures"))
	mustBindPFlag("webhook.cb_reset_timeout", f.Lookup("webhook-cb-reset"))

	// Rate limit flags for webhook.
	f.Float64("webhook-rate-limit", 0, "webhook rate limit in events/second (0 = unlimited)")
	f.Int("webhook-rate-burst", 0, "webhook rate limit burst size")
	mustBindPFlag("webhook.rate_limit", f.Lookup("webhook-rate-limit"))
	mustBindPFlag("webhook.rate_limit_burst", f.Lookup("webhook-rate-burst"))

	// Circuit breaker flags for embedding.
	f.Int("embedding-cb-failures", 0, "embedding circuit breaker max failures (0 = disabled)")
	f.Duration("embedding-cb-reset", 60*time.Second, "embedding circuit breaker reset timeout")
	mustBindPFlag("embedding.cb_max_failures", f.Lookup("embedding-cb-failures"))
	mustBindPFlag("embedding.cb_reset_timeout", f.Lookup("embedding-cb-reset"))

	// Rate limit flags for embedding.
	f.Float64("embedding-rate-limit", 0, "embedding rate limit in events/second (0 = unlimited)")
	f.Int("embedding-rate-burst", 0, "embedding rate limit burst size")
	mustBindPFlag("embedding.rate_limit", f.Lookup("embedding-rate-limit"))
	mustBindPFlag("embedding.rate_limit_burst", f.Lookup("embedding-rate-burst"))

	// OpenTelemetry flags (read directly, not viper-bound).
	f.String("otel-exporter", "none", "OTel trace exporter: none, stdout, or otlp")
	f.String("otel-endpoint", "", "OTLP gRPC endpoint (overrides OTEL_EXPORTER_OTLP_ENDPOINT)")
	f.Float64("otel-sample-ratio", 1.0, "trace sampling ratio (0.0-1.0)")

	mustBindPFlag("otel.exporter", f.Lookup("otel-exporter"))
	mustBindPFlag("otel.endpoint", f.Lookup("otel-endpoint"))
	mustBindPFlag("otel.sample_ratio", f.Lookup("otel-sample-ratio"))

	mustBindPFlag("dlq.type", f.Lookup("dlq"))
	mustBindPFlag("dlq.table", f.Lookup("dlq-table"))
	mustBindPFlag("dlq.db_url", f.Lookup("dlq-db"))

	mustBindPFlag("search.engine", f.Lookup("search-engine"))
	mustBindPFlag("search.url", f.Lookup("search-url"))
	mustBindPFlag("search.api_key", f.Lookup("search-api-key"))
	mustBindPFlag("search.index", f.Lookup("search-index"))
	mustBindPFlag("search.id_column", f.Lookup("search-id-column"))
	mustBindPFlag("search.batch_size", f.Lookup("search-batch-size"))
	mustBindPFlag("search.batch_interval", f.Lookup("search-batch-interval"))

	mustBindPFlag("redis.url", f.Lookup("redis-url"))
	mustBindPFlag("redis.mode", f.Lookup("redis-mode"))
	mustBindPFlag("redis.key_prefix", f.Lookup("redis-key-prefix"))
	mustBindPFlag("redis.id_column", f.Lookup("redis-id-column"))

	mustBindPFlag("grpc.addr", f.Lookup("grpc-addr"))

	mustBindPFlag("kafkaserver.addr", f.Lookup("kafkaserver-addr"))
	mustBindPFlag("kafkaserver.partition_count", f.Lookup("kafkaserver-partitions"))
	mustBindPFlag("kafkaserver.buffer_size", f.Lookup("kafkaserver-buffer-size"))
	mustBindPFlag("kafkaserver.session_timeout", f.Lookup("kafkaserver-session-timeout"))
	mustBindPFlag("kafkaserver.checkpoint_db", f.Lookup("kafkaserver-checkpoint-db"))
	mustBindPFlag("kafkaserver.key_column", f.Lookup("kafkaserver-key-column"))

	mustBindPFlag("outbox.table", f.Lookup("outbox-table"))
	mustBindPFlag("outbox.poll_interval", f.Lookup("outbox-poll-interval"))
	mustBindPFlag("outbox.batch_size", f.Lookup("outbox-batch-size"))
	mustBindPFlag("outbox.keep_processed", f.Lookup("outbox-keep-processed"))

	mustBindPFlag("nats.url", f.Lookup("nats-url"))
	mustBindPFlag("nats.subject", f.Lookup("nats-subject"))
	mustBindPFlag("nats.stream", f.Lookup("nats-stream"))
	mustBindPFlag("nats.cred_file", f.Lookup("nats-cred-file"))
	mustBindPFlag("nats.max_age", f.Lookup("nats-max-age"))

	mustBindPFlag("kafka.brokers", f.Lookup("kafka-brokers"))
	mustBindPFlag("kafka.topic", f.Lookup("kafka-topic"))
	mustBindPFlag("kafka.sasl_mechanism", f.Lookup("kafka-sasl-mechanism"))
	mustBindPFlag("kafka.sasl_username", f.Lookup("kafka-sasl-username"))
	mustBindPFlag("kafka.sasl_password", f.Lookup("kafka-sasl-password"))
	mustBindPFlag("kafka.tls", f.Lookup("kafka-tls"))
	mustBindPFlag("kafka.tls_ca_file", f.Lookup("kafka-tls-ca-file"))
	mustBindPFlag("kafka.transactional_id", f.Lookup("kafka-transactional-id"))
	mustBindPFlag("kafka.encoding", f.Lookup("kafka-encoding"))
	mustBindPFlag("nats.encoding", f.Lookup("nats-encoding"))
	mustBindPFlag("encoding.schema_registry_url", f.Lookup("schema-registry-url"))
	mustBindPFlag("encoding.schema_registry_username", f.Lookup("schema-registry-username"))
	mustBindPFlag("encoding.schema_registry_password", f.Lookup("schema-registry-password"))

	mustBindPFlag("embedding.api_url", f.Lookup("embedding-api-url"))
	mustBindPFlag("embedding.api_key", f.Lookup("embedding-api-key"))
	mustBindPFlag("embedding.model", f.Lookup("embedding-model"))
	mustBindPFlag("embedding.columns", f.Lookup("embedding-columns"))
	mustBindPFlag("embedding.id_column", f.Lookup("embedding-id-column"))
	mustBindPFlag("embedding.table", f.Lookup("embedding-table"))
	mustBindPFlag("embedding.db_url", f.Lookup("embedding-db-url"))
	mustBindPFlag("embedding.dimension", f.Lookup("embedding-dimension"))

	mustBindPFlag("bus.mode", f.Lookup("bus-mode"))

	mustBindPFlag("channels", f.Lookup("channel"))
	mustBindPFlag("adapters", f.Lookup("adapter"))
	mustBindPFlag("webhook.url", f.Lookup("url"))
	mustBindPFlag("sse.addr", f.Lookup("sse-addr"))
	mustBindPFlag("database_url", f.Lookup("db"))
	mustBindPFlag("webhook.max_retries", f.Lookup("retries"))
	mustBindPFlag("webhook.signing_key", f.Lookup("signing-key"))
	mustBindPFlag("metrics_addr", f.Lookup("metrics-addr"))
	mustBindPFlag("detector.type", f.Lookup("detector"))
	mustBindPFlag("detector.publication", f.Lookup("publication"))
	mustBindPFlag("detector.tx_metadata", f.Lookup("tx-metadata"))
	mustBindPFlag("detector.tx_markers", f.Lookup("tx-markers"))
	mustBindPFlag("file.path", f.Lookup("file-path"))
	mustBindPFlag("file.max_size", f.Lookup("file-max-size"))
	mustBindPFlag("file.max_files", f.Lookup("file-max-files"))
	mustBindPFlag("exec.command", f.Lookup("exec-command"))
	mustBindPFlag("pg_table.url", f.Lookup("pg-table-url"))
	mustBindPFlag("pg_table.table", f.Lookup("pg-table-name"))
	mustBindPFlag("websocket.ping_interval", f.Lookup("ws-ping-interval"))
}

func runListen(cmd *cobra.Command, args []string) error {
	// Load config with defaults.
	cfg := config.Default()
	if err := viper.Unmarshal(&cfg); err != nil {
		return fmt.Errorf("unmarshal config: %w", err)
	}

	// tx-markers implies tx-metadata.
	if cfg.Detector.TxMarkers {
		cfg.Detector.TxMetadata = true
	}

	// Snapshot-first flags (read directly to avoid viper key collisions).
	snapshotFirst, _ := cmd.Flags().GetBool("snapshot-first")
	snapshotTable, _ := cmd.Flags().GetString("snapshot-table")
	snapshotWhere, _ := cmd.Flags().GetString("snapshot-where")

	// TOAST cache flags (read directly to avoid viper key collisions).
	toastCache, _ := cmd.Flags().GetBool("toast-cache")
	toastCacheMaxEntries, _ := cmd.Flags().GetInt("toast-cache-max-entries")

	// Persistent slot flags.
	persistentSlot, _ := cmd.Flags().GetBool("persistent-slot")
	slotName, _ := cmd.Flags().GetString("slot-name")
	checkpointDB, _ := cmd.Flags().GetString("checkpoint-db")

	// Schema flags.
	includeSchema, _ := cmd.Flags().GetBool("include-schema")
	schemaEvents, _ := cmd.Flags().GetBool("schema-events")

	// Heartbeat flags.
	heartbeatInterval, _ := cmd.Flags().GetDuration("heartbeat-interval")
	heartbeatTable, _ := cmd.Flags().GetString("heartbeat-table")
	slotLagWarn, _ := cmd.Flags().GetInt64("slot-lag-warn")

	// Incremental snapshot flags (read directly to avoid viper collisions).
	incrementalSnapshot, _ := cmd.Flags().GetBool("incremental-snapshot")
	snapshotSignalTable, _ := cmd.Flags().GetString("snapshot-signal-table")
	snapshotChunkSize, _ := cmd.Flags().GetInt("snapshot-chunk-size")
	snapshotChunkDelay, _ := cmd.Flags().GetDuration("snapshot-chunk-delay")
	snapshotProgressDB, _ := cmd.Flags().GetString("snapshot-progress-db")

	// Cooperative checkpoint flag (read directly to avoid viper collisions).
	cooperativeCheckpoint, _ := cmd.Flags().GetBool("cooperative-checkpoint")

	// Backpressure flags (read directly to avoid viper collisions).
	bpEnabled, _ := cmd.Flags().GetBool("backpressure")
	bpWarnThreshold, _ := cmd.Flags().GetInt64("bp-warn-threshold")
	bpCriticalThreshold, _ := cmd.Flags().GetInt64("bp-critical-threshold")
	bpMaxThrottle, _ := cmd.Flags().GetDuration("bp-max-throttle")
	bpPollInterval, _ := cmd.Flags().GetDuration("bp-poll-interval")
	adapterPriorityFlags, _ := cmd.Flags().GetStringSlice("adapter-priority")

	// all-tables flag (read directly — no viper binding needed).
	allTables, _ := cmd.Flags().GetBool("all-tables")
	if allTables {
		if cfg.Detector.Type == "outbox" {
			return fmt.Errorf("--all-tables is not compatible with --detector outbox")
		}
		if cfg.Detector.Type == "listen_notify" {
			cfg.Detector.Type = "wal"
		}
		if cfg.Detector.Publication == "" {
			cfg.Detector.Publication = "all"
		}
	}

	// Parse bus mode from config.
	var busMode bus.BusMode
	switch cfg.Bus.Mode {
	case "reliable":
		busMode = bus.BusModeReliable
	case "fast", "":
		busMode = bus.BusModeFast
	default:
		return fmt.Errorf("unknown bus mode: %q (expected fast or reliable)", cfg.Bus.Mode)
	}

	// Validation.
	if cfg.Detector.Type != "wal" && (cfg.Detector.TxMetadata || cfg.Detector.TxMarkers) {
		return fmt.Errorf("--tx-metadata and --tx-markers require --detector wal")
	}
	if cfg.DatabaseURL == "" {
		return fmt.Errorf("no database URL specified; use --db, set database_url in config, or export PGCDC_DATABASE_URL")
	}
	if cfg.Detector.Type == "listen_notify" && len(cfg.Channels) == 0 {
		return fmt.Errorf("no channels specified; use --channel or set channels in config file")
	}
	if cfg.Detector.Type == "wal" && cfg.Detector.Publication == "" && !allTables {
		return fmt.Errorf("WAL detector requires a publication; use --publication or set detector.publication in config")
	}
	if snapshotFirst && cfg.Detector.Type != "wal" {
		return fmt.Errorf("--snapshot-first requires --detector wal")
	}
	if cooperativeCheckpoint && cfg.Detector.Type != "wal" {
		return fmt.Errorf("--cooperative-checkpoint requires --detector wal")
	}
	if cooperativeCheckpoint && !persistentSlot {
		return fmt.Errorf("--cooperative-checkpoint requires --persistent-slot")
	}
	if snapshotFirst && snapshotTable == "" {
		return fmt.Errorf("--snapshot-first requires --snapshot-table")
	}
	if incrementalSnapshot && cfg.Detector.Type != "wal" {
		return fmt.Errorf("--incremental-snapshot requires --detector wal")
	}
	if bpEnabled && cfg.Detector.Type != "wal" {
		return fmt.Errorf("--backpressure requires --detector wal")
	}
	if bpEnabled && !persistentSlot {
		return fmt.Errorf("--backpressure requires --persistent-slot")
	}
	if toastCache && cfg.Detector.Type != "wal" {
		return fmt.Errorf("--toast-cache requires --detector wal")
	}

	// Validate adapters and check requirements early.
	hasWebhook := false
	hasSSE := false
	hasFile := false
	hasExec := false
	hasPGTable := false
	hasWS := false
	hasEmbedding := false
	hasIceberg := false
	hasNats := false
	hasSearch := false
	hasRedis := false
	hasKafka := false
	hasS3 := false
	for _, name := range cfg.Adapters {
		switch name {
		case "stdout":
			// ok
		case "webhook":
			hasWebhook = true
		case "sse":
			hasSSE = true
		case "file":
			hasFile = true
		case "exec":
			hasExec = true
		case "pg_table":
			hasPGTable = true
		case "ws":
			hasWS = true
		case "embedding":
			hasEmbedding = true
		case "iceberg":
			hasIceberg = true
		case "nats":
			hasNats = true
		case "search":
			hasSearch = true
		case "redis":
			hasRedis = true
		case "kafka":
			hasKafka = true
		case "kafkaserver":
			// kafkaserver adapter has no required config (addr has default).
		case "s3":
			hasS3 = true
		case "grpc":
			// gRPC adapter has no required config (addr has default).
		case "view":
			// view adapter is auto-created from views: config section.
		default:
			return fmt.Errorf("unknown adapter: %q (expected stdout, webhook, sse, file, exec, pg_table, ws, embedding, iceberg, nats, search, redis, kafka, kafkaserver, s3, grpc, or view)", name)
		}
	}
	if hasWebhook && cfg.Webhook.URL == "" {
		return fmt.Errorf("webhook adapter requires a URL; use --url or set webhook.url in config")
	}
	if hasFile && cfg.File.Path == "" {
		return fmt.Errorf("file adapter requires a path; use --file-path or set file.path in config")
	}
	if hasExec && cfg.Exec.Command == "" {
		return fmt.Errorf("exec adapter requires a command; use --exec-command or set exec.command in config")
	}
	if hasPGTable && cfg.PGTable.URL == "" && cfg.DatabaseURL == "" {
		return fmt.Errorf("pg_table adapter requires a database URL; use --db or --pg-table-url")
	}
	if hasEmbedding && cfg.Embedding.APIURL == "" {
		return fmt.Errorf("embedding adapter requires an API URL; use --embedding-api-url or set embedding.api_url in config")
	}
	if hasEmbedding && len(cfg.Embedding.Columns) == 0 {
		return fmt.Errorf("embedding adapter requires at least one column; use --embedding-columns or set embedding.columns in config")
	}
	if hasIceberg && cfg.Iceberg.Warehouse == "" {
		return fmt.Errorf("iceberg adapter requires a warehouse; use --iceberg-warehouse or set iceberg.warehouse in config")
	}
	if hasIceberg && cfg.Iceberg.Table == "" {
		return fmt.Errorf("iceberg adapter requires a table name; use --iceberg-table or set iceberg.table in config")
	}
	if hasIceberg && cfg.Iceberg.Mode == "upsert" && len(cfg.Iceberg.PrimaryKeys) == 0 {
		return fmt.Errorf("iceberg upsert mode requires primary keys; use --iceberg-pk or set iceberg.primary_keys in config")
	}
	if hasNats && cfg.Nats.URL == "" {
		return fmt.Errorf("nats adapter requires a URL; use --nats-url or set nats.url in config")
	}
	if hasSearch && cfg.Search.URL == "" {
		return fmt.Errorf("search adapter requires a URL; use --search-url or set search.url in config")
	}
	if hasSearch && cfg.Search.Index == "" {
		return fmt.Errorf("search adapter requires an index; use --search-index or set search.index in config")
	}
	if hasRedis && cfg.Redis.URL == "" {
		return fmt.Errorf("redis adapter requires a URL; use --redis-url or set redis.url in config")
	}
	if hasKafka && len(cfg.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka adapter requires at least one broker; use --kafka-brokers or set kafka.brokers in config")
	}
	if hasS3 && cfg.S3.Bucket == "" {
		return fmt.Errorf("s3 adapter requires a bucket; use --s3-bucket or set s3.bucket in config")
	}

	logger := slog.Default()

	// Root context: cancelled on SIGINT or SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Set up OpenTelemetry tracing.
	tp, otelShutdown, otelErr := tracing.Setup(ctx, tracing.Config{
		Exporter:     cfg.OTel.Exporter,
		Endpoint:     cfg.OTel.Endpoint,
		SampleRatio:  cfg.OTel.SampleRatio,
		DetectorType: cfg.Detector.Type,
	}, logger)
	if otelErr != nil {
		return fmt.Errorf("setup otel tracing: %w", otelErr)
	}
	defer otelShutdown()

	// Build pipeline options (declared early so detector can append).
	opts := []pgcdc.Option{
		pgcdc.WithBusBuffer(cfg.Bus.BufferSize),
		pgcdc.WithBusMode(busMode),
		pgcdc.WithLogger(logger),
		pgcdc.WithTracerProvider(tp),
		pgcdc.WithShutdownTimeout(cfg.ShutdownTimeout),
	}
	if cfg.SkipValidation {
		opts = append(opts, pgcdc.WithSkipValidation(true))
	}
	if cooperativeCheckpoint {
		opts = append(opts, pgcdc.WithCooperativeCheckpoint(true))
	}

	// Backpressure controller.
	var bpCtrl *backpressure.Controller
	if bpEnabled {
		bpCtrl = backpressure.New(
			bpWarnThreshold,
			bpCriticalThreshold,
			bpMaxThrottle,
			bpPollInterval,
			nil, // health checker injected by pipeline
			logger,
		)
		for _, ap := range adapterPriorityFlags {
			parts := strings.SplitN(ap, "=", 2)
			if len(parts) != 2 {
				logger.Warn("ignoring malformed adapter-priority (expected name=level)", "value", ap)
				continue
			}
			var prio backpressure.AdapterPriority
			switch parts[1] {
			case "critical":
				prio = backpressure.PriorityCritical
			case "normal":
				prio = backpressure.PriorityNormal
			case "best-effort":
				prio = backpressure.PriorityBestEffort
			default:
				return fmt.Errorf("unknown adapter priority %q for %q (expected critical, normal, or best-effort)", parts[1], parts[0])
			}
			bpCtrl.SetAdapterPriority(parts[0], prio)
		}
		opts = append(opts, pgcdc.WithBackpressure(bpCtrl))
	}

	// Build encoders for Kafka and NATS (nil = JSON passthrough).
	kafkaEncoder, natsEncoder, encErr := buildEncoders(cfg, logger)
	if encErr != nil {
		return encErr
	}
	defer func() {
		if kafkaEncoder != nil {
			_ = kafkaEncoder.Close()
		}
		if natsEncoder != nil {
			_ = natsEncoder.Close()
		}
	}()

	// Wasm runtime (created eagerly when any plugin is configured).
	var wasmRT any
	if anyPluginsConfigured(cfg, cmd) {
		var wasmCleanup func()
		wasmRT, wasmCleanup = initPluginRuntime(ctx, logger)
		_ = wasmCleanup
	}

	// Set up DLQ.
	var dlqInstance dlq.DLQ
	switch cfg.DLQ.Type {
	case "stderr", "":
		dlqInstance = dlq.NewStderrDLQ(logger)
	case "pg_table":
		dlqDB := cfg.DLQ.DBURL
		if dlqDB == "" {
			dlqDB = cfg.DatabaseURL
		}
		dlqInstance = dlq.NewPGTableDLQ(dlqDB, cfg.DLQ.Table, logger)
	case "none":
		dlqInstance = dlq.NopDLQ{}
	case "plugin":
		dlqPluginPath, _ := cmd.Flags().GetString("dlq-plugin-path")
		if dlqPluginPath == "" && cfg.Plugins.DLQ != nil {
			dlqPluginPath = cfg.Plugins.DLQ.Path
		}
		if dlqPluginPath == "" {
			return fmt.Errorf("--dlq plugin requires --dlq-plugin-path or plugins.dlq.path in config")
		}
		dlqPluginCfgStr, _ := cmd.Flags().GetString("dlq-plugin-config")
		dlqPluginCfg := map[string]any{}
		if dlqPluginCfgStr != "" {
			if err := json.Unmarshal([]byte(dlqPluginCfgStr), &dlqPluginCfg); err != nil {
				return fmt.Errorf("parse --dlq-plugin-config: %w", err)
			}
		} else if cfg.Plugins.DLQ != nil && cfg.Plugins.DLQ.Config != nil {
			dlqPluginCfg = cfg.Plugins.DLQ.Config
		}
		var dlqErr error
		dlqInstance, dlqErr = makePluginDLQ(ctx, wasmRT, dlqPluginPath, dlqPluginCfg, logger)
		if dlqErr != nil {
			return fmt.Errorf("create wasm dlq: %w", dlqErr)
		}
	default:
		return fmt.Errorf("unknown DLQ type: %q (expected stderr, pg_table, plugin, or none)", cfg.DLQ.Type)
	}
	opts = append(opts, pgcdc.WithDLQ(dlqInstance))

	// Parse routes (CLI + YAML merged).
	cliRoutes := buildCLIRoutes(cmd)
	allRoutes := mergeRoutes(cliRoutes, cfg.Routes)
	for adapterName, channels := range allRoutes {
		opts = append(opts, pgcdc.WithRoute(adapterName, channels...))
	}

	// Auto-create FOR ALL TABLES publication when --all-tables is set.
	if allTables {
		if err := ensureAllTablesPublication(ctx, cfg.DatabaseURL, cfg.Detector.Publication, logger); err != nil {
			return fmt.Errorf("ensure all-tables publication: %w", err)
		}
	}

	// Create detector.
	var det detector.Detector
	switch cfg.Detector.Type {
	case "listen_notify", "":
		det = listennotify.New(cfg.DatabaseURL, cfg.Channels, cfg.Detector.BackoffBase, cfg.Detector.BackoffCap, logger)
	case "wal":
		walDet := walreplication.New(cfg.DatabaseURL, cfg.Detector.Publication, cfg.Detector.BackoffBase, cfg.Detector.BackoffCap, cfg.Detector.TxMetadata, cfg.Detector.TxMarkers, logger)
		if snapshotFirst {
			walDet.SetSnapshotFirst(snapshotTable, snapshotWhere, cfg.Snapshot.BatchSize)
		}
		if persistentSlot {
			name := slotName
			if name == "" {
				name = "pgcdc_" + cfg.Detector.Publication
			}
			walDet.SetPersistentSlot(name)

			// Set up checkpoint store (plugin or PG).
			cpPluginPath, _ := cmd.Flags().GetString("checkpoint-plugin")
			if cpPluginPath == "" && cfg.Plugins.Checkpoint != nil {
				cpPluginPath = cfg.Plugins.Checkpoint.Path
			}
			if cpPluginPath != "" {
				cpPluginCfgStr, _ := cmd.Flags().GetString("checkpoint-plugin-config")
				cpPluginCfg := map[string]any{}
				if cpPluginCfgStr != "" {
					if err := json.Unmarshal([]byte(cpPluginCfgStr), &cpPluginCfg); err != nil {
						return fmt.Errorf("parse --checkpoint-plugin-config: %w", err)
					}
				} else if cfg.Plugins.Checkpoint != nil && cfg.Plugins.Checkpoint.Config != nil {
					cpPluginCfg = cfg.Plugins.Checkpoint.Config
				}
				cpStore, cpErr := makePluginCheckpoint(ctx, wasmRT, cpPluginPath, cpPluginCfg, logger)
				if cpErr != nil {
					return fmt.Errorf("create wasm checkpoint store: %w", cpErr)
				}
				walDet.SetCheckpointStore(cpStore)
				opts = append(opts, pgcdc.WithCheckpointStore(cpStore))
			} else {
				cpDB := checkpointDB
				if cpDB == "" {
					cpDB = cfg.DatabaseURL
				}
				cpStore, cpErr := checkpoint.NewPGStore(ctx, cpDB, logger)
				if cpErr != nil {
					return fmt.Errorf("create checkpoint store: %w", cpErr)
				}
				walDet.SetCheckpointStore(cpStore)
				opts = append(opts, pgcdc.WithCheckpointStore(cpStore))
			}
		}
		if includeSchema {
			walDet.SetIncludeSchema(true)
		}
		if schemaEvents {
			walDet.SetSchemaEvents(true)
		}
		if heartbeatInterval > 0 {
			walDet.SetHeartbeat(heartbeatInterval, heartbeatTable, cfg.DatabaseURL)
		}
		if slotLagWarn > 0 {
			walDet.SetSlotLagWarn(slotLagWarn)
		}
		if toastCache {
			walDet.SetToastCache(toastCacheMaxEntries)
		}
		if incrementalSnapshot {
			walDet.SetIncrementalSnapshot(snapshotSignalTable, snapshotChunkSize, snapshotChunkDelay)
			progDB := snapshotProgressDB
			if progDB == "" {
				progDB = cfg.DatabaseURL
			}
			progStore, progErr := snapshot.NewPGProgressStore(ctx, progDB, logger)
			if progErr != nil {
				return fmt.Errorf("create snapshot progress store: %w", progErr)
			}
			walDet.SetProgressStore(progStore)
			defer func() { _ = progStore.Close() }()
		}
		det = walDet
	case "outbox":
		det = outbox.New(
			cfg.DatabaseURL,
			cfg.Outbox.Table,
			cfg.Outbox.PollInterval,
			cfg.Outbox.BatchSize,
			cfg.Outbox.KeepProcessed,
			cfg.Outbox.BackoffBase,
			cfg.Outbox.BackoffCap,
			logger,
		)
	case "mysql":
		if cfg.MySQL.ServerID == 0 {
			return fmt.Errorf("--mysql-server-id is required and must be > 0 for MySQL detector")
		}
		if cfg.MySQL.Addr == "" {
			return fmt.Errorf("--mysql-addr is required for MySQL detector")
		}
		mysqlDet := mysqldetector.New(
			cfg.MySQL.Addr, cfg.MySQL.User, cfg.MySQL.Password,
			cfg.MySQL.ServerID, cfg.MySQL.Tables, cfg.MySQL.UseGTID,
			cfg.MySQL.Flavor, cfg.MySQL.BinlogPrefix,
			cfg.MySQL.BackoffBase, cfg.MySQL.BackoffCap,
			logger,
		)
		if tp != nil {
			mysqlDet.SetTracer(tp.Tracer("pgcdc"))
		}
		det = mysqlDet
	case "mongodb":
		if cfg.MongoDB.URI == "" {
			return fmt.Errorf("--mongodb-uri is required for MongoDB detector")
		}
		if cfg.MongoDB.Scope != "cluster" && cfg.MongoDB.Database == "" {
			return fmt.Errorf("--mongodb-database is required for MongoDB detector (unless --mongodb-scope=cluster)")
		}
		mongoDet := mongodbdetector.New(
			cfg.MongoDB.URI, cfg.MongoDB.Scope, cfg.MongoDB.Database,
			cfg.MongoDB.Collections, cfg.MongoDB.FullDocument,
			cfg.MongoDB.MetadataDB, cfg.MongoDB.MetadataColl,
			cfg.MongoDB.BackoffBase, cfg.MongoDB.BackoffCap,
			logger,
		)
		if tp != nil {
			mongoDet.SetTracer(tp.Tracer("pgcdc"))
		}
		det = mongoDet
	default:
		return fmt.Errorf("unknown detector type: %q (expected listen_notify, wal, outbox, mysql, or mongodb)", cfg.Detector.Type)
	}

	// Create adapters.
	var sseBroker *sse.Broker
	var wsBroker *ws.Broker
	for _, name := range cfg.Adapters {
		switch name {
		case "stdout":
			opts = append(opts, pgcdc.WithAdapter(stdout.New(nil, logger)))
		case "webhook":
			opts = append(opts, pgcdc.WithAdapter(webhook.New(
				cfg.Webhook.URL,
				cfg.Webhook.Headers,
				cfg.Webhook.SigningKey,
				cfg.Webhook.MaxRetries,
				cfg.Webhook.Timeout,
				cfg.Webhook.BackoffBase,
				cfg.Webhook.BackoffCap,
				logger,
			)))
		case "sse":
			sseBroker = sse.New(cfg.Bus.BufferSize, cfg.SSE.HeartbeatInterval, logger)
			opts = append(opts, pgcdc.WithAdapter(sseBroker))
		case "file":
			opts = append(opts, pgcdc.WithAdapter(fileadapter.New(cfg.File.Path, cfg.File.MaxSize, cfg.File.MaxFiles, logger)))
		case "exec":
			opts = append(opts, pgcdc.WithAdapter(execadapter.New(cfg.Exec.Command, cfg.Exec.BackoffBase, cfg.Exec.BackoffCap, logger)))
		case "pg_table":
			pgTableURL := cfg.PGTable.URL
			if pgTableURL == "" {
				pgTableURL = cfg.DatabaseURL
			}
			opts = append(opts, pgcdc.WithAdapter(pgtable.New(pgTableURL, cfg.PGTable.Table, cfg.PGTable.BackoffBase, cfg.PGTable.BackoffCap, logger)))
		case "ws":
			wsBroker = ws.New(cfg.Bus.BufferSize, cfg.WebSocket.PingInterval, logger)
			opts = append(opts, pgcdc.WithAdapter(wsBroker))
		case "embedding":
			embDBURL := cfg.Embedding.DBURL
			if embDBURL == "" {
				embDBURL = cfg.DatabaseURL
			}
			opts = append(opts, pgcdc.WithAdapter(embeddingadapter.New(
				cfg.Embedding.APIURL,
				cfg.Embedding.APIKey,
				cfg.Embedding.Model,
				cfg.Embedding.Columns,
				cfg.Embedding.IDColumn,
				embDBURL,
				cfg.Embedding.Table,
				cfg.Embedding.Dimension,
				cfg.Embedding.MaxRetries,
				cfg.Embedding.Timeout,
				cfg.Embedding.BackoffBase,
				cfg.Embedding.BackoffCap,
				logger,
			)))
		case "iceberg":
			a, aErr := makeIcebergAdapter(cfg, logger)
			if aErr != nil {
				return aErr
			}
			opts = append(opts, pgcdc.WithAdapter(a))
		case "nats":
			a, aErr := makeNATSAdapter(cfg, natsEncoder, logger)
			if aErr != nil {
				return aErr
			}
			opts = append(opts, pgcdc.WithAdapter(a))
		case "search":
			opts = append(opts, pgcdc.WithAdapter(searchadapter.New(
				cfg.Search.Engine,
				cfg.Search.URL,
				cfg.Search.APIKey,
				cfg.Search.Index,
				cfg.Search.IDColumn,
				cfg.Search.BatchSize,
				cfg.Search.BatchInterval,
				cfg.Search.BackoffBase,
				cfg.Search.BackoffCap,
				logger,
			)))
		case "redis":
			a, aErr := makeRedisAdapter(cfg, logger)
			if aErr != nil {
				return aErr
			}
			opts = append(opts, pgcdc.WithAdapter(a))
		case "grpc":
			a, aErr := makeGRPCAdapter(cfg, logger)
			if aErr != nil {
				return aErr
			}
			opts = append(opts, pgcdc.WithAdapter(a))
		case "kafka":
			a, aErr := makeKafkaAdapter(cfg, kafkaEncoder, logger)
			if aErr != nil {
				return aErr
			}
			opts = append(opts, pgcdc.WithAdapter(a))
		case "kafkaserver":
			var ksCpStore checkpoint.Store
			ksCheckpointDB := cfg.KafkaServer.CheckpointDB
			if ksCheckpointDB == "" {
				ksCheckpointDB = cfg.DatabaseURL
			}
			if ksCheckpointDB != "" {
				var cpErr error
				ksCpStore, cpErr = checkpoint.NewPGStore(ctx, ksCheckpointDB, logger)
				if cpErr != nil {
					return fmt.Errorf("create kafkaserver checkpoint store: %w", cpErr)
				}
			}
			a, aErr := makeKafkaServerAdapter(cfg, ksCpStore, logger)
			if aErr != nil {
				return aErr
			}
			opts = append(opts, pgcdc.WithAdapter(a))
		case "s3":
			a, aErr := makeS3Adapter(cfg, logger)
			if aErr != nil {
				return aErr
			}
			opts = append(opts, pgcdc.WithAdapter(a))
		case "view":
			a, aErr := makeViewAdapter(cfg, logger)
			if aErr != nil {
				return aErr
			}
			opts = append(opts, pgcdc.WithAdapter(a))
		}
	}

	// Parse --view-query CLI flags and merge with YAML views (CLI wins on name conflict).
	viewQueryFlags, _ := cmd.Flags().GetStringSlice("view-query")
	for _, vq := range viewQueryFlags {
		parts := strings.SplitN(vq, ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf("invalid --view-query format %q: expected name:query", vq)
		}
		// Check for duplicate name in YAML views — CLI wins.
		replaced := false
		for i, vc := range cfg.Views {
			if vc.Name == parts[0] {
				cfg.Views[i].Query = parts[1]
				replaced = true
				break
			}
		}
		if !replaced {
			cfg.Views = append(cfg.Views, config.ViewConfig{
				Name:  parts[0],
				Query: parts[1],
			})
		}
	}

	// Auto-create view adapter from YAML views: config or --view-query (if not already added via --adapter view).
	if len(cfg.Views) > 0 {
		hasViewAdapter := false
		for _, name := range cfg.Adapters {
			if name == "view" {
				hasViewAdapter = true
				break
			}
		}
		if !hasViewAdapter {
			a, aErr := makeViewAdapter(cfg, logger)
			if aErr != nil {
				return aErr
			}
			opts = append(opts, pgcdc.WithAdapter(a))
		}
	}

	// Create plugin adapters from CLI flags and config.
	pluginAdapterOpts, pluginAdapterCleanup, paErr := wirePluginAdapters(ctx, wasmRT, cmd, cfg, logger)
	if paErr != nil {
		return paErr
	}
	defer pluginAdapterCleanup()
	opts = append(opts, pluginAdapterOpts...)

	// Build transform options from CLI flags and config.
	globalTransforms, adapterTransforms := buildTransformOpts(cfg, cmd)
	for _, fn := range globalTransforms {
		opts = append(opts, pgcdc.WithTransform(fn))
	}
	for adapterName, fns := range adapterTransforms {
		for _, fn := range fns {
			opts = append(opts, pgcdc.WithAdapterTransform(adapterName, fn))
		}
	}
	// Plugin transforms.
	pluginTransformOpts, pluginTfx, pluginTransformCleanup := buildPluginTransformOpts(ctx, wasmRT, cmd, cfg, logger)
	opts = append(opts, pluginTransformOpts...)

	// Capture immutable CLI transforms for SIGHUP reload.
	immutableCLITransforms := buildCLITransforms(cmd)

	// Run schema migrations (unless --skip-migrations).
	if !cfg.SkipMigrations && cfg.DatabaseURL != "" {
		if err := migrate.Run(ctx, cfg.DatabaseURL, logger); err != nil {
			logger.Warn("schema migration failed (use --skip-migrations to skip)", "error", err)
		}
	}

	// Build pipeline.
	p := pgcdc.NewPipeline(det, opts...)
	defer func() { _ = dlqInstance.Close() }()
	defer pluginTransformCleanup()
	defer closePluginRuntime(ctx, wasmRT)

	// Use an errgroup to run the pipeline alongside CLI-specific HTTP servers.
	g, gCtx := errgroup.WithContext(ctx)

	// Run the core pipeline (detector + bus + adapters).
	g.Go(func() error {
		return p.Run(gCtx)
	})

	// SIGHUP handler: reload transforms and routes from YAML config.
	sighupCh := make(chan os.Signal, 1)
	signal.Notify(sighupCh, syscall.SIGHUP)
	g.Go(func() error {
		defer signal.Stop(sighupCh)
		for {
			select {
			case <-gCtx.Done():
				return nil
			case <-sighupCh:
				logger.Info("SIGHUP received, reloading config")

				// Re-read config file.
				if err := viper.ReadInConfig(); err != nil {
					logger.Error("config reload: read config file", "error", err)
					metrics.ConfigReloadErrors.Inc()
					continue
				}
				var newCfg config.Config
				newCfg = config.Default()
				if err := viper.Unmarshal(&newCfg); err != nil {
					logger.Error("config reload: unmarshal config", "error", err)
					metrics.ConfigReloadErrors.Inc()
					continue
				}

				// Rebuild YAML transforms (reloadable portion).
				yamlGlobal, yamlAdapter := buildYAMLTransforms(newCfg)

				// Merge: CLI (immutable) + plugin (immutable) + YAML (reloaded).
				allGlobal := make([]transform.TransformFunc, 0, len(immutableCLITransforms)+len(pluginTfx.global)+len(yamlGlobal))
				allGlobal = append(allGlobal, immutableCLITransforms...)
				allGlobal = append(allGlobal, pluginTfx.global...)
				allGlobal = append(allGlobal, yamlGlobal...)

				allAdapter := make(map[string][]transform.TransformFunc)
				for k, v := range pluginTfx.adapter {
					allAdapter[k] = append(allAdapter[k], v...)
				}
				for k, v := range yamlAdapter {
					allAdapter[k] = append(allAdapter[k], v...)
				}

				// Merge routes: CLI (immutable) + YAML (reloaded).
				reloadedRoutes := mergeRoutes(cliRoutes, newCfg.Routes)

				if err := p.Reload(pgcdc.ReloadConfig{
					Transforms:        allGlobal,
					AdapterTransforms: allAdapter,
					Routes:            reloadedRoutes,
				}); err != nil {
					logger.Error("config reload: apply", "error", err)
					metrics.ConfigReloadErrors.Inc()
					continue
				}
			}
		}
	})

	// If SSE or WS is active, start the HTTP server with SSE/WS + metrics + health.
	if (hasSSE && sseBroker != nil) || (hasWS && wsBroker != nil) {
		httpServer := server.New(sseBroker, wsBroker, cfg.SSE.CORSOrigins, cfg.SSE.ReadTimeout, cfg.SSE.IdleTimeout, p.Health())
		httpServer.Addr = cfg.SSE.Addr

		g.Go(func() error {
			logger.Info("http server starting", "addr", cfg.SSE.Addr)
			ln, err := net.Listen("tcp", cfg.SSE.Addr)
			if err != nil {
				return fmt.Errorf("http listen: %w", err)
			}
			if err := httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("http serve: %w", err)
			}
			return nil
		})

		g.Go(func() error {
			<-gCtx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			defer cancel()
			logger.Info("shutting down http server")
			return httpServer.Shutdown(shutdownCtx)
		})
	}

	// If --metrics-addr is set, start a dedicated metrics/health server.
	if cfg.MetricsAddr != "" {
		metricsServer := server.NewMetricsServer(p.Health())
		metricsServer.Addr = cfg.MetricsAddr

		g.Go(func() error {
			logger.Info("metrics server starting", "addr", cfg.MetricsAddr)
			ln, err := net.Listen("tcp", cfg.MetricsAddr)
			if err != nil {
				return fmt.Errorf("metrics listen: %w", err)
			}
			if err := metricsServer.Serve(ln); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("metrics serve: %w", err)
			}
			return nil
		})

		g.Go(func() error {
			<-gCtx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			defer cancel()
			logger.Info("shutting down metrics server")
			return metricsServer.Shutdown(shutdownCtx)
		})
	}

	// Wait for errgroup to finish (happens when context is cancelled).
	err := g.Wait()

	// context.Canceled is expected on clean shutdown.
	if err != nil && errors.Is(err, context.Canceled) {
		logger.Info("shutdown complete")
		return nil
	}

	return err
}

// ensureAllTablesPublication creates a FOR ALL TABLES publication if it doesn't already exist.
func ensureAllTablesPublication(ctx context.Context, dbURL, publication string, logger *slog.Logger) error {
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, fmt.Sprintf(
		"CREATE PUBLICATION %s FOR ALL TABLES",
		pgx.Identifier{publication}.Sanitize(),
	))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42710" { // duplicate_object
			logger.Info("all-tables publication already exists, reusing", "publication", publication)
			return nil
		}
		return fmt.Errorf("create publication: %w", err)
	}
	logger.Info("created all-tables publication", "publication", publication)
	return nil
}

// buildEncoders creates encoders for Kafka and NATS based on config.
// Returns nil encoders for JSON encoding (default, no-op passthrough).
func buildEncoders(cfg config.Config, logger *slog.Logger) (kafkaEnc, natsEnc encoding.Encoder, err error) {
	// Parse encoding types.
	kafkaEncType, err := encoding.ParseEncodingType(cfg.Kafka.Encoding)
	if err != nil {
		return nil, nil, fmt.Errorf("kafka encoding: %w", err)
	}
	natsEncType, err := encoding.ParseEncodingType(cfg.Nats.Encoding)
	if err != nil {
		return nil, nil, fmt.Errorf("nats encoding: %w", err)
	}

	// If both are JSON, no encoders needed.
	if kafkaEncType == encoding.EncodingJSON && natsEncType == encoding.EncodingJSON {
		return nil, nil, nil
	}

	// Build schema registry client if any non-JSON encoding is configured.
	var regClient *registry.Client
	if cfg.Encoding.SchemaRegistryURL != "" {
		regClient = registry.New(
			cfg.Encoding.SchemaRegistryURL,
			cfg.Encoding.SchemaRegistryUsername,
			cfg.Encoding.SchemaRegistryPassword,
		)
	}

	kafkaEnc = makeEncoder(kafkaEncType, regClient, logger)
	natsEnc = makeEncoder(natsEncType, regClient, logger)

	return kafkaEnc, natsEnc, nil
}

func makeEncoder(encType encoding.EncodingType, regClient *registry.Client, logger *slog.Logger) encoding.Encoder {
	switch encType {
	case encoding.EncodingAvro:
		opts := []encoding.AvroOption{}
		if regClient != nil {
			opts = append(opts, encoding.WithRegistry(regClient))
		}
		return encoding.NewAvroEncoder(logger, opts...)
	case encoding.EncodingProtobuf:
		return encoding.NewProtobufEncoder(regClient, logger)
	default:
		return nil // JSON = nil encoder, adapters use raw bytes
	}
}
