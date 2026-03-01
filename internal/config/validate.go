package config

import (
	"fmt"
	"strings"
	"time"
)

// knownAdapters is the set of valid adapter names.
var knownAdapters = map[string]bool{
	"stdout": true, "webhook": true, "sse": true, "file": true,
	"exec": true, "pg_table": true, "ws": true, "embedding": true,
	"iceberg": true, "nats": true, "search": true, "redis": true,
	"kafka": true, "kafkaserver": true, "s3": true, "grpc": true,
	"view": true, "graphql": true, "arrow": true, "duckdb": true,
}

// Validate performs structural validation on the config.
func (c Config) Validate() error {
	var errs []string

	// --- Top-level ---
	if c.DatabaseURL == "" && c.Detector.Type != "mysql" && c.Detector.Type != "mongodb" {
		errs = append(errs, "database_url is required")
	}
	if c.Bus.BufferSize <= 0 {
		errs = append(errs, fmt.Sprintf("bus.buffer_size must be > 0, got %d", c.Bus.BufferSize))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, "shutdown_timeout must be > 0")
	}
	if c.Detector.Type == "listen_notify" && len(c.Channels) == 0 {
		errs = append(errs, "no channels specified for listen_notify detector")
	}

	adapterSet := make(map[string]bool, len(c.Adapters))
	for _, a := range c.Adapters {
		if !knownAdapters[a] {
			errs = append(errs, fmt.Sprintf("unknown adapter %q", a))
		}
		adapterSet[a] = true
	}
	for name := range c.Routes {
		if !adapterSet[name] {
			errs = append(errs, fmt.Sprintf("route references unknown adapter %q", name))
		}
	}

	// --- Helper for duration checks ---
	checkDur := func(path string, d time.Duration) {
		if d <= 0 {
			errs = append(errs, fmt.Sprintf("%s must be > 0", path))
		}
	}

	// --- Detector durations (always validated) ---
	checkDur("detector.backoff_base", c.Detector.BackoffBase)
	checkDur("detector.backoff_cap", c.Detector.BackoffCap)

	// --- Adapter-specific: required fields + durations ---
	if adapterSet["webhook"] {
		if c.Webhook.URL == "" {
			errs = append(errs, "webhook.url is required")
		}
		checkDur("webhook.timeout", c.Webhook.Timeout)
		checkDur("webhook.backoff_base", c.Webhook.BackoffBase)
		checkDur("webhook.backoff_cap", c.Webhook.BackoffCap)
	}

	if adapterSet["sse"] {
		checkDur("sse.heartbeat_interval", c.SSE.HeartbeatInterval)
		checkDur("sse.read_timeout", c.SSE.ReadTimeout)
		checkDur("sse.idle_timeout", c.SSE.IdleTimeout)
	}

	if adapterSet["file"] {
		if c.File.Path == "" {
			errs = append(errs, "file.path is required")
		}
	}

	if adapterSet["exec"] {
		if c.Exec.Command == "" {
			errs = append(errs, "exec.command is required")
		}
		checkDur("exec.backoff_base", c.Exec.BackoffBase)
		checkDur("exec.backoff_cap", c.Exec.BackoffCap)
	}

	if adapterSet["pg_table"] {
		if c.PGTable.URL == "" && c.DatabaseURL == "" {
			errs = append(errs, "pg_table requires a database URL (pg_table.url or database_url)")
		}
		checkDur("pg_table.backoff_base", c.PGTable.BackoffBase)
		checkDur("pg_table.backoff_cap", c.PGTable.BackoffCap)
	}

	if adapterSet["ws"] {
		checkDur("websocket.ping_interval", c.WebSocket.PingInterval)
	}

	if adapterSet["embedding"] {
		if c.Embedding.APIURL == "" {
			errs = append(errs, "embedding.api_url is required")
		}
		if len(c.Embedding.Columns) == 0 {
			errs = append(errs, "embedding.columns must not be empty")
		}
		checkDur("embedding.timeout", c.Embedding.Timeout)
		checkDur("embedding.backoff_base", c.Embedding.BackoffBase)
		checkDur("embedding.backoff_cap", c.Embedding.BackoffCap)
	}

	if adapterSet["nats"] {
		if c.Nats.URL == "" {
			errs = append(errs, "nats.url is required")
		}
		checkDur("nats.backoff_base", c.Nats.BackoffBase)
		checkDur("nats.backoff_cap", c.Nats.BackoffCap)
	}

	if adapterSet["kafka"] {
		if len(c.Kafka.Brokers) == 0 {
			errs = append(errs, "kafka.brokers must not be empty")
		}
		checkDur("kafka.backoff_base", c.Kafka.BackoffBase)
		checkDur("kafka.backoff_cap", c.Kafka.BackoffCap)
	}

	if adapterSet["search"] {
		if c.Search.URL == "" {
			errs = append(errs, "search.url is required")
		}
		if c.Search.Index == "" {
			errs = append(errs, "search.index is required")
		}
		if c.Search.BatchSize <= 0 {
			errs = append(errs, fmt.Sprintf("search.batch_size must be > 0, got %d", c.Search.BatchSize))
		}
		checkDur("search.batch_interval", c.Search.BatchInterval)
		checkDur("search.backoff_base", c.Search.BackoffBase)
		checkDur("search.backoff_cap", c.Search.BackoffCap)
	}

	if adapterSet["redis"] {
		if c.Redis.URL == "" {
			errs = append(errs, "redis.url is required")
		}
		checkDur("redis.backoff_base", c.Redis.BackoffBase)
		checkDur("redis.backoff_cap", c.Redis.BackoffCap)
	}

	if adapterSet["s3"] {
		if c.S3.Bucket == "" {
			errs = append(errs, "s3.bucket is required")
		}
		if c.S3.FlushSize <= 0 {
			errs = append(errs, fmt.Sprintf("s3.flush_size must be > 0, got %d", c.S3.FlushSize))
		}
		checkDur("s3.flush_interval", c.S3.FlushInterval)
		checkDur("s3.drain_timeout", c.S3.DrainTimeout)
		checkDur("s3.backoff_base", c.S3.BackoffBase)
		checkDur("s3.backoff_cap", c.S3.BackoffCap)
	}

	if adapterSet["iceberg"] {
		if c.Iceberg.Warehouse == "" {
			errs = append(errs, "iceberg.warehouse is required")
		}
		if c.Iceberg.Table == "" {
			errs = append(errs, "iceberg.table is required")
		}
		if c.Iceberg.Mode == "upsert" && len(c.Iceberg.PrimaryKeys) == 0 {
			errs = append(errs, "iceberg upsert mode requires primary_keys")
		}
		checkDur("iceberg.flush_interval", c.Iceberg.FlushInterval)
		checkDur("iceberg.backoff_base", c.Iceberg.BackoffBase)
		checkDur("iceberg.backoff_cap", c.Iceberg.BackoffCap)
	}

	if adapterSet["kafkaserver"] {
		if c.KafkaServer.PartitionCount <= 0 {
			errs = append(errs, fmt.Sprintf("kafkaserver.partition_count must be > 0, got %d", c.KafkaServer.PartitionCount))
		}
		if c.KafkaServer.BufferSize <= 0 {
			errs = append(errs, fmt.Sprintf("kafkaserver.buffer_size must be > 0, got %d", c.KafkaServer.BufferSize))
		}
		checkDur("kafkaserver.session_timeout", c.KafkaServer.SessionTimeout)
	}

	// --- Outbox detector durations ---
	if c.Detector.Type == "outbox" {
		checkDur("outbox.poll_interval", c.Outbox.PollInterval)
		checkDur("outbox.backoff_base", c.Outbox.BackoffBase)
		checkDur("outbox.backoff_cap", c.Outbox.BackoffCap)
	}

	// --- MySQL detector ---
	if c.Detector.Type == "mysql" {
		if c.MySQL.Addr == "" {
			errs = append(errs, "mysql.addr is required")
		}
		if c.MySQL.ServerID == 0 {
			errs = append(errs, "mysql.server_id must be > 0")
		}
		checkDur("mysql.backoff_base", c.MySQL.BackoffBase)
		checkDur("mysql.backoff_cap", c.MySQL.BackoffCap)
	}

	// --- MongoDB detector ---
	if c.Detector.Type == "mongodb" {
		if c.MongoDB.URI == "" {
			errs = append(errs, "mongodb.uri is required")
		}
		if c.MongoDB.Scope != "cluster" && c.MongoDB.Database == "" {
			errs = append(errs, "mongodb.database is required (unless scope is \"cluster\")")
		}
		checkDur("mongodb.backoff_base", c.MongoDB.BackoffBase)
		checkDur("mongodb.backoff_cap", c.MongoDB.BackoffCap)
	}

	// --- Backpressure (when enabled) ---
	if c.Backpressure.Enabled {
		checkDur("backpressure.max_throttle", c.Backpressure.MaxThrottle)
		checkDur("backpressure.poll_interval", c.Backpressure.PollInterval)
		if c.Backpressure.WarnThreshold <= 0 {
			errs = append(errs, "backpressure.warn_threshold must be > 0")
		}
		if c.Backpressure.CriticalThreshold <= 0 {
			errs = append(errs, "backpressure.critical_threshold must be > 0")
		}
		if c.Backpressure.CriticalThreshold > 0 && c.Backpressure.WarnThreshold > 0 &&
			c.Backpressure.CriticalThreshold <= c.Backpressure.WarnThreshold {
			errs = append(errs, fmt.Sprintf(
				"backpressure.critical_threshold (%d) must be > warn_threshold (%d)",
				c.Backpressure.CriticalThreshold, c.Backpressure.WarnThreshold))
		}
	}

	// --- WAL publication ---
	if c.Detector.Type == "wal" && c.Detector.Publication == "" {
		errs = append(errs, "WAL detector requires a publication")
	}

	// --- Snapshot-first requires snapshot table ---
	if c.Detector.SnapshotFirst && c.Snapshot.Table == "" {
		errs = append(errs, "snapshot-first requires a snapshot table")
	}

	// --- Detector-feature compatibility ---
	if c.Detector.Type != "wal" {
		walOnly := []struct {
			name    string
			enabled bool
		}{
			{"tx-metadata", c.Detector.TxMetadata},
			{"tx-markers", c.Detector.TxMarkers},
			{"include-schema", c.Detector.IncludeSchema},
			{"schema-events", c.Detector.SchemaEvents},
			{"toast-cache", c.Detector.ToastCache},
			{"snapshot-first", c.Detector.SnapshotFirst},
			{"persistent-slot", c.Detector.PersistentSlot},
			{"incremental-snapshot", c.IncrementalSnapshot.Enabled},
		}
		for _, f := range walOnly {
			if f.enabled {
				errs = append(errs, fmt.Sprintf("%s requires --detector wal", f.name))
			}
		}
	}
	if c.Detector.CooperativeCheckpoint && !c.Detector.PersistentSlot {
		errs = append(errs, "cooperative-checkpoint requires --persistent-slot")
	}
	if c.Detector.CooperativeCheckpoint && c.Detector.Type != "wal" {
		errs = append(errs, "cooperative-checkpoint requires --detector wal")
	}
	if c.Backpressure.Enabled && !c.Detector.PersistentSlot {
		errs = append(errs, "backpressure requires --persistent-slot")
	}
	if c.Backpressure.Enabled && c.Detector.Type != "wal" {
		errs = append(errs, "backpressure requires --detector wal")
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation: %s", strings.Join(errs, "; "))
	}
	return nil
}
