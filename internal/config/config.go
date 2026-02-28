package config

import "time"

type Config struct {
	DatabaseURL         string                      `mapstructure:"database_url"`
	Channels            []string                    `mapstructure:"channels"`
	Adapters            []string                    `mapstructure:"adapters"`
	LogLevel            string                      `mapstructure:"log_level"`
	LogFormat           string                      `mapstructure:"log_format"`
	ShutdownTimeout     time.Duration               `mapstructure:"shutdown_timeout"`
	MetricsAddr         string                      `mapstructure:"metrics_addr"`
	SkipValidation      bool                        `mapstructure:"skip_validation"`
	SkipMigrations      bool                        `mapstructure:"skip_migrations"`
	Bus                 BusConfig                   `mapstructure:"bus"`
	Webhook             WebhookConfig               `mapstructure:"webhook"`
	SSE                 SSEConfig                   `mapstructure:"sse"`
	File                FileConfig                  `mapstructure:"file"`
	Exec                ExecConfig                  `mapstructure:"exec"`
	PGTable             PGTableConfig               `mapstructure:"pg_table"`
	WebSocket           WebSocketConfig             `mapstructure:"websocket"`
	Detector            DetectorConfig              `mapstructure:"detector"`
	Snapshot            SnapshotConfig              `mapstructure:"snapshot"`
	Embedding           EmbeddingConfig             `mapstructure:"embedding"`
	Iceberg             IcebergConfig               `mapstructure:"iceberg"`
	Nats                NatsConfig                  `mapstructure:"nats"`
	Outbox              OutboxConfig                `mapstructure:"outbox"`
	DLQ                 DLQConfig                   `mapstructure:"dlq"`
	Search              SearchConfig                `mapstructure:"search"`
	Redis               RedisConfig                 `mapstructure:"redis"`
	GRPC                GRPCConfig                  `mapstructure:"grpc"`
	Kafka               KafkaConfig                 `mapstructure:"kafka"`
	S3                  S3Config                    `mapstructure:"s3"`
	IncrementalSnapshot IncrementalSnapshotConfig   `mapstructure:"incremental_snapshot"`
	Transforms          TransformConfig             `mapstructure:"transforms"`
	Routes              map[string][]string         `mapstructure:"routes"` // adapter -> channels
	Backpressure        BackpressureConfig          `mapstructure:"backpressure"`
	Plugins             PluginConfig                `mapstructure:"plugins"`
	Encoding            EncodingConfig              `mapstructure:"encoding"`
	MySQL               MySQLConfig                 `mapstructure:"mysql"`
	MongoDB             MongoDBConfig               `mapstructure:"mongodb"`
	OTel                OTelConfig                  `mapstructure:"otel"`
	KafkaServer         KafkaServerConfig           `mapstructure:"kafkaserver"`
	Views               []ViewConfig                `mapstructure:"views"`
	Middleware          map[string]MiddlewareConfig `mapstructure:"middleware"` // adapter name -> middleware config
	Inspector           InspectorConfig             `mapstructure:"inspector"`
	Schema              SchemaConfig                `mapstructure:"schema"`
	DetectorMode        string                      `mapstructure:"detector_mode"` // sequential, parallel, or failover (for multi-detector)
	PipelineName        string                      `mapstructure:"pipeline_name"` // per-pipeline metric label
}

type OTelConfig struct {
	Exporter    string  `mapstructure:"exporter"`
	Endpoint    string  `mapstructure:"endpoint"`
	SampleRatio float64 `mapstructure:"sample_ratio"`
}

type BusConfig struct {
	BufferSize int    `mapstructure:"buffer_size"`
	Mode       string `mapstructure:"mode"` // "fast" (default) or "reliable"
}

type DLQConfig struct {
	Type            string `mapstructure:"type"`
	Table           string `mapstructure:"table"`
	DBURL           string `mapstructure:"db_url"`
	WindowSize      int    `mapstructure:"window_size"`      // nack window size (default 100)
	WindowThreshold int    `mapstructure:"window_threshold"` // nack count to trigger pause (default window_size/2)
}

type InspectorConfig struct {
	BufferSize int `mapstructure:"buffer_size"` // ring buffer size per tap point (default 100)
}

type SchemaConfig struct {
	Enabled bool   `mapstructure:"enabled"` // enable schema versioning
	Store   string `mapstructure:"store"`   // "memory" or "pg" (default "memory")
	DBURL   string `mapstructure:"db_url"`  // PostgreSQL URL for pg store (default: same as --db)
}

type TransformConfig struct {
	Global  []TransformSpec            `mapstructure:"global"`
	Adapter map[string][]TransformSpec `mapstructure:"adapter"`
}

type TransformSpec struct {
	Type        string            `mapstructure:"type"`        // drop_columns, rename_fields, mask, filter, debezium, cloudevents, cel_filter
	Columns     []string          `mapstructure:"columns"`     // for drop_columns
	Mapping     map[string]string `mapstructure:"mapping"`     // for rename_fields
	Fields      []MaskFieldSpec   `mapstructure:"fields"`      // for mask
	Filter      FilterSpec        `mapstructure:"filter"`      // for filter
	Debezium    DebeziumSpec      `mapstructure:"debezium"`    // for debezium
	CloudEvents CloudEventsSpec   `mapstructure:"cloudevents"` // for cloudevents
	Expression  string            `mapstructure:"expression"`  // for cel_filter
}

type CloudEventsSpec struct {
	Source     string `mapstructure:"source"`
	TypePrefix string `mapstructure:"type_prefix"`
}

type DebeziumSpec struct {
	ConnectorName string `mapstructure:"connector_name"`
	Database      string `mapstructure:"database"`
}

type MaskFieldSpec struct {
	Field string `mapstructure:"field"`
	Mode  string `mapstructure:"mode"`
}

type FilterSpec struct {
	Field      string   `mapstructure:"field"`
	Equals     string   `mapstructure:"equals"`
	In         []string `mapstructure:"in"`
	Operations []string `mapstructure:"operations"`
}

type BackpressureConfig struct {
	Enabled           bool              `mapstructure:"enabled"`
	WarnThreshold     int64             `mapstructure:"warn_threshold"`
	CriticalThreshold int64             `mapstructure:"critical_threshold"`
	MaxThrottle       time.Duration     `mapstructure:"max_throttle"`
	PollInterval      time.Duration     `mapstructure:"poll_interval"`
	AdapterPriorities map[string]string `mapstructure:"adapter_priorities"` // name -> "critical"|"normal"|"best-effort"
}

func Default() Config {
	return Config{
		Channels:        []string{},
		Adapters:        []string{"stdout"},
		LogLevel:        "info",
		LogFormat:       "text",
		ShutdownTimeout: 5 * time.Second,
		Bus: BusConfig{
			BufferSize: 1024,
		},
		Webhook: WebhookConfig{
			MaxRetries:  5,
			Headers:     map[string]string{},
			Timeout:     10 * time.Second,
			BackoffBase: 1 * time.Second,
			BackoffCap:  32 * time.Second,
		},
		SSE: SSEConfig{
			Addr:              ":8080",
			CORSOrigins:       []string{"*"},
			HeartbeatInterval: 15 * time.Second,
			ReadTimeout:       5 * time.Second,
			IdleTimeout:       120 * time.Second,
		},
		File: FileConfig{
			MaxSize:  100 * 1024 * 1024, // 100 MB
			MaxFiles: 5,
		},
		Exec: ExecConfig{
			BackoffBase: 1 * time.Second,
			BackoffCap:  30 * time.Second,
		},
		PGTable: PGTableConfig{
			Table:       "pgcdc_events",
			BackoffBase: 1 * time.Second,
			BackoffCap:  30 * time.Second,
		},
		WebSocket: WebSocketConfig{
			PingInterval: 15 * time.Second,
		},
		Snapshot: SnapshotConfig{
			BatchSize: 1000,
		},
		Detector: DetectorConfig{
			Type:                 "listen_notify",
			BackoffBase:          5 * time.Second,
			BackoffCap:           60 * time.Second,
			HeartbeatInterval:    30 * time.Second,
			HeartbeatTable:       "pgcdc_heartbeat",
			SlotLagWarn:          100 * 1024 * 1024, // 100 MB
			ToastCacheMaxEntries: 100000,
		},
		Iceberg: IcebergConfig{
			CatalogType:   "hadoop",
			Namespace:     "pgcdc",
			Mode:          "append",
			SchemaMode:    "raw",
			FlushInterval: 1 * time.Minute,
			FlushSize:     10000,
			BackoffBase:   5 * time.Second,
			BackoffCap:    60 * time.Second,
		},
		Nats: NatsConfig{
			Subject:     "pgcdc",
			Stream:      "pgcdc",
			MaxAge:      24 * time.Hour,
			BackoffBase: 1 * time.Second,
			BackoffCap:  30 * time.Second,
		},
		Outbox: OutboxConfig{
			Table:        "pgcdc_outbox",
			PollInterval: 500 * time.Millisecond,
			BatchSize:    100,
			BackoffBase:  1 * time.Second,
			BackoffCap:   30 * time.Second,
		},
		DLQ: DLQConfig{
			Type:  "stderr",
			Table: "pgcdc_dead_letters",
		},
		Search: SearchConfig{
			Engine:        "typesense",
			IDColumn:      "id",
			BatchSize:     100,
			BatchInterval: 1 * time.Second,
			BackoffBase:   1 * time.Second,
			BackoffCap:    30 * time.Second,
		},
		Redis: RedisConfig{
			Mode:        "invalidate",
			IDColumn:    "id",
			BackoffBase: 1 * time.Second,
			BackoffCap:  30 * time.Second,
		},
		GRPC: GRPCConfig{
			Addr: ":9090",
		},
		S3: S3Config{
			Region:        "us-east-1",
			Format:        "jsonl",
			FlushInterval: 1 * time.Minute,
			FlushSize:     10000,
			DrainTimeout:  30 * time.Second,
			BackoffBase:   5 * time.Second,
			BackoffCap:    60 * time.Second,
		},
		Kafka: KafkaConfig{
			Brokers:     []string{"localhost:9092"},
			BackoffBase: 1 * time.Second,
			BackoffCap:  30 * time.Second,
		},
		IncrementalSnapshot: IncrementalSnapshotConfig{
			SignalTable: "pgcdc_signals",
			ChunkSize:   1000,
		},
		Backpressure: BackpressureConfig{
			WarnThreshold:     500 * 1024 * 1024,      // 500 MB
			CriticalThreshold: 2 * 1024 * 1024 * 1024, // 2 GB
			MaxThrottle:       500 * time.Millisecond,
			PollInterval:      10 * time.Second,
		},
		MySQL: MySQLConfig{
			Flavor:       "mysql",
			BinlogPrefix: "mysql-bin",
			BackoffBase:  5 * time.Second,
			BackoffCap:   60 * time.Second,
		},
		MongoDB: MongoDBConfig{
			Scope:        "collection",
			FullDocument: "updateLookup",
			MetadataColl: "pgcdc_resume_tokens",
			BackoffBase:  5 * time.Second,
			BackoffCap:   60 * time.Second,
		},
		OTel: OTelConfig{
			Exporter:    "none",
			SampleRatio: 1.0,
		},
		KafkaServer: KafkaServerConfig{
			Addr:           ":9092",
			PartitionCount: 8,
			BufferSize:     10000,
			SessionTimeout: 30 * time.Second,
			KeyColumn:      "id",
		},
		Inspector: InspectorConfig{
			BufferSize: 100,
		},
		Schema: SchemaConfig{
			Store: "memory",
		},
		Embedding: EmbeddingConfig{
			Model:       "text-embedding-3-small",
			IDColumn:    "id",
			Table:       "pgcdc_embeddings",
			Dimension:   1536,
			MaxRetries:  3,
			Timeout:     30 * time.Second,
			BackoffBase: 2 * time.Second,
			BackoffCap:  60 * time.Second,
		},
	}
}
