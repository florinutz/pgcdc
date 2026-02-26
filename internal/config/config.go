package config

import "time"

type Config struct {
	DatabaseURL         string                    `mapstructure:"database_url"`
	Channels            []string                  `mapstructure:"channels"`
	Adapters            []string                  `mapstructure:"adapters"`
	LogLevel            string                    `mapstructure:"log_level"`
	LogFormat           string                    `mapstructure:"log_format"`
	ShutdownTimeout     time.Duration             `mapstructure:"shutdown_timeout"`
	MetricsAddr         string                    `mapstructure:"metrics_addr"`
	SkipValidation      bool                      `mapstructure:"skip_validation"`
	SkipMigrations      bool                      `mapstructure:"skip_migrations"`
	Bus                 BusConfig                 `mapstructure:"bus"`
	Webhook             WebhookConfig             `mapstructure:"webhook"`
	SSE                 SSEConfig                 `mapstructure:"sse"`
	File                FileConfig                `mapstructure:"file"`
	Exec                ExecConfig                `mapstructure:"exec"`
	PGTable             PGTableConfig             `mapstructure:"pg_table"`
	WebSocket           WebSocketConfig           `mapstructure:"websocket"`
	Detector            DetectorConfig            `mapstructure:"detector"`
	Snapshot            SnapshotConfig            `mapstructure:"snapshot"`
	Embedding           EmbeddingConfig           `mapstructure:"embedding"`
	Iceberg             IcebergConfig             `mapstructure:"iceberg"`
	Nats                NatsConfig                `mapstructure:"nats"`
	Outbox              OutboxConfig              `mapstructure:"outbox"`
	DLQ                 DLQConfig                 `mapstructure:"dlq"`
	Search              SearchConfig              `mapstructure:"search"`
	Redis               RedisConfig               `mapstructure:"redis"`
	GRPC                GRPCConfig                `mapstructure:"grpc"`
	Kafka               KafkaConfig               `mapstructure:"kafka"`
	S3                  S3Config                  `mapstructure:"s3"`
	IncrementalSnapshot IncrementalSnapshotConfig `mapstructure:"incremental_snapshot"`
	Transforms          TransformConfig           `mapstructure:"transforms"`
	Routes              map[string][]string       `mapstructure:"routes"` // adapter -> channels
	Backpressure        BackpressureConfig        `mapstructure:"backpressure"`
	Plugins             PluginConfig              `mapstructure:"plugins"`
	Encoding            EncodingConfig            `mapstructure:"encoding"`
	MySQL               MySQLConfig               `mapstructure:"mysql"`
	MongoDB             MongoDBConfig             `mapstructure:"mongodb"`
	OTel                OTelConfig                `mapstructure:"otel"`
	KafkaServer         KafkaServerConfig         `mapstructure:"kafkaserver"`
	Views               []ViewConfig              `mapstructure:"views"`
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

type WebhookConfig struct {
	URL            string            `mapstructure:"url"`
	Headers        map[string]string `mapstructure:"headers"`
	SigningKey     string            `mapstructure:"signing_key"`
	MaxRetries     int               `mapstructure:"max_retries"`
	Timeout        time.Duration     `mapstructure:"timeout"`
	BackoffBase    time.Duration     `mapstructure:"backoff_base"`
	BackoffCap     time.Duration     `mapstructure:"backoff_cap"`
	CBMaxFailures  int               `mapstructure:"cb_max_failures"`
	CBResetTimeout time.Duration     `mapstructure:"cb_reset_timeout"`
	RateLimit      float64           `mapstructure:"rate_limit"`
	RateLimitBurst int               `mapstructure:"rate_limit_burst"`
}

type SSEConfig struct {
	Addr              string        `mapstructure:"addr"`
	CORSOrigins       []string      `mapstructure:"cors_origins"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
	ReadTimeout       time.Duration `mapstructure:"read_timeout"`
	IdleTimeout       time.Duration `mapstructure:"idle_timeout"`
}

type FileConfig struct {
	Path     string `mapstructure:"path"`
	MaxSize  int64  `mapstructure:"max_size"`
	MaxFiles int    `mapstructure:"max_files"`
}

type ExecConfig struct {
	Command     string        `mapstructure:"command"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`
}

type PGTableConfig struct {
	URL         string        `mapstructure:"url"`
	Table       string        `mapstructure:"table"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`
}

type WebSocketConfig struct {
	PingInterval time.Duration `mapstructure:"ping_interval"`
}

type SnapshotConfig struct {
	Table     string `mapstructure:"table"`
	Where     string `mapstructure:"where"`
	BatchSize int    `mapstructure:"batch_size"`
}

type EmbeddingConfig struct {
	APIURL         string        `mapstructure:"api_url"`
	APIKey         string        `mapstructure:"api_key"`
	Model          string        `mapstructure:"model"`
	Columns        []string      `mapstructure:"columns"`
	IDColumn       string        `mapstructure:"id_column"`
	Table          string        `mapstructure:"table"`
	DBURL          string        `mapstructure:"db_url"`
	Dimension      int           `mapstructure:"dimension"`
	MaxRetries     int           `mapstructure:"max_retries"`
	Timeout        time.Duration `mapstructure:"timeout"`
	BackoffBase    time.Duration `mapstructure:"backoff_base"`
	BackoffCap     time.Duration `mapstructure:"backoff_cap"`
	CBMaxFailures  int           `mapstructure:"cb_max_failures"`
	CBResetTimeout time.Duration `mapstructure:"cb_reset_timeout"`
	RateLimit      float64       `mapstructure:"rate_limit"`
	RateLimitBurst int           `mapstructure:"rate_limit_burst"`
}

type IcebergConfig struct {
	CatalogType   string        `mapstructure:"catalog_type"`
	CatalogURI    string        `mapstructure:"catalog_uri"`
	Warehouse     string        `mapstructure:"warehouse"`
	Namespace     string        `mapstructure:"namespace"`
	Table         string        `mapstructure:"table"`
	Mode          string        `mapstructure:"mode"`
	SchemaMode    string        `mapstructure:"schema_mode"`
	PrimaryKeys   []string      `mapstructure:"primary_keys"`
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	FlushSize     int           `mapstructure:"flush_size"`
	BackoffBase   time.Duration `mapstructure:"backoff_base"`
	BackoffCap    time.Duration `mapstructure:"backoff_cap"`
}

type DetectorConfig struct {
	Type              string        `mapstructure:"type"`
	BackoffBase       time.Duration `mapstructure:"backoff_base"`
	BackoffCap        time.Duration `mapstructure:"backoff_cap"`
	Publication       string        `mapstructure:"publication"`
	TxMetadata        bool          `mapstructure:"tx_metadata"`
	TxMarkers         bool          `mapstructure:"tx_markers"`
	PersistentSlot    bool          `mapstructure:"persistent_slot"`
	SlotName          string        `mapstructure:"slot_name"`
	CheckpointDB      string        `mapstructure:"checkpoint_db"`
	IncludeSchema     bool          `mapstructure:"include_schema"`
	SchemaEvents      bool          `mapstructure:"schema_events"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
	HeartbeatTable    string        `mapstructure:"heartbeat_table"`
	SlotLagWarn       int64         `mapstructure:"slot_lag_warn"`
}

type NatsConfig struct {
	URL         string        `mapstructure:"url"`
	Subject     string        `mapstructure:"subject"`
	Stream      string        `mapstructure:"stream"`
	CredFile    string        `mapstructure:"cred_file"`
	MaxAge      time.Duration `mapstructure:"max_age"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`
	Encoding    string        `mapstructure:"encoding"` // json, avro, protobuf
}

type OutboxConfig struct {
	Table         string        `mapstructure:"table"`
	PollInterval  time.Duration `mapstructure:"poll_interval"`
	BatchSize     int           `mapstructure:"batch_size"`
	KeepProcessed bool          `mapstructure:"keep_processed"`
	BackoffBase   time.Duration `mapstructure:"backoff_base"`
	BackoffCap    time.Duration `mapstructure:"backoff_cap"`
}

type DLQConfig struct {
	Type  string `mapstructure:"type"`
	Table string `mapstructure:"table"`
	DBURL string `mapstructure:"db_url"`
}

type SearchConfig struct {
	Engine        string        `mapstructure:"engine"`
	URL           string        `mapstructure:"url"`
	APIKey        string        `mapstructure:"api_key"`
	Index         string        `mapstructure:"index"`
	IDColumn      string        `mapstructure:"id_column"`
	BatchSize     int           `mapstructure:"batch_size"`
	BatchInterval time.Duration `mapstructure:"batch_interval"`
	BackoffBase   time.Duration `mapstructure:"backoff_base"`
	BackoffCap    time.Duration `mapstructure:"backoff_cap"`
}

type RedisConfig struct {
	URL         string        `mapstructure:"url"`
	Mode        string        `mapstructure:"mode"`
	KeyPrefix   string        `mapstructure:"key_prefix"`
	IDColumn    string        `mapstructure:"id_column"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`
}

type GRPCConfig struct {
	Addr string `mapstructure:"addr"`
}

type KafkaConfig struct {
	Brokers         []string      `mapstructure:"brokers"`
	Topic           string        `mapstructure:"topic"`
	SASLMechanism   string        `mapstructure:"sasl_mechanism"`
	SASLUsername    string        `mapstructure:"sasl_username"`
	SASLPassword    string        `mapstructure:"sasl_password"`
	TLS             bool          `mapstructure:"tls"`
	TLSCAFile       string        `mapstructure:"tls_ca_file"`
	BackoffBase     time.Duration `mapstructure:"backoff_base"`
	BackoffCap      time.Duration `mapstructure:"backoff_cap"`
	Encoding        string        `mapstructure:"encoding"`         // json, avro, protobuf
	TransactionalID string        `mapstructure:"transactional_id"` // empty = idempotent only
	CBMaxFailures   int           `mapstructure:"cb_max_failures"`
	CBResetTimeout  time.Duration `mapstructure:"cb_reset_timeout"`
	RateLimit       float64       `mapstructure:"rate_limit"`
	RateLimitBurst  int           `mapstructure:"rate_limit_burst"`
}

type S3Config struct {
	Bucket          string        `mapstructure:"bucket"`
	Prefix          string        `mapstructure:"prefix"`
	Endpoint        string        `mapstructure:"endpoint"`
	Region          string        `mapstructure:"region"`
	AccessKeyID     string        `mapstructure:"access_key_id"`
	SecretAccessKey string        `mapstructure:"secret_access_key"`
	Format          string        `mapstructure:"format"` // "jsonl" or "parquet"
	FlushInterval   time.Duration `mapstructure:"flush_interval"`
	FlushSize       int           `mapstructure:"flush_size"`
	DrainTimeout    time.Duration `mapstructure:"drain_timeout"`
	BackoffBase     time.Duration `mapstructure:"backoff_base"`
	BackoffCap      time.Duration `mapstructure:"backoff_cap"`
}

type IncrementalSnapshotConfig struct {
	Enabled     bool          `mapstructure:"enabled"`
	SignalTable string        `mapstructure:"signal_table"`
	ChunkSize   int           `mapstructure:"chunk_size"`
	ChunkDelay  time.Duration `mapstructure:"chunk_delay"`
	ProgressDB  string        `mapstructure:"progress_db"`
}

type TransformConfig struct {
	Global  []TransformSpec            `mapstructure:"global"`
	Adapter map[string][]TransformSpec `mapstructure:"adapter"`
}

type TransformSpec struct {
	Type        string            `mapstructure:"type"`        // drop_columns, rename_fields, mask, filter, debezium, cloudevents
	Columns     []string          `mapstructure:"columns"`     // for drop_columns
	Mapping     map[string]string `mapstructure:"mapping"`     // for rename_fields
	Fields      []MaskFieldSpec   `mapstructure:"fields"`      // for mask
	Filter      FilterSpec        `mapstructure:"filter"`      // for filter
	Debezium    DebeziumSpec      `mapstructure:"debezium"`    // for debezium
	CloudEvents CloudEventsSpec   `mapstructure:"cloudevents"` // for cloudevents
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

type PluginConfig struct {
	Transforms []PluginTransformSpec `mapstructure:"transforms"`
	Adapters   []PluginAdapterSpec   `mapstructure:"adapters"`
	DLQ        *PluginSpec           `mapstructure:"dlq"`
	Checkpoint *PluginSpec           `mapstructure:"checkpoint"`
}

type PluginTransformSpec struct {
	Name     string         `mapstructure:"name"`
	Path     string         `mapstructure:"path"`
	Config   map[string]any `mapstructure:"config"`
	Encoding string         `mapstructure:"encoding"` // "json" (default) or "protobuf"
	Scope    any            `mapstructure:"scope"`    // "global" or map with "adapter" key
}

type PluginAdapterSpec struct {
	Name   string         `mapstructure:"name"`
	Path   string         `mapstructure:"path"`
	Config map[string]any `mapstructure:"config"`
}

type PluginSpec struct {
	Path   string         `mapstructure:"path"`
	Config map[string]any `mapstructure:"config"`
}

type MySQLConfig struct {
	Addr         string        `mapstructure:"addr"` // host:port
	User         string        `mapstructure:"user"`
	Password     string        `mapstructure:"password"`
	ServerID     uint32        `mapstructure:"server_id"`
	Tables       []string      `mapstructure:"tables"` // schema.table filter
	UseGTID      bool          `mapstructure:"use_gtid"`
	Flavor       string        `mapstructure:"flavor"`        // "mysql" or "mariadb"
	BinlogPrefix string        `mapstructure:"binlog_prefix"` // default: "mysql-bin"
	BackoffBase  time.Duration `mapstructure:"backoff_base"`
	BackoffCap   time.Duration `mapstructure:"backoff_cap"`
}

type MongoDBConfig struct {
	URI          string        `mapstructure:"uri"`
	Scope        string        `mapstructure:"scope"` // "collection", "database", or "cluster"
	Database     string        `mapstructure:"database"`
	Collections  []string      `mapstructure:"collections"`
	FullDocument string        `mapstructure:"full_document"` // "updateLookup", "default", etc.
	MetadataDB   string        `mapstructure:"metadata_db"`
	MetadataColl string        `mapstructure:"metadata_coll"`
	BackoffBase  time.Duration `mapstructure:"backoff_base"`
	BackoffCap   time.Duration `mapstructure:"backoff_cap"`
}

type KafkaServerConfig struct {
	Addr           string        `mapstructure:"addr"`
	PartitionCount int           `mapstructure:"partition_count"`
	BufferSize     int           `mapstructure:"buffer_size"`
	SessionTimeout time.Duration `mapstructure:"session_timeout"`
	CheckpointDB   string        `mapstructure:"checkpoint_db"`
	KeyColumn      string        `mapstructure:"key_column"`
}

type ViewConfig struct {
	Name      string `mapstructure:"name"`
	Query     string `mapstructure:"query"`
	Emit      string `mapstructure:"emit"`       // "row" (default) or "batch"
	MaxGroups int    `mapstructure:"max_groups"` // default 100000
}

type EncodingConfig struct {
	SchemaRegistryURL      string `mapstructure:"schema_registry_url"`
	SchemaRegistryUsername string `mapstructure:"schema_registry_username"`
	SchemaRegistryPassword string `mapstructure:"schema_registry_password"`
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
			Type:              "listen_notify",
			BackoffBase:       5 * time.Second,
			BackoffCap:        60 * time.Second,
			HeartbeatInterval: 30 * time.Second,
			HeartbeatTable:    "pgcdc_heartbeat",
			SlotLagWarn:       100 * 1024 * 1024, // 100 MB
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
