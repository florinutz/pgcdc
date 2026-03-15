package config

import "time"

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
	TLSCertFile       string        `mapstructure:"tls_cert_file"`
	TLSKeyFile        string        `mapstructure:"tls_key_file"`
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
	SkipUnchanged  bool          `mapstructure:"skip_unchanged"`
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

type KafkaServerConfig struct {
	Addr           string        `mapstructure:"addr"`
	PartitionCount int           `mapstructure:"partition_count"`
	BufferSize     int           `mapstructure:"buffer_size"`
	SessionTimeout time.Duration `mapstructure:"session_timeout"`
	CheckpointDB   string        `mapstructure:"checkpoint_db"`
	KeyColumn      string        `mapstructure:"key_column"`
}

type ViewConfig struct {
	Name           string `mapstructure:"name"`
	Query          string `mapstructure:"query"`
	Emit           string `mapstructure:"emit"`             // "row" (default) or "batch"
	MaxGroups      int    `mapstructure:"max_groups"`       // default 100000
	EventTimeField string `mapstructure:"event_time_field"` // payload field path for event-time watermarks
}

type ArrowConfig struct {
	Addr       string `mapstructure:"addr"`
	BufferSize int    `mapstructure:"buffer_size"`
}

type GraphQLConfig struct {
	Path              string        `mapstructure:"path"`
	SchemaAware       bool          `mapstructure:"schema_aware"`
	BufferSize        int           `mapstructure:"buffer_size"`
	KeepaliveInterval time.Duration `mapstructure:"keepalive_interval"`
}

type DuckDBConfig struct {
	Path          string        `mapstructure:"path"`
	Retention     time.Duration `mapstructure:"retention"`
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	FlushSize     int           `mapstructure:"flush_size"`
}

type ClickHouseConfig struct {
	DSN           string            `mapstructure:"dsn"`
	Table         string            `mapstructure:"table"`
	AutoCreate    bool              `mapstructure:"auto_create"`
	AsyncInsert   bool              `mapstructure:"async_insert"`
	Settings      map[string]string `mapstructure:"settings"`
	BatchSize     int               `mapstructure:"batch_size"`
	FlushInterval time.Duration     `mapstructure:"flush_interval"`
	BackoffBase   time.Duration     `mapstructure:"backoff_base"`
	BackoffCap    time.Duration     `mapstructure:"backoff_cap"`
}

type MiddlewareConfig struct {
	Retry          *MiddlewareRetryConfig          `mapstructure:"retry"`
	CircuitBreaker *MiddlewareCircuitBreakerConfig `mapstructure:"circuit_breaker"`
	RateLimit      *MiddlewareRateLimitConfig      `mapstructure:"rate_limit"`
}

type MiddlewareRetryConfig struct {
	MaxRetries  int           `mapstructure:"max_retries"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`
}

type MiddlewareCircuitBreakerConfig struct {
	MaxFailures  int           `mapstructure:"max_failures"`
	ResetTimeout time.Duration `mapstructure:"reset_timeout"`
}

type MiddlewareRateLimitConfig struct {
	EventsPerSecond float64 `mapstructure:"events_per_second"`
	Burst           int     `mapstructure:"burst"`
}

type EncodingConfig struct {
	SchemaRegistryURL      string `mapstructure:"schema_registry_url"`
	SchemaRegistryUsername string `mapstructure:"schema_registry_username"`
	SchemaRegistryPassword string `mapstructure:"schema_registry_password"`
}
