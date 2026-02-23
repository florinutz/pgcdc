package config

import "time"

type Config struct {
	DatabaseURL     string          `mapstructure:"database_url"`
	Channels        []string        `mapstructure:"channels"`
	Adapters        []string        `mapstructure:"adapters"`
	LogLevel        string          `mapstructure:"log_level"`
	LogFormat       string          `mapstructure:"log_format"`
	ShutdownTimeout time.Duration   `mapstructure:"shutdown_timeout"`
	MetricsAddr     string          `mapstructure:"metrics_addr"`
	Bus             BusConfig       `mapstructure:"bus"`
	Webhook         WebhookConfig   `mapstructure:"webhook"`
	SSE             SSEConfig       `mapstructure:"sse"`
	File            FileConfig      `mapstructure:"file"`
	Exec            ExecConfig      `mapstructure:"exec"`
	PGTable         PGTableConfig   `mapstructure:"pg_table"`
	WebSocket       WebSocketConfig `mapstructure:"websocket"`
	Detector        DetectorConfig  `mapstructure:"detector"`
	Snapshot        SnapshotConfig  `mapstructure:"snapshot"`
	Embedding       EmbeddingConfig `mapstructure:"embedding"`
	Iceberg         IcebergConfig   `mapstructure:"iceberg"`
}

type BusConfig struct {
	BufferSize int `mapstructure:"buffer_size"`
}

type WebhookConfig struct {
	URL         string            `mapstructure:"url"`
	Headers     map[string]string `mapstructure:"headers"`
	SigningKey  string            `mapstructure:"signing_key"`
	MaxRetries  int               `mapstructure:"max_retries"`
	Timeout     time.Duration     `mapstructure:"timeout"`
	BackoffBase time.Duration     `mapstructure:"backoff_base"`
	BackoffCap  time.Duration     `mapstructure:"backoff_cap"`
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
	APIURL      string        `mapstructure:"api_url"`
	APIKey      string        `mapstructure:"api_key"`
	Model       string        `mapstructure:"model"`
	Columns     []string      `mapstructure:"columns"`
	IDColumn    string        `mapstructure:"id_column"`
	Table       string        `mapstructure:"table"`
	DBURL       string        `mapstructure:"db_url"`
	Dimension   int           `mapstructure:"dimension"`
	MaxRetries  int           `mapstructure:"max_retries"`
	Timeout     time.Duration `mapstructure:"timeout"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`
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
	Type        string        `mapstructure:"type"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`
	Publication string        `mapstructure:"publication"`
	TxMetadata  bool          `mapstructure:"tx_metadata"`
	TxMarkers   bool          `mapstructure:"tx_markers"`
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
			Type:        "listen_notify",
			BackoffBase: 5 * time.Second,
			BackoffCap:  60 * time.Second,
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
