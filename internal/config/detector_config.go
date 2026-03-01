package config

import "time"

type DetectorConfig struct {
	// Core detector selection and reconnection.
	Type        string        `mapstructure:"type"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`

	// WAL replication settings.
	Publication          string        `mapstructure:"publication"`
	TxMetadata           bool          `mapstructure:"tx_metadata"`
	TxMarkers            bool          `mapstructure:"tx_markers"`
	PersistentSlot       bool          `mapstructure:"persistent_slot"`
	SlotName             string        `mapstructure:"slot_name"`
	CheckpointDB         string        `mapstructure:"checkpoint_db"`
	IncludeSchema        bool          `mapstructure:"include_schema"`
	SchemaEvents         bool          `mapstructure:"schema_events"`
	HeartbeatInterval    time.Duration `mapstructure:"heartbeat_interval"`
	HeartbeatTable       string        `mapstructure:"heartbeat_table"`
	SlotLagWarn          int64         `mapstructure:"slot_lag_warn"`
	ToastCache           bool          `mapstructure:"toast_cache"`
	ToastCacheMaxEntries int           `mapstructure:"toast_cache_max_entries"`

	// Pipeline-level flags bound to detector viper keys for CLI convenience.
	// Backpressure and incremental snapshot have their own top-level config sections
	// (Config.Backpressure.Enabled, Config.IncrementalSnapshot.Enabled).
	CooperativeCheckpoint bool `mapstructure:"cooperative_checkpoint"`
	SnapshotFirst         bool `mapstructure:"snapshot_first"`
	AllTables             bool `mapstructure:"all_tables"`
}

type SnapshotConfig struct {
	Table     string `mapstructure:"table"`
	Where     string `mapstructure:"where"`
	BatchSize int    `mapstructure:"batch_size"`
}

type OutboxConfig struct {
	Table         string        `mapstructure:"table"`
	PollInterval  time.Duration `mapstructure:"poll_interval"`
	BatchSize     int           `mapstructure:"batch_size"`
	KeepProcessed bool          `mapstructure:"keep_processed"`
	BackoffBase   time.Duration `mapstructure:"backoff_base"`
	BackoffCap    time.Duration `mapstructure:"backoff_cap"`
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

type SQLiteConfig struct {
	DBPath        string        `mapstructure:"db_path"`
	PollInterval  time.Duration `mapstructure:"poll_interval"`
	BatchSize     int           `mapstructure:"batch_size"`
	KeepProcessed bool          `mapstructure:"keep_processed"`
}

type IncrementalSnapshotConfig struct {
	Enabled     bool          `mapstructure:"enabled"`
	SignalTable string        `mapstructure:"signal_table"`
	ChunkSize   int           `mapstructure:"chunk_size"`
	ChunkDelay  time.Duration `mapstructure:"chunk_delay"`
	ProgressDB  string        `mapstructure:"progress_db"`
}

type WebhookGatewayConfig struct {
	MaxBodySize int64                          `mapstructure:"max_body_size"`
	Sources     map[string]WebhookSourceConfig `mapstructure:"sources"`
	CLISources  []string                       `mapstructure:"cli_sources"` // populated from --webhookgw-source CLI flag
}

type WebhookSourceConfig struct {
	Secret          string `mapstructure:"secret"`
	SignatureHeader string `mapstructure:"signature_header"`
	ChannelPrefix   string `mapstructure:"channel_prefix"`
}
