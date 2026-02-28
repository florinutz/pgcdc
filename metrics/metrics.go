package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_events_received_total",
		Help: "Total number of events received by the bus.",
	})

	EventsDelivered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_events_delivered_total",
		Help: "Total number of events delivered by adapters.",
	}, []string{"adapter"})

	EventsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_events_dropped_total",
		Help: "Total number of events dropped due to full subscriber channels.",
	}, []string{"adapter"})

	WebhookRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_webhook_retries_total",
		Help: "Total number of webhook delivery retries.",
	})

	WebhookDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgcdc_webhook_duration_seconds",
		Help:    "Duration of webhook HTTP requests.",
		Buckets: prometheus.DefBuckets,
	})

	SSEClientsActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_sse_clients_active",
		Help: "Number of active SSE client connections.",
	}, []string{"channel"})

	BusSubscribers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgcdc_bus_subscribers",
		Help: "Number of active bus subscribers.",
	})

	FileRotations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_file_rotations_total",
		Help: "Total number of file rotations.",
	})

	ExecRestarts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_exec_restarts_total",
		Help: "Total number of exec subprocess restarts.",
	})

	WSClientsActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_ws_clients_active",
		Help: "Number of active WebSocket client connections.",
	}, []string{"channel"})

	SnapshotRowsExported = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_snapshot_rows_exported_total",
		Help: "Total number of rows exported during snapshots.",
	})

	SnapshotDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgcdc_snapshot_duration_seconds",
		Help:    "Duration of snapshot operations.",
		Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600},
	})

	EmbeddingAPIRequests = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_embedding_api_requests_total",
		Help: "Total number of embedding API requests.",
	})

	EmbeddingAPIDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgcdc_embedding_api_duration_seconds",
		Help:    "Duration of embedding API requests.",
		Buckets: prometheus.DefBuckets,
	})

	EmbeddingAPIErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_embedding_api_errors_total",
		Help: "Total number of embedding API errors after retries exhausted.",
	})

	EmbeddingTokensUsed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_embedding_tokens_total",
		Help: "Total number of tokens consumed by the embedding API.",
	})

	EmbeddingSkipped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_embedding_skipped_total",
		Help: "Total number of embedding operations skipped due to unchanged columns.",
	})

	IcebergFlushes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_iceberg_flushes_total",
		Help: "Total number of Iceberg flush operations.",
	}, []string{"mode"})

	IcebergFlushSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgcdc_iceberg_flush_size",
		Help:    "Number of events per Iceberg flush.",
		Buckets: []float64{10, 100, 500, 1000, 5000, 10000, 50000},
	})

	IcebergFlushDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgcdc_iceberg_flush_duration_seconds",
		Help:    "Duration of Iceberg flush operations.",
		Buckets: prometheus.DefBuckets,
	})

	IcebergBufferSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgcdc_iceberg_buffer_size",
		Help: "Current number of events buffered for Iceberg flush.",
	})

	IcebergDataFilesWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_iceberg_data_files_written_total",
		Help: "Total number of Parquet data files written.",
	})

	IcebergBytesWritten = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_iceberg_bytes_written_total",
		Help: "Total bytes written to Iceberg storage.",
	})

	// S3 adapter metrics.

	S3Flushes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_s3_flushes_total",
		Help: "Total number of S3 flush operations.",
	}, []string{"status"})

	S3FlushSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgcdc_s3_flush_size",
		Help:    "Number of events per S3 flush.",
		Buckets: []float64{10, 100, 500, 1000, 5000, 10000, 50000},
	})

	S3FlushDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgcdc_s3_flush_duration_seconds",
		Help:    "Duration of S3 flush operations.",
		Buckets: prometheus.DefBuckets,
	})

	S3BufferSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgcdc_s3_buffer_size",
		Help: "Current number of events buffered for S3 flush.",
	})

	S3ObjectsUploaded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_s3_objects_uploaded_total",
		Help: "Total number of objects uploaded to S3.",
	})

	S3BytesUploaded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_s3_bytes_uploaded_total",
		Help: "Total bytes uploaded to S3.",
	})

	// Incremental snapshot metrics.

	IncrementalSnapshotRowsExported = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_incremental_snapshot_rows_exported_total",
		Help: "Total number of rows exported during incremental snapshots.",
	})

	IncrementalSnapshotChunks = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_incremental_snapshot_chunks_total",
		Help: "Total number of chunks read during incremental snapshots.",
	})

	IncrementalSnapshotDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgcdc_incremental_snapshot_duration_seconds",
		Help:    "Duration of incremental snapshot operations.",
		Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
	})

	IncrementalSnapshotsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgcdc_incremental_snapshots_active",
		Help: "Number of currently active incremental snapshots.",
	})

	// Checkpoint metrics.

	CheckpointLSN = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgcdc_checkpoint_lsn",
		Help: "Last persisted LSN value.",
	})

	CheckpointLagBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgcdc_checkpoint_lag_bytes",
		Help: "WAL bytes between current position and last checkpoint.",
	})

	CheckpointTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_checkpoint_total",
		Help: "Total number of checkpoints written.",
	})

	// Slot metrics.

	SlotLagBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgcdc_slot_lag_bytes",
		Help: "Bytes of WAL retained by the replication slot.",
	})

	// NATS metrics.

	NatsPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_nats_published_total",
		Help: "Total number of events published to NATS.",
	})

	NatsPublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgcdc_nats_publish_duration_seconds",
		Help:    "Duration of NATS publish operations.",
		Buckets: prometheus.DefBuckets,
	})

	NatsErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_nats_errors_total",
		Help: "Total number of NATS publish errors.",
	})

	// Kafka adapter metrics.

	KafkaPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafka_published_total",
		Help: "Total number of events published to Kafka.",
	})

	KafkaPublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgcdc_kafka_publish_duration_seconds",
		Help:    "Duration of Kafka publish operations.",
		Buckets: prometheus.DefBuckets,
	})

	KafkaErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafka_errors_total",
		Help: "Total number of Kafka publish errors.",
	})

	KafkaTransactions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafka_transactions_total",
		Help: "Total number of Kafka transactions committed.",
	})

	KafkaTransactionErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafka_transaction_errors_total",
		Help: "Total number of Kafka transaction errors (aborts).",
	})

	// Listen/Notify detector metrics.

	ListenNotifyErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_listennotify_errors_total",
		Help: "Total number of LISTEN/NOTIFY detector reconnect errors.",
	})

	// WAL replication detector metrics.

	WalReplicationErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_walreplication_errors_total",
		Help: "Total number of WAL replication detector reconnect errors.",
	})

	// Outbox metrics.

	OutboxPolled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_outbox_polled_total",
		Help: "Total number of outbox poll cycles.",
	})

	OutboxEventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_outbox_events_processed_total",
		Help: "Total number of outbox events processed.",
	})

	OutboxErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_outbox_errors_total",
		Help: "Total number of outbox processing errors.",
	})

	// DLQ metrics.

	DLQRecords = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_dlq_records_total",
		Help: "Total number of events recorded to the dead letter queue.",
	}, []string{"adapter"})

	DLQErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_dlq_errors_total",
		Help: "Total number of errors writing to the dead letter queue.",
	})

	// Search adapter metrics.

	SearchUpserted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_search_upserted_total",
		Help: "Total number of documents upserted to search engine.",
	})

	SearchDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_search_deleted_total",
		Help: "Total number of documents deleted from search engine.",
	})

	SearchBatchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgcdc_search_batch_duration_seconds",
		Help:    "Duration of search batch operations.",
		Buckets: prometheus.DefBuckets,
	})

	SearchErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_search_errors_total",
		Help: "Total number of search engine errors.",
	})

	// Redis adapter metrics.

	RedisOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_redis_operations_total",
		Help: "Total number of Redis operations.",
	}, []string{"operation"})

	RedisErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_redis_errors_total",
		Help: "Total number of Redis errors.",
	})

	// gRPC adapter metrics.

	GRPCActiveClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgcdc_grpc_active_clients",
		Help: "Number of active gRPC streaming clients.",
	})

	GRPCEventsSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_grpc_events_sent_total",
		Help: "Total number of events sent to gRPC clients.",
	})

	// Bus backpressure metric (reliable mode).

	BusBackpressure = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_bus_backpressure_total",
		Help: "Events that required blocking send in reliable bus mode.",
	}, []string{"adapter"})

	// Cooperative checkpoint metrics.

	AckPosition = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_ack_position",
		Help: "Highest acknowledged LSN per adapter.",
	}, []string{"adapter"})

	CooperativeCheckpointLSN = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgcdc_cooperative_checkpoint_lsn",
		Help: "Cooperative checkpoint LSN (minimum across all adapters).",
	})

	// Transform metrics.

	TransformDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_transform_dropped_total",
		Help: "Total number of events dropped by transforms.",
	}, []string{"adapter"})

	TransformErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_transform_errors_total",
		Help: "Total number of transform errors.",
	}, []string{"adapter"})

	// Plugin metrics.

	PluginCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_plugin_calls_total",
		Help: "Total Wasm plugin invocations.",
	}, []string{"plugin", "type"})

	PluginDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgcdc_plugin_duration_seconds",
		Help:    "Wasm plugin call duration.",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
	}, []string{"plugin", "type"})

	PluginErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_plugin_errors_total",
		Help: "Total Wasm plugin errors.",
	}, []string{"plugin", "type"})

	// Encoding metrics.

	EncodingEncoded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_encoding_encoded_total",
		Help: "Total number of events encoded (Avro/Protobuf).",
	})

	EncodingErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_encoding_errors_total",
		Help: "Total number of encoding errors.",
	})

	SchemaRegistryRegistrations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_schema_registry_registrations_total",
		Help: "Total number of schema registrations with Schema Registry.",
	})

	SchemaRegistryErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_schema_registry_errors_total",
		Help: "Total number of Schema Registry errors.",
	})

	// Backpressure metrics.

	BackpressureState = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgcdc_backpressure_state",
		Help: "Current backpressure zone: 0=green, 1=yellow, 2=red.",
	})

	BackpressureThrottleDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgcdc_backpressure_throttle_duration_seconds",
		Help:    "Observed throttle sleep durations between WAL reads.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5},
	})

	BackpressureLoadShed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_backpressure_load_shed_total",
		Help: "Events auto-acked due to backpressure load shedding.",
	}, []string{"adapter"})

	// MySQL detector metrics.

	MySQLEventsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_mysql_events_received_total",
		Help: "Total number of events received from MySQL binlog.",
	})

	MySQLErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_mysql_errors_total",
		Help: "Total number of MySQL binlog replication errors.",
	})

	// MongoDB detector metrics.

	MongoDBEventsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_mongodb_events_received_total",
		Help: "Total number of events received from MongoDB change streams.",
	})

	MongoDBErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_mongodb_errors_total",
		Help: "Total number of MongoDB change stream errors.",
	})

	MongoDBResumeTokenSaves = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_mongodb_resume_token_saves_total",
		Help: "Total number of resume token saves to MongoDB.",
	})

	// Kafka server adapter metrics.

	KafkaServerConnectionsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgcdc_kafkaserver_connections_active",
		Help: "Number of active Kafka protocol server connections.",
	})

	KafkaServerFetchRequests = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafkaserver_fetch_requests_total",
		Help: "Total number of Kafka Fetch requests received.",
	})

	KafkaServerRecordsFetched = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafkaserver_records_fetched_total",
		Help: "Total number of records returned in Kafka Fetch responses.",
	})

	KafkaServerGroupRebalances = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafkaserver_group_rebalances_total",
		Help: "Total number of consumer group rebalances.",
	})

	KafkaServerOffsetCommits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafkaserver_offset_commits_total",
		Help: "Total number of offset commits.",
	})

	KafkaServerProtocolErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_kafkaserver_protocol_errors_total",
		Help: "Total number of Kafka protocol errors.",
	})

	KafkaServerBufferUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_kafkaserver_buffer_usage",
		Help: "Current buffer usage per topic/partition.",
	}, []string{"topic", "partition"})

	// View adapter metrics.

	ViewWindowsEmitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_view_windows_emitted_total",
		Help: "Total number of view result events emitted.",
	}, []string{"view"})

	ViewEventsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_view_events_processed_total",
		Help: "Total number of events processed by views.",
	}, []string{"view"})

	ViewWindowDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgcdc_view_window_duration_seconds",
		Help:    "Duration of view window flush operations.",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
	}, []string{"view"})

	ViewGroups = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_view_groups",
		Help: "Number of groups in the last window flush.",
	}, []string{"view"})

	ViewTypeErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_view_type_errors_total",
		Help: "Total number of type coercion errors in view aggregation.",
	}, []string{"view"})

	ViewGroupsOverflow = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_view_groups_overflow_total",
		Help: "Total events dropped due to group cardinality exceeding max_groups.",
	}, []string{"view"})

	// Middleware metrics.

	MiddlewareRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_middleware_retries_total",
		Help: "Total number of delivery retries across all middleware-wrapped adapters.",
	})

	MiddlewareDeliveryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgcdc_middleware_delivery_duration_seconds",
		Help:    "Duration of event delivery through the middleware chain.",
		Buckets: prometheus.DefBuckets,
	}, []string{"adapter"})

	// Batch runner metrics.

	BatchFlushes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_batch_flushes_total",
		Help: "Total number of batch flush operations.",
	}, []string{"adapter", "status"})

	BatchFlushSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgcdc_batch_flush_size",
		Help:    "Number of events per batch flush.",
		Buckets: []float64{1, 10, 50, 100, 500, 1000, 5000, 10000},
	}, []string{"adapter"})

	BatchFlushDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgcdc_batch_flush_duration_seconds",
		Help:    "Duration of batch flush operations.",
		Buckets: prometheus.DefBuckets,
	}, []string{"adapter"})

	BatchBufferSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_batch_buffer_size",
		Help: "Current number of events buffered for batch flush.",
	}, []string{"adapter"})

	// Config reload metrics.

	ConfigReloads = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_config_reloads_total",
		Help: "Total number of successful config reloads via SIGHUP.",
	})

	ConfigReloadErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_config_reload_errors_total",
		Help: "Total number of failed config reload attempts.",
	})

	// Panic recovery metrics.

	PanicsRecovered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_panics_recovered_total",
		Help: "Total number of panics recovered by safe goroutine wrapper.",
	}, []string{"component"})

	// Chain adapter metrics.

	ChainEventsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_chain_events_processed_total",
		Help: "Total number of events successfully processed through adapter chain links.",
	}, []string{"chain"})

	ChainLinkErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_chain_link_errors_total",
		Help: "Total number of events skipped due to chain link errors.",
	}, []string{"chain"})

	// Adapter pause metrics (nack window).

	AdapterPaused = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "pgcdc",
		Name:      "adapter_paused",
		Help:      "Whether an adapter is paused due to nack threshold (1=paused, 0=running).",
	}, []string{"adapter"})

	NackWindowExceeded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_nack_window_exceeded_total",
		Help: "Total number of events skipped due to nack window threshold exceeded.",
	}, []string{"adapter"})

	// Circuit breaker metrics.

	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgcdc_circuit_breaker_state",
		Help: "Current circuit breaker state: 0=closed, 1=open, 2=half_open.",
	}, []string{"adapter"})

	CircuitBreakerTrips = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_circuit_breaker_trips_total",
		Help: "Total number of circuit breaker trips to open state.",
	}, []string{"adapter"})

	// Rate limiter metrics.

	RateLimitWaits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgcdc_rate_limit_waits_total",
		Help: "Total number of rate limit waits.",
	}, []string{"adapter"})

	RateLimitWaitDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgcdc_rate_limit_wait_duration_seconds",
		Help:    "Duration of rate limit waits.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
	}, []string{"adapter"})

	// Validation metrics.

	ValidationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgcdc_validation_duration_seconds",
		Help:    "Duration of adapter startup validation.",
		Buckets: prometheus.DefBuckets,
	}, []string{"adapter"})

	// TOAST cache metrics.

	ToastCacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_toast_cache_hits_total",
		Help: "Total number of TOAST cache hits (unchanged columns resolved from cache).",
	})

	ToastCacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_toast_cache_misses_total",
		Help: "Total number of TOAST cache misses (unchanged columns emitted as metadata).",
	})

	ToastCacheEvictions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgcdc_toast_cache_evictions_total",
		Help: "Total number of TOAST cache LRU evictions.",
	})

	ToastCacheEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgcdc_toast_cache_entries",
		Help: "Current number of entries in the TOAST cache.",
	})
)
