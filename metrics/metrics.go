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
)
