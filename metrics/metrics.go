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
)
