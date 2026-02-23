package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgpipe_events_received_total",
		Help: "Total number of events received by the bus.",
	}, []string{"channel"})

	EventsDelivered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgpipe_events_delivered_total",
		Help: "Total number of events delivered by adapters.",
	}, []string{"adapter"})

	EventsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgpipe_events_dropped_total",
		Help: "Total number of events dropped due to full subscriber channels.",
	}, []string{"adapter"})

	WebhookRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgpipe_webhook_retries_total",
		Help: "Total number of webhook delivery retries.",
	})

	WebhookDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgpipe_webhook_duration_seconds",
		Help:    "Duration of webhook HTTP requests.",
		Buckets: prometheus.DefBuckets,
	})

	SSEClientsActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgpipe_sse_clients_active",
		Help: "Number of active SSE client connections.",
	}, []string{"channel"})

	BusSubscribers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgpipe_bus_subscribers",
		Help: "Number of active bus subscribers.",
	})
)
