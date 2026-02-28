package server

import (
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/florinutz/pgcdc/adapter/sse"
	"github.com/florinutz/pgcdc/adapter/ws"
	"github.com/florinutz/pgcdc/health"
	"github.com/florinutz/pgcdc/inspect"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// New creates an HTTP server wired to the SSE and/or WebSocket brokers.
// corsOrigins controls which origins are allowed via Access-Control-Allow-Origin;
// an empty slice disables the CORS header entirely.
// readTimeout and idleTimeout configure the corresponding http.Server fields;
// zero values leave them unset.
// If checker is nil, /healthz is not registered. Metrics are mounted at /metrics.
func New(sseBroker *sse.Broker, wsBroker *ws.Broker, corsOrigins []string, readTimeout, idleTimeout time.Duration, checker *health.Checker, readiness *health.ReadinessChecker, opts ...ServerOption) *http.Server {
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)

	if len(corsOrigins) > 0 {
		r.Use(corsMiddleware(corsOrigins))
	}

	if checker != nil {
		r.Get("/healthz", checker.ServeHTTP)
	}
	if readiness != nil {
		r.Get("/readyz", readiness.ServeHTTP)
	}

	r.Handle("/metrics", promhttp.Handler())

	// Runtime profiling endpoints.
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)
	r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	r.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	r.Handle("/debug/pprof/block", pprof.Handler("block"))
	r.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	if sseBroker != nil {
		r.Get("/events", sseBroker.ServeHTTP)

		r.Get("/events/{channel}", func(w http.ResponseWriter, r *http.Request) {
			channel := chi.URLParam(r, "channel")
			ctx := sse.WithChannel(r.Context(), channel)
			sseBroker.ServeHTTP(w, r.WithContext(ctx))
		})
	}

	if wsBroker != nil {
		r.Get("/ws", wsBroker.ServeHTTP)

		r.Get("/ws/{channel}", func(w http.ResponseWriter, r *http.Request) {
			channel := chi.URLParam(r, "channel")
			ctx := ws.WithChannel(r.Context(), channel)
			wsBroker.ServeHTTP(w, r.WithContext(ctx))
		})
	}

	// Apply optional server options (e.g., inspector routes).
	for _, opt := range opts {
		opt(r)
	}

	return &http.Server{
		Handler:      r,
		ReadTimeout:  readTimeout,
		WriteTimeout: 0, // required for SSE/WS streaming
		IdleTimeout:  idleTimeout,
	}
}

// NewMetricsServer creates a standalone HTTP server for metrics and health
// endpoints only (no SSE). Used when --metrics-addr is set and SSE is not active.
func NewMetricsServer(checker *health.Checker, readiness *health.ReadinessChecker) *http.Server {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	if checker != nil {
		r.Get("/healthz", checker.ServeHTTP)
	}
	if readiness != nil {
		r.Get("/readyz", readiness.ServeHTTP)
	}
	r.Handle("/metrics", promhttp.Handler())

	// Runtime profiling endpoints.
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.HandleFunc("/debug/pprof/trace", pprof.Trace)
	r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	r.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	r.Handle("/debug/pprof/block", pprof.Handler("block"))
	r.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return &http.Server{
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
}

// ServerOption configures optional features on the HTTP server.
type ServerOption func(r chi.Router)

// WithInspector mounts inspector HTTP endpoints on the server.
func WithInspector(insp *inspect.Inspector) ServerOption {
	return func(r chi.Router) {
		if insp == nil {
			return
		}
		r.Get("/inspect", inspect.Handler(insp))
		r.Get("/inspect/stream", inspect.SSEHandler(insp))
	}
}

// corsMiddleware returns a middleware that sets Access-Control-Allow-Origin
// for requests whose Origin header matches one of the allowed origins.
// The wildcard "*" matches every origin.
func corsMiddleware(origins []string) func(http.Handler) http.Handler {
	allowed := make(map[string]bool, len(origins))
	allowAll := false
	for _, o := range origins {
		if o == "*" {
			allowAll = true
			break
		}
		allowed[o] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if allowAll {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else if origin != "" && allowed[origin] {
				w.Header().Set("Access-Control-Allow-Origin", origin)
			}
			if r.Method == http.MethodOptions {
				w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Last-Event-ID")
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
