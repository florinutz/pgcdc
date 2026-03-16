package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"

	"github.com/florinutz/pgcdc/adapter/sse"
	"github.com/florinutz/pgcdc/adapter/ws"
	"github.com/florinutz/pgcdc/health"
	"github.com/florinutz/pgcdc/inspect"
	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/internal/server"
	"github.com/go-chi/chi/v5"
	"golang.org/x/sync/errgroup"
)

// startHTTPServers registers goroutines in g for the SSE/WS HTTP server (when either broker is active)
// and the standalone metrics server (when --metrics-addr is set).
func startHTTPServers(g *errgroup.Group, gCtx context.Context, cfg config.Config, sseBroker *sse.Broker, wsBroker *ws.Broker, checker *health.Checker, readiness *health.ReadinessChecker, insp *inspect.Inspector, mountFns []func(chi.Router), logger *slog.Logger) {
	if sseBroker != nil || wsBroker != nil || insp != nil || len(mountFns) > 0 {
		var serverOpts []server.ServerOption
		if insp != nil {
			serverOpts = append(serverOpts, server.WithInspector(insp))
		}
		for _, fn := range mountFns {
			serverOpts = append(serverOpts, server.WithDetectorRoutes(fn))
		}
		httpServer := server.New(sseBroker, wsBroker, cfg.SSE.CORSOrigins, cfg.SSE.ReadTimeout, cfg.SSE.IdleTimeout, checker, readiness, serverOpts...)
		httpServer.Addr = cfg.SSE.Addr

		g.Go(func() error {
			logger.Info("http server starting", "addr", cfg.SSE.Addr)
			logger.Info("HTTP endpoints are unauthenticated; deploy behind a reverse proxy (nginx, Caddy, Envoy) in production")
			ln, err := net.Listen("tcp", cfg.SSE.Addr)
			if err != nil {
				return fmt.Errorf("http listen: %w", err)
			}
			if cfg.SSE.TLSCertFile != "" && cfg.SSE.TLSKeyFile != "" {
				cert, err := tls.LoadX509KeyPair(cfg.SSE.TLSCertFile, cfg.SSE.TLSKeyFile)
				if err != nil {
					return fmt.Errorf("load TLS cert: %w", err)
				}
				ln = tls.NewListener(ln, &tls.Config{
					Certificates: []tls.Certificate{cert},
					MinVersion:   tls.VersionTLS12,
				})
				logger.Info("http server TLS enabled", "addr", cfg.SSE.Addr)
			}
			if err := httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("http serve: %w", err)
			}
			return nil
		})

		g.Go(func() error {
			<-gCtx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			defer cancel()
			logger.Info("shutting down http server")
			return httpServer.Shutdown(shutdownCtx)
		})
	}

	if cfg.MetricsAddr != "" {
		metricsServer := server.NewMetricsServer(checker, readiness)
		metricsServer.Addr = cfg.MetricsAddr

		g.Go(func() error {
			logger.Info("metrics server starting", "addr", cfg.MetricsAddr)
			ln, err := net.Listen("tcp", cfg.MetricsAddr)
			if err != nil {
				return fmt.Errorf("metrics listen: %w", err)
			}
			if cfg.MetricsTLSCertFile != "" && cfg.MetricsTLSKeyFile != "" {
				cert, err := tls.LoadX509KeyPair(cfg.MetricsTLSCertFile, cfg.MetricsTLSKeyFile)
				if err != nil {
					return fmt.Errorf("load metrics TLS cert: %w", err)
				}
				ln = tls.NewListener(ln, &tls.Config{
					Certificates: []tls.Certificate{cert},
					MinVersion:   tls.VersionTLS12,
				})
				logger.Info("metrics server TLS enabled", "addr", cfg.MetricsAddr)
			}
			if err := metricsServer.Serve(ln); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("metrics serve: %w", err)
			}
			return nil
		})

		g.Go(func() error {
			<-gCtx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			defer cancel()
			logger.Info("shutting down metrics server")
			return metricsServer.Shutdown(shutdownCtx)
		})
	}
}
