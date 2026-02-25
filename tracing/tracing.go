package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Config holds OpenTelemetry tracing configuration.
type Config struct {
	Exporter       string  // "none", "stdout", "otlp"
	Endpoint       string  // OTLP endpoint override (empty = use OTEL_EXPORTER_OTLP_ENDPOINT env)
	SampleRatio    float64 // 0.0-1.0, wrapped in ParentBased sampler
	ServiceVersion string
	DetectorType   string // "listen_notify", "wal", "outbox"
}

// Setup initialises an OTel TracerProvider based on cfg. Returns the provider,
// a shutdown function, and any error. The caller must call shutdown on exit.
//
// When Exporter is "none" (the default), a noop provider is returned — zero
// allocations on the hot path.
func Setup(ctx context.Context, cfg Config, logger *slog.Logger) (trace.TracerProvider, func(), error) {
	if cfg.Exporter == "" || cfg.Exporter == "none" {
		return noop.NewTracerProvider(), func() {}, nil
	}

	if logger == nil {
		logger = slog.Default()
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("pgcdc"),
			semconv.ServiceVersion(cfg.ServiceVersion),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create otel resource: %w", err)
	}

	var exporter sdktrace.SpanExporter
	switch cfg.Exporter {
	case "stdout":
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, nil, fmt.Errorf("create stdout exporter: %w", err)
		}
	case "otlp":
		opts := []otlptracegrpc.Option{}
		if cfg.Endpoint != "" {
			opts = append(opts, otlptracegrpc.WithEndpoint(cfg.Endpoint), otlptracegrpc.WithInsecure())
		}
		exporter, err = otlptracegrpc.New(ctx, opts...)
		if err != nil {
			return nil, nil, fmt.Errorf("create otlp exporter: %w", err)
		}
	default:
		return nil, nil, fmt.Errorf("unknown otel exporter: %q (expected none, stdout, or otlp)", cfg.Exporter)
	}

	ratio := cfg.SampleRatio
	if ratio <= 0 {
		ratio = 1.0
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)

	// Set global propagator for W3C TraceContext.
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(tp)

	shutdown := func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			logger.Warn("otel tracer provider shutdown error", "error", err)
		}
	}

	logger.Info("otel tracing enabled", "exporter", cfg.Exporter, "sample_ratio", ratio)
	return tp, shutdown, nil
}

// InjectHTTP injects the trace context from ctx into HTTP headers using the
// global W3C TraceContext propagator.
func InjectHTTP(ctx context.Context, h http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(h))
}

// FormatTraceparent returns the W3C traceparent string for a SpanContext,
// or empty string if the SpanContext is not valid.
func FormatTraceparent(sc trace.SpanContext) string {
	if !sc.IsValid() {
		return ""
	}
	return fmt.Sprintf("00-%s-%s-%s",
		sc.TraceID().String(),
		sc.SpanID().String(),
		sc.TraceFlags().String(),
	)
}
