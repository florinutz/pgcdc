package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestSetup_None(t *testing.T) {
	tp, shutdown, err := Setup(context.Background(), Config{Exporter: "none"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer shutdown()

	// Must return a noop provider.
	if _, ok := tp.(noop.TracerProvider); !ok {
		t.Fatalf("expected noop.TracerProvider, got %T", tp)
	}
}

func TestSetup_EmptyExporter(t *testing.T) {
	tp, shutdown, err := Setup(context.Background(), Config{Exporter: ""}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer shutdown()

	if _, ok := tp.(noop.TracerProvider); !ok {
		t.Fatalf("expected noop.TracerProvider, got %T", tp)
	}
}

func TestSetup_Stdout(t *testing.T) {
	tp, shutdown, err := Setup(context.Background(), Config{Exporter: "stdout", SampleRatio: 1.0}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer shutdown()

	// Must return a real (non-noop) provider.
	if _, ok := tp.(noop.TracerProvider); ok {
		t.Fatal("expected real TracerProvider, got noop")
	}

	// Must produce a tracer that creates valid spans.
	tracer := tp.Tracer("test")
	_, span := tracer.Start(context.Background(), "test-span")
	defer span.End()

	if !span.SpanContext().IsValid() {
		t.Fatal("expected valid SpanContext from stdout exporter")
	}
}

func TestSetup_UnknownExporter(t *testing.T) {
	_, _, err := Setup(context.Background(), Config{Exporter: "kafka"}, nil)
	if err == nil {
		t.Fatal("expected error for unknown exporter")
	}
}

func TestFormatTraceparent_Valid(t *testing.T) {
	// Create a real span to get a valid SpanContext.
	tp, shutdown, err := Setup(context.Background(), Config{Exporter: "stdout", SampleRatio: 1.0}, nil)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	defer shutdown()

	tracer := tp.Tracer("test")
	_, span := tracer.Start(context.Background(), "test")
	defer span.End()

	result := FormatTraceparent(span.SpanContext())
	if result == "" {
		t.Fatal("expected non-empty traceparent")
	}

	// W3C traceparent format: 00-{trace_id}-{span_id}-{flags}
	// Total length: 2 + 1 + 32 + 1 + 16 + 1 + 2 = 55
	if len(result) != 55 {
		t.Fatalf("expected traceparent length 55, got %d: %q", len(result), result)
	}
	if result[:3] != "00-" {
		t.Fatalf("expected traceparent to start with '00-', got %q", result[:3])
	}
}

func TestFormatTraceparent_Invalid(t *testing.T) {
	result := FormatTraceparent(trace.SpanContext{})
	if result != "" {
		t.Fatalf("expected empty string for invalid SpanContext, got %q", result)
	}
}
