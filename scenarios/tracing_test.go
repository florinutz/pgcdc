//go:build integration

package scenarios

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/florinutz/pgcdc"
	"github.com/florinutz/pgcdc/adapter/webhook"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/florinutz/pgcdc/tracing"

	"golang.org/x/sync/errgroup"
)

func TestScenario_Tracing(t *testing.T) {
	connStr := startPostgres(t)

	// W3C traceparent: 00-{32 hex}-{16 hex}-{2 hex}
	traceparentRE := regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

	t.Run("traceparent in webhook", func(t *testing.T) {
		// Set up a real (stdout) tracer provider so spans are created.
		tp, shutdown, err := tracing.Setup(context.Background(), tracing.Config{
			Exporter:    "stdout",
			SampleRatio: 1.0,
		}, nil)
		if err != nil {
			t.Fatalf("tracing setup: %v", err)
		}
		defer shutdown()

		receiver := newWebhookReceiver(t, alwaysOK)
		whAdapter := webhook.New(receiver.Server.URL, nil, "", 3, 0, 0, 0, testLogger())
		det := listennotify.New(connStr, []string{"tracing_happy"}, 0, 0, testLogger())

		ctx, cancel := context.WithCancel(context.Background())
		pipeline := pgcdc.NewPipeline(det,
			pgcdc.WithAdapter(whAdapter),
			pgcdc.WithBusBuffer(64),
			pgcdc.WithLogger(testLogger()),
			pgcdc.WithTracerProvider(tp),
		)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error { return pipeline.Run(gCtx) })

		t.Cleanup(func() {
			cancel()
			g.Wait()
		})

		// Wait for detector to connect.
		waitForDetectorWebhook(t, connStr, "tracing_happy", receiver)

		sendNotify(t, connStr, "tracing_happy", `{"op":"INSERT","table":"orders","row":{"id":1}}`)

		req := receiver.waitRequest(t, 5*time.Second)

		tp_header := req.Headers.Get("Traceparent")
		if tp_header == "" {
			t.Fatal("expected Traceparent header in webhook request, got none")
		}
		if !traceparentRE.MatchString(tp_header) {
			t.Fatalf("Traceparent header %q does not match W3C format", tp_header)
		}
	})
}
