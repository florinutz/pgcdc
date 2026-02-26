package s3

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/dlq"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

const (
	defaultFlushInterval = 1 * time.Minute
	defaultFlushSize     = 10000
	defaultDrainTimeout  = 30 * time.Second
	defaultBackoffBase   = 5 * time.Second
	defaultBackoffCap    = 60 * time.Second
	maxConsecFailures    = 10
	maxBufferSize        = 100000
)

// Adapter streams CDC events to S3-compatible object storage.
type Adapter struct {
	// S3 config
	bucket          string
	prefix          string
	endpoint        string
	region          string
	accessKeyID     string
	secretAccessKey string
	format          string // "jsonl" or "parquet"

	// Flush config
	flushInterval time.Duration
	flushSize     int
	drainTimeout  time.Duration

	// Runtime
	client *s3.Client
	buffer []event.Event
	mu     sync.Mutex

	// DLQ + Ack
	dlq   dlq.DLQ
	ackFn adapter.AckFunc

	// Reconnect
	backoffBase time.Duration
	backoffCap  time.Duration

	logger *slog.Logger
}

// New creates an S3 adapter.
// Duration parameters default to sensible values when zero.
func New(
	bucket, prefix, endpoint, region string,
	accessKeyID, secretAccessKey string,
	format string,
	flushInterval time.Duration,
	flushSize int,
	drainTimeout time.Duration,
	backoffBase, backoffCap time.Duration,
	logger *slog.Logger,
) *Adapter {
	if flushInterval <= 0 {
		flushInterval = defaultFlushInterval
	}
	if flushSize <= 0 {
		flushSize = defaultFlushSize
	}
	if drainTimeout <= 0 {
		drainTimeout = defaultDrainTimeout
	}
	if backoffBase <= 0 {
		backoffBase = defaultBackoffBase
	}
	if backoffCap <= 0 {
		backoffCap = defaultBackoffCap
	}
	if logger == nil {
		logger = slog.Default()
	}
	if format == "" {
		format = "jsonl"
	}
	if region == "" {
		region = "us-east-1"
	}

	return &Adapter{
		bucket:          bucket,
		prefix:          prefix,
		endpoint:        endpoint,
		region:          region,
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		format:          format,
		flushInterval:   flushInterval,
		flushSize:       flushSize,
		drainTimeout:    drainTimeout,
		buffer:          make([]event.Event, 0, flushSize),
		backoffBase:     backoffBase,
		backoffCap:      backoffCap,
		logger:          logger.With("adapter", "s3"),
	}
}

// Name returns the adapter name.
func (a *Adapter) Name() string {
	return "s3"
}

// SetDLQ sets the dead letter queue for failed events.
func (a *Adapter) SetDLQ(d dlq.DLQ) {
	a.dlq = d
}

// SetAckFunc sets the cooperative checkpoint acknowledgement function.
func (a *Adapter) SetAckFunc(fn adapter.AckFunc) {
	a.ackFn = fn
}

// Validate checks S3 bucket accessibility via HeadBucket.
func (a *Adapter) Validate(ctx context.Context) error {
	if err := a.initClient(ctx); err != nil {
		return fmt.Errorf("init s3 client: %w", err)
	}
	_, err := a.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: &a.bucket,
	})
	if err != nil {
		return fmt.Errorf("head bucket %q: %w", a.bucket, err)
	}
	return nil
}

// Drain flushes remaining buffered events with the drain timeout.
func (a *Adapter) Drain(ctx context.Context) error {
	return a.flush(ctx)
}

// Start blocks, consuming events from the channel and flushing batches to S3.
func (a *Adapter) Start(ctx context.Context, events <-chan event.Event) error {
	a.logger.Info("s3 adapter started",
		"bucket", a.bucket,
		"prefix", a.prefix,
		"endpoint", a.endpoint,
		"region", a.region,
		"format", a.format,
		"flush_interval", a.flushInterval,
		"flush_size", a.flushSize,
	)

	var attempt int
	for {
		if err := a.initClient(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			delay := backoff.Jitter(attempt, a.backoffBase, a.backoffCap)
			a.logger.Error("s3 client init failed, retrying",
				"error", err,
				"attempt", attempt+1,
				"delay", delay,
			)
			attempt++
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		break
	}

	attempt = 0
	for {
		runErr := a.run(ctx, events)
		if ctx.Err() != nil {
			a.drainFlush()
			return ctx.Err()
		}
		if runErr == nil {
			return nil
		}

		delay := backoff.Jitter(attempt, a.backoffBase, a.backoffCap)
		a.logger.Error("s3 flush cycle failed, retrying",
			"error", runErr,
			"attempt", attempt+1,
			"delay", delay,
		)
		attempt++

		select {
		case <-ctx.Done():
			a.drainFlush()
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// initClient creates the S3 client with optional static credentials and custom endpoint.
func (a *Adapter) initClient(ctx context.Context) error {
	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(a.region))

	if a.accessKeyID != "" && a.secretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(a.accessKeyID, a.secretAccessKey, ""),
		))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if a.endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = &a.endpoint
			o.UsePathStyle = true
		})
	}

	a.client = s3.NewFromConfig(cfg, s3Opts...)
	return nil
}

// run is the main event loop. Returns an error if too many consecutive flushes fail.
func (a *Adapter) run(ctx context.Context, events <-chan event.Event) error {
	ticker := time.NewTicker(a.flushInterval)
	defer ticker.Stop()

	var consecFailures int

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case ev, ok := <-events:
			if !ok {
				if err := a.flush(ctx); err != nil {
					a.logger.Error("final flush failed", "error", err)
					return fmt.Errorf("final flush: %w", err)
				}
				return nil
			}

			// Skip transaction markers.
			if ev.Channel == "pgcdc:_txn" {
				continue
			}

			a.mu.Lock()
			a.buffer = append(a.buffer, ev)
			bufLen := len(a.buffer)

			// Drop oldest events if buffer exceeds max.
			if bufLen > maxBufferSize {
				dropped := bufLen - maxBufferSize
				a.buffer = a.buffer[dropped:]
				bufLen = maxBufferSize
				a.logger.Warn("buffer overflow, dropped oldest events", "dropped", dropped)
			}
			a.mu.Unlock()

			metrics.S3BufferSize.Set(float64(bufLen))
			metrics.EventsDelivered.WithLabelValues("s3").Inc()

			// Size trigger.
			if bufLen >= a.flushSize {
				if err := a.flush(ctx); err != nil {
					consecFailures++
					if consecFailures >= maxConsecFailures {
						return &pgcdcerr.S3UploadError{
							Attempts: consecFailures,
							Err:      err,
						}
					}
				} else {
					consecFailures = 0
				}
			}

		case <-ticker.C:
			if err := a.flush(ctx); err != nil {
				consecFailures++
				if consecFailures >= maxConsecFailures {
					return &pgcdcerr.S3UploadError{
						Attempts: consecFailures,
						Err:      err,
					}
				}
			} else {
				consecFailures = 0
			}
		}
	}
}

// flush atomically swaps the buffer and uploads partitioned objects to S3.
func (a *Adapter) flush(ctx context.Context) error {
	a.mu.Lock()
	if len(a.buffer) == 0 {
		a.mu.Unlock()
		return nil
	}
	batch := a.buffer
	a.buffer = make([]event.Event, 0, a.flushSize)
	a.mu.Unlock()

	metrics.S3BufferSize.Set(0)

	start := time.Now()
	err := a.uploadBatch(ctx, batch)
	duration := time.Since(start).Seconds()

	metrics.S3FlushDuration.Observe(duration)
	if err != nil {
		metrics.S3Flushes.WithLabelValues("error").Inc()
		a.logger.Error("flush failed",
			"events", len(batch),
			"duration", duration,
			"error", err,
		)
		// Put events back in the buffer for retry (all-or-nothing).
		a.mu.Lock()
		a.buffer = append(batch, a.buffer...)
		bufLen := len(a.buffer)
		a.mu.Unlock()
		metrics.S3BufferSize.Set(float64(bufLen))
		return err
	}

	metrics.S3Flushes.WithLabelValues("ok").Inc()
	metrics.S3FlushSize.Observe(float64(len(batch)))

	// Ack all events after successful upload.
	if a.ackFn != nil {
		for _, ev := range batch {
			if ev.LSN > 0 {
				a.ackFn(ev.LSN)
			}
		}
	}

	a.logger.Info("flush complete",
		"events", len(batch),
		"partitions", countChannels(batch),
		"duration_s", fmt.Sprintf("%.3f", duration),
	)
	return nil
}

// uploadBatch partitions events by channel and uploads each partition as an object.
func (a *Adapter) uploadBatch(ctx context.Context, batch []event.Event) error {
	partitions := partitionByChannel(batch)

	for channel, events := range partitions {
		data, contentType, err := a.encode(events)
		if err != nil {
			return fmt.Errorf("encode partition %s: %w", channel, err)
		}

		key := a.objectKey(channel, events[0].CreatedAt)
		if err := a.upload(ctx, key, data, contentType); err != nil {
			return fmt.Errorf("upload partition %s: %w", channel, err)
		}

		metrics.S3ObjectsUploaded.Inc()
		metrics.S3BytesUploaded.Add(float64(len(data)))
	}

	return nil
}

// encode converts events to the configured format.
func (a *Adapter) encode(events []event.Event) ([]byte, string, error) {
	switch a.format {
	case "parquet":
		data, err := writeParquet(events)
		if err != nil {
			return nil, "", err
		}
		return data, "application/octet-stream", nil
	default: // jsonl
		data, err := writeJSONL(events)
		if err != nil {
			return nil, "", err
		}
		return data, "application/x-ndjson", nil
	}
}

// upload puts an object to S3.
func (a *Adapter) upload(ctx context.Context, key string, data []byte, contentType string) error {
	_, err := a.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &a.bucket,
		Key:         &key,
		Body:        bytes.NewReader(data),
		ContentType: &contentType,
	})
	if err != nil {
		return fmt.Errorf("put object %s: %w", key, err)
	}
	return nil
}

// objectKey generates a Hive-style partitioned object key.
// Format: {prefix}/channel={safeChannel}/year=YYYY/month=MM/day=DD/{uuid}.{ext}
func (a *Adapter) objectKey(channel string, ts time.Time) string {
	safeChannel := strings.ReplaceAll(channel, ":", "_")
	ext := "jsonl"
	if a.format == "parquet" {
		ext = "parquet"
	}

	prefix := strings.TrimRight(a.prefix, "/")

	parts := []string{}
	if prefix != "" {
		parts = append(parts, prefix)
	}
	parts = append(parts, fmt.Sprintf("channel=%s", safeChannel))
	parts = append(parts, fmt.Sprintf("year=%d", ts.Year()))
	parts = append(parts, fmt.Sprintf("month=%02d", ts.Month()))
	parts = append(parts, fmt.Sprintf("day=%02d", ts.Day()))
	parts = append(parts, fmt.Sprintf("%s.%s", uuid.New().String(), ext))

	return strings.Join(parts, "/")
}

// drainFlush attempts to flush remaining buffered events with a timeout.
func (a *Adapter) drainFlush() {
	ctx, cancel := context.WithTimeout(context.Background(), a.drainTimeout)
	defer cancel()
	if err := a.flush(ctx); err != nil {
		a.logger.Error("shutdown drain flush failed", "error", err)
	}
}

// partitionByChannel groups events by their channel.
func partitionByChannel(events []event.Event) map[string][]event.Event {
	m := make(map[string][]event.Event)
	for _, ev := range events {
		m[ev.Channel] = append(m[ev.Channel], ev)
	}
	return m
}

// countChannels returns the number of unique channels in a batch.
func countChannels(events []event.Event) int {
	seen := make(map[string]struct{})
	for _, ev := range events {
		seen[ev.Channel] = struct{}{}
	}
	return len(seen)
}
