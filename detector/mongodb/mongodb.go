package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/backoff"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
)

const sourceName = "mongodb_changestream"

const (
	defaultBackoffBase = 5 * time.Second
	defaultBackoffCap  = 60 * time.Second
)

// Detector implements detector.Detector using MongoDB Change Streams.
type Detector struct {
	uri          string
	scope        string // "collection", "database", or "cluster"
	database     string
	collections  []string
	fullDocument string
	metadataDB   string
	metadataColl string
	backoffBase  time.Duration
	backoffCap   time.Duration
	logger       *slog.Logger
	tracer       trace.Tracer
}

// New creates a MongoDB Change Streams detector.
func New(
	uri string,
	scope string,
	database string,
	collections []string,
	fullDocument string,
	metadataDB string,
	metadataColl string,
	backoffBase, backoffCap time.Duration,
	logger *slog.Logger,
) *Detector {
	if backoffBase <= 0 {
		backoffBase = defaultBackoffBase
	}
	if backoffCap <= 0 {
		backoffCap = defaultBackoffCap
	}
	if logger == nil {
		logger = slog.Default()
	}
	if scope == "" {
		scope = "collection"
	}
	if fullDocument == "" {
		fullDocument = "updateLookup"
	}
	if metadataDB == "" {
		metadataDB = database
	}
	if metadataColl == "" {
		metadataColl = "pgcdc_resume_tokens"
	}

	return &Detector{
		uri:          uri,
		scope:        scope,
		database:     database,
		collections:  collections,
		fullDocument: fullDocument,
		metadataDB:   metadataDB,
		metadataColl: metadataColl,
		backoffBase:  backoffBase,
		backoffCap:   backoffCap,
		logger:       logger.With("detector", sourceName),
	}
}

// Name returns the detector name.
func (d *Detector) Name() string {
	return sourceName
}

// SetTracer sets the OpenTelemetry tracer for creating per-event spans.
func (d *Detector) SetTracer(t trace.Tracer) {
	d.tracer = t
}

// Start connects to MongoDB and streams change events.
// It blocks until ctx is cancelled. The caller owns the events channel.
func (d *Detector) Start(ctx context.Context, events chan<- event.Event) error {
	var attempt int
	for {
		runErr := d.run(ctx, events)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		mongoErr := &pgcdcerr.MongoDBChangeStreamError{
			URI: d.uri,
			Err: runErr,
		}

		delay := backoff.Jitter(attempt, d.backoffBase, d.backoffCap)
		d.logger.Error("connection lost, reconnecting",
			"error", mongoErr,
			"attempt", attempt+1,
			"delay", delay,
		)
		metrics.MongoDBErrors.Inc()
		attempt++

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// run performs a single connect → watch → event-loop cycle.
func (d *Detector) run(ctx context.Context, events chan<- event.Event) error {
	client, err := mongo.Connect(options.Client().ApplyURI(d.uri))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	// Ping to verify connectivity.
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	// Load resume token if available.
	resumeToken, err := d.loadResumeToken(ctx, client)
	if err != nil {
		d.logger.Warn("failed to load resume token, starting from current position", "error", err)
	}

	// Build pipeline filter for multi-collection database-level watch.
	pipeline := d.buildPipeline()

	// Build change stream options.
	csOpts := options.ChangeStream().SetFullDocument(options.FullDocument(d.fullDocument))
	if resumeToken != nil {
		csOpts.SetResumeAfter(resumeToken)
	}

	// Open change stream based on scope.
	var cs *mongo.ChangeStream
	switch d.scope {
	case "cluster":
		cs, err = client.Watch(ctx, pipeline, csOpts)
	case "database":
		cs, err = client.Database(d.database).Watch(ctx, pipeline, csOpts)
	default:
		// collection scope — single collection or database-level with $match
		if len(d.collections) == 1 {
			cs, err = client.Database(d.database).Collection(d.collections[0]).Watch(ctx, pipeline, csOpts)
		} else {
			cs, err = client.Database(d.database).Watch(ctx, pipeline, csOpts)
		}
	}
	if err != nil {
		return fmt.Errorf("watch: %w", err)
	}
	defer func() { _ = cs.Close(ctx) }()

	d.logger.Info("change stream started",
		"uri", d.uri,
		"scope", d.scope,
		"database", d.database,
		"collections", d.collections,
	)

	// Event loop.
	for cs.Next(ctx) {
		var changeDoc changeEvent
		if err := cs.Decode(&changeDoc); err != nil {
			d.logger.Error("decode change event", "error", err)
			continue
		}

		if err := d.emitChangeEvent(ctx, events, &changeDoc); err != nil {
			return err
		}

		// Persist resume token after each event.
		if token := cs.ResumeToken(); token != nil {
			if err := d.saveResumeToken(ctx, client, token); err != nil {
				d.logger.Warn("failed to save resume token", "error", err)
			} else {
				metrics.MongoDBResumeTokenSaves.Inc()
			}
		}
	}

	if err := cs.Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("change stream: %w", err)
	}
	return nil
}

// changeEvent represents a MongoDB change stream event document.
type changeEvent struct {
	OperationType string                 `bson:"operationType"`
	FullDocument  bson.Raw               `bson:"fullDocument"`
	NS            changeEventNS          `bson:"ns"`
	DocumentKey   map[string]interface{} `bson:"documentKey"`
	UpdateDesc    *updateDescription     `bson:"updateDescription"`
}

type changeEventNS struct {
	DB   string `bson:"db"`
	Coll string `bson:"coll"`
}

type updateDescription struct {
	UpdatedFields map[string]interface{} `bson:"updatedFields"`
	RemovedFields []string               `bson:"removedFields"`
}

// buildPipeline constructs the aggregation pipeline for filtering.
func (d *Detector) buildPipeline() mongo.Pipeline {
	// For multi-collection database-level watch, filter by collection names.
	if d.scope == "collection" && len(d.collections) > 1 {
		return mongo.Pipeline{
			{{Key: "$match", Value: bson.D{
				{Key: "ns.coll", Value: bson.D{
					{Key: "$in", Value: d.collections},
				}},
			}}},
		}
	}
	return mongo.Pipeline{}
}

// emitChangeEvent maps a MongoDB change event to a pgcdc event and sends it.
func (d *Detector) emitChangeEvent(ctx context.Context, events chan<- event.Event, change *changeEvent) error {
	op := mapOperation(change.OperationType)
	if op == "" {
		// System event — emit on system channel.
		return d.emitSystemEvent(ctx, events, change)
	}

	channel := channelName(change.NS.DB, change.NS.Coll)

	payload := map[string]any{
		"op":         op,
		"database":   change.NS.DB,
		"collection": change.NS.Coll,
	}

	if change.FullDocument != nil {
		var doc map[string]interface{}
		if err := bson.Unmarshal(change.FullDocument, &doc); err == nil {
			payload["row"] = doc
		}
	}

	if change.DocumentKey != nil {
		payload["document_key"] = change.DocumentKey
	}

	if change.UpdateDesc != nil {
		payload["update_description"] = map[string]any{
			"updated_fields": change.UpdateDesc.UpdatedFields,
			"removed_fields": change.UpdateDesc.RemovedFields,
		}
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		d.logger.Error("marshal payload failed", "error", err)
		return nil
	}

	ev, err := event.New(channel, op, payloadJSON, sourceName)
	if err != nil {
		d.logger.Error("create event failed", "error", err)
		return nil
	}

	// No WAL LSN concept — leave ev.LSN = 0.

	if d.tracer != nil {
		_, span := d.tracer.Start(ctx, "pgcdc.detect",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				attribute.String("pgcdc.event.id", ev.ID),
				attribute.String("pgcdc.channel", ev.Channel),
				attribute.String("pgcdc.operation", op),
				attribute.String("pgcdc.source", sourceName),
				attribute.String("pgcdc.database", change.NS.DB),
				attribute.String("pgcdc.collection", change.NS.Coll),
			),
		)
		ev.SpanContext = span.SpanContext()
		span.End()
	}

	metrics.MongoDBEventsReceived.Inc()

	select {
	case events <- ev:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// emitSystemEvent emits system events (drop, rename, invalidate) on pgcdc:_mongo channel.
func (d *Detector) emitSystemEvent(ctx context.Context, events chan<- event.Event, change *changeEvent) error {
	payload := map[string]any{
		"operation_type": change.OperationType,
		"database":       change.NS.DB,
		"collection":     change.NS.Coll,
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		d.logger.Error("marshal system event payload failed", "error", err)
		return nil
	}

	ev, err := event.New("pgcdc:_mongo", change.OperationType, payloadJSON, sourceName)
	if err != nil {
		d.logger.Error("create system event failed", "error", err)
		return nil
	}

	metrics.MongoDBEventsReceived.Inc()

	select {
	case events <- ev:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// mapOperation maps MongoDB operation types to pgcdc operations.
// Returns empty string for system events that should go to the _mongo channel.
func mapOperation(opType string) string {
	switch opType {
	case "insert":
		return "INSERT"
	case "update":
		return "UPDATE"
	case "replace":
		return "UPDATE"
	case "delete":
		return "DELETE"
	default:
		return ""
	}
}

// channelName returns the pgcdc channel name for a MongoDB collection.
func channelName(db, coll string) string {
	return "pgcdc:" + db + "." + coll
}
