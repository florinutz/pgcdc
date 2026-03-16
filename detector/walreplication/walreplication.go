package walreplication

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"

	"go.opentelemetry.io/otel/trace"

	"github.com/florinutz/pgcdc/backpressure"
	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/detector/walreplication/toastcache"
	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/reconnect"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"
	"github.com/florinutz/pgcdc/schema"
	"github.com/florinutz/pgcdc/snapshot"
)

const (
	source     = "wal_replication"
	txnChannel = event.TransactionMarkerChannel
)

const (
	defaultBackoffBase    = 5 * time.Second
	defaultBackoffCap     = 60 * time.Second
	standbyStatusInterval = 10 * time.Second
)

// Detector implements detector.Detector using PostgreSQL WAL logical replication.
// It uses the pgoutput plugin to decode changes and emits events for INSERT,
// UPDATE, DELETE, and TRUNCATE operations. No triggers required — changes are
// captured directly from the write-ahead log.
type Detector struct {
	dbURL       string
	publication string
	backoffBase time.Duration
	backoffCap  time.Duration
	txMetadata  bool
	txMarkers   bool
	logger      *slog.Logger

	// snapshot-first fields: when snapshotTable is set, the detector runs a
	// table snapshot using the replication slot's exported snapshot before
	// transitioning to live WAL streaming.
	snapshotTable     string
	snapshotWhere     string
	snapshotBatchSize int
	snapshotDone      bool

	// Persistent slot fields: when persistentSlot is true, the detector uses
	// a named, non-temporary replication slot that survives disconnects.
	persistentSlot  bool
	slotName        string
	checkpointStore checkpoint.Store

	// Schema inclusion: when includeSchema is true, events include column
	// type metadata from RelationMessages.
	includeSchema bool

	// Schema events: when schemaEvents is true, the detector emits
	// SCHEMA_CHANGE events when relation metadata changes.
	schemaEvents bool

	// Heartbeat fields: periodic writes to keep the replication slot advancing.
	heartbeatInterval time.Duration
	heartbeatTable    string
	heartbeatDBURL    string

	// Slot lag warning threshold in bytes.
	slotLagWarn int64

	// Incremental snapshot fields.
	incrementalEnabled bool
	signalTable        string
	snapshotChunkSize  int
	snapshotChunkDelay time.Duration
	progressStore      snapshot.ProgressStore
	activeSnapshots    map[string]context.CancelFunc // table -> cancel
	snapshotMu         sync.Mutex

	// Cooperative checkpointing: when set, the standby status update and
	// checkpoint use the minimum acked LSN across all adapters instead of
	// the detector's own receive position.
	cooperativeLSNFn func() uint64

	// Backpressure: lastLagBytes stores the most recent slot lag value written
	// by monitorSlotLag, read by the backpressure controller's lagFn.
	lastLagBytes atomic.Int64

	// backpressureCtrl is the backpressure controller that throttles/pauses
	// the detector when WAL lag is too high.
	backpressureCtrl *backpressure.Controller

	// tracer creates spans for event detection. Nil/noop when tracing disabled.
	tracer trace.Tracer

	// toastCacheMaxEntries enables the in-memory TOAST column cache when > 0.
	// The cache stores recent full rows keyed by (RelationID, PK) to backfill
	// unchanged TOAST columns on UPDATE events without REPLICA IDENTITY FULL.
	toastCacheMaxEntries int

	// schemaStore, when set, registers schema versions from RelationMessages.
	schemaStore schema.Store
}

// New creates a WAL logical replication detector for the given publication.
// Duration parameters default to sensible values when zero.
// When txMetadata is true, events include transaction info (xid, commit_time, seq).
// When txMarkers is true, synthetic BEGIN/COMMIT events are emitted (implies txMetadata).
func New(dbURL string, publication string, backoffBase, backoffCap time.Duration, txMetadata, txMarkers bool, logger *slog.Logger) *Detector {
	if txMarkers {
		txMetadata = true
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
	return &Detector{
		dbURL:       dbURL,
		publication: publication,
		backoffBase: backoffBase,
		backoffCap:  backoffCap,
		txMetadata:  txMetadata,
		txMarkers:   txMarkers,
		logger:      logger.With("detector", source),
	}
}

// Name returns the detector name.
func (d *Detector) Name() string {
	return source
}

// SetSnapshotFirst configures the detector to run a table snapshot before
// starting live WAL streaming. The snapshot uses the replication slot's
// exported snapshot for zero-gap delivery.
func (d *Detector) SetSnapshotFirst(table, where string, batchSize int) {
	d.snapshotTable = table
	d.snapshotWhere = where
	d.snapshotBatchSize = batchSize
}

// SetPersistentSlot configures the detector to use a named, non-temporary
// replication slot that survives disconnects.
func (d *Detector) SetPersistentSlot(name string) {
	d.persistentSlot = true
	d.slotName = name
}

// SetCheckpointStore sets the checkpoint store for LSN persistence.
func (d *Detector) SetCheckpointStore(store checkpoint.Store) {
	d.checkpointStore = store
}

// SetIncludeSchema enables column type metadata in events.
func (d *Detector) SetIncludeSchema(include bool) {
	d.includeSchema = include
}

// SetSchemaEvents enables SCHEMA_CHANGE event emission.
func (d *Detector) SetSchemaEvents(enabled bool) {
	d.schemaEvents = enabled
}

// SetHeartbeat configures periodic writes to keep the replication slot advancing.
func (d *Detector) SetHeartbeat(interval time.Duration, table, dbURL string) {
	d.heartbeatInterval = interval
	d.heartbeatTable = table
	d.heartbeatDBURL = dbURL
}

// SetSlotLagWarn sets the slot lag warning threshold in bytes.
func (d *Detector) SetSlotLagWarn(bytes int64) {
	d.slotLagWarn = bytes
}

// SetIncrementalSnapshot enables signal-triggered incremental snapshots.
func (d *Detector) SetIncrementalSnapshot(signalTable string, chunkSize int, chunkDelay time.Duration) {
	d.incrementalEnabled = true
	d.signalTable = signalTable
	if chunkSize <= 0 {
		chunkSize = 1000
	}
	d.snapshotChunkSize = chunkSize
	d.snapshotChunkDelay = chunkDelay
	d.activeSnapshots = make(map[string]context.CancelFunc)
}

// SetProgressStore sets the progress store for incremental snapshot persistence.
func (d *Detector) SetProgressStore(store snapshot.ProgressStore) {
	d.progressStore = store
}

// SetCooperativeLSN sets a function that returns the minimum acknowledged LSN
// across all adapters. When set, standby status updates and checkpoints use
// this value instead of the detector's own receive position, preventing WAL
// recycling past what adapters have confirmed.
func (d *Detector) SetCooperativeLSN(fn func() uint64) {
	d.cooperativeLSNFn = fn
}

// SetTracer sets the OpenTelemetry tracer for creating per-event spans.
func (d *Detector) SetTracer(t trace.Tracer) {
	d.tracer = t
}

// SetToastCache enables the in-memory TOAST column cache with the given
// maximum number of entries. When enabled, the cache backfills unchanged
// TOAST columns from previous INSERT/UPDATE events.
func (d *Detector) SetToastCache(maxEntries int) {
	d.toastCacheMaxEntries = maxEntries
}

// SetSchemaStore sets the schema store for auto-registering schema versions
// from RelationMessages. When set, every new or changed RelationMessage
// triggers a schema registration, and events include schema subject/version
// metadata.
func (d *Detector) SetSchemaStore(store schema.Store) {
	d.schemaStore = store
}

// SetBackpressureController sets the backpressure controller that will
// throttle or pause the detector when WAL lag is too high. Also wires the
// controller's lag function to read the detector's last observed lag.
// Requires persistent slot for lag monitoring; logs a warning otherwise.
func (d *Detector) SetBackpressureController(ctrl *backpressure.Controller) {
	if !d.persistentSlot {
		d.logger.Warn("backpressure controller set but persistent slot is disabled; lag monitoring will not be active")
	}
	d.backpressureCtrl = ctrl
	ctrl.SetLagFunc(func() int64 { return d.lastLagBytes.Load() })
}

// Start connects to PostgreSQL, creates a temporary replication slot, and
// streams WAL changes as events. It blocks until ctx is cancelled.
// The caller owns the events channel; Start does NOT close it.
func (d *Detector) Start(ctx context.Context, events chan<- event.Event) error {
	return reconnect.Loop(ctx, d.Name(), d.backoffBase, d.backoffCap,
		d.logger, metrics.WalReplicationErrors,
		func(ctx context.Context) error {
			err := d.run(ctx, events)
			if err != nil {
				return &pgcdcerr.DetectorDisconnectedError{
					Source: source,
					Err:    err,
				}
			}
			return nil
		},
	)
}

// run performs a single connect-replicate cycle.
func (d *Detector) run(ctx context.Context, events chan<- event.Event) error {
	connStr := ensureReplicationParam(d.dbURL)

	conn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	var slotName string
	var consistentPoint pglogrepl.LSN
	var result pglogrepl.CreateReplicationSlotResult

	if d.persistentSlot && d.slotName != "" {
		slotName = d.slotName

		// Try to load checkpoint LSN for resume.
		if d.checkpointStore != nil {
			savedLSN, loadErr := d.checkpointStore.Load(ctx, slotName)
			if loadErr != nil {
				d.logger.Warn("failed to load checkpoint, will start fresh", "error", loadErr)
			} else if savedLSN > 0 {
				consistentPoint = pglogrepl.LSN(savedLSN)
				d.logger.Info("resuming from checkpoint",
					"slot", slotName,
					"lsn", consistentPoint.String(),
				)
			}
		}

		// Create slot if it doesn't exist (persistent, non-temporary).
		result, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Temporary: false})
		if err != nil {
			// Slot already exists — expected for persistent slots on reconnect.
			if isSlotAlreadyExists(err) {
				d.logger.Info("persistent slot already exists, reusing",
					"slot", slotName,
				)
			} else {
				return fmt.Errorf("create replication slot: %w", err)
			}
		} else {
			d.logger.Info("persistent replication slot created",
				"slot", slotName,
				"consistent_point", result.ConsistentPoint,
			)
			// Only use consistent point from slot creation if we don't have a checkpoint.
			if consistentPoint == 0 {
				parsed, parseErr := pglogrepl.ParseLSN(result.ConsistentPoint)
				if parseErr != nil {
					return fmt.Errorf("parse consistent point: %w", parseErr)
				}
				consistentPoint = parsed
			}
		}
	} else {
		// Temporary slot (original behavior).
		slotName, err = randomSlotName()
		if err != nil {
			return fmt.Errorf("generate slot name: %w", err)
		}

		result, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Temporary: true})
		if err != nil {
			return fmt.Errorf("create replication slot: %w", err)
		}

		d.logger.Info("replication slot created",
			"slot", slotName,
			"consistent_point", result.ConsistentPoint,
		)

		parsed, parseErr := pglogrepl.ParseLSN(result.ConsistentPoint)
		if parseErr != nil {
			return fmt.Errorf("parse consistent point: %w", parseErr)
		}
		consistentPoint = parsed
	}

	// If snapshot-first is configured, export existing rows using the slot's
	// snapshot before starting replication. The snapshot runs on a separate
	// regular connection with SET TRANSACTION SNAPSHOT to see exactly the
	// data state at the slot's consistent point — zero gap.
	if d.snapshotTable != "" && !d.snapshotDone {
		if result.SnapshotName == "" {
			return fmt.Errorf("snapshot-first: replication slot did not export a snapshot name")
		}

		snap := snapshot.New(d.dbURL, d.snapshotTable, d.snapshotWhere, d.snapshotBatchSize, d.logger)
		if err := snap.SetSnapshotName(result.SnapshotName); err != nil {
			return fmt.Errorf("snapshot-first: %w", err)
		}

		d.logger.Info("snapshot-first: exporting existing rows",
			"table", d.snapshotTable,
			"snapshot_name", result.SnapshotName,
		)

		if err := snap.Run(ctx, events); err != nil {
			return fmt.Errorf("snapshot-first: %w", err)
		}

		d.snapshotDone = true
		d.logger.Info("snapshot-first complete, transitioning to live WAL streaming")
	}

	// Start replication.
	err = pglogrepl.StartReplication(ctx, conn, slotName, consistentPoint,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", d.publication),
			},
		})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	d.logger.Info("replication started",
		"publication", d.publication,
		"slot", slotName,
	)

	// Start heartbeat goroutine if configured.
	if d.heartbeatInterval > 0 && d.heartbeatTable != "" {
		heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
		defer heartbeatCancel()
		go d.runHeartbeat(heartbeatCtx)
	}

	// Start slot lag monitor goroutine if persistent slot.
	if d.persistentSlot {
		go d.runSlotLagMonitor(ctx, slotName)
	}

	// Resume incomplete incremental snapshots from progress store.
	if d.incrementalEnabled && d.progressStore != nil {
		d.resumeIncompleteSnapshots(ctx, events)
	}

	// Create TOAST cache if configured (local to this run cycle — fresh on reconnect).
	var tc *toastcache.Cache
	if d.toastCacheMaxEntries > 0 {
		tc = toastcache.New(d.toastCacheMaxEntries, d.logger)
		d.logger.Info("TOAST column cache enabled", "max_entries", d.toastCacheMaxEntries)
	}

	return d.streamMessages(ctx, conn, events, consistentPoint, slotName, tc)
}
