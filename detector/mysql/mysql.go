package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	mysqldriver "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/florinutz/pgcdc/event"
	"github.com/florinutz/pgcdc/internal/reconnect"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/florinutz/pgcdc/pgcdcerr"

	_ "github.com/go-sql-driver/mysql"
)

const sourceName = "mysql_binlog"

const (
	defaultBackoffBase = 5 * time.Second
	defaultBackoffCap  = 60 * time.Second
)

// Detector implements detector.Detector using MySQL binlog replication.
type Detector struct {
	addr         string
	user         string
	password     string
	serverID     uint32
	tables       []string
	useGTID      bool
	flavor       string
	binlogPrefix string
	backoffBase  time.Duration
	backoffCap   time.Duration
	logger       *slog.Logger
	tracer       trace.Tracer

	// Table filter built from tables slice.
	tableFilter map[string]bool

	// startPos for resuming (file-based replication).
	startPos mysqldriver.Position

	// schemaCache caches column names per table (schema.table -> []string).
	schemaCache map[string][]string
}

// New creates a MySQL binlog detector.
func New(addr, user, password string, serverID uint32, tables []string, useGTID bool, flavor string, binlogPrefix string, backoffBase, backoffCap time.Duration, logger *slog.Logger) *Detector {
	if backoffBase <= 0 {
		backoffBase = defaultBackoffBase
	}
	if backoffCap <= 0 {
		backoffCap = defaultBackoffCap
	}
	if logger == nil {
		logger = slog.Default()
	}
	if flavor == "" {
		flavor = "mysql"
	}
	if binlogPrefix == "" {
		binlogPrefix = "mysql-bin"
	}

	filter := make(map[string]bool, len(tables))
	for _, t := range tables {
		filter[t] = true
	}

	return &Detector{
		addr:         addr,
		user:         user,
		password:     password,
		serverID:     serverID,
		tables:       tables,
		useGTID:      useGTID,
		flavor:       flavor,
		binlogPrefix: binlogPrefix,
		backoffBase:  backoffBase,
		backoffCap:   backoffCap,
		logger:       logger.With("detector", sourceName),
		tableFilter:  filter,
		schemaCache:  make(map[string][]string),
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

// Start connects to MySQL and streams binlog changes as events.
// It blocks until ctx is cancelled. The caller owns the events channel.
func (d *Detector) Start(ctx context.Context, events chan<- event.Event) error {
	return reconnect.Loop(ctx, d.Name(), d.backoffBase, d.backoffCap,
		d.logger, metrics.MySQLErrors,
		func(ctx context.Context) error {
			err := d.run(ctx, events)
			if err != nil {
				return &pgcdcerr.MySQLReplicationError{
					Addr: d.addr,
					Err:  err,
				}
			}
			return nil
		},
	)
}

// run performs a single connect-replicate cycle.
func (d *Detector) run(ctx context.Context, events chan<- event.Event) error {
	host, portStr := parseAddr(d.addr)
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return fmt.Errorf("parse port from addr %q: %w", d.addr, err)
	}

	// Validate binlog_format = ROW before starting replication.
	if err := d.validateBinlogFormat(ctx); err != nil {
		return err
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID:  d.serverID,
		Flavor:    d.flavor,
		Host:      host,
		Port:      uint16(port),
		User:      d.user,
		Password:  d.password,
		ParseTime: true,
	}

	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	var streamer *replication.BinlogStreamer
	if d.useGTID {
		// For GTID mode, start from the beginning (empty set).
		gset, parseErr := mysqldriver.ParseMysqlGTIDSet("")
		if parseErr != nil {
			return fmt.Errorf("parse gtid set: %w", parseErr)
		}
		streamer, err = syncer.StartSyncGTID(gset)
	} else {
		pos := d.startPos
		if pos.Name == "" {
			// Start from the server's current position.
			pos, err = d.getCurrentBinlogPosition(ctx)
			if err != nil {
				return fmt.Errorf("get current binlog position: %w", err)
			}
		}
		streamer, err = syncer.StartSync(pos)
	}
	if err != nil {
		return fmt.Errorf("start sync: %w", err)
	}

	d.logger.Info("binlog replication started",
		"addr", d.addr,
		"server_id", d.serverID,
		"use_gtid", d.useGTID,
	)

	// Event loop.
	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("get event: %w", err)
		}

		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			d.startPos = mysqldriver.Position{
				Name: string(e.NextLogName),
				Pos:  uint32(e.Position),
			}

		case *replication.RowsEvent:
			schema := string(e.Table.Schema)
			table := string(e.Table.Table)
			fqTable := schema + "." + table

			// Table filter.
			if len(d.tableFilter) > 0 && !d.tableFilter[fqTable] && !d.tableFilter[table] {
				continue
			}

			// Resolve column names.
			colNames := d.resolveColumnNames(ctx, e.Table, fqTable)

			// Current position for LSN encoding.
			currentPos := mysqldriver.Position{
				Name: d.startPos.Name,
				Pos:  ev.Header.LogPos,
			}

			if err := d.emitRowsEvent(ctx, events, e, schema, table, colNames, currentPos); err != nil {
				return err
			}
		}
	}
}

// emitRowsEvent converts MySQL RowsEvent into pgcdc events.
func (d *Detector) emitRowsEvent(
	ctx context.Context,
	events chan<- event.Event,
	e *replication.RowsEvent,
	schema, table string,
	colNames []string,
	currentPos mysqldriver.Position,
) error {
	var op string
	switch e.Type() {
	case replication.EnumRowsEventTypeInsert:
		op = "INSERT"
	case replication.EnumRowsEventTypeUpdate:
		op = "UPDATE"
	case replication.EnumRowsEventTypeDelete:
		op = "DELETE"
	default:
		return nil
	}

	channel := channelName(schema, table)
	encodedLSN, _ := encodePosition(currentPos)

	switch op {
	case "INSERT":
		for _, row := range e.Rows {
			if err := d.emitSingleEvent(ctx, events, channel, op, schema, table, rowToMap(colNames, row), nil, encodedLSN); err != nil {
				return err
			}
		}
	case "DELETE":
		for _, row := range e.Rows {
			if err := d.emitSingleEvent(ctx, events, channel, op, schema, table, rowToMap(colNames, row), nil, encodedLSN); err != nil {
				return err
			}
		}
	case "UPDATE":
		// UPDATE rows come in pairs: [before, after, before, after, ...]
		for i := 0; i+1 < len(e.Rows); i += 2 {
			before := rowToMap(colNames, e.Rows[i])
			after := rowToMap(colNames, e.Rows[i+1])
			if err := d.emitSingleEvent(ctx, events, channel, op, schema, table, after, before, encodedLSN); err != nil {
				return err
			}
		}
	}

	return nil
}

// emitSingleEvent builds and sends a single event to the events channel.
func (d *Detector) emitSingleEvent(
	ctx context.Context,
	events chan<- event.Event,
	channel, op, schema, table string,
	row, old map[string]any,
	encodedLSN uint64,
) error {
	rec := &event.Record{
		Position:  event.NewPosition(encodedLSN),
		Operation: event.ParseOperation(op),
		Metadata: event.Metadata{
			event.MetaTable:  table,
			event.MetaSchema: schema,
		},
	}

	switch op {
	case "DELETE":
		rec.Change.Before = event.NewStructuredDataFromMap(row)
	case "UPDATE":
		rec.Change.After = event.NewStructuredDataFromMap(row)
		rec.Change.Before = event.NewStructuredDataFromMap(old)
	default:
		rec.Change.After = event.NewStructuredDataFromMap(row)
	}

	ev, err := event.NewFromRecord(channel, rec, sourceName)
	if err != nil {
		d.logger.Error("create event failed", "error", err)
		return nil
	}

	if d.tracer != nil {
		_, span := d.tracer.Start(ctx, "pgcdc.detect",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				attribute.String("pgcdc.event.id", ev.ID),
				attribute.String("pgcdc.channel", ev.Channel),
				attribute.String("pgcdc.operation", op),
				attribute.String("pgcdc.source", sourceName),
				attribute.Int64("pgcdc.lsn", int64(encodedLSN)),
				attribute.String("pgcdc.table", table),
			),
		)
		ev.SpanContext = span.SpanContext()
		span.End()
	}

	metrics.MySQLEventsReceived.Inc()

	select {
	case events <- ev:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// resolveColumnNames returns column names for a table. It tries three sources:
// 1. TableMapEvent.ColumnName (MySQL 8.0.1+ with optional metadata)
// 2. Cached schema from information_schema query
// 3. Fallback to col_0, col_1, ...
func (d *Detector) resolveColumnNames(ctx context.Context, tme *replication.TableMapEvent, fqTable string) []string {
	// Source 1: Optional metadata from TableMapEvent (MySQL 8.0.1+).
	if len(tme.ColumnName) > 0 {
		names := make([]string, len(tme.ColumnName))
		for i, n := range tme.ColumnName {
			names[i] = string(n)
		}
		d.schemaCache[fqTable] = names
		return names
	}

	// Source 2: Schema cache (populated from information_schema).
	if cached, ok := d.schemaCache[fqTable]; ok && len(cached) == int(tme.ColumnCount) {
		return cached
	}

	// Source 2b: Query information_schema.
	names, err := d.queryColumnNames(ctx, string(tme.Schema), string(tme.Table))
	if err == nil && len(names) == int(tme.ColumnCount) {
		d.schemaCache[fqTable] = names
		return names
	}

	// Source 3: Fallback.
	names = make([]string, tme.ColumnCount)
	for i := range names {
		names[i] = fmt.Sprintf("col_%d", i)
	}
	d.schemaCache[fqTable] = names
	return names
}

// queryColumnNames queries information_schema.columns for column names.
func (d *Detector) queryColumnNames(ctx context.Context, schema, table string) ([]string, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?timeout=5s", d.user, d.password, d.addr, schema)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(queryCtx,
		"SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
		schema, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// validateBinlogFormat checks that binlog_format is ROW.
func (d *Detector) validateBinlogFormat(ctx context.Context) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?timeout=5s", d.user, d.password, d.addr)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("validate binlog format: %w", err)
	}
	defer func() { _ = db.Close() }()

	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var format string
	if err := db.QueryRowContext(queryCtx, "SELECT @@binlog_format").Scan(&format); err != nil {
		return fmt.Errorf("query binlog_format: %w", err)
	}

	if strings.ToUpper(format) != "ROW" {
		return fmt.Errorf("binlog_format is %q, must be ROW for CDC replication", format)
	}

	return nil
}

// getCurrentBinlogPosition queries the server's current binlog position.
func (d *Detector) getCurrentBinlogPosition(ctx context.Context) (mysqldriver.Position, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?timeout=5s", d.user, d.password, d.addr)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return mysqldriver.Position{}, fmt.Errorf("connect for binlog position: %w", err)
	}
	defer func() { _ = db.Close() }()

	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(queryCtx, "SHOW MASTER STATUS")
	if err != nil {
		return mysqldriver.Position{}, fmt.Errorf("show master status: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return mysqldriver.Position{}, fmt.Errorf("SHOW MASTER STATUS returned no rows (is binary logging enabled?)")
	}

	// SHOW MASTER STATUS returns: File, Position, Binlog_Do_DB, Binlog_Ignore_DB, Executed_Gtid_Set
	cols, err := rows.Columns()
	if err != nil {
		return mysqldriver.Position{}, err
	}

	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return mysqldriver.Position{}, err
	}

	file := vals[0].String
	pos, _ := strconv.ParseUint(vals[1].String, 10, 32)

	return mysqldriver.Position{Name: file, Pos: uint32(pos)}, nil
}

// channelName returns the pgcdc channel name for a MySQL table.
func channelName(schema, table string) string {
	return "pgcdc:" + schema + "." + table
}

// rowToMap converts a binlog row ([]any) into a map keyed by column name.
func rowToMap(colNames []string, row []any) map[string]any {
	m := make(map[string]any, len(row))
	for i, v := range row {
		var name string
		if i < len(colNames) {
			name = colNames[i]
		} else {
			name = fmt.Sprintf("col_%d", i)
		}
		m[name] = v
	}
	return m
}

// parseAddr splits host:port. Returns defaults if malformed.
func parseAddr(addr string) (string, string) {
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return addr, "3306"
	}
	return addr[:idx], addr[idx+1:]
}
