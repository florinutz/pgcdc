//go:build integration

package scenarios

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/bus"
	mysqldetector "github.com/florinutz/pgcdc/detector/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"golang.org/x/sync/errgroup"
)

func startMySQL(t *testing.T) (addr, user, password, dsn string) {
	t.Helper()
	ctx := context.Background()

	container, err := tcmysql.Run(ctx, "mysql:8.0",
		tcmysql.WithUsername("root"),
		tcmysql.WithPassword("test"),
		tcmysql.WithDatabase("testdb"),
		testcontainers.WithCmd(
			"--server-id=1",
			"--log-bin=mysql-bin",
			"--binlog-format=ROW",
			"--binlog-row-image=FULL",
			"--gtid-mode=ON",
			"--enforce-gtid-consistency=ON",
		),
	)
	if err != nil {
		t.Fatalf("start mysql container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("get mysql host: %v", err)
	}
	port, err := container.MappedPort(ctx, "3306")
	if err != nil {
		t.Fatalf("get mysql port: %v", err)
	}

	addr = fmt.Sprintf("%s:%s", host, port.Port())
	user = "root"
	password = "test"
	dsn = fmt.Sprintf("%s:%s@tcp(%s)/testdb?parseTime=true", user, password, addr)

	// Wait for MySQL to be ready with a connection test.
	var db *sql.DB
	for i := 0; i < 30; i++ {
		db, err = sql.Open("mysql", dsn)
		if err == nil {
			if err = db.Ping(); err == nil {
				db.Close()
				return
			}
			db.Close()
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("mysql not ready after 30s: %v", err)
	return
}

func mysqlExec(t *testing.T, dsn, query string, args ...any) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("mysqlExec open: %v", err)
	}
	defer db.Close()
	_, err = db.Exec(query, args...)
	if err != nil {
		t.Fatalf("mysqlExec %q: %v", query, err)
	}
}

func startMySQLPipeline(t *testing.T, addr, user, password string, tables []string, adapters ...adapter.Adapter) {
	t.Helper()

	logger := testLogger()
	ctx, cancel := context.WithCancel(context.Background())

	det := mysqldetector.New(mysqldetector.Config{
		Addr:         addr,
		User:         user,
		Password:     password,
		ServerID:     100,
		Tables:       tables,
		Flavor:       "mysql",
		BinlogPrefix: "mysql-bin",
	}, logger)
	b := bus.New(64, logger)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error { return b.Start(gCtx) })
	g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

	for _, a := range adapters {
		sub, err := b.Subscribe(a.Name())
		if err != nil {
			cancel()
			t.Fatalf("subscribe %s: %v", a.Name(), err)
		}
		a := a
		g.Go(func() error { return a.Start(gCtx, sub) })
	}

	t.Cleanup(func() {
		cancel()
		g.Wait()
	})
}

func TestScenario_MySQL(t *testing.T) {
	t.Parallel()
	if os.Getenv("PGCDC_TEST_MYSQL") == "" && os.Getenv("CI") == "" {
		t.Skip("set PGCDC_TEST_MYSQL=1 or CI=1 to run MySQL scenario (requires Docker with MySQL image)")
	}

	t.Run("happy path", func(t *testing.T) {
		addr, user, password, dsn := startMySQL(t)

		// Create test table.
		mysqlExec(t, dsn, `CREATE TABLE users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255)
		)`)

		// Wait a moment for binlog to settle.
		time.Sleep(500 * time.Millisecond)

		// Wire detector → bus → stdout.
		capture := newLineCapture()
		stdoutAdapter := stdout.New(capture, testLogger())

		startMySQLPipeline(t, addr, user, password, []string{"testdb.users"}, stdoutAdapter)

		// Give detector time to connect and start replicating.
		time.Sleep(2 * time.Second)

		// INSERT
		mysqlExec(t, dsn, `INSERT INTO users (name, email) VALUES (?, ?)`, "alice", "alice@example.com")

		line := capture.waitLine(t, 10*time.Second)
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		if ev["source"] != "mysql_binlog" {
			t.Errorf("source: got %v, want mysql_binlog", ev["source"])
		}
		if ev["channel"] != "pgcdc:testdb.users" {
			t.Errorf("channel: got %v, want pgcdc:testdb.users", ev["channel"])
		}
		if ev["operation"] != "INSERT" {
			t.Errorf("operation: got %v, want INSERT", ev["operation"])
		}

		var payload map[string]any
		payloadBytes, _ := json.Marshal(ev["payload"])
		json.Unmarshal(payloadBytes, &payload)

		if payload["op"] != "INSERT" {
			t.Errorf("payload.op: got %v, want INSERT", payload["op"])
		}
		if payload["table"] != "users" {
			t.Errorf("payload.table: got %v, want users", payload["table"])
		}

		// UPDATE
		mysqlExec(t, dsn, `UPDATE users SET email = ? WHERE name = ?`, "newalice@example.com", "alice")

		line = capture.waitLine(t, 10*time.Second)
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal update event: %v", err)
		}
		if ev["operation"] != "UPDATE" {
			t.Errorf("update operation: got %v, want UPDATE", ev["operation"])
		}
		payloadBytes, _ = json.Marshal(ev["payload"])
		json.Unmarshal(payloadBytes, &payload)
		if payload["old"] == nil {
			t.Error("update payload.old is nil, expected old row")
		}

		// DELETE
		mysqlExec(t, dsn, `DELETE FROM users WHERE name = ?`, "alice")

		line = capture.waitLine(t, 10*time.Second)
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal delete event: %v", err)
		}
		if ev["operation"] != "DELETE" {
			t.Errorf("delete operation: got %v, want DELETE", ev["operation"])
		}
	})

	t.Run("reconnect after disconnect", func(t *testing.T) {
		addr, user, password, dsn := startMySQL(t)

		// Create test table.
		mysqlExec(t, dsn, `CREATE TABLE reconnect_test (
			id INT AUTO_INCREMENT PRIMARY KEY,
			value VARCHAR(255) NOT NULL
		)`)

		time.Sleep(500 * time.Millisecond)

		capture := newLineCapture()
		stdoutAdapter := stdout.New(capture, testLogger())

		startMySQLPipeline(t, addr, user, password, []string{"testdb.reconnect_test"}, stdoutAdapter)

		// Wait for connection.
		time.Sleep(2 * time.Second)

		// Insert first row.
		mysqlExec(t, dsn, `INSERT INTO reconnect_test (value) VALUES (?)`, "before")
		line := capture.waitLine(t, 10*time.Second)
		var ev map[string]any
		json.Unmarshal([]byte(line), &ev)
		if ev["operation"] != "INSERT" {
			t.Fatalf("first event op: got %v, want INSERT", ev["operation"])
		}

		// Kill the replication connection.
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("open for kill: %v", err)
		}
		rows, err := db.Query("SHOW PROCESSLIST")
		if err != nil {
			t.Fatalf("show processlist: %v", err)
		}
		for rows.Next() {
			var id int64
			var user, host, dbName, command sql.NullString
			var timeVal sql.NullInt64
			var state, info sql.NullString
			rows.Scan(&id, &user, &host, &dbName, &command, &timeVal, &state, &info)
			if command.String == "Binlog Dump" || command.String == "Binlog Dump GTID" {
				db.Exec(fmt.Sprintf("KILL %d", id))
			}
		}
		rows.Close()
		db.Close()

		// Wait for reconnect.
		time.Sleep(3 * time.Second)

		// Insert second row — should arrive after reconnect.
		mysqlExec(t, dsn, `INSERT INTO reconnect_test (value) VALUES (?)`, "after")
		line = capture.waitLine(t, 15*time.Second)
		json.Unmarshal([]byte(line), &ev)
		if ev["operation"] != "INSERT" {
			t.Errorf("reconnect event op: got %v, want INSERT", ev["operation"])
		}
	})
}
