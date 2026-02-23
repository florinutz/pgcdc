//go:build integration

package scenarios

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	execadapter "github.com/florinutz/pgpipe/adapter/exec"
	"github.com/florinutz/pgpipe/event"
)

func TestScenario_ExecDelivery(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		tmpDir := t.TempDir()
		outPath := filepath.Join(tmpDir, "exec_out.jsonl")

		// cat appends stdin to file (long-running).
		cmd := "cat >> " + outPath
		ea := execadapter.New(cmd, 0, 0, testLogger())
		startPipeline(t, connStr, []string{"exec_test_happy"}, ea)

		time.Sleep(500 * time.Millisecond)

		sendNotify(t, connStr, "exec_test_happy", `{"action":"buy"}`)

		var lines []string
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			data, err := os.ReadFile(outPath)
			if err == nil && len(data) > 0 {
				for _, line := range splitLines(data) {
					if len(line) > 0 {
						lines = append(lines, string(line))
					}
				}
				if len(lines) > 0 {
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
		}

		if len(lines) == 0 {
			t.Fatal("no events written by exec process")
		}

		var ev event.Event
		if err := json.Unmarshal([]byte(lines[0]), &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		if ev.Channel != "exec_test_happy" {
			t.Errorf("channel = %q, want exec_test_happy", ev.Channel)
		}
	})

	t.Run("restart after exit", func(t *testing.T) {
		tmpDir := t.TempDir()
		outPath := filepath.Join(tmpDir, "exec_restart.jsonl")

		// head -n 1 exits after reading 1 line, forcing a restart.
		cmd := "head -n 1 >> " + outPath
		ea := execadapter.New(cmd, 100*time.Millisecond, 500*time.Millisecond, testLogger())
		startPipeline(t, connStr, []string{"exec_test_restart"}, ea)

		time.Sleep(500 * time.Millisecond)

		// Send first event — consumed by first process.
		sendNotify(t, connStr, "exec_test_restart", `{"n":1}`)
		time.Sleep(1 * time.Second)

		// Send second event — should be handled by restarted process.
		sendNotify(t, connStr, "exec_test_restart", `{"n":2}`)

		// Wait for restart + write.
		deadline := time.Now().Add(10 * time.Second)
		var lines []string
		for time.Now().Before(deadline) {
			data, err := os.ReadFile(outPath)
			if err == nil && len(data) > 0 {
				lines = nil
				for _, line := range splitLines(data) {
					if len(line) > 0 {
						lines = append(lines, string(line))
					}
				}
				if len(lines) >= 2 {
					break
				}
			}
			time.Sleep(200 * time.Millisecond)
		}

		if len(lines) < 2 {
			t.Fatalf("expected at least 2 lines after restart, got %d", len(lines))
		}
	})
}
