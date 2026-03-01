//go:build integration

package scenarios

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	execadapter "github.com/florinutz/pgcdc/adapter/exec"
	"github.com/florinutz/pgcdc/event"
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

		// Wait for detector to connect by sending probes until file has content.
		waitFor(t, 10*time.Second, func() bool {
			sendNotify(t, connStr, "exec_test_happy", `{"__probe":true}`)
			time.Sleep(100 * time.Millisecond)
			data, err := os.ReadFile(outPath)
			return err == nil && len(data) > 0
		})

		sendNotify(t, connStr, "exec_test_happy", `{"action":"buy"}`)

		var lines []string
		waitFor(t, 5*time.Second, func() bool {
			data, err := os.ReadFile(outPath)
			if err != nil || len(data) == 0 {
				return false
			}
			lines = nil
			for _, line := range splitLines(data) {
				if len(line) > 0 {
					lines = append(lines, string(line))
				}
			}
			return len(lines) >= 2
		})

		if len(lines) < 2 {
			t.Fatal("no events written by exec process")
		}

		// Last line should be the real event.
		lastLine := lines[len(lines)-1]
		var ev event.Event
		if err := json.Unmarshal([]byte(lastLine), &ev); err != nil {
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

		// Wait for detector to connect by sending probes until file has content.
		waitFor(t, 10*time.Second, func() bool {
			sendNotify(t, connStr, "exec_test_restart", `{"__probe":true}`)
			time.Sleep(100 * time.Millisecond)
			data, err := os.ReadFile(outPath)
			return err == nil && len(data) > 0
		})

		// Send second event — should be handled by restarted process.
		sendNotify(t, connStr, "exec_test_restart", `{"n":2}`)

		// Wait for restart + write.
		var lines []string
		waitFor(t, 10*time.Second, func() bool {
			data, err := os.ReadFile(outPath)
			if err != nil || len(data) == 0 {
				return false
			}
			lines = nil
			for _, line := range splitLines(data) {
				if len(line) > 0 {
					lines = append(lines, string(line))
				}
			}
			return len(lines) >= 2
		})

		if len(lines) < 2 {
			t.Fatalf("expected at least 2 lines after restart, got %d", len(lines))
		}
	})
}
