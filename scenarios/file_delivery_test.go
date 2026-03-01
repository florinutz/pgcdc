//go:build integration

package scenarios

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	fileadapter "github.com/florinutz/pgcdc/adapter/file"
	"github.com/florinutz/pgcdc/event"
)

func TestScenario_FileDelivery(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		tmpDir := t.TempDir()
		outPath := filepath.Join(tmpDir, "events.jsonl")

		fa := fileadapter.New(outPath, 0, 0, testLogger())
		startPipeline(t, connStr, []string{"file_test_happy"}, fa)

		// Wait for detector to connect by sending probes until the file has content.
		waitFor(t, 10*time.Second, func() bool {
			sendNotify(t, connStr, "file_test_happy", `{"__probe":true}`)
			time.Sleep(100 * time.Millisecond)
			data, err := os.ReadFile(outPath)
			return err == nil && len(data) > 0
		})

		sendNotify(t, connStr, "file_test_happy", `{"item":"widget"}`)

		// Wait for the real event to be written.
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
			// At least 2 lines: probe + real event.
			return len(lines) >= 2
		})

		// The last line should be our real event.
		lastLine := lines[len(lines)-1]
		var ev event.Event
		if err := json.Unmarshal([]byte(lastLine), &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		if ev.Channel != "file_test_happy" {
			t.Errorf("channel = %q, want file_test_happy", ev.Channel)
		}
	})

	t.Run("rotation", func(t *testing.T) {
		tmpDir := t.TempDir()
		outPath := filepath.Join(tmpDir, "events.jsonl")

		// Use tiny maxSize to force rotation quickly.
		fa := fileadapter.New(outPath, 100, 3, testLogger())
		startPipeline(t, connStr, []string{"file_test_rotate"}, fa)

		// Wait for detector to connect.
		waitFor(t, 10*time.Second, func() bool {
			sendNotify(t, connStr, "file_test_rotate", `{"__probe":true}`)
			time.Sleep(100 * time.Millisecond)
			data, err := os.ReadFile(outPath)
			return err == nil && len(data) > 0
		})

		// Send enough events to trigger rotation.
		for i := range 10 {
			sendNotify(t, connStr, "file_test_rotate", `{"n":`+itoa(i)+`}`)
			time.Sleep(50 * time.Millisecond)
		}

		// Wait for rotation to happen.
		rotated := outPath + ".1"
		waitFor(t, 5*time.Second, func() bool {
			_, err := os.Stat(rotated)
			return err == nil
		})
	})
}

func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}

func itoa(i int) string {
	return []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}[i]
}
