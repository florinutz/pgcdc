//go:build integration

package scenarios

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	fileadapter "github.com/florinutz/pgpipe/adapter/file"
	"github.com/florinutz/pgpipe/event"
)

func TestScenario_FileDelivery(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		tmpDir := t.TempDir()
		outPath := filepath.Join(tmpDir, "events.jsonl")

		fa := fileadapter.New(outPath, 0, 0, testLogger())
		startPipeline(t, connStr, []string{"file_test_happy"}, fa)

		// Allow detector to connect.
		time.Sleep(500 * time.Millisecond)

		sendNotify(t, connStr, "file_test_happy", `{"item":"widget"}`)

		// Wait for file to be written.
		var lines []string
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			data, err := os.ReadFile(outPath)
			if err == nil && len(data) > 0 {
				// Parse JSON lines.
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
			t.Fatal("no events written to file")
		}

		var ev event.Event
		if err := json.Unmarshal([]byte(lines[0]), &ev); err != nil {
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

		time.Sleep(500 * time.Millisecond)

		// Send enough events to trigger rotation.
		for i := range 10 {
			sendNotify(t, connStr, "file_test_rotate", `{"n":`+itoa(i)+`}`)
			time.Sleep(50 * time.Millisecond)
		}

		// Wait for writes + rotation.
		time.Sleep(2 * time.Second)

		rotated := outPath + ".1"
		if _, err := os.Stat(rotated); os.IsNotExist(err) {
			t.Error("expected rotated file .1 to exist")
		}
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
