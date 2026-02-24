//go:build integration

package scenarios

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	searchadapter "github.com/florinutz/pgcdc/adapter/search"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/listennotify"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"
)

func TestScenario_Search(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		ctx := context.Background()

		// Start a Typesense container.
		req := testcontainers.ContainerRequest{
			Image:        "typesense/typesense:27.1",
			ExposedPorts: []string{"8108/tcp"},
			Env: map[string]string{
				"TYPESENSE_API_KEY":  "xyz",
				"TYPESENSE_DATA_DIR": "/data",
			},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Tmpfs = map[string]string{"/data": "rw"}
			},
			WaitingFor: wait.ForHTTP("/health").WithPort("8108").WithStatusCodeMatcher(func(status int) bool {
				return status == 200
			}),
		}
		tsContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Fatalf("start typesense container: %v", err)
		}
		t.Cleanup(func() { _ = tsContainer.Terminate(context.Background()) })

		host, err := tsContainer.Host(ctx)
		if err != nil {
			t.Fatalf("get typesense host: %v", err)
		}
		port, err := tsContainer.MappedPort(ctx, "8108")
		if err != nil {
			t.Fatalf("get typesense mapped port: %v", err)
		}
		tsURL := fmt.Sprintf("http://%s:%s", host, port.Port())
		apiKey := "xyz"
		indexName := "search_test"

		// Create a Typesense collection (schema).
		createCollection(t, tsURL, apiKey, indexName)

		// Create the search adapter.
		logger := testLogger()
		a := searchadapter.New(
			"typesense", tsURL, apiKey, indexName, "id",
			1,                    // batchSize=1 for immediate flush
			100*time.Millisecond, // short batch interval
			0, 0,
			logger,
		)

		// Wire pipeline: LISTEN/NOTIFY detector -> bus -> search adapter.
		pipelineCtx, pipelineCancel := context.WithCancel(context.Background())
		channel := "search_test"
		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		b := bus.New(64, logger)

		g, gCtx := errgroup.WithContext(pipelineCtx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		sub, err := b.Subscribe(a.Name())
		if err != nil {
			pipelineCancel()
			t.Fatalf("subscribe search: %v", err)
		}
		g.Go(func() error { return a.Start(gCtx, sub) })

		t.Cleanup(func() {
			pipelineCancel()
			g.Wait()
		})

		// Wait for pipeline to be ready.
		time.Sleep(2 * time.Second)

		// Send an INSERT event.
		payload := `{"op":"INSERT","table":"articles","row":{"id":"42","title":"Hello World","body":"Test body"}}`
		sendNotify(t, connStr, channel, payload)

		// Wait for the search adapter to flush.
		time.Sleep(2 * time.Second)

		// Verify document exists in Typesense.
		doc := getTypesenseDocument(t, tsURL, apiKey, indexName, "42")
		if doc["title"] != "Hello World" {
			t.Errorf("title = %v, want %q", doc["title"], "Hello World")
		}

		// Send a DELETE event.
		deletePayload := `{"op":"DELETE","table":"articles","row":{"id":"42","title":"Hello World","body":"Test body"}}`
		sendNotify(t, connStr, channel, deletePayload)

		// Wait for the delete to process.
		time.Sleep(2 * time.Second)

		// Verify document is removed.
		if docExists(t, tsURL, apiKey, indexName, "42") {
			t.Error("document should have been deleted from Typesense")
		}
	})
}

// createCollection creates a Typesense collection for testing.
func createCollection(t *testing.T, tsURL, apiKey, name string) {
	t.Helper()

	schema := map[string]interface{}{
		"name": name,
		"fields": []map[string]interface{}{
			{"name": "id", "type": "string"},
			{"name": "title", "type": "string"},
			{"name": "body", "type": "string"},
		},
	}
	body, _ := json.Marshal(schema)
	req, _ := http.NewRequest(http.MethodPost, tsURL+"/collections", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-TYPESENSE-API-KEY", apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create typesense collection: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("create collection returned %d: %s", resp.StatusCode, respBody)
	}
}

// getTypesenseDocument retrieves a document from Typesense by ID.
func getTypesenseDocument(t *testing.T, tsURL, apiKey, collection, id string) map[string]interface{} {
	t.Helper()

	url := fmt.Sprintf("%s/collections/%s/documents/%s", tsURL, collection, id)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-TYPESENSE-API-KEY", apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get typesense document: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("get document returned %d: %s", resp.StatusCode, respBody)
	}

	var doc map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatalf("decode document: %v", err)
	}
	return doc
}

// docExists checks if a document exists in Typesense.
func docExists(t *testing.T, tsURL, apiKey, collection, id string) bool {
	t.Helper()

	url := fmt.Sprintf("%s/collections/%s/documents/%s", tsURL, collection, id)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-TYPESENSE-API-KEY", apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("check typesense document: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == 200
}
