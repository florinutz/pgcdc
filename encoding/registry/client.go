// Package registry is a Confluent Schema Registry HTTP client for
// registering and retrieving Avro and Protobuf schemas used in
// Kafka/NATS wire format encoding.
//
// This is distinct from the top-level schema package, which tracks
// versioned column definitions detected from WAL replication events.
package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/florinutz/pgcdc/pgcdcerr"
)

// SchemaType identifies the schema format registered with the registry.
type SchemaType string

const (
	SchemaTypeAvro     SchemaType = "AVRO"
	SchemaTypeProtobuf SchemaType = "PROTOBUF"
)

// Client is a Confluent Schema Registry HTTP client.
type Client struct {
	baseURL  string
	username string
	password string
	client   *http.Client

	// In-memory cache: subject+schema → schemaID (avoids re-registration).
	mu    sync.RWMutex
	cache map[string]int // key = subject + "\x00" + schema
}

// New creates a Schema Registry client.
func New(baseURL, username, password string) *Client {
	return &Client{
		baseURL:  baseURL,
		username: username,
		password: password,
		client:   &http.Client{},
		cache:    make(map[string]int),
	}
}

type registerRequest struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
}

type registerResponse struct {
	ID int `json:"id"`
}

// Register registers a schema under the given subject. Returns the schema ID.
// Results are cached: repeated calls with the same subject+schema return the
// cached ID without a network round-trip.
func (c *Client) Register(ctx context.Context, subject, schema string, schemaType SchemaType) (int, error) {
	cacheKey := subject + "\x00" + schema
	c.mu.RLock()
	if id, ok := c.cache[cacheKey]; ok {
		c.mu.RUnlock()
		return id, nil
	}
	c.mu.RUnlock()

	body, err := json.Marshal(registerRequest{
		Schema:     schema,
		SchemaType: string(schemaType),
	})
	if err != nil {
		return 0, fmt.Errorf("marshal register request: %w", err)
	}

	url := fmt.Sprintf("%s/subjects/%s/versions", c.baseURL, subject)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, &pgcdcerr.SchemaRegistryError{Subject: subject, Operation: "register", Err: err}
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, &pgcdcerr.SchemaRegistryError{
			Subject:    subject,
			Operation:  "register",
			StatusCode: resp.StatusCode,
			Err:        fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody)),
		}
	}

	var result registerResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return 0, fmt.Errorf("unmarshal register response: %w", err)
	}

	c.mu.Lock()
	c.cache[cacheKey] = result.ID
	c.mu.Unlock()

	return result.ID, nil
}

type getSchemaResponse struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
	ID         int    `json:"id"`
}

// GetByID retrieves a schema by its global ID.
func (c *Client) GetByID(ctx context.Context, id int) (string, SchemaType, error) {
	url := fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return "", "", &pgcdcerr.SchemaRegistryError{Operation: "get_by_id", Err: err}
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", "", &pgcdcerr.SchemaRegistryError{
			Operation:  "get_by_id",
			StatusCode: resp.StatusCode,
			Err:        fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody)),
		}
	}

	var result getSchemaResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", "", fmt.Errorf("unmarshal schema response: %w", err)
	}

	return result.Schema, SchemaType(result.SchemaType), nil
}
