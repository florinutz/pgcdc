//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/florinutz/pgcdc/adapter/graphql"
	"github.com/go-chi/chi/v5"
)

func TestScenario_GraphQL(t *testing.T) {
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		table := "gql_orders"
		channel := createTrigger(t, connStr, table)

		adapter := graphql.New("/graphql", false, 256, 0, nil, testLogger())

		r := chi.NewRouter()
		adapter.MountHTTP(r)
		srv := httptest.NewServer(r)
		defer srv.Close()

		startPipeline(t, connStr, []string{channel}, adapter)

		// Connect via WebSocket with graphql-transport-ws subprotocol.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		conn, _, err := websocket.Dial(ctx, srv.URL+"/graphql", &websocket.DialOptions{
			Subprotocols: []string{graphql.Subprotocol},
		})
		if err != nil {
			t.Fatalf("websocket dial: %v", err)
		}
		defer conn.CloseNow()

		// connection_init → connection_ack
		gqlWrite(t, conn, ctx, map[string]any{"type": "connection_init"})
		ack := gqlRead(t, conn, ctx)
		if ack["type"] != "connection_ack" {
			t.Fatalf("expected connection_ack, got %v", ack["type"])
		}

		// subscribe with channel filter
		gqlWrite(t, conn, ctx, map[string]any{
			"id":   "1",
			"type": "subscribe",
			"payload": map[string]any{
				"query":     "subscription { events { id channel operation payload } }",
				"variables": map[string]any{"channel": channel},
			},
		})

		// Wait for detector, then insert a row.
		time.Sleep(3 * time.Second)
		insertRow(t, connStr, table, map[string]any{"item": "gql-widget"})

		// Read next message with event data.
		// The detector might need a retry if it wasn't ready for the first NOTIFY.
		var next map[string]any
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
			_, data, err := conn.Read(readCtx)
			readCancel()
			if err != nil {
				// Timeout — retry insert.
				insertRow(t, connStr, table, map[string]any{"item": "gql-retry"})
				continue
			}
			var msg map[string]json.RawMessage
			json.Unmarshal(data, &msg)
			var msgType string
			json.Unmarshal(msg["type"], &msgType)
			if msgType == "next" {
				json.Unmarshal(data, &next)
				break
			}
			// Skip pings, etc.
		}
		if next == nil {
			t.Fatal("timed out waiting for next message")
		}

		if next["id"] != "1" {
			t.Errorf("next.id = %v, want 1", next["id"])
		}

		// Parse payload.data.events
		payloadRaw, _ := json.Marshal(next["payload"])
		var payload map[string]any
		json.Unmarshal(payloadRaw, &payload)
		data, _ := payload["data"].(map[string]any)
		events, _ := data["events"].(map[string]any)

		if events["channel"] != channel {
			t.Errorf("event.channel = %v, want %s", events["channel"], channel)
		}
		if events["operation"] != "INSERT" {
			t.Errorf("event.operation = %v, want INSERT", events["operation"])
		}
		if events["id"] == nil || events["id"] == "" {
			t.Error("event.id is empty")
		}

		conn.Close(websocket.StatusNormalClosure, "done")
	})

	t.Run("channel filter excludes non-matching events", func(t *testing.T) {
		table1 := "gql_filter_orders"
		table2 := "gql_filter_users"
		channel1 := createTrigger(t, connStr, table1)
		channel2 := createTrigger(t, connStr, table2)

		adapter := graphql.New("/graphql", false, 256, 0, nil, testLogger())

		r := chi.NewRouter()
		adapter.MountHTTP(r)
		srv := httptest.NewServer(r)
		defer srv.Close()

		startPipeline(t, connStr, []string{channel1, channel2}, adapter)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		conn, _, err := websocket.Dial(ctx, srv.URL+"/graphql", &websocket.DialOptions{
			Subprotocols: []string{graphql.Subprotocol},
		})
		if err != nil {
			t.Fatalf("websocket dial: %v", err)
		}
		defer conn.CloseNow()

		gqlWrite(t, conn, ctx, map[string]any{"type": "connection_init"})
		_ = gqlRead(t, conn, ctx) // ack

		// Subscribe only to channel1.
		gqlWrite(t, conn, ctx, map[string]any{
			"id":   "1",
			"type": "subscribe",
			"payload": map[string]any{
				"query":     "subscription { events { channel } }",
				"variables": map[string]any{"channel": channel1},
			},
		})

		time.Sleep(3 * time.Second)

		// Insert into channel2 (should not match), then channel1 (should match).
		insertRow(t, connStr, table2, map[string]any{"name": "bob"})
		time.Sleep(500 * time.Millisecond)
		insertRow(t, connStr, table1, map[string]any{"item": "filtered"})

		// The first "next" we receive should be for channel1.
		var received map[string]any
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
			_, data, err := conn.Read(readCtx)
			readCancel()
			if err != nil {
				insertRow(t, connStr, table1, map[string]any{"item": "retry"})
				continue
			}
			var msg map[string]json.RawMessage
			json.Unmarshal(data, &msg)
			var msgType string
			json.Unmarshal(msg["type"], &msgType)
			if msgType == "next" {
				json.Unmarshal(data, &received)
				break
			}
		}
		if received == nil {
			t.Fatal("timed out waiting for filtered event")
		}

		payloadRaw, _ := json.Marshal(received["payload"])
		var payload map[string]any
		json.Unmarshal(payloadRaw, &payload)
		data2, _ := payload["data"].(map[string]any)
		events, _ := data2["events"].(map[string]any)

		if events["channel"] != channel1 {
			t.Errorf("filtered event channel = %v, want %s (not %s)", events["channel"], channel1, channel2)
		}

		conn.Close(websocket.StatusNormalClosure, "done")
	})
}

func gqlWrite(t *testing.T, conn *websocket.Conn, ctx context.Context, msg map[string]any) {
	t.Helper()
	data, _ := json.Marshal(msg)
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("gqlWrite: %v", err)
	}
}

func gqlRead(t *testing.T, conn *websocket.Conn, ctx context.Context) map[string]any {
	t.Helper()
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, data, err := conn.Read(readCtx)
	if err != nil {
		t.Fatalf("gqlRead: %v", err)
	}
	var msg map[string]any
	json.Unmarshal(data, &msg)
	return msg
}
