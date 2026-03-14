//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	redisadapter "github.com/florinutz/pgcdc/adapter/redis"
	"github.com/florinutz/pgcdc/bus"
	"github.com/florinutz/pgcdc/detector/listennotify"
	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"
)

func TestScenario_Redis(t *testing.T) {
	t.Parallel()
	connStr := startPostgres(t)

	t.Run("happy path", func(t *testing.T) {
		ctx := context.Background()

		// Start a Redis container.
		req := testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections"),
		}
		redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Fatalf("start redis container: %v", err)
		}
		t.Cleanup(func() { _ = redisContainer.Terminate(context.Background()) })

		host, err := redisContainer.Host(ctx)
		if err != nil {
			t.Fatalf("get redis host: %v", err)
		}
		port, err := redisContainer.MappedPort(ctx, "6379")
		if err != nil {
			t.Fatalf("get redis mapped port: %v", err)
		}
		redisURL := fmt.Sprintf("redis://%s:%s", host, port.Port())

		// Create the Redis adapter in invalidate mode.
		logger := testLogger()
		a := redisadapter.New(redisURL, "invalidate", "test:", "id", 0, 0, logger)

		// Wire pipeline: LISTEN/NOTIFY detector -> bus -> Redis adapter.
		pipelineCtx, pipelineCancel := context.WithCancel(context.Background())
		channel := "redis_test"
		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		b := bus.New(64, logger)

		g, gCtx := errgroup.WithContext(pipelineCtx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		sub, err := b.Subscribe(a.Name())
		if err != nil {
			pipelineCancel()
			t.Fatalf("subscribe redis: %v", err)
		}
		g.Go(func() error { return a.Start(gCtx, sub) })

		t.Cleanup(func() {
			pipelineCancel()
			g.Wait()
		})

		// Wait for pipeline to connect.
		time.Sleep(2 * time.Second)

		// Pre-populate a key in Redis to test invalidation.
		rdb := goredis.NewClient(&goredis.Options{
			Addr: fmt.Sprintf("%s:%s", host, port.Port()),
		})
		defer rdb.Close()

		if err := rdb.Set(ctx, "test:42", `{"cached":"data"}`, 0).Err(); err != nil {
			t.Fatalf("pre-populate redis key: %v", err)
		}

		// Verify the key exists.
		val, err := rdb.Get(ctx, "test:42").Result()
		if err != nil {
			t.Fatalf("get pre-populated key: %v", err)
		}
		if val != `{"cached":"data"}` {
			t.Fatalf("unexpected pre-populated value: %s", val)
		}

		// Send an INSERT event — in invalidate mode, this should DEL the key.
		payload := `{"op":"INSERT","table":"items","row":{"id":"42","name":"widget"}}`
		sendNotify(t, connStr, channel, payload)

		// Wait for the Redis adapter to process the invalidation.
		waitFor(t, 10*time.Second, func() bool {
			_, getErr := rdb.Get(ctx, "test:42").Result()
			return getErr == goredis.Nil
		})

		// Also test that a DELETE event works the same way — first set the key again.
		if err := rdb.Set(ctx, "test:42", `{"cached":"again"}`, 0).Err(); err != nil {
			t.Fatalf("re-populate redis key: %v", err)
		}

		deletePayload := `{"op":"DELETE","table":"items","row":{"id":"42","name":"widget"}}`
		sendNotify(t, connStr, channel, deletePayload)

		// Wait for the Redis adapter to process the DELETE invalidation.
		waitFor(t, 10*time.Second, func() bool {
			_, getErr := rdb.Get(ctx, "test:42").Result()
			return getErr == goredis.Nil
		})
	})

	t.Run("sync mode", func(t *testing.T) {
		ctx := context.Background()

		// Start a Redis container.
		req := testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections"),
		}
		redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Fatalf("start redis container: %v", err)
		}
		t.Cleanup(func() { _ = redisContainer.Terminate(context.Background()) })

		host, err := redisContainer.Host(ctx)
		if err != nil {
			t.Fatalf("get redis host: %v", err)
		}
		port, err := redisContainer.MappedPort(ctx, "6379")
		if err != nil {
			t.Fatalf("get redis mapped port: %v", err)
		}
		redisURL := fmt.Sprintf("redis://%s:%s", host, port.Port())

		// Create the Redis adapter in sync mode.
		logger := testLogger()
		a := redisadapter.New(redisURL, "sync", "sync:", "id", 0, 0, logger)

		// Wire pipeline.
		pipelineCtx, pipelineCancel := context.WithCancel(context.Background())
		channel := "redis_sync_test"
		det := listennotify.New(connStr, []string{channel}, 0, 0, logger)
		b := bus.New(64, logger)

		g, gCtx := errgroup.WithContext(pipelineCtx)
		g.Go(func() error { return b.Start(gCtx) })
		g.Go(func() error { return det.Start(gCtx, b.Ingest()) })

		sub, err := b.Subscribe(a.Name())
		if err != nil {
			pipelineCancel()
			t.Fatalf("subscribe redis: %v", err)
		}
		g.Go(func() error { return a.Start(gCtx, sub) })

		t.Cleanup(func() {
			pipelineCancel()
			g.Wait()
		})

		time.Sleep(2 * time.Second)

		rdb := goredis.NewClient(&goredis.Options{
			Addr: fmt.Sprintf("%s:%s", host, port.Port()),
		})
		defer rdb.Close()

		// Send an INSERT — sync mode should SET the key with row JSON.
		payload := `{"op":"INSERT","table":"items","row":{"id":"99","name":"gadget","price":19.99}}`
		sendNotify(t, connStr, channel, payload)

		// Wait for the key to appear in Redis.
		var val string
		waitFor(t, 10*time.Second, func() bool {
			var getErr error
			val, getErr = rdb.Get(ctx, "sync:99").Result()
			return getErr == nil
		})

		var row map[string]interface{}
		if err := json.Unmarshal([]byte(val), &row); err != nil {
			t.Fatalf("unmarshal cached row: %v", err)
		}
		if row["name"] != "gadget" {
			t.Errorf("cached name = %v, want %q", row["name"], "gadget")
		}

		// Send a DELETE — sync mode should DEL the key.
		deletePayload := `{"op":"DELETE","table":"items","row":{"id":"99","name":"gadget","price":19.99}}`
		sendNotify(t, connStr, channel, deletePayload)

		// Wait for the key to be deleted.
		waitFor(t, 10*time.Second, func() bool {
			_, getErr := rdb.Get(ctx, "sync:99").Result()
			return getErr == goredis.Nil
		})
	})
}
