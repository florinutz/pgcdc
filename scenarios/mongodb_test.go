//go:build integration

package scenarios

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/florinutz/pgcdc/adapter"
	"github.com/florinutz/pgcdc/adapter/stdout"
	"github.com/florinutz/pgcdc/bus"
	mongodbdetector "github.com/florinutz/pgcdc/detector/mongodb"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"
)

func startMongoDB(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := mongodb.Run(ctx, "mongo:7.0", mongodb.WithReplicaSet("rs0"))
	if err != nil {
		t.Fatalf("start mongodb container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("get mongodb connection string: %v", err)
	}

	// Append directConnection=true for single-node replica set in Docker.
	// The replica set member advertises its internal address, so the driver
	// can't discover topology from outside the container.
	if strings.Contains(uri, "?") {
		uri += "&directConnection=true"
	} else {
		uri += "?directConnection=true"
	}

	// Wait for MongoDB replica set to be ready.
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect to mongodb: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	for i := 0; i < 30; i++ {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if err := client.Ping(pingCtx, nil); err == nil {
			cancel()
			return uri
		}
		cancel()
		time.Sleep(time.Second)
	}
	t.Fatal("mongodb not ready after 30s")
	return ""
}

func mongoConnect(t *testing.T, uri string) *mongo.Client {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}

func mongoInsert(t *testing.T, uri, db, coll string, doc bson.M) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := mongoConnect(t, uri)
	_, err := client.Database(db).Collection(coll).InsertOne(ctx, doc)
	if err != nil {
		t.Fatalf("mongoInsert: %v", err)
	}
}

func mongoUpdate(t *testing.T, uri, db, coll string, filter, update bson.M) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := mongoConnect(t, uri)
	_, err := client.Database(db).Collection(coll).UpdateOne(ctx, filter, update)
	if err != nil {
		t.Fatalf("mongoUpdate: %v", err)
	}
}

func mongoDelete(t *testing.T, uri, db, coll string, filter bson.M) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := mongoConnect(t, uri)
	_, err := client.Database(db).Collection(coll).DeleteOne(ctx, filter)
	if err != nil {
		t.Fatalf("mongoDelete: %v", err)
	}
}

func startMongoDBPipeline(t *testing.T, uri, database string, collections []string, adapters ...adapter.Adapter) context.CancelFunc {
	t.Helper()

	logger := testLogger()
	ctx, cancel := context.WithCancel(context.Background())

	det := mongodbdetector.New(uri, "collection", database, collections, "updateLookup", "", "", 0, 0, logger)
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

	return cancel
}

func TestScenario_MongoDB(t *testing.T) {
	if os.Getenv("PGCDC_TEST_MONGODB") == "" && os.Getenv("CI") == "" {
		t.Skip("set PGCDC_TEST_MONGODB=1 or CI=1 to run MongoDB scenario (requires Docker with MongoDB image)")
	}

	t.Run("happy path", func(t *testing.T) {
		uri := startMongoDB(t)

		capture := newLineCapture()
		stdoutAdapter := stdout.New(capture, testLogger())

		startMongoDBPipeline(t, uri, "testdb", []string{"users"}, stdoutAdapter)

		// Give detector time to connect and start watching.
		time.Sleep(3 * time.Second)

		// INSERT
		mongoInsert(t, uri, "testdb", "users", bson.M{"name": "alice", "email": "alice@example.com"})

		line := capture.waitLine(t, 10*time.Second)
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		if ev["source"] != "mongodb_changestream" {
			t.Errorf("source: got %v, want mongodb_changestream", ev["source"])
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
		if payload["collection"] != "users" {
			t.Errorf("payload.collection: got %v, want users", payload["collection"])
		}
		if payload["row"] == nil {
			t.Error("payload.row is nil, expected document")
		}

		// UPDATE
		mongoUpdate(t, uri, "testdb", "users",
			bson.M{"name": "alice"},
			bson.M{"$set": bson.M{"email": "newalice@example.com"}},
		)

		line = capture.waitLine(t, 10*time.Second)
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal update event: %v", err)
		}
		if ev["operation"] != "UPDATE" {
			t.Errorf("update operation: got %v, want UPDATE", ev["operation"])
		}
		payloadBytes, _ = json.Marshal(ev["payload"])
		json.Unmarshal(payloadBytes, &payload)
		if payload["row"] == nil {
			t.Error("update payload.row is nil, expected full document with updateLookup")
		}

		// DELETE
		mongoDelete(t, uri, "testdb", "users", bson.M{"name": "alice"})

		line = capture.waitLine(t, 10*time.Second)
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("unmarshal delete event: %v", err)
		}
		if ev["operation"] != "DELETE" {
			t.Errorf("delete operation: got %v, want DELETE", ev["operation"])
		}
	})

	t.Run("resume after disconnect", func(t *testing.T) {
		uri := startMongoDB(t)

		capture := newLineCapture()
		stdoutAdapter := stdout.New(capture, testLogger())

		cancel := startMongoDBPipeline(t, uri, "testdb", []string{"resume_test"}, stdoutAdapter)

		// Wait for connection.
		time.Sleep(3 * time.Second)

		// Insert first document.
		mongoInsert(t, uri, "testdb", "resume_test", bson.M{"value": "before"})
		line := capture.waitLine(t, 10*time.Second)
		var ev map[string]any
		json.Unmarshal([]byte(line), &ev)
		if ev["operation"] != "INSERT" {
			t.Fatalf("first event op: got %v, want INSERT", ev["operation"])
		}

		// Stop detector.
		cancel()
		time.Sleep(2 * time.Second)

		// Insert while detector is down.
		mongoInsert(t, uri, "testdb", "resume_test", bson.M{"value": "during"})
		time.Sleep(time.Second)

		// Restart pipeline with a new capture.
		capture2 := newLineCapture()
		stdoutAdapter2 := stdout.New(capture2, testLogger())
		startMongoDBPipeline(t, uri, "testdb", []string{"resume_test"}, stdoutAdapter2)

		// Wait for reconnect and resume token pickup.
		time.Sleep(3 * time.Second)

		// Insert another event to ensure stream is alive.
		mongoInsert(t, uri, "testdb", "resume_test", bson.M{"value": "after"})

		// We should get at least the "after" insert (and possibly "during" from resume).
		line = capture2.waitLine(t, 15*time.Second)
		json.Unmarshal([]byte(line), &ev)
		if ev["operation"] != "INSERT" {
			t.Errorf("resumed event op: got %v, want INSERT", ev["operation"])
		}
	})
}
