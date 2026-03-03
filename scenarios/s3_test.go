//go:build integration

package scenarios

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3adapter "github.com/florinutz/pgcdc/adapter/s3"
	"github.com/parquet-go/parquet-go"
	minitc "github.com/testcontainers/testcontainers-go/modules/minio"
)

// startMinIO starts a MinIO container and returns the endpoint URL.
func startMinIO(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	mc, err := minitc.Run(ctx, "minio/minio:latest")
	if err != nil {
		t.Fatalf("start minio container: %v", err)
	}
	t.Cleanup(func() { _ = mc.Terminate(context.Background()) })

	url, err := mc.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("get minio endpoint: %v", err)
	}
	return "http://" + url
}

// newMinIOClient creates an S3 client pointed at the MinIO endpoint.
func newMinIOClient(t *testing.T, endpoint string) *s3.Client {
	t.Helper()
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
		),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})
}

// createBucket creates a bucket in MinIO.
func createBucket(t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	ctx := context.Background()
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		t.Fatalf("create bucket %s: %v", bucket, err)
	}
}

// listObjects lists all objects in a bucket with the given prefix.
func listObjects(t *testing.T, client *s3.Client, bucket, prefix string) []string {
	t.Helper()
	ctx := context.Background()

	out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}

	var keys []string
	for _, obj := range out.Contents {
		keys = append(keys, *obj.Key)
	}
	return keys
}

// getObject downloads an object from MinIO.
func getObject(t *testing.T, client *s3.Client, bucket, key string) []byte {
	t.Helper()
	ctx := context.Background()

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("get object %s: %v", key, err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read object %s: %v", key, err)
	}
	return data
}

func TestScenario_S3(t *testing.T) {
	t.Parallel()
	connStr := startPostgres(t)
	channel := createTrigger(t, connStr, "s3_events")
	endpoint := startMinIO(t)
	minioClient := newMinIOClient(t, endpoint)

	t.Run("happy path jsonl", func(t *testing.T) {
		bucket := "test-jsonl"
		createBucket(t, minioClient, bucket)

		a := s3adapter.New(
			bucket,
			"cdc/",
			endpoint,
			"us-east-1",
			"minioadmin",
			"minioadmin",
			"jsonl",
			1*time.Second, // flush interval (fast for test)
			100,           // flush size
			30*time.Second,
			0, 0,
			testLogger(),
		)

		startPipeline(t, connStr, []string{channel}, a)

		// Insert 3 rows. Some may be lost if the detector isn't connected
		// yet, so we'll poll S3 and retry inserts if needed.
		for i := range 3 {
			insertRow(t, connStr, "s3_events", map[string]any{"item": i + 1})
		}

		// Poll S3 until at least 3 JSON lines are present across all objects.
		var totalLines int
		waitFor(t, 15*time.Second, func() bool {
			totalLines = 0
			keys := listObjects(t, minioClient, bucket, "cdc/")
			for _, key := range keys {
				if strings.Contains(key, "channel=pgcdc_s3_events") && strings.Contains(key, ".jsonl") {
					data := getObject(t, minioClient, bucket, key)
					scanner := bufio.NewScanner(bytes.NewReader(data))
					for scanner.Scan() {
						var ev map[string]any
						if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
							return false
						}
						if _, ok := ev["id"]; !ok {
							return false
						}
						if _, ok := ev["channel"]; !ok {
							return false
						}
						totalLines++
					}
				}
			}
			if totalLines < 3 {
				// Re-send events in case detector wasn't ready yet.
				for i := range 3 {
					insertRow(t, connStr, "s3_events", map[string]any{"item": i + 1})
				}
			}
			return totalLines >= 3
		})

		if totalLines < 3 {
			t.Fatalf("expected at least 3 JSON lines, got %d", totalLines)
		}
	})

	t.Run("parquet format", func(t *testing.T) {
		bucket := "test-parquet"
		createBucket(t, minioClient, bucket)

		a := s3adapter.New(
			bucket,
			"cdc/",
			endpoint,
			"us-east-1",
			"minioadmin",
			"minioadmin",
			"parquet",
			1*time.Second,
			100,
			30*time.Second,
			0, 0,
			testLogger(),
		)

		startPipeline(t, connStr, []string{channel}, a)

		// Insert 3 rows.
		for i := range 3 {
			insertRow(t, connStr, "s3_events", map[string]any{"item": i + 1})
		}

		// Poll S3 until at least 3 parquet rows are present across all objects.
		var totalRows int64
		waitFor(t, 15*time.Second, func() bool {
			totalRows = 0
			keys := listObjects(t, minioClient, bucket, "cdc/")
			for _, key := range keys {
				if strings.Contains(key, "channel=pgcdc_s3_events") && strings.Contains(key, ".parquet") {
					data := getObject(t, minioClient, bucket, key)
					pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
					if err != nil {
						return false
					}
					totalRows += pf.NumRows()
				}
			}
			if totalRows < 3 {
				// Re-send events in case detector wasn't ready yet.
				for i := range 3 {
					insertRow(t, connStr, "s3_events", map[string]any{"item": i + 1})
				}
			}
			return totalRows >= 3
		})

		if totalRows < 3 {
			t.Fatalf("expected at least 3 parquet rows, got %d", totalRows)
		}
	})
}
