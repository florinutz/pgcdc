//go:build !no_s3

package cmd

import (
	s3adapter "github.com/florinutz/pgcdc/adapter/s3"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterAdapter(registry.AdapterEntry{
		Name:        "s3",
		Description: "S3-compatible object storage (JSON Lines / Parquet)",
		Create: func(ctx registry.AdapterContext) (registry.AdapterResult, error) {
			cfg := ctx.Cfg
			return registry.AdapterResult{
				Adapter: s3adapter.New(
					cfg.S3.Bucket,
					cfg.S3.Prefix,
					cfg.S3.Endpoint,
					cfg.S3.Region,
					cfg.S3.AccessKeyID,
					cfg.S3.SecretAccessKey,
					cfg.S3.Format,
					cfg.S3.FlushInterval,
					cfg.S3.FlushSize,
					cfg.S3.DrainTimeout,
					cfg.S3.BackoffBase,
					cfg.S3.BackoffCap,
					ctx.Logger,
				),
			}, nil
		},
		ViperKeys: [][2]string{
			{"s3-bucket", "s3.bucket"},
			{"s3-prefix", "s3.prefix"},
			{"s3-endpoint", "s3.endpoint"},
			{"s3-region", "s3.region"},
			{"s3-access-key-id", "s3.access_key_id"},
			{"s3-secret-access-key", "s3.secret_access_key"},
			{"s3-format", "s3.format"},
			{"s3-flush-interval", "s3.flush_interval"},
			{"s3-flush-size", "s3.flush_size"},
			{"s3-drain-timeout", "s3.drain_timeout"},
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "s3-bucket",
				Type:        "string",
				Required:    true,
				Description: "S3 bucket name",
			},
			{
				Name:        "s3-prefix",
				Type:        "string",
				Description: "S3 object key prefix",
			},
			{
				Name:        "s3-endpoint",
				Type:        "string",
				Description: "S3-compatible endpoint URL (e.g. MinIO, R2)",
			},
			{
				Name:        "s3-region",
				Type:        "string",
				Default:     "us-east-1",
				Description: "S3 region",
			},
			{
				Name:        "s3-access-key-id",
				Type:        "string",
				Description: "S3 access key ID (default: AWS default chain)",
			},
			{
				Name:        "s3-secret-access-key",
				Type:        "string",
				Description: "S3 secret access key (default: AWS default chain)",
			},
			{
				Name:        "s3-format",
				Type:        "string",
				Default:     "jsonl",
				Description: "S3 output format",
				Validations: []string{"oneof:jsonl,parquet"},
			},
			{
				Name:        "s3-flush-interval",
				Type:        "duration",
				Default:     "1m",
				Description: "S3 flush interval",
			},
			{
				Name:        "s3-flush-size",
				Type:        "int",
				Default:     10000,
				Description: "S3 flush size in events",
				Validations: []string{"min:1"},
			},
			{
				Name:        "s3-drain-timeout",
				Type:        "duration",
				Default:     "30s",
				Description: "S3 shutdown drain timeout",
			},
		},
	})

	// S3 adapter flags.
	f := listenCmd.Flags()
	f.String("s3-bucket", "", "S3 bucket name")
	f.String("s3-prefix", "", "S3 object key prefix")
	f.String("s3-endpoint", "", "S3-compatible endpoint URL (e.g. MinIO, R2)")
	f.String("s3-region", "us-east-1", "S3 region")
	f.String("s3-access-key-id", "", "S3 access key ID (default: AWS default chain)")
	f.String("s3-secret-access-key", "", "S3 secret access key (default: AWS default chain)")
	f.String("s3-format", "jsonl", "S3 output format: jsonl or parquet")
	f.Duration("s3-flush-interval", 0, "S3 flush interval (default 1m)")
	f.Int("s3-flush-size", 0, "S3 flush size in events (default 10000)")
	f.Duration("s3-drain-timeout", 0, "S3 shutdown drain timeout (default 30s)")
}
