package cmd

import (
	"fmt"

	mongodbdetector "github.com/florinutz/pgcdc/detector/mongodb"
	"github.com/florinutz/pgcdc/registry"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "mongodb",
		Description: "MongoDB Change Streams",
		ViperKeys: [][2]string{
			{"mongodb-uri", "mongodb.uri"},
			{"mongodb-scope", "mongodb.scope"},
			{"mongodb-database", "mongodb.database"},
			{"mongodb-collections", "mongodb.collections"},
			{"mongodb-full-document", "mongodb.full_document"},
			{"mongodb-metadata-db", "mongodb.metadata_db"},
			{"mongodb-metadata-coll", "mongodb.metadata_coll"},
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "mongodb-uri",
				Type:        "string",
				Required:    true,
				Description: "MongoDB connection URI",
			},
			{
				Name:        "mongodb-database",
				Type:        "string",
				Description: "MongoDB database name (required unless --mongodb-scope=cluster)",
			},
			{
				Name:        "mongodb-collections",
				Type:        "[]string",
				Description: "MongoDB collections to watch (repeatable)",
			},
			{
				Name:        "mongodb-scope",
				Type:        "string",
				Default:     "collection",
				Description: "MongoDB watch scope: collection, database, or cluster",
				Validations: []string{"oneof:collection,database,cluster"},
			},
			{
				Name:        "mongodb-full-document",
				Type:        "string",
				Default:     "updateLookup",
				Description: "MongoDB fullDocument option: updateLookup, default, whenAvailable, required",
				Validations: []string{"oneof:updateLookup,default,whenAvailable,required"},
			},
			{
				Name:        "mongodb-metadata-db",
				Type:        "string",
				Description: "MongoDB database for resume token storage (default: same as --mongodb-database)",
			},
			{
				Name:        "mongodb-metadata-coll",
				Type:        "string",
				Default:     "pgcdc_resume_tokens",
				Description: "MongoDB collection for resume token storage",
			},
		},
		Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
			cfg := ctx.Cfg
			if cfg.MongoDB.URI == "" {
				return registry.DetectorResult{}, fmt.Errorf("--mongodb-uri is required for MongoDB detector")
			}
			if cfg.MongoDB.Scope != "cluster" && cfg.MongoDB.Database == "" {
				return registry.DetectorResult{}, fmt.Errorf("--mongodb-database is required for MongoDB detector (unless --mongodb-scope=cluster)")
			}
			det := mongodbdetector.New(
				cfg.MongoDB.URI, cfg.MongoDB.Scope, cfg.MongoDB.Database,
				cfg.MongoDB.Collections, cfg.MongoDB.FullDocument,
				cfg.MongoDB.MetadataDB, cfg.MongoDB.MetadataColl,
				cfg.MongoDB.BackoffBase, cfg.MongoDB.BackoffCap,
				ctx.Logger,
			)
			if ctx.TracerProvider != nil {
				det.SetTracer(ctx.TracerProvider.Tracer("pgcdc"))
			}
			return registry.DetectorResult{Detector: det}, nil
		},
	})
}
