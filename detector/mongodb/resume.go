package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// resumeDoc is the document stored in the metadata collection.
type resumeDoc struct {
	ID          string    `bson:"_id"`
	ResumeToken bson.Raw  `bson:"resume_token"`
	UpdatedAt   time.Time `bson:"updated_at"`
}

// scopeKey returns a unique key for the resume token based on the watch scope.
func (d *Detector) scopeKey() string {
	switch d.scope {
	case "cluster":
		return "_cluster"
	case "database":
		return d.database
	default:
		// collection scope — use db.coll (first collection if multiple, we watch at db level)
		if len(d.collections) == 1 {
			return d.database + "." + d.collections[0]
		}
		return d.database
	}
}

// loadResumeToken loads a previously saved resume token from the metadata collection.
func (d *Detector) loadResumeToken(ctx context.Context, client *mongo.Client) (bson.Raw, error) {
	coll := client.Database(d.metadataDB).Collection(d.metadataColl)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var doc resumeDoc
	err := coll.FindOne(ctx, bson.M{"_id": d.scopeKey()}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("load resume token: %w", err)
	}
	return doc.ResumeToken, nil
}

// saveResumeToken persists the resume token to the metadata collection.
func (d *Detector) saveResumeToken(ctx context.Context, client *mongo.Client, token bson.Raw) error {
	coll := client.Database(d.metadataDB).Collection(d.metadataColl)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	doc := resumeDoc{
		ID:          d.scopeKey(),
		ResumeToken: token,
		UpdatedAt:   time.Now().UTC(),
	}

	opts := options.Replace().SetUpsert(true)
	_, err := coll.ReplaceOne(ctx, bson.M{"_id": d.scopeKey()}, doc, opts)
	if err != nil {
		return fmt.Errorf("save resume token: %w", err)
	}
	return nil
}
