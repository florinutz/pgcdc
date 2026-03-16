package embedding_test

import (
	"testing"

	"github.com/florinutz/pgcdc/adapter/embedding"
)

func TestEmbeddingAdapter_Name(t *testing.T) {
	a := embedding.New(embedding.Config{
		APIURL:  "http://localhost:11434/v1/embeddings",
		APIKey:  "test-key",
		Columns: []string{"title", "body"},
		DBURL:   "postgres://test",
	}, nil)
	if got := a.Name(); got != "embedding" {
		t.Errorf("Name() = %q, want %q", got, "embedding")
	}
}
