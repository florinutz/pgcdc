package search_test

import (
	"testing"

	"github.com/florinutz/pgcdc/adapter/search"
)

func TestSearchAdapter_Name(t *testing.T) {
	a := search.New(search.Config{
		Engine: "typesense",
		URL:    "http://localhost:8108",
		APIKey: "test-key",
		Index:  "test-index",
	}, nil)
	if got := a.Name(); got != "search" {
		t.Errorf("Name() = %q, want %q", got, "search")
	}
}
