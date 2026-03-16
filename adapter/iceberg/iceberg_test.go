package iceberg_test

import (
	"testing"

	"github.com/florinutz/pgcdc/adapter/iceberg"
)

func TestIcebergAdapter_Name(t *testing.T) {
	a := iceberg.New(iceberg.Config{
		CatalogType: "hadoop",
		Warehouse:   "/tmp/warehouse",
		Namespace:   "default",
		TableName:   "test_table",
	}, nil)
	if got := a.Name(); got != "iceberg" {
		t.Errorf("Name() = %q, want %q", got, "iceberg")
	}
}
