package s3_test

import (
	"testing"

	adapters3 "github.com/florinutz/pgcdc/adapter/s3"
)

func TestS3Adapter_Name(t *testing.T) {
	a := adapters3.New(adapters3.Config{
		Bucket: "test-bucket",
	}, nil)
	if got := a.Name(); got != "s3" {
		t.Errorf("Name() = %q, want %q", got, "s3")
	}
}
