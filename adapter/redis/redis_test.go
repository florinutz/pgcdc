package redis_test

import (
	"testing"

	adapterredis "github.com/florinutz/pgcdc/adapter/redis"
)

func TestRedisAdapter_Name(t *testing.T) {
	a := adapterredis.New(adapterredis.Config{
		URL: "redis://localhost:6379",
	}, nil)
	if got := a.Name(); got != "redis" {
		t.Errorf("Name() = %q, want %q", got, "redis")
	}
}
