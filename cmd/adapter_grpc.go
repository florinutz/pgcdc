//go:build !no_grpc

package cmd

import (
	"log/slog"

	"github.com/florinutz/pgcdc/adapter"
	grpcadapter "github.com/florinutz/pgcdc/adapter/grpc"
	"github.com/florinutz/pgcdc/internal/config"
)

func makeGRPCAdapter(cfg config.Config, logger *slog.Logger) (adapter.Adapter, error) {
	return grpcadapter.New(cfg.GRPC.Addr, logger), nil
}
