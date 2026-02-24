#!/bin/bash
# pgcdc benchmark runner
# Usage: ./bench/run.sh
#
# Requires Docker for testcontainers.

set -euo pipefail

echo "=== pgcdc Benchmark Suite ==="
echo ""

# Build binary to measure size.
echo "--- Binary size ---"
go build -ldflags "-s -w" -o /tmp/pgcdc-bench ./cmd/pgcdc
ls -lh /tmp/pgcdc-bench | awk '{print "Binary size:", $5}'
rm /tmp/pgcdc-bench
echo ""

# Run benchmarks.
echo "--- Throughput (WAL -> devnull adapter) ---"
go test -tags=integration -bench=BenchmarkThroughput -benchtime=1x -timeout=10m -count=1 ./bench/ 2>/dev/null

echo ""
echo "--- Latency (WAL -> devnull adapter) ---"
go test -tags=integration -bench=BenchmarkLatency -benchtime=1x -timeout=5m -count=1 ./bench/ 2>/dev/null

echo ""
echo "--- Memory ---"
go test -tags=integration -bench=BenchmarkMemory -benchtime=1x -timeout=5m -count=1 ./bench/ 2>/dev/null

echo ""
echo "=== Done ==="
