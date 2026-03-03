VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/florinutz/pgcdc/cmd.Version=$(VERSION)

.PHONY: build build-slim build-slim-stripped size test test-scenarios test-scenarios-fast test-scenarios-docker test-all lint vet fmt bench bench-unit bench-compare bench-save bench-debezium fuzz coverage coverage-gate test-smoke docker-build docker-up docker-down clean help

SLIM_TAGS := no_kafka,no_grpc,no_iceberg,no_nats,no_redis,no_plugins,no_views

## build: Compile the binary
build:
	go build -ldflags "$(LDFLAGS)" -o pgcdc ./cmd/pgcdc

## build-slim: Binary without Kafka/gRPC/Iceberg/NATS/Redis/Wasm/Views
build-slim:
	go build -tags "$(SLIM_TAGS)" -ldflags "$(LDFLAGS)" -o pgcdc-slim ./cmd/pgcdc

## build-slim-stripped: Slim binary with debug symbols stripped
build-slim-stripped:
	go build -tags "$(SLIM_TAGS)" -ldflags "$(LDFLAGS) -s -w" -o pgcdc-slim ./cmd/pgcdc

## size: Compare full and slim binary sizes
size: build build-slim
	@echo "Full:"; ls -lh pgcdc
	@echo "Slim:"; ls -lh pgcdc-slim

## test: Run unit tests only (fast, no Docker)
test:
	go test -race -count=1 -timeout=30s $$(go list ./... | grep -v /scenarios)

# Tests requiring external Docker containers (run at lower parallelism)
DOCKER_SCENARIOS = TestScenario_KafkaAdapter|TestScenario_Encoding|TestScenario_MongoDB|TestScenario_MySQL|TestScenario_S3|TestScenario_Iceberg|TestScenario_Redis|TestScenario_Search|TestScenario_Nats

## test-scenarios-fast: PG-only scenario tests (no external containers)
test-scenarios-fast:
	go test -race -count=1 -timeout=300s -tags=integration -parallel 8 \
		-skip '$(DOCKER_SCENARIOS)' ./scenarios/...

## test-scenarios-docker: Scenario tests with external containers
test-scenarios-docker:
	go test -race -count=1 -timeout=600s -tags=integration -parallel 4 \
		-run '$(DOCKER_SCENARIOS)' ./scenarios/...

## test-scenarios: All scenario tests (fast tier then docker tier)
test-scenarios: test-scenarios-fast test-scenarios-docker

## test-all: Run unit + scenario tests
test-all: test test-scenarios

## lint: Run golangci-lint
lint:
	golangci-lint run ./...

## vet: Run go vet
vet:
	go vet ./...

## fmt: Check formatting
fmt:
	@test -z "$$(gofmt -l .)" || (echo "Files need formatting:"; gofmt -l .; exit 1)

## docker-build: Build Docker image
docker-build:
	docker build -t pgcdc:dev .

## docker-up: Start postgres + pgcdc via docker compose
docker-up:
	docker compose up -d

## docker-down: Stop docker compose services
docker-down:
	docker compose down

## bench: Run integration benchmarks (requires Docker)
bench:
	./bench/run.sh

## bench-unit: Run micro-benchmarks (no Docker needed)
bench-unit:
	go test -bench=. -benchmem -count=3 -timeout=120s $$(go list ./... | grep -v /scenarios)

## bench-compare: Compare current benchmarks against baseline
bench-compare:
	go test -bench=. -benchmem -count=5 -timeout=120s $$(go list ./... | grep -v /scenarios) | tee bench-current.txt
	benchstat bench-baseline.txt bench-current.txt

## bench-save: Save current benchmark results as baseline
bench-save:
	go test -bench=. -benchmem -count=5 -timeout=120s $$(go list ./... | grep -v /scenarios) > bench-baseline.txt

## bench-debezium: Run Debezium comparison benchmarks (requires Docker, slow)
bench-debezium:
	go test -tags=debezium -bench=BenchmarkComparison -benchtime=1x -timeout=15m -count=1 -v ./bench/

## fuzz: Run all fuzz tests for 30s each
fuzz:
	go test -fuzz=Fuzz -fuzztime=30s ./adapter/kafkaserver/
	go test -fuzz=Fuzz -fuzztime=30s ./transform/
	go test -fuzz=FuzzParse -fuzztime=30s ./view/
	go test -fuzz=FuzzCompile -fuzztime=30s ./cel/

## coverage: Generate test coverage report
coverage:
	go test -coverprofile=coverage.out $$(go list ./... | grep -v /scenarios)
	go tool cover -func=coverage.out

## coverage-gate: Fail if coverage of pure-logic packages drops below 75%
coverage-gate:
	go test -coverprofile=coverage-gate.out -race -count=1 \
		./transform/... ./view/... ./cel/... ./bus/... ./ack/... \
		./backpressure/... ./encoding/... \
		./internal/circuitbreaker/... ./internal/ratelimit/...
	@go tool cover -func=coverage-gate.out | awk '/total:/ { \
		gsub(/%/, "", $$3); \
		if ($$3+0 < 75) { \
			printf "Coverage %.1f%% is below 75%% threshold\n", $$3+0; \
			exit 1 \
		} else { \
			printf "Coverage gate passed: %.1f%%\n", $$3+0 \
		} \
	}'

## test-smoke: Quick smoke test (<15s, unit tests only)
test-smoke:
	go test -race -count=1 -timeout=15s \
		./transform/... ./event/... ./bus/... ./ack/... ./backpressure/... \
		./cel/... ./internal/circuitbreaker/... ./internal/ratelimit/...

## clean: Remove build artifacts
clean:
	rm -f pgcdc pgcdc-slim
	rm -rf dist/

## help: Show this help
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //' | column -t -s ':'
