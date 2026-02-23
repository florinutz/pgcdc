VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/florinutz/pgpipe/cmd.Version=$(VERSION)

.PHONY: build test test-scenarios test-all lint vet fmt coverage docker-build docker-up docker-down clean help

## build: Compile the binary
build:
	go build -ldflags "$(LDFLAGS)" -o pgpipe ./cmd/pgpipe

## test: Run unit tests only (fast, no Docker)
test:
	go test -race -count=1 -timeout=30s $$(go list ./... | grep -v /scenarios)

## test-scenarios: Run scenario tests (requires Docker)
test-scenarios:
	go test -race -count=1 -timeout=300s -tags=integration ./scenarios/...

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
	docker build -t pgpipe:dev .

## docker-up: Start postgres + pgpipe via docker compose
docker-up:
	docker compose up -d

## docker-down: Stop docker compose services
docker-down:
	docker compose down

## coverage: Generate test coverage report
coverage:
	go test -coverprofile=coverage.out $$(go list ./... | grep -v /scenarios)
	go tool cover -func=coverage.out

## clean: Remove build artifacts
clean:
	rm -f pgpipe
	rm -rf dist/

## help: Show this help
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //' | column -t -s ':'
