FROM golang:1.25-alpine AS builder

ARG VERSION=dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -tags "no_duckdb,no_sqlite" -ldflags "-s -w -X github.com/florinutz/pgcdc/cmd.Version=${VERSION}" -o /pgcdc ./cmd/pgcdc

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /pgcdc /usr/local/bin/pgcdc
ENTRYPOINT ["pgcdc"]
