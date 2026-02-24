# Getting Started with pgcdc

pgcdc streams PostgreSQL changes to any destination. This guide gets you running in 5 minutes.

## Prerequisites

- PostgreSQL 12+ (for LISTEN/NOTIFY) or 14+ (for WAL logical replication)
- Go 1.22+ (for `go install`) or Docker

## Install

```bash
go install github.com/florinutz/pgcdc/cmd/pgcdc@latest
```

Or build from source:

```bash
git clone https://github.com/florinutz/pgcdc.git && cd pgcdc && make build
```

## 1. LISTEN/NOTIFY (quickest start)

Generate a trigger for your table:

```bash
pgcdc init --table orders | psql mydb
```

Start streaming:

```bash
pgcdc listen -c pgcdc:orders --db postgres://localhost:5432/mydb
```

Insert a row in another terminal:

```sql
INSERT INTO orders (data) VALUES ('{"item": "widget"}');
```

You'll see:

```json
{"id":"...","channel":"pgcdc:orders","operation":"INSERT","payload":{"op":"INSERT","table":"orders","row":{"id":1,"data":{"item":"widget"}},"old":null},"source":"listen_notify","created_at":"..."}
```

## 2. WAL Logical Replication (no triggers, production-ready)

Requires `wal_level=logical` in `postgresql.conf`:

```bash
pgcdc init --table orders --detector wal | psql mydb
pgcdc listen --detector wal --publication pgcdc_orders --persistent-slot --db postgres://...
```

## 3. Add an Adapter

```bash
# Webhook
pgcdc listen -c pgcdc:orders -a webhook -u https://example.com/hook --db postgres://...

# Multiple adapters
pgcdc listen -c pgcdc:orders -a stdout -a webhook -a redis \
  -u https://example.com/hook --redis-url redis://localhost:6379 --db postgres://...
```

## 4. Route Events

```bash
pgcdc listen --detector wal --publication pgcdc_all \
  -a webhook -a redis \
  --route webhook=pgcdc:orders \
  --route redis=pgcdc:orders,pgcdc:inventory \
  --db postgres://...
```

## Next Steps

- [Adapters reference](adapters.md)
- [Configuration reference](configuration.md)
- [RAG pipeline guide](guides/rag-pipeline.md)
- [Search sync guide](guides/search-sync.md)
