# Redis Cache Invalidation with pgcdc

Never serve stale cache again. pgcdc watches PostgreSQL for changes and automatically invalidates or updates Redis keys.

## Modes

### Invalidate Mode (default)

Any change (INSERT, UPDATE, DELETE) deletes the Redis key. Your application re-fetches from PostgreSQL on the next cache miss.

```bash
pgcdc listen --detector wal --publication pgcdc_orders \
  -a redis --redis-url redis://localhost:6379 \
  --redis-mode invalidate --redis-key-prefix orders: \
  --db postgres://...
```

When `orders` row with `id=42` changes: `DEL orders:42`

### Sync Mode

INSERT/UPDATE sets the key to the full row JSON. DELETE removes the key.

```bash
pgcdc listen --detector wal --publication pgcdc_orders \
  -a redis --redis-url redis://localhost:6379 \
  --redis-mode sync --redis-key-prefix orders: \
  --db postgres://...
```

When row `{id: 42, name: "Widget"}` is inserted:
```
SET orders:42 '{"id":42,"name":"Widget",...}'
```

## With Routing

Only invalidate cache for specific tables:

```bash
pgcdc listen --detector wal --publication pgcdc_all \
  -a stdout -a redis \
  --redis-url redis://localhost:6379 \
  --redis-key-prefix orders: \
  --route redis=pgcdc:orders \
  --db postgres://...
```

## Key Format

Keys are constructed as `<prefix><id_column_value>`:
- `--redis-key-prefix orders:` + row `id=42` = key `orders:42`
- `--redis-id-column` controls which row field to use (default: `id`)
