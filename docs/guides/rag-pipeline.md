# Build a RAG Pipeline with pgcdc + pgvector

Keep your vector search index automatically synchronized with PostgreSQL data changes. When articles are inserted, updated, or deleted, pgcdc calls an embedding API and updates the pgvector table in real-time.

## Architecture

```
PostgreSQL (articles table)
  │
  ├── INSERT/UPDATE ──▶ pgcdc ──▶ Embedding API ──▶ pgvector table (UPSERT)
  └── DELETE ──────────▶ pgcdc ──────────────────▶ pgvector table (DELETE)
```

## Setup

### 1. Create the embeddings table

```bash
pgcdc init --table article_embeddings --adapter embedding | psql mydb
```

### 2. Embed existing data

```bash
pgcdc snapshot --table articles -a embedding \
  --embedding-api-url https://api.openai.com/v1/embeddings \
  --embedding-api-key $OPENAI_API_KEY \
  --embedding-columns title,body \
  --db postgres://localhost:5432/mydb
```

### 3. Stream live changes

```bash
pgcdc listen --detector wal --publication pgcdc_articles \
  -a embedding \
  --embedding-api-url https://api.openai.com/v1/embeddings \
  --embedding-api-key $OPENAI_API_KEY \
  --embedding-columns title,body \
  --db postgres://localhost:5432/mydb
```

### 4. Query with pgvector

```sql
SELECT a.*, e.embedding <=> '[0.1, 0.2, ...]'::vector AS distance
FROM articles a
JOIN article_embeddings e ON e.source_id = a.id::text
ORDER BY distance
LIMIT 10;
```

## Using Ollama (local, free)

```bash
pgcdc listen -c pgcdc:articles -a embedding \
  --embedding-api-url http://localhost:11434/api/embeddings \
  --embedding-model nomic-embed-text \
  --embedding-dimension 768 \
  --embedding-columns title,body \
  --db postgres://...
```

## Snapshot-First + Live Streaming

For zero-gap initial sync:

```bash
pgcdc listen --detector wal --publication pgcdc_articles \
  --snapshot-first --snapshot-table articles \
  -a embedding \
  --embedding-api-url https://api.openai.com/v1/embeddings \
  --embedding-api-key $OPENAI_API_KEY \
  --embedding-columns title,body \
  --db postgres://localhost:5432/mydb
```
