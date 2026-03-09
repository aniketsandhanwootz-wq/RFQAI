# RFQAI

AI-powered RFQ ingestion and semantic indexing pipeline for Glide CRM data.

## Overview

RFQAI reads RFQ data from Glide tables, stores normalized entities in Postgres (`rfq.*`), extracts documents/files, chunks text, creates embeddings, and writes vectors to `rfq.chunks`.

Primary characteristics:

- Glide is strictly read-only (`queryTables` only; no `mutateTables`).
- Parent-child integrity is enforced (`rfq_id` FK safety for products/queries/shares).
- Upserts are idempotent (`row_hash`-based change detection + conflict handling).
- Vector writes are scoped and safe for re-runs.

## Current Stack

- Python 3.13 (works with 3.10+)
- FastAPI 0.133
- Postgres + pgvector
- LangGraph 1.0.10
- LangChain Core 1.2.17
- Gemini embeddings (`1536` dims)
- Optional Google Document AI OCR for PDFs

See exact versions in [service/requirements.txt](/Users/aniketsandhan/Desktop/RFQAI/service/requirements.txt).

## Data Model and ID Mapping

Canonical primary keys are Glide row identity:

- `rfq_id` = `all_rfq.$rowID` (fallback: `rowID/RowID/id`)
- `product_id` = `all_products.$rowID`
- `query_id` = `queries.$rowID`
- `share_id` = `supplier_shares.$rowID`

FK links from Glide child tables:

- `all_products["3E2xY"] -> rfq_id`
- `queries["iFLE0"] -> rfq_id`
- `supplier_shares["fipwH"] -> rfq_id`

Source contract file:

- [glide_tables.yaml](/Users/aniketsandhan/Desktop/RFQAI/packages/contracts/glide_tables.yaml)

## Pipeline Modes

### 1) Table-only ingestion

Reads Glide tables and upserts:

- `rfq.rfqs`
- `rfq.products`
- `rfq.queries`
- `rfq.supplier_shares`

Command:

```bash
source .venv/bin/activate
set -a; source .env; set +a
python service/app/scripts/ingest_tables_only.py --mode cron --migrate
```

### 2) Full backfill (tables + files + chunks + vectors)

Command:

```bash
source .venv/bin/activate
set -a; source .env; set +a
export GLIDE_MAX_ROWS_PER_CALL=5000
export GLIDE_HARD_MAX_LIMIT=10000
python service/app/scripts/backfill.py --mode backfill --rfq_batch_size 200 --migrate
```

Notes:

- `--rfq_batch_size` controls post-table processing batch size.
- `--no_files` can be used to skip files/vectors stage.

### 3) Single RFQ full ingestion

```bash
source .venv/bin/activate
set -a; source .env; set +a
python service/app/scripts/ingest_one.py --rfq_id <ALL_RFQ_ROW_ID>
```

## Setup

### 1) Create virtual env and install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r service/requirements.txt
```

### 2) Postgres migrations

```bash
source .venv/bin/activate
set -a; source .env; set +a
python - <<'PY'
from pathlib import Path
from service.app.config import Settings
from service.app.tools.db_tool import DB, apply_migrations, ping

s = Settings()
db = DB(s.database_url)
ping(db)
apply_migrations(db, Path("packages/db/migrations"))
print("migrations_applied=OK")
PY
```

## Required Environment Variables

Minimum for table ingestion:

- `DATABASE_URL`
- `DB_SCHEMA` (default `rfq`)
- `GLIDE_API_KEY`
- `GLIDE_APP_ID`

Required for file/chunk/vector stage:

- `GEMINI_API_KEY`
- `GEMINI_EMBED_MODEL` (default `gemini-embedding-001`)
- `EMBED_DIM` (must be `1536`)

Drive/file crawling:

- `GDRIVE_SA_JSON_PATH`

`GDRIVE_SA_JSON_PATH` supports:

- path to service account JSON file
- raw JSON string
- base64-encoded JSON string

Optional PDF OCR via Document AI:

- `DOCAI_PROJECT_ID`
- `DOCAI_LOCATION`
- `DOCAI_PROCESSOR_ID`
- `DOCAI_PROCESSOR_VERSION` (optional)

Useful tuning knobs:

- `GLIDE_MAX_ROWS_PER_CALL`
- `GLIDE_HARD_MAX_LIMIT`
- `CHUNK_SIZE`
- `CHUNK_OVERLAP`
- `INGEST_HTTP_TIMEOUT_SEC`
- `INGEST_FILE_MAX_MB`

## Read-Only Glide Guarantee

Glide integration is locked to `queryTables`. Writes to Glide are intentionally blocked.

Implementation:

- [glide_client.py](/Users/aniketsandhan/Desktop/RFQAI/service/app/integrations/glide_client.py)

## Security Note (LangGraph Checkpoint Advisory Context)

This project compiles the ingest graph with `checkpointer=None` (in-memory flow; no persistent checkpoint restore path in this pipeline).

Reference:

- [ingest_graph.py](/Users/aniketsandhan/Desktop/RFQAI/service/app/pipeline/ingest_graph.py)

## Sanity and Smoke Checks

ID mapping and FK scan against Glide:

```bash
source .venv/bin/activate
set -a; source .env; set +a
python service/app/scripts/print_pk_fk_report.py --limit 1000 --max_pages 200
```

Dry table-ingest smoke:

```bash
source .venv/bin/activate
python service/app/scripts/smoke_table_ingest_dry.py
```

## Common Issues

### Gemini 429 (quota exhausted)

- Reduce load: smaller `--rfq_batch_size` and/or `--limit`
- Retry after quota reset or billing increase

### High `rows_skipped` in child tables

Likely causes:

- orphan child rows in source data
- `rfq_id` missing in child row
- table order violation (RFQs must load before child tables)

### HTML junk in chunks

HTML extraction has been hardened to remove script/style noise. Existing garbage chunks should be cleaned once and reprocessed.

## Repo Layout

- `service/app/pipeline/` - ingestion graph and table pipeline
- `service/app/integrations/` - Glide/Drive/DocumentAI/HTTP clients
- `service/app/tools/` - extractors, embedding, vector DB tools
- `service/app/scripts/` - operational scripts
- `packages/contracts/` - Glide table/column mappings
- `packages/db/migrations/` - SQL schema migrations
