-- packages/db/migrations/004_incremental_ingest.sql
-- Incremental/table-stream ingestion metadata and row-hash idempotency support.

-- -----------------------------------------------------
-- 1) Run tracking
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS rfq.ingest_runs (
  run_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  mode         text NOT NULL,                      -- backfill | cron
  status       text NOT NULL DEFAULT 'RUNNING',    -- RUNNING | SUCCESS | FAILED
  started_at   timestamptz NOT NULL DEFAULT now(),
  finished_at  timestamptz,
  error        text,
  summary      jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_rfq_ingest_runs_started_at
  ON rfq.ingest_runs (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_rfq_ingest_runs_mode_status
  ON rfq.ingest_runs (mode, status);

-- -----------------------------------------------------
-- 2) Per-table run progress (page/row counters + errors)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS rfq.ingest_run_tables (
  run_id          uuid NOT NULL REFERENCES rfq.ingest_runs(run_id) ON DELETE CASCADE,
  table_key       text NOT NULL,       -- all_rfq | all_products | queries | supplier_shares
  table_name      text NOT NULL,
  status          text NOT NULL DEFAULT 'RUNNING',
  pages           integer NOT NULL DEFAULT 0,
  rows_seen       integer NOT NULL DEFAULT 0,
  rows_changed    integer NOT NULL DEFAULT 0,
  rows_unchanged  integer NOT NULL DEFAULT 0,
  rows_skipped    integer NOT NULL DEFAULT 0,
  last_token      text,
  last_token_kind text,
  error           text,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, table_key)
);

CREATE INDEX IF NOT EXISTS idx_rfq_ingest_run_tables_status
  ON rfq.ingest_run_tables (status, updated_at DESC);

-- -----------------------------------------------------
-- 3) Optional pagination checkpoints (resume safety)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS rfq.glide_cursors (
  table_key    text PRIMARY KEY,
  table_name   text NOT NULL,
  next_token   text,
  token_kind   text,
  updated_at   timestamptz NOT NULL DEFAULT now(),
  last_run_id  uuid REFERENCES rfq.ingest_runs(run_id) ON DELETE SET NULL
);

-- -----------------------------------------------------
-- 4) Changed RFQs detected in a run (for batched file/vector stage)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS rfq.ingest_run_changed_rfqs (
  run_id      uuid NOT NULL REFERENCES rfq.ingest_runs(run_id) ON DELETE CASCADE,
  rfq_id      text NOT NULL REFERENCES rfq.rfqs(rfq_id) ON DELETE CASCADE,
  changed_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, rfq_id)
);

CREATE INDEX IF NOT EXISTS idx_rfq_ingest_changed_rfqs_rfq
  ON rfq.ingest_run_changed_rfqs (rfq_id);

-- -----------------------------------------------------
-- 5) Row-level hash columns for incremental upsert skip
-- -----------------------------------------------------
ALTER TABLE rfq.rfqs
  ADD COLUMN IF NOT EXISTS row_hash text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS last_changed_run_id uuid;

ALTER TABLE rfq.products
  ADD COLUMN IF NOT EXISTS row_hash text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS last_changed_run_id uuid;

ALTER TABLE rfq.queries
  ADD COLUMN IF NOT EXISTS row_hash text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS last_changed_run_id uuid;

ALTER TABLE rfq.supplier_shares
  ADD COLUMN IF NOT EXISTS row_hash text NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS last_changed_run_id uuid;

CREATE INDEX IF NOT EXISTS idx_rfq_rfqs_last_changed_run_id
  ON rfq.rfqs (last_changed_run_id);

CREATE INDEX IF NOT EXISTS idx_rfq_products_last_changed_run_id
  ON rfq.products (last_changed_run_id);

CREATE INDEX IF NOT EXISTS idx_rfq_queries_last_changed_run_id
  ON rfq.queries (last_changed_run_id);

CREATE INDEX IF NOT EXISTS idx_rfq_supplier_shares_last_changed_run_id
  ON rfq.supplier_shares (last_changed_run_id);
