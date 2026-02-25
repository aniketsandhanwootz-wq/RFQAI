-- packages/db/migrations/003_indexes.sql
-- Indexes for fast filtering + vector search.

-- ---- rfq.rfqs ----
CREATE INDEX IF NOT EXISTS idx_rfq_rfqs_ingested_at
  ON rfq.rfqs (ingested_at);

-- ---- rfq.products ----
CREATE INDEX IF NOT EXISTS idx_rfq_products_rfq_id
  ON rfq.products (rfq_id);

-- ---- rfq.queries ----
CREATE INDEX IF NOT EXISTS idx_rfq_queries_rfq_id
  ON rfq.queries (rfq_id);

CREATE INDEX IF NOT EXISTS idx_rfq_queries_thread_id
  ON rfq.queries (thread_id);

-- ---- rfq.supplier_shares ----
CREATE INDEX IF NOT EXISTS idx_rfq_supplier_shares_rfq_id
  ON rfq.supplier_shares (rfq_id);

-- ---- rfq.files ----
CREATE INDEX IF NOT EXISTS idx_rfq_files_rfq_id
  ON rfq.files (rfq_id);

CREATE INDEX IF NOT EXISTS idx_rfq_files_provider_provider_id
  ON rfq.files (provider, provider_id);

CREATE INDEX IF NOT EXISTS idx_rfq_files_fetch_status
  ON rfq.files (fetch_status);

CREATE INDEX IF NOT EXISTS idx_rfq_files_parse_status
  ON rfq.files (parse_status);

-- ---- rfq.chunks ----
CREATE INDEX IF NOT EXISTS idx_rfq_chunks_rfq_id
  ON rfq.chunks (rfq_id);

CREATE INDEX IF NOT EXISTS idx_rfq_chunks_doc_type
  ON rfq.chunks (doc_type);

CREATE INDEX IF NOT EXISTS idx_rfq_chunks_file_id
  ON rfq.chunks (file_id);

-- JSONB filter index (optional but useful)
CREATE INDEX IF NOT EXISTS idx_rfq_chunks_meta_gin
  ON rfq.chunks USING gin (meta);

-- Vector index (HNSW preferred if your pgvector supports it)
-- NOTE: HNSW works well without "training" unlike IVFFLAT.
CREATE INDEX IF NOT EXISTS idx_rfq_chunks_embedding_hnsw_cosine
  ON rfq.chunks USING hnsw (embedding vector_cosine_ops);

-- Alternative: IVFFLAT (commented). Use only if you prefer it.
-- CREATE INDEX IF NOT EXISTS idx_rfq_chunks_embedding_ivfflat_cosine
--   ON rfq.chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 200);