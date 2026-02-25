-- packages/db/migrations/001_extensions.sql
-- RFQAI (STRIKE) uses same Render Postgres as ZAI, but isolated via schema `rfq`.
-- This file is safe to run multiple times.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Optional later:
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;