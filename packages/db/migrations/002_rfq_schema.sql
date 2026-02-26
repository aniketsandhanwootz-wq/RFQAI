-- packages/db/migrations/002_rfq_schema.sql
-- Creates schema + core tables for RFQ ingestion + vectors.
-- Idempotent: uses IF NOT EXISTS wherever possible.

CREATE SCHEMA IF NOT EXISTS rfq;

-- =========================
-- rfq.rfqs  (ALL RFQ)
-- Primary key = Glide ALL RFQ RowID
-- =========================
CREATE TABLE IF NOT EXISTS rfq.rfqs (
  rfq_id                 text PRIMARY KEY,

  title                  text,
  deadline               timestamptz,
  industry               text,
  geography              text,
  standard               text,
  customer_name          text,

  quotation_folder_link  text,
  screen_url             text,
  color_queries          text,

  current_status         text,
  team                   text,
  required_by            text,

  archive                boolean,
  received_date          timestamptz,
  rfq_created_date       timestamptz,

  created_by             text,
  sales_por              text,
  shared_members         jsonb,
  rfq_poc                text,

  last_status_updated_by text,
  last_status_updated_at timestamptz,
  last_status_comments   text,
  urgent                 boolean,

  raw_glide              jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_updated_at      timestamptz,
  ingested_at            timestamptz NOT NULL DEFAULT now()
);

-- =========================
-- rfq.products  (All Products)
-- Primary key = Glide product RowID
-- FK -> rfq.rfqs(rfq_id)
-- =========================
CREATE TABLE IF NOT EXISTS rfq.products (
  product_id            text PRIMARY KEY,
  rfq_id                text NOT NULL REFERENCES rfq.rfqs(rfq_id) ON DELETE CASCADE,

  name                  text,
  qty                   numeric,
  qty_raw               text,
  details               text,
  target_price          numeric,
  target_price_raw      text,

  dwg_link              text,
  rep_url               text,

  addl_photos           jsonb NOT NULL DEFAULT '[]'::jsonb,
  addl_files            jsonb NOT NULL DEFAULT '[]'::jsonb,
  addl_files_internal   jsonb NOT NULL DEFAULT '{}'::jsonb,
  product_photo         jsonb NOT NULL DEFAULT '[]'::jsonb,

  sr_no                 text,
  choice_all            jsonb NOT NULL DEFAULT '{}'::jsonb,
  archive               boolean,

  raw_glide             jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_updated_at     timestamptz,
  ingested_at           timestamptz NOT NULL DEFAULT now()
);

-- =========================
-- rfq.queries  (Queries)
-- Primary key = Glide query RowID
-- FK -> rfq.rfqs(rfq_id)
-- =========================
CREATE TABLE IF NOT EXISTS rfq.queries (
  query_id            text PRIMARY KEY,
  rfq_id              text NOT NULL REFERENCES rfq.rfqs(rfq_id) ON DELETE CASCADE,

  thread_id           text,
  query_type          text,
  comment             text,
  "user"              text,

  time_added          timestamptz,
  status              text,
  show_upload         boolean,

  images_attached     jsonb NOT NULL DEFAULT '[]'::jsonb,
  products_selected   jsonb NOT NULL DEFAULT '[]'::jsonb,

  raw_glide           jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_updated_at   timestamptz,
  ingested_at         timestamptz NOT NULL DEFAULT now()
);

-- =========================
-- rfq.supplier_shares  (Suppliers Shared)
-- Primary key = Glide supplier-share RowID
-- FK -> rfq.rfqs(rfq_id)
-- =========================
CREATE TABLE IF NOT EXISTS rfq.supplier_shares (
  share_id              text PRIMARY KEY,
  rfq_id                text NOT NULL REFERENCES rfq.rfqs(rfq_id) ON DELETE CASCADE,

  supplier_name         text,
  status                text,
  shared_by             text,
  user_email            text,
  rfq_link              text,

  shared_products       jsonb NOT NULL DEFAULT '[]'::jsonb,

  shared_date           timestamptz,
  quotation_shared_date timestamptz,
  quotation_received_by text,

  raw_glide             jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_updated_at     timestamptz,
  ingested_at           timestamptz NOT NULL DEFAULT now()
);

-- =========================
-- rfq.files  (Discovered via crawling links/attachments)
-- Internal PK = uuid
-- FK -> rfq.rfqs(rfq_id)
-- Idempotent key: (rfq_id, provider, provider_id, is_folder, path)
-- =========================
CREATE TABLE IF NOT EXISTS rfq.files (
  file_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  rfq_id             text NOT NULL REFERENCES rfq.rfqs(rfq_id) ON DELETE CASCADE,

  product_id         text,
  query_id           text,

  source_kind        text NOT NULL, -- RFQ_FOLDER|PRODUCT_LINK|QUERY_ATTACHMENT|DIRECT_URL
  root_url           text NOT NULL,

  provider           text NOT NULL, -- gdrive|http|glide_media|other
  provider_id        text NOT NULL, -- drive file/folder id or url (stable id)

  is_folder          boolean NOT NULL DEFAULT false,
  parent_provider_id text,
  path               text NOT NULL DEFAULT '',
  name               text,
  mime               text,

  size_bytes         bigint,
  modified_at        timestamptz,

  checksum_sha256    text,

  fetch_status       text NOT NULL DEFAULT 'PENDING',
  parse_status       text NOT NULL DEFAULT 'PENDING',
  error              text,

  ingested_at        timestamptz NOT NULL DEFAULT now(),

  UNIQUE (rfq_id, provider, provider_id, is_folder, path)
);
-- =========================
-- rfq.chunks  (Vectors stored here)
-- Internal PK = uuid
-- embedding vector(1536) for Gemini output_dimensionality=1536
-- =========================
CREATE TABLE IF NOT EXISTS rfq.chunks (
  chunk_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  rfq_id          text NOT NULL REFERENCES rfq.rfqs(rfq_id) ON DELETE CASCADE,
  doc_type        text NOT NULL, -- RFQ_BRIEF|PRODUCT_CARD|THREAD_MESSAGE|FILE_CHUNK

  product_id      text,
  query_id        text,
  file_id         uuid REFERENCES rfq.files(file_id) ON DELETE SET NULL,

  page_num        int,
  chunk_idx       int NOT NULL,

  content_text    text NOT NULL,
  content_sha     text NOT NULL, -- idempotency key
  meta            jsonb NOT NULL DEFAULT '{}'::jsonb,

  embedding       vector(1536) NOT NULL,

  created_at      timestamptz NOT NULL DEFAULT now(),

  UNIQUE(rfq_id, doc_type, content_sha)
);