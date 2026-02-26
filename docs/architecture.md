# RFQAI Architecture

## Core idea
- Glide is the source of truth for RFQs (4 tables).
- Postgres (Render) stores:
  - normalized RFQ entities (`rfq.rfqs`, `rfq.products`, `rfq.queries`, `rfq.supplier_shares`)
  - file inventory (`rfq.files`)
  - vectors (`rfq.chunks` with `embedding vector(1536)`)

## Ingestion pipeline (LangGraph)
1) Load Glide (bulk fetch 4 tables, filter by rfq_id)
2) Upsert entity rows into `rfq.*`
3) Build internal documents:
   - RFQ_BRIEF
   - PRODUCT_CARD
   - THREAD_MESSAGE
4) Resolve sources (folder links, drawing links, attachments)
5) Extract files:
   - PDF: text + vision on low-text pages
   - XLSX: cell text + vision on embedded images
   - PPTX/DOCX: text + vision on embedded images
   - Images: vision
6) Chunk text (LangChain text splitter)
7) Embed (Gemini embedding 1536)
8) Upsert vectors into `rfq.chunks`

## Providers
- `gdrive`: Google Drive folder/file crawl (read-only)
- `http`: direct URL downloads (read-only)
- Future: `msgraph` provider can be added without schema changes.

## Prompts
All prompts are stored in `packages/prompts/`. Code only loads prompt files.