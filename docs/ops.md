# RFQAI Ops

## Required env vars
- DATABASE_URL
- DB_SCHEMA=rfq
- GLIDE_API_KEY (read-only)
- GLIDE_APP_ID
- GEMINI_API_KEY
- EMBED_DIM=1536

Optional:
- GDRIVE_SA_JSON_PATH
- GEMINI_VISION_MODEL (default gemini-1.5-flash)

## Run migrations (idempotent)
```bash
python -m service.app.scripts.ingest_one --rfq_id DUMMY --migrate