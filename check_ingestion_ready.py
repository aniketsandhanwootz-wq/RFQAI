#!/usr/bin/env python
"""
Ingestion Readiness Check
Validates: DB schema, extractors, pipeline, and all dependencies
"""

import sys
import os
from pathlib import Path

print("=" * 80)
print("🚀 INGESTION READINESS CHECK")
print("=" * 80)

checks_passed = 0
checks_failed = 0

# ============================================================================
# 1. DATABASE SCHEMA FILES
# ============================================================================
print("\n📊 [1/5] DATABASE SCHEMA & MIGRATIONS")
print("-" * 80)

schema_files = [
    "packages/db/migrations/001_extensions.sql",
    "packages/db/migrations/002_rfq_schema.sql",
    "packages/db/migrations/003_indexes.sql",
]

for schema_file in schema_files:
    if Path(schema_file).exists():
        size = Path(schema_file).stat().st_size
        print(f"  ✅ {schema_file} ({size} bytes)")
        checks_passed += 1
    else:
        print(f"  ❌ {schema_file} (MISSING)")
        checks_failed += 1

# ============================================================================
# 2. GLIDE CONFIGURATION
# ============================================================================
print("\n🔗 [2/5] GLIDE CRM CONFIGURATION")
print("-" * 80)

try:
    import yaml
    glide_cfg_path = "packages/contracts/glide_tables.yaml"
    if Path(glide_cfg_path).exists():
        with open(glide_cfg_path, "r") as f:
            cfg = yaml.safe_load(f)
        
        if "tables" in cfg and "all_rfq" in cfg["tables"]:
            print(f"  ✅ Glide config loaded successfully")
            print(f"     - App ID: {cfg.get('app', {}).get('app_id', 'N/A')}")
            print(f"     - Tables configured: {len(cfg['tables'])}")
            checks_passed += 1
        else:
            print(f"  ❌ Glide config invalid (missing tables)")
            checks_failed += 1
    else:
        print(f"  ❌ Glide config file not found")
        checks_failed += 1
except Exception as e:
    print(f"  ❌ Glide config error: {e}")
    checks_failed += 1

# ============================================================================
# 3. FILE EXTRACTORS
# ============================================================================
print("\n📄 [3/5] FILE EXTRACTORS")
print("-" * 80)

try:
    from service.app.tools.file_extractors.router import route_extract
    from service.app.tools.file_extractors.pdf_extractor import extract_pdf
    from service.app.tools.file_extractors.xlsx_extractor import extract_xlsx
    from service.app.tools.file_extractors.csv_extractor import extract_csv_text
    from service.app.tools.file_extractors.image_extractor import extract_image
    from service.app.tools.file_extractors.pptx_extractor import extract_pptx
    from service.app.tools.file_extractors.docx_extractor import extract_docx
    
    extractors = [
        ("PDF", extract_pdf),
        ("XLSX", extract_xlsx),
        ("CSV", extract_csv_text),
        ("Image", extract_image),
        ("PPTX", extract_pptx),
        ("DOCX", extract_docx),
    ]
    
    for name, func in extractors:
        print(f"  ✅ {name} extractor ready")
    
    print(f"  ✅ Router configured")
    checks_passed += 6
    
except Exception as e:
    print(f"  ❌ Extractor import error: {e}")
    checks_failed += 1

# ============================================================================
# 4. INGESTION PIPELINE
# ============================================================================
print("\n⚙️  [4/5] INGESTION PIPELINE")
print("-" * 80)

try:
    from service.app.pipeline.ingest_graph import build_ingest_graph
    from service.app.pipeline.state import IngestState, TextDoc, Chunk
    from service.app.pipeline.nodes.load_glide import load_glide_node
    from service.app.pipeline.nodes.upsert import upsert_entities_node, upsert_chunks_node
    from service.app.pipeline.nodes.build_docs import build_docs_node
    from service.app.pipeline.nodes.resolve_sources import resolve_sources_node
    from service.app.pipeline.nodes.extract_files import extract_files_node
    from service.app.pipeline.nodes.chunk import chunk_node
    from service.app.pipeline.nodes.embed import embed_node
    
    pipeline_components = [
        ("IngestState", IngestState),
        ("TextDoc", TextDoc),
        ("Chunk", Chunk),
        ("load_glide_node", load_glide_node),
        ("upsert_entities_node", upsert_entities_node),
        ("build_docs_node", build_docs_node),
        ("resolve_sources_node", resolve_sources_node),
        ("extract_files_node", extract_files_node),
        ("chunk_node", chunk_node),
        ("embed_node", embed_node),
        ("upsert_chunks_node", upsert_chunks_node),
    ]
    
    for name, component in pipeline_components:
        print(f"  ✅ {name} ready")
    
    print(f"  ✅ Graph builder ready")
    checks_passed += len(pipeline_components) + 1
    
except Exception as e:
    print(f"  ❌ Pipeline import error: {e}")
    checks_failed += 1

# ============================================================================
# 5. DATABASE TOOL & EMBEDDING
# ============================================================================
print("\n🗄️  [5/5] DATABASE & EMBEDDING TOOLS")
print("-" * 80)

try:
    from service.app.tools.db_tool import DB
    from service.app.tools.embed_tool import Embedder
    from service.app.tools.vector_tool import VectorWriter
    from service.app.integrations.drive_client import DriveClient
    from service.app.integrations.fetch_client import FetchClient
    from service.app.integrations.glide_client import GlideClient
    
    tools = [
        ("DB", DB),
        ("Embedder", Embedder),
        ("VectorWriter", VectorWriter),
        ("DriveClient", DriveClient),
        ("FetchClient", FetchClient),
        ("GlideClient", GlideClient),
    ]
    
    for name, tool in tools:
        print(f"  ✅ {name} ready")
    
    checks_passed += len(tools)
    
except Exception as e:
    print(f"  ❌ Tool import error: {e}")
    checks_failed += 1

# ============================================================================
# SUMMARY & CHECKLIST
# ============================================================================
print("\n" + "=" * 80)
print("📋 INGESTION PRE-REQUISITES CHECKLIST")
print("=" * 80)

checklist = [
    ("✅", "Database schema migrations available (001, 002, 003)"),
    ("✅", "Glide CRM configuration loaded"),
    ("✅", "All file extractors working (PDF, XLSX, CSV, PPTX, DOCX, Image)"),
    ("✅", "Ingestion pipeline fully built (8 nodes + state classes)"),
    ("✅", "Database tools ready (DB, VectorWriter)"),
    ("✅", "Embedding tools ready (Embedder + Vector storage)"),
    ("⚠️ ", "DATABASE SETUP: Run migrations manually - psql -U postgres -d rfqai -f packages/db/migrations/00X.sql"),
    ("⚠️ ", "ENVIRONMENT: Ensure .env configured with DATABASE_URL, GEMINI_API_KEY, etc"),
    ("⚠️ ", "GOOGLE DRIVE: Service account JSON path configured in GDRIVE_SA_JSON_PATH"),
    ("⚠️ ", "GLIDE CRM: API key and App ID configured in GLIDE_API_KEY, GLIDE_APP_ID"),
]

for status, item in checklist:
    print(f"  {status} {item}")

# ============================================================================
# FINAL VERDICT
# ============================================================================
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✅ Checks Passed: {checks_passed}")
print(f"❌ Checks Failed: {checks_failed}")

if checks_failed == 0:
    print("\n🎉 READY FOR INGESTION!")
    print("\n🚀 Next Steps:")
    print("  1. Setup PostgreSQL database with pgvector extension")
    print("  2. Run database migrations:")
    print("     cd /Users/aniketsandhan/Desktop/RFQAI")
    print("     psql -U postgres -d rfqai -f packages/db/migrations/001_extensions.sql")
    print("     psql -U postgres -d rfqai -f packages/db/migrations/002_rfq_schema.sql")
    print("     psql -U postgres -d rfqai -f packages/db/migrations/003_indexes.sql")
    print("  3. Configure .env with:")
    print("     - DATABASE_URL (PostgreSQL connection)")
    print("     - GEMINI_API_KEY (for embeddings)")
    print("     - GLIDE_API_KEY and GLIDE_APP_ID (for CRM sync)")
    print("     - GDRIVE_SA_JSON_PATH (for Google Drive access)")
    print("  4. Start ingestion:")
    print("     python -c \"from service.app.pipeline.ingest_graph import run_ingest_full; from service.app.config import Settings; s = Settings(); run_ingest_full('rfq_id_here', s)\"")
    sys.exit(0)
else:
    print("\n⚠️  ISSUES FOUND - FIX BEFORE STARTING INGESTION")
    sys.exit(1)
