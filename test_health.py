#!/usr/bin/env python
"""Codebase health check script"""

import sys

test_results = []

try:
    from service.app.main import app
    test_results.append("✅ FastAPI app")
except Exception as e:
    test_results.append(f"❌ FastAPI app: {e}")

try:
    from service.app.config import Settings
    test_results.append("✅ Settings/Config")
except Exception as e:
    test_results.append(f"❌ Config: {e}")

try:
    from service.app.pipeline.ingest_graph import run_ingest_full
    test_results.append("✅ Ingestion pipeline")
except Exception as e:
    test_results.append(f"❌ Pipeline: {e}")

try:
    from service.app.tools.file_extractors.router import route_extract
    test_results.append("✅ File extractors")
except Exception as e:
    test_results.append(f"❌ Extractors: {e}")

try:
    from service.app.integrations.drive_client import DriveItem
    test_results.append("✅ Google Drive integration")
except Exception as e:
    test_results.append(f"❌ Drive: {e}")

try:
    from service.app.integrations.glide_client import GlideClient
    test_results.append("✅ Glide CRM integration")
except Exception as e:
    test_results.append(f"❌ Glide: {e}")

try:
    from service.app.tools.db_tool import DB
    test_results.append("✅ Database tool")
except Exception as e:
    test_results.append(f"❌ DB: {e}")

try:
    from service.app.routers.health import router as health_router
    from service.app.routers.ingest import router as ingest_router
    test_results.append("✅ API routers")
except Exception as e:
    test_results.append(f"❌ Routers: {e}")

print("=" * 70)
print("CODEBASE HEALTH CHECK RESULTS")
print("=" * 70)
for result in test_results:
    print(result)
print("=" * 70)

# Check if all passed
all_passed = all("✅" in r for r in test_results)
if all_passed:
    print("✅ CODEBASE IS HEALTHY - NO BREAKS FOUND")
    sys.exit(0)
else:
    print("❌ Some issues found")
    sys.exit(1)
