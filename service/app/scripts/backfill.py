# service/app/scripts/backfill.py
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, Optional

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from service.app.config import Settings
from service.app.integrations.glide_client import GlideClient
from service.app.pipeline.ingest_graph import run_ingest_full_prefetched
from service.app.tools.db_tool import DB, apply_migrations, ping


def _row_id(row: Dict[str, Any]) -> Optional[str]:
    return row.get("rowID") or row.get("RowID") or row.get("id")


def main() -> int:
    ap = argparse.ArgumentParser(description="RFQAI backfill (FULL pipeline).")
    ap.add_argument("--limit", type=int, default=0, help="Limit RFQs to first N (0 = all)")
    ap.add_argument("--migrate", action="store_true", help="Apply migrations before running")
    args = ap.parse_args()

    settings = Settings()
    settings.validate_runtime()

    db = DB(settings.database_url)
    if args.migrate:
        apply_migrations(db, ROOT / "packages" / "db" / "migrations")
    ping(db)

    # Minimum calls: fetch tables once, then loop RFQs.
    glide = GlideClient(settings)
    tables = glide.fetch_all_4_tables()
    rfqs = tables["all_rfq"]

    rfq_ids = [rid for rid in (_row_id(r) for r in rfqs) if rid]
    if args.limit and args.limit > 0:
        rfq_ids = rfq_ids[: args.limit]

    ok = 0
    fail = 0
    for rfq_id in rfq_ids:
        st = run_ingest_full_prefetched(rfq_id, settings, prefetched_tables=tables)
        if st.errors:
            fail += 1
            print(f"[FAIL] rfq_id={rfq_id} errors={st.errors[:2]}")
        else:
            ok += 1
            print(f"[OK] rfq_id={rfq_id} products={len(st.products_rows)} queries={len(st.queries_rows)} chunks={len(st.chunks)}")

    print(f"[DONE] ok={ok} fail={fail} total={ok+fail}")
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())