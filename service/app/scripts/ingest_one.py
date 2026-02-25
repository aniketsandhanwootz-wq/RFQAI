# service/app/scripts/ingest_one.py
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from service.app.config import Settings
from service.app.tools.db_tool import DB, apply_migrations, ping
from service.app.pipeline.ingest_graph import run_ingest_full


def main() -> int:
    ap = argparse.ArgumentParser(description="RFQAI: ingest a single RFQ (FULL pipeline).")
    ap.add_argument("--rfq_id", required=True, help="Glide ALL RFQ RowID (canonical RFQ id)")
    ap.add_argument("--migrate", action="store_true", help="Apply SQL migrations before running.")
    ap.add_argument(
        "--migrations_dir",
        default=str(ROOT / "packages" / "db" / "migrations"),
        help="Path to migrations folder",
    )
    args = ap.parse_args()

    settings = Settings()
    settings.validate_runtime()

    db = DB(settings.database_url)
    if args.migrate:
        apply_migrations(db, args.migrations_dir)
    ping(db)

    st = run_ingest_full(args.rfq_id, settings)

    if st.errors:
        print("[FAIL] errors:")
        for e in st.errors:
            print(" -", e)
        return 2

    print(f"[OK] ingested rfq_id={args.rfq_id}")
    print(
        f"  products={len(st.products_rows)} "
        f"queries={len(st.queries_rows)} "
        f"shares={len(st.shares_rows)} "
        f"docs={len(st.docs)} "
        f"chunks={len(st.chunks)}"
    )
    if st.warnings:
        print("[WARN] warnings (first 10):")
        for w in st.warnings[:10]:
            print(" -", w)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())