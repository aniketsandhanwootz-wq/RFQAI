from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from service.app.config import Settings
from service.app.pipeline.table_ingest import ingest_glide_tables
from service.app.tools.db_tool import DB, apply_migrations, ping


def main() -> int:
    ap = argparse.ArgumentParser(description="RFQAI: ingest Glide tables only (no files/vectors).")
    ap.add_argument("--mode", choices=["backfill", "cron"], default="cron")
    ap.add_argument("--migrate", action="store_true", help="Apply SQL migrations before running")
    args = ap.parse_args()

    settings = Settings()
    settings.validate_runtime()

    db = DB(settings.database_url)
    if args.migrate:
        apply_migrations(db, ROOT / "packages" / "db" / "migrations")
    ping(db)

    try:
        result = ingest_glide_tables(settings, mode=args.mode)
    except Exception as e:
        print(f"[FAIL] table ingestion failed: {e}")
        return 2

    print(f"[OK] run_id={result.run_id} mode={result.mode} changed_rfq_count={result.changed_rfq_count}")
    for key, p in result.table_progress.items():
        print(
            f"  - {key}: pages={p.pages} rows_seen={p.rows_seen} "
            f"rows_changed={p.rows_changed} rows_unchanged={p.rows_unchanged} rows_skipped={p.rows_skipped}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
