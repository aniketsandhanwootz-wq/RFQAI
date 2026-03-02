from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from service.app.config import Settings
from service.app.pipeline.table_ingest import (
    ingest_glide_tables,
    iter_changed_rfq_batches,
    merge_run_summary,
    run_rfq_postprocess_from_db,
)
from service.app.tools.db_tool import DB, apply_migrations, ping


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "RFQAI Glide ingestion (table-by-table, page-by-page) with optional "
            "post-table files/vectors processing in RFQ batches."
        )
    )
    ap.add_argument("--mode", choices=["backfill", "cron"], default="backfill")
    ap.add_argument("--rfq_batch_size", type=int, default=200, help="Batch size for changed RFQ post-processing")
    ap.add_argument("--no_files", action="store_true", help="Skip file crawling and vectors stage")
    ap.add_argument("--limit", type=int, default=0, help="Optional cap on changed RFQs processed for files stage")
    ap.add_argument("--migrate", action="store_true", help="Apply SQL migrations before running")
    args = ap.parse_args()

    settings = Settings()
    settings.validate_runtime()

    db = DB(settings.database_url)
    if args.migrate:
        apply_migrations(db, ROOT / "packages" / "db" / "migrations")
    ping(db)

    # Minimal Glide API calls under Glide limitations:
    # one paginated scan per table (all_rfq -> all_products -> queries -> supplier_shares).
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

    if args.no_files:
        return 0

    ok = 0
    fail = 0

    for batch in iter_changed_rfq_batches(
        db,
        result.run_id,
        batch_size=args.rfq_batch_size,
        limit=args.limit,
    ):
        for rfq_id in batch:
            st = run_rfq_postprocess_from_db(rfq_id, settings)
            if st.errors:
                fail += 1
                print(f"[FAIL] rfq_id={rfq_id} errors={st.errors[:2]}")
                continue

            ok += 1
            print(
                f"[OK] rfq_id={rfq_id} products={len(st.products_rows)} "
                f"queries={len(st.queries_rows)} docs={len(st.docs)} chunks={len(st.chunks)}"
            )

    merge_run_summary(
        db,
        result.run_id,
        {
            "files_stage": {
                "processed_ok": ok,
                "processed_failed": fail,
                "batch_size": int(args.rfq_batch_size),
                "limit": int(args.limit),
                "skipped": bool(args.no_files),
            }
        },
    )

    print(f"[DONE] run_id={result.run_id} files_ok={ok} files_fail={fail}")
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
