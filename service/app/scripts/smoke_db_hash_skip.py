from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from service.app.config import Settings
from service.app.pipeline.nodes.upsert_tables import upsert_rfqs
from service.app.tools.db_tool import DB, apply_migrations, ping, tx


def _load_rfq_cols(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["tables"]["all_rfq"]["columns"]


def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke test: migration + row_hash skip behavior")
    ap.add_argument("--migrate", action="store_true", help="Apply migrations before running")
    args = ap.parse_args()

    settings = Settings()
    db = DB(settings.database_url)

    if args.migrate:
        apply_migrations(db, ROOT / "packages" / "db" / "migrations")
    ping(db)

    rfq_cols = _load_rfq_cols(ROOT / "packages" / "contracts" / "glide_tables.yaml")

    with tx(db) as cur:
        cur.execute(
            """
            INSERT INTO rfq.ingest_runs (mode, status)
            VALUES ('cron', 'RUNNING')
            RETURNING run_id
            """
        )
        run_id = str(cur.fetchone()[0])

    sample = {
        "rowID": "SMOKE_RFQ_HASH_SKIP",
        rfq_cols["title"]: "Hash Skip Smoke",
        rfq_cols["industry"]: "test",
        rfq_cols["customer_name"]: "smoke",
    }

    first = upsert_rfqs([sample], db=db, rfq_cols=rfq_cols, run_id=run_id)
    second = upsert_rfqs([sample], db=db, rfq_cols=rfq_cols, run_id=run_id)

    print(f"first: seen={first.seen} changed={first.changed} unchanged={first.unchanged} skipped={first.skipped}")
    print(f"second: seen={second.seen} changed={second.changed} unchanged={second.unchanged} skipped={second.skipped}")

    if first.changed != 1:
        print("[FAIL] expected first upsert to change exactly 1 row")
        return 2
    if second.changed != 0 or second.unchanged != 1:
        print("[FAIL] expected second upsert to be hash-skip unchanged")
        return 2

    with tx(db) as cur:
        cur.execute(
            """
            UPDATE rfq.ingest_runs
            SET status='SUCCESS', finished_at=now(), summary='{"smoke":true}'::jsonb
            WHERE run_id=%(run_id)s::uuid
            """,
            {"run_id": run_id},
        )

    print("[OK] hash skip smoke test passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
