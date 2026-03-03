from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Set, Tuple

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from service.app.config import Settings
from service.app.integrations.glide_client import GlideClient


def _row_id(row: Dict[str, Any]) -> str:
    rid = row.get("$rowID") or row.get("rowID") or row.get("RowID") or row.get("id")
    return str(rid).strip() if rid is not None else ""


def _iter_table_rows(glide: GlideClient, table_key: str, *, page_limit: int, max_pages: int) -> Iterable[Dict[str, Any]]:
    table_name = glide.tables[table_key]["table_name"]
    for page in glide.fetch_table_rows_paginated(table_name, limit=page_limit, max_pages=max_pages):
        for row in page.rows:
            yield row


def _scan_rowid_presence(
    glide: GlideClient,
    table_key: str,
    *,
    page_limit: int,
    max_pages: int,
) -> Tuple[int, int, int]:
    rows_total = 0
    rows_with_dollar_rowid = 0
    rows_with_any_rowid = 0

    for row in _iter_table_rows(glide, table_key, page_limit=page_limit, max_pages=max_pages):
        rows_total += 1
        if "$rowID" in row and str(row.get("$rowID") or "").strip():
            rows_with_dollar_rowid += 1
        if _row_id(row):
            rows_with_any_rowid += 1

    return rows_total, rows_with_dollar_rowid, rows_with_any_rowid


def _collect_all_rfq_ids(glide: GlideClient, *, page_limit: int, max_pages: int) -> Set[str]:
    out: Set[str] = set()
    for row in _iter_table_rows(glide, "all_rfq", page_limit=page_limit, max_pages=max_pages):
        rid = _row_id(row)
        if rid:
            out.add(rid)
    return out


def _scan_orphans(
    glide: GlideClient,
    table_key: str,
    fk_col: str,
    rfq_ids: Set[str],
    *,
    page_limit: int,
    max_pages: int,
) -> Tuple[int, int, int]:
    total = 0
    blank = 0
    orphan = 0

    for row in _iter_table_rows(glide, table_key, page_limit=page_limit, max_pages=max_pages):
        total += 1
        ref = str(row.get(fk_col, "") or "").strip()
        if not ref:
            blank += 1
            continue
        if ref not in rfq_ids:
            orphan += 1

    return total, blank, orphan


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Read-only Glide sanity for ID mapping. Uses queryTables only and prints "
            "$rowID presence + orphan child RFQ reference counts."
        )
    )
    ap.add_argument("--contracts", default="packages/contracts/glide_tables.yaml", help="Path to Glide contracts YAML")
    ap.add_argument("--max_pages", type=int, default=200, help="Max pages per table scan")
    ap.add_argument("--limit", type=int, default=1000, help="Rows per queryTables call")
    args = ap.parse_args()

    settings = Settings()
    if not settings.glide_api_key:
        print("[FAIL] GLIDE_API_KEY is missing in environment/.env")
        return 2

    glide = GlideClient(settings, contracts_path=args.contracts)
    page_limit = glide.max_allowed_limit(args.limit)
    max_pages = max(1, int(args.max_pages))

    print("ID Mapping Sanity (read-only queryTables)")
    print(f"app_id={glide.app_id}")
    print(f"page_limit={page_limit} max_pages={max_pages}")
    print("")

    table_keys = ("all_rfq", "all_products", "queries", "supplier_shares")
    for table_key in table_keys:
        rows_total, rows_with_dollar_rowid, rows_with_any_rowid = _scan_rowid_presence(
            glide, table_key, page_limit=page_limit, max_pages=max_pages
        )
        has_dollar_rowid = "YES" if rows_with_dollar_rowid > 0 else "NO"
        print(
            f"{table_key}: rows={rows_total} has_$rowID={has_dollar_rowid} "
            f"rows_with_$rowID={rows_with_dollar_rowid} rows_with_any_rowid={rows_with_any_rowid}"
        )

    print("")
    rfq_ids = _collect_all_rfq_ids(glide, page_limit=page_limit, max_pages=max_pages)
    print(f"all_rfq_ids_collected={len(rfq_ids)}")

    child_specs = (
        ("all_products", glide.tables["all_products"]["columns"]["rfq_id"], "3E2xY"),
        ("queries", glide.tables["queries"]["columns"]["rfq"], "iFLE0"),
        ("supplier_shares", glide.tables["supplier_shares"]["columns"]["rfq"], "fipwH"),
    )

    print("orphan_child_refs:")
    for table_key, fk_col, expected in child_specs:
        total, blank, orphan = _scan_orphans(
            glide,
            table_key,
            fk_col,
            rfq_ids,
            page_limit=page_limit,
            max_pages=max_pages,
        )
        label = f"{table_key}[{fk_col}]"
        if fk_col != expected:
            label += f" (expected_contract={expected})"
        print(f"  {label}: total={total} blank={blank} orphan={orphan}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
