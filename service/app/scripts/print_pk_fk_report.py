from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import yaml

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from service.app.config import Settings
from service.app.integrations.glide_client import GlideClient


@dataclass
class TableScan:
    total_scanned: int = 0
    missing_dollar_rowid: int = 0
    blank_fk: int = 0
    orphan_fk: int = 0
    orphan_samples: List[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.orphan_samples is None:
            self.orphan_samples = []


def _row_id(row: Dict[str, Any]) -> str:
    rid = row.get("$rowID") or row.get("rowID") or row.get("RowID") or row.get("id")
    return str(rid).strip() if rid is not None else ""


def _load_contracts(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    if not isinstance(cfg, dict):
        raise RuntimeError(f"Invalid contracts YAML: {path}")
    return cfg


def _iter_rows(glide: GlideClient, table_name: str, *, page_limit: int, max_pages: int) -> Iterable[Dict[str, Any]]:
    for page in glide.fetch_table_rows_paginated(table_name, limit=page_limit, max_pages=max_pages):
        for row in page.rows:
            yield row


def _scan_rowid_and_collect_rfq_ids(
    glide: GlideClient,
    tables_cfg: Dict[str, Any],
    *,
    page_limit: int,
    max_pages: int,
) -> Tuple[Dict[str, TableScan], Set[str]]:
    scans: Dict[str, TableScan] = {}
    rfq_ids: Set[str] = set()
    table_keys = ("all_rfq", "all_products", "queries", "supplier_shares")

    for table_key in table_keys:
        table_name = tables_cfg[table_key]["table_name"]
        scan = TableScan()
        for row in _iter_rows(glide, table_name, page_limit=page_limit, max_pages=max_pages):
            scan.total_scanned += 1
            if not str(row.get("$rowID") or "").strip():
                scan.missing_dollar_rowid += 1
            if table_key == "all_rfq":
                rid = _row_id(row)
                if rid:
                    rfq_ids.add(rid)
        scans[table_key] = scan

    return scans, rfq_ids


def _scan_child_fk_integrity(
    glide: GlideClient,
    tables_cfg: Dict[str, Any],
    rfq_ids: Set[str],
    scans: Dict[str, TableScan],
    *,
    page_limit: int,
    max_pages: int,
) -> None:
    child_fk_map = {
        "all_products": tables_cfg["all_products"]["columns"]["rfq_id"],
        "queries": tables_cfg["queries"]["columns"]["rfq"],
        "supplier_shares": tables_cfg["supplier_shares"]["columns"]["rfq"],
    }

    for table_key, fk_col in child_fk_map.items():
        table_name = tables_cfg[table_key]["table_name"]
        scan = scans[table_key]
        seen_orphans: Set[str] = set()
        for row in _iter_rows(glide, table_name, page_limit=page_limit, max_pages=max_pages):
            ref = str(row.get(fk_col, "") or "").strip()
            if not ref:
                scan.blank_fk += 1
                continue
            if ref not in rfq_ids:
                scan.orphan_fk += 1
                if ref not in seen_orphans and len(scan.orphan_samples) < 10:
                    scan.orphan_samples.append(ref)
                    seen_orphans.add(ref)


def _extract_expected_fk_constraints(migration_path: str) -> List[str]:
    lines = Path(migration_path).read_text(encoding="utf-8").splitlines()
    out: List[str] = []
    current_table: Optional[str] = None

    for idx, raw in enumerate(lines, start=1):
        line = raw.strip()
        if line.startswith("CREATE TABLE IF NOT EXISTS rfq."):
            current_table = line.split("CREATE TABLE IF NOT EXISTS rfq.", 1)[1].split(" ", 1)[0]
            continue
        if current_table in ("products", "queries", "supplier_shares") and "REFERENCES rfq.rfqs(rfq_id)" in line:
            out.append(f"rfq.{current_table}:line {idx}: {line}")

    return out


def _find_row_id_return_line(path: str) -> Optional[Tuple[int, str]]:
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    for i, line in enumerate(lines):
        if line.strip().startswith("def _row_id("):
            for j in range(i + 1, min(i + 8, len(lines))):
                if "return " in lines[j]:
                    return (j + 1, lines[j].strip())
    return None


def _rowid_priority_ok(return_expr: str) -> bool:
    idx_dollar = return_expr.find('row.get("$rowID")')
    idx_plain = return_expr.find('row.get("rowID")')
    return idx_dollar != -1 and (idx_plain == -1 or idx_dollar < idx_plain)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Print PK/FK mapping report for Glide->Postgres ingestion and verify with read-only queryTables calls."
        )
    )
    ap.add_argument("--contracts", default="packages/contracts/glide_tables.yaml", help="Path to glide_tables.yaml")
    ap.add_argument("--migration", default="packages/db/migrations/002_rfq_schema.sql", help="Path to schema SQL")
    ap.add_argument("--limit", type=int, default=1000, help="Rows per queryTables call")
    ap.add_argument("--max_pages", type=int, default=200, help="Max pages per table")
    args = ap.parse_args()

    settings = Settings()
    if not settings.glide_api_key:
        print("[D] Exit status: 2 (GLIDE_API_KEY missing)")
        return 2

    cfg = _load_contracts(args.contracts)
    tables_cfg = cfg.get("tables") or {}

    required_tables = ("all_rfq", "all_products", "queries", "supplier_shares")
    mapping_mismatch_reasons: List[str] = []
    for t in required_tables:
        if t not in tables_cfg:
            mapping_mismatch_reasons.append(f"missing table mapping: {t}")

    expected_fk_cols = {
        "all_products": ("rfq_id", "3E2xY"),
        "queries": ("rfq", "iFLE0"),
        "supplier_shares": ("rfq", "fipwH"),
    }
    for table_key, (logical_col, expected_id) in expected_fk_cols.items():
        try:
            actual = str(tables_cfg[table_key]["columns"][logical_col])
            if actual != expected_id:
                mapping_mismatch_reasons.append(
                    f"{table_key}.{logical_col} expected {expected_id} but found {actual}"
                )
        except Exception:
            mapping_mismatch_reasons.append(f"missing FK column mapping: {table_key}.{logical_col}")

    glide = GlideClient(settings, contracts_path=args.contracts)
    page_limit = glide.max_allowed_limit(args.limit)
    max_pages = max(1, int(args.max_pages))

    print("[A] Mapping Summary (PK/FK)")
    print("PK mappings:")
    print("  all_rfq.$rowID -> rfq.rfqs.rfq_id")
    print("  all_products.$rowID -> rfq.products.product_id")
    print("  queries.$rowID -> rfq.queries.query_id")
    print("  supplier_shares.$rowID -> rfq.supplier_shares.share_id")
    print("FK mappings (from glide_tables.yaml):")
    products_fk = tables_cfg["all_products"]["columns"]["rfq_id"]
    queries_fk = tables_cfg["queries"]["columns"]["rfq"]
    shares_fk = tables_cfg["supplier_shares"]["columns"]["rfq"]
    print(f"  all_products[{products_fk}] -> rfq.rfqs.rfq_id")
    print(f"  queries[{queries_fk}] -> rfq.rfqs.rfq_id")
    print(f"  supplier_shares[{shares_fk}] -> rfq.rfqs.rfq_id")
    print("Postgres FK constraints expected from 002_rfq_schema.sql:")
    fk_constraints = _extract_expected_fk_constraints(args.migration)
    for c in fk_constraints:
        print(f"  {c}")
    print("")

    scans, rfq_ids = _scan_rowid_and_collect_rfq_ids(
        glide, tables_cfg, page_limit=page_limit, max_pages=max_pages
    )
    _scan_child_fk_integrity(
        glide, tables_cfg, rfq_ids, scans, page_limit=page_limit, max_pages=max_pages
    )

    print("[B] Live Glide Integrity Scan (counts + sample orphans)")
    print(f"scan_config: page_limit={page_limit} max_pages={max_pages}")
    print(f"all_rfq_ids_collected={len(rfq_ids)}")
    print("table_counts:")
    for table_key in ("all_rfq", "all_products", "queries", "supplier_shares"):
        s = scans[table_key]
        print(
            f"  {table_key}: total_scanned={s.total_scanned} "
            f"missing_$rowID={s.missing_dollar_rowid} blank_fk={s.blank_fk} orphan_fk={s.orphan_fk}"
        )
        if s.orphan_samples:
            print(f"    first_10_orphan_ids={s.orphan_samples}")
    print("")

    print("[C] Code Alignment (files/lines checked + patches if needed)")
    files_to_check = [
        "service/app/pipeline/nodes/upsert_tables.py",
        "service/app/pipeline/nodes/load_glide.py",
    ]
    code_alignment_ok = True
    for path in files_to_check:
        info = _find_row_id_return_line(path)
        if info is None:
            code_alignment_ok = False
            print(f"  {path}: _row_id return line not found")
            continue
        line_no, expr = info
        ok = _rowid_priority_ok(expr)
        if not ok:
            code_alignment_ok = False
        print(f"  {path}:{line_no}: {expr}  [dollar_rowid_first={'YES' if ok else 'NO'}]")
    if code_alignment_ok:
        print("  patches: none needed")
    else:
        print("  patches: required (rowID prioritized over $rowID)")
    print("")

    wide_missing_reasons: List[str] = []
    for table_key, s in scans.items():
        if s.total_scanned <= 0:
            continue
        missing_ratio = s.missing_dollar_rowid / float(s.total_scanned)
        if missing_ratio >= 0.10:
            wide_missing_reasons.append(
                f"{table_key} missing_$rowID_ratio={missing_ratio:.2%} (>=10%)"
            )

    exit_code = 0
    reasons: List[str] = []
    if mapping_mismatch_reasons:
        exit_code = 2
        reasons.extend(mapping_mismatch_reasons)
    if wide_missing_reasons:
        exit_code = 2
        reasons.extend(wide_missing_reasons)
    if not code_alignment_ok:
        exit_code = 2
        reasons.append("code alignment failed: $rowID not prioritized in _row_id")

    print("[D] Exit status")
    if exit_code == 0:
        print("exit_code=0")
        print("reason=all required checks passed (orphans may exist and are reported above)")
    else:
        print("exit_code=2")
        for r in reasons:
            print(f"reason={r}")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
