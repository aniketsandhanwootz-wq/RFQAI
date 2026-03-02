from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
import json
from typing import Any, Dict, List, Optional, Set

from dateutil import parser as dtparser

from ...tools.db_tool import DB, tx


@dataclass
class PageUpsertStats:
    seen: int = 0
    changed: int = 0
    unchanged: int = 0
    skipped: int = 0
    changed_rfq_ids: Set[str] = field(default_factory=set)


def _row_id(row: Dict[str, Any]) -> Optional[str]:
    return row.get("rowID") or row.get("RowID") or row.get("id")


def _to_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return None


def _to_ts(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        return dtparser.parse(s)
    except Exception:
        return None


def _to_json_list(v: Any) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    s = str(v).strip()
    if not s:
        return []
    if "," in s:
        return [x.strip() for x in s.split(",") if x.strip()]
    return [s]


def _canonical_json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=True, separators=(",", ":"), sort_keys=True, default=str)


def _row_hash(row: Dict[str, Any]) -> str:
    return sha256(_canonical_json(row).encode("utf-8")).hexdigest()


def _record_changed_rfq(cur: Any, run_id: str, rfq_id: str) -> None:
    cur.execute(
        """
        INSERT INTO rfq.ingest_run_changed_rfqs (run_id, rfq_id)
        VALUES (%(run_id)s, %(rfq_id)s)
        ON CONFLICT (run_id, rfq_id) DO NOTHING
        """,
        {"run_id": run_id, "rfq_id": rfq_id},
    )


def _existing_rfq_ids_for_page(cur: Any, rfq_ids: Set[str]) -> Set[str]:
    if not rfq_ids:
        return set()

    cur.execute(
        """
        SELECT rfq_id
        FROM rfq.rfqs
        WHERE rfq_id = ANY(%s::text[])
        """,
        (list(rfq_ids),),
    )
    return {str(r[0]) for r in cur.fetchall()}


def upsert_rfqs(rows_page: List[Dict[str, Any]], *, db: DB, rfq_cols: Dict[str, str], run_id: str) -> PageUpsertStats:
    stats = PageUpsertStats()

    with tx(db) as cur:
        for rfq in rows_page:
            stats.seen += 1
            rfq_id = _row_id(rfq)
            if not rfq_id:
                stats.skipped += 1
                continue

            row_hash = _row_hash(rfq)
            cur.execute(
                """
                INSERT INTO rfq.rfqs (
                  rfq_id, title, deadline, industry, geography, standard, customer_name,
                  quotation_folder_link, screen_url, color_queries,
                  current_status, team, required_by,
                  archive, received_date, rfq_created_date,
                  created_by, sales_por, shared_members, rfq_poc,
                  last_status_updated_by, last_status_updated_at, last_status_comments, urgent,
                  raw_glide, source_updated_at, row_hash, last_changed_run_id, ingested_at
                )
                VALUES (
                  %(rfq_id)s, %(title)s, %(deadline)s, %(industry)s, %(geography)s, %(standard)s, %(customer)s,
                  %(folder)s, %(screen)s, %(color)s,
                  %(status)s, %(team)s, %(required_by)s,
                  %(archive)s, %(received)s, %(created_date)s,
                  %(created_by)s, %(sales_por)s, %(shared_members)s, %(rfq_poc)s,
                  %(last_by)s, %(last_at)s, %(last_comments)s, %(urgent)s,
                  %(raw)s, %(source_updated_at)s, %(row_hash)s, %(run_id)s, now()
                )
                ON CONFLICT (rfq_id) DO UPDATE SET
                  title=EXCLUDED.title,
                  deadline=EXCLUDED.deadline,
                  industry=EXCLUDED.industry,
                  geography=EXCLUDED.geography,
                  standard=EXCLUDED.standard,
                  customer_name=EXCLUDED.customer_name,
                  quotation_folder_link=EXCLUDED.quotation_folder_link,
                  screen_url=EXCLUDED.screen_url,
                  color_queries=EXCLUDED.color_queries,
                  current_status=EXCLUDED.current_status,
                  team=EXCLUDED.team,
                  required_by=EXCLUDED.required_by,
                  archive=EXCLUDED.archive,
                  received_date=EXCLUDED.received_date,
                  rfq_created_date=EXCLUDED.rfq_created_date,
                  created_by=EXCLUDED.created_by,
                  sales_por=EXCLUDED.sales_por,
                  shared_members=EXCLUDED.shared_members,
                  rfq_poc=EXCLUDED.rfq_poc,
                  last_status_updated_by=EXCLUDED.last_status_updated_by,
                  last_status_updated_at=EXCLUDED.last_status_updated_at,
                  last_status_comments=EXCLUDED.last_status_comments,
                  urgent=EXCLUDED.urgent,
                  raw_glide=EXCLUDED.raw_glide,
                  source_updated_at=EXCLUDED.source_updated_at,
                  row_hash=EXCLUDED.row_hash,
                  last_changed_run_id=EXCLUDED.last_changed_run_id,
                  ingested_at=now()
                WHERE rfq.rfqs.row_hash IS DISTINCT FROM EXCLUDED.row_hash
                RETURNING rfq_id
                """,
                {
                    "rfq_id": rfq_id,
                    "title": rfq.get(rfq_cols["title"]),
                    "deadline": _to_ts(rfq.get(rfq_cols["deadline"])),
                    "industry": rfq.get(rfq_cols["industry"]),
                    "geography": rfq.get(rfq_cols["geography"]),
                    "standard": rfq.get(rfq_cols["standard"]),
                    "customer": rfq.get(rfq_cols["customer_name"]),
                    "folder": rfq.get(rfq_cols["quotation_folder_link"]),
                    "screen": rfq.get(rfq_cols["screen_url"]),
                    "color": rfq.get(rfq_cols["color_queries"]),
                    "status": rfq.get(rfq_cols["current_status"]),
                    "team": rfq.get(rfq_cols["team"]),
                    "required_by": rfq.get(rfq_cols["required_by"]),
                    "archive": _to_bool(rfq.get(rfq_cols["archive"])),
                    "received": _to_ts(rfq.get(rfq_cols["received_date"])),
                    "created_date": _to_ts(rfq.get(rfq_cols["rfq_created_date"])),
                    "created_by": rfq.get(rfq_cols["created_by"]),
                    "sales_por": rfq.get(rfq_cols["sales_por"]),
                    "shared_members": _to_json_list(rfq.get(rfq_cols["shared_members"])),
                    "rfq_poc": rfq.get(rfq_cols["rfq_poc"]),
                    "last_by": rfq.get(rfq_cols["last_status_updated_by"]),
                    "last_at": _to_ts(rfq.get(rfq_cols["last_status_updated_at"])),
                    "last_comments": rfq.get(rfq_cols["last_status_comments"]),
                    "urgent": _to_bool(rfq.get(rfq_cols["urgent"])),
                    "raw": rfq,
                    "source_updated_at": _to_ts(rfq.get(rfq_cols.get("last_updated_date", ""))),
                    "row_hash": row_hash,
                    "run_id": run_id,
                },
            )
            changed = cur.fetchone()
            if changed:
                stats.changed += 1
                stats.changed_rfq_ids.add(rfq_id)
                _record_changed_rfq(cur, run_id, rfq_id)
            else:
                stats.unchanged += 1

    return stats


def upsert_products(rows_page: List[Dict[str, Any]], *, db: DB, prod_cols: Dict[str, str], run_id: str) -> PageUpsertStats:
    stats = PageUpsertStats()

    with tx(db) as cur:
        page_rfq_ids = {
            str(p.get(prod_cols["rfq_id"], "")).strip()
            for p in rows_page
            if str(p.get(prod_cols["rfq_id"], "")).strip()
        }
        existing_rfq_ids = _existing_rfq_ids_for_page(cur, page_rfq_ids)

        for p in rows_page:
            stats.seen += 1
            product_id = _row_id(p)
            if not product_id:
                stats.skipped += 1
                continue

            rfq_id = str(p.get(prod_cols["rfq_id"], "")).strip()
            if not rfq_id:
                stats.skipped += 1
                continue
            if rfq_id not in existing_rfq_ids:
                stats.skipped += 1
                continue

            qty_raw = p.get(prod_cols["qty"])
            tp_raw = p.get(prod_cols["target_price"])
            qty_num = None
            tp_num = None
            try:
                qty_num = float(qty_raw) if qty_raw not in (None, "") else None
            except Exception:
                qty_num = None
            try:
                tp_num = float(tp_raw) if tp_raw not in (None, "") else None
            except Exception:
                tp_num = None

            row_hash = _row_hash(p)
            cur.execute(
                """
                INSERT INTO rfq.products (
                  product_id, rfq_id,
                  name, qty, qty_raw, details,
                  target_price, target_price_raw,
                  dwg_link, rep_url,
                  addl_photos, addl_files, addl_files_internal, product_photo,
                  sr_no, choice_all, archive,
                  raw_glide, source_updated_at, row_hash, last_changed_run_id, ingested_at
                )
                VALUES (
                  %(product_id)s, %(rfq_id)s,
                  %(name)s, %(qty)s, %(qty_raw)s, %(details)s,
                  %(tp)s, %(tp_raw)s,
                  %(dwg)s, %(rep)s,
                  %(photos)s, %(files)s, %(files_internal)s, %(photo)s,
                  %(sr_no)s, %(choice_all)s, %(archive)s,
                  %(raw)s, %(source_updated_at)s, %(row_hash)s, %(run_id)s, now()
                )
                ON CONFLICT (product_id) DO UPDATE SET
                  rfq_id=EXCLUDED.rfq_id,
                  name=EXCLUDED.name,
                  qty=EXCLUDED.qty,
                  qty_raw=EXCLUDED.qty_raw,
                  details=EXCLUDED.details,
                  target_price=EXCLUDED.target_price,
                  target_price_raw=EXCLUDED.target_price_raw,
                  dwg_link=EXCLUDED.dwg_link,
                  rep_url=EXCLUDED.rep_url,
                  addl_photos=EXCLUDED.addl_photos,
                  addl_files=EXCLUDED.addl_files,
                  addl_files_internal=EXCLUDED.addl_files_internal,
                  product_photo=EXCLUDED.product_photo,
                  sr_no=EXCLUDED.sr_no,
                  choice_all=EXCLUDED.choice_all,
                  archive=EXCLUDED.archive,
                  raw_glide=EXCLUDED.raw_glide,
                  source_updated_at=EXCLUDED.source_updated_at,
                  row_hash=EXCLUDED.row_hash,
                  last_changed_run_id=EXCLUDED.last_changed_run_id,
                  ingested_at=now()
                WHERE rfq.products.row_hash IS DISTINCT FROM EXCLUDED.row_hash
                RETURNING rfq_id
                """,
                {
                    "product_id": product_id,
                    "rfq_id": rfq_id,
                    "name": p.get(prod_cols["name"]),
                    "qty": qty_num,
                    "qty_raw": None if qty_raw is None else str(qty_raw),
                    "details": p.get(prod_cols["details"]),
                    "tp": tp_num,
                    "tp_raw": None if tp_raw is None else str(tp_raw),
                    "dwg": p.get(prod_cols["dwg_link"]),
                    "rep": p.get(prod_cols["rep_url"]),
                    "photos": p.get(prod_cols["addl_photos"]) or [],
                    "files": p.get(prod_cols["addl_files"]) or [],
                    "files_internal": p.get(prod_cols["addl_files_internal"]) or {},
                    "photo": p.get(prod_cols["product_photo"]) or [],
                    "sr_no": p.get(prod_cols["sr_no"]),
                    "choice_all": p.get(prod_cols["choice_all"]) or {},
                    "archive": _to_bool(p.get(prod_cols["archive"])),
                    "raw": p,
                    "source_updated_at": None,
                    "row_hash": row_hash,
                    "run_id": run_id,
                },
            )
            changed = cur.fetchone()
            if changed:
                stats.changed += 1
                changed_rfq_id = str(changed[0])
                stats.changed_rfq_ids.add(changed_rfq_id)
                _record_changed_rfq(cur, run_id, changed_rfq_id)
            else:
                stats.unchanged += 1

    return stats


def upsert_queries(rows_page: List[Dict[str, Any]], *, db: DB, q_cols: Dict[str, str], run_id: str) -> PageUpsertStats:
    stats = PageUpsertStats()

    with tx(db) as cur:
        page_rfq_ids = {
            str(q.get(q_cols["rfq"], "")).strip()
            for q in rows_page
            if str(q.get(q_cols["rfq"], "")).strip()
        }
        existing_rfq_ids = _existing_rfq_ids_for_page(cur, page_rfq_ids)

        for q in rows_page:
            stats.seen += 1
            query_id = _row_id(q)
            if not query_id:
                stats.skipped += 1
                continue

            rfq_id = str(q.get(q_cols["rfq"], "")).strip()
            if not rfq_id:
                stats.skipped += 1
                continue
            if rfq_id not in existing_rfq_ids:
                stats.skipped += 1
                continue

            row_hash = _row_hash(q)
            cur.execute(
                """
                INSERT INTO rfq.queries (
                  query_id, rfq_id,
                  thread_id, query_type, comment, "user",
                  time_added, status, show_upload,
                  images_attached, products_selected,
                  raw_glide, source_updated_at, row_hash, last_changed_run_id, ingested_at
                )
                VALUES (
                  %(query_id)s, %(rfq_id)s,
                  %(thread_id)s, %(query_type)s, %(comment)s, %(user)s,
                  %(time_added)s, %(status)s, %(show_upload)s,
                  %(images)s, %(products)s,
                  %(raw)s, %(source_updated_at)s, %(row_hash)s, %(run_id)s, now()
                )
                ON CONFLICT (query_id) DO UPDATE SET
                  rfq_id=EXCLUDED.rfq_id,
                  thread_id=EXCLUDED.thread_id,
                  query_type=EXCLUDED.query_type,
                  comment=EXCLUDED.comment,
                  "user"=EXCLUDED."user",
                  time_added=EXCLUDED.time_added,
                  status=EXCLUDED.status,
                  show_upload=EXCLUDED.show_upload,
                  images_attached=EXCLUDED.images_attached,
                  products_selected=EXCLUDED.products_selected,
                  raw_glide=EXCLUDED.raw_glide,
                  source_updated_at=EXCLUDED.source_updated_at,
                  row_hash=EXCLUDED.row_hash,
                  last_changed_run_id=EXCLUDED.last_changed_run_id,
                  ingested_at=now()
                WHERE rfq.queries.row_hash IS DISTINCT FROM EXCLUDED.row_hash
                RETURNING rfq_id
                """,
                {
                    "query_id": query_id,
                    "rfq_id": rfq_id,
                    "thread_id": q.get(q_cols["thread_id"]),
                    "query_type": q.get(q_cols["query_type"]),
                    "comment": q.get(q_cols["comment"]),
                    "user": q.get(q_cols["user"]),
                    "time_added": _to_ts(q.get(q_cols["time_added"])),
                    "status": q.get(q_cols["status"]),
                    "show_upload": _to_bool(q.get(q_cols["show_upload"])),
                    "images": q.get(q_cols["images_attached"]) or [],
                    "products": q.get(q_cols["products_selected"]) or [],
                    "raw": q,
                    "source_updated_at": None,
                    "row_hash": row_hash,
                    "run_id": run_id,
                },
            )
            changed = cur.fetchone()
            if changed:
                stats.changed += 1
                changed_rfq_id = str(changed[0])
                stats.changed_rfq_ids.add(changed_rfq_id)
                _record_changed_rfq(cur, run_id, changed_rfq_id)
            else:
                stats.unchanged += 1

    return stats


def upsert_supplier_shares(
    rows_page: List[Dict[str, Any]], *, db: DB, s_cols: Dict[str, str], run_id: str
) -> PageUpsertStats:
    stats = PageUpsertStats()

    with tx(db) as cur:
        page_rfq_ids = {
            str(s.get(s_cols["rfq"], "")).strip()
            for s in rows_page
            if str(s.get(s_cols["rfq"], "")).strip()
        }
        existing_rfq_ids = _existing_rfq_ids_for_page(cur, page_rfq_ids)

        for s in rows_page:
            stats.seen += 1
            share_id = _row_id(s)
            if not share_id:
                stats.skipped += 1
                continue

            rfq_id = str(s.get(s_cols["rfq"], "")).strip()
            if not rfq_id:
                stats.skipped += 1
                continue
            if rfq_id not in existing_rfq_ids:
                stats.skipped += 1
                continue

            row_hash = _row_hash(s)
            cur.execute(
                """
                INSERT INTO rfq.supplier_shares (
                  share_id, rfq_id,
                  supplier_name, status, shared_by, user_email, rfq_link,
                  shared_products, shared_date, quotation_shared_date, quotation_received_by,
                  raw_glide, source_updated_at, row_hash, last_changed_run_id, ingested_at
                )
                VALUES (
                  %(share_id)s, %(rfq_id)s,
                  %(supplier)s, %(status)s, %(shared_by)s, %(email)s, %(rfq_link)s,
                  %(shared_products)s, %(shared_date)s, %(q_shared_date)s, %(q_received_by)s,
                  %(raw)s, %(source_updated_at)s, %(row_hash)s, %(run_id)s, now()
                )
                ON CONFLICT (share_id) DO UPDATE SET
                  rfq_id=EXCLUDED.rfq_id,
                  supplier_name=EXCLUDED.supplier_name,
                  status=EXCLUDED.status,
                  shared_by=EXCLUDED.shared_by,
                  user_email=EXCLUDED.user_email,
                  rfq_link=EXCLUDED.rfq_link,
                  shared_products=EXCLUDED.shared_products,
                  shared_date=EXCLUDED.shared_date,
                  quotation_shared_date=EXCLUDED.quotation_shared_date,
                  quotation_received_by=EXCLUDED.quotation_received_by,
                  raw_glide=EXCLUDED.raw_glide,
                  source_updated_at=EXCLUDED.source_updated_at,
                  row_hash=EXCLUDED.row_hash,
                  last_changed_run_id=EXCLUDED.last_changed_run_id,
                  ingested_at=now()
                WHERE rfq.supplier_shares.row_hash IS DISTINCT FROM EXCLUDED.row_hash
                RETURNING rfq_id
                """,
                {
                    "share_id": share_id,
                    "rfq_id": rfq_id,
                    "supplier": s.get(s_cols["supplier"]),
                    "status": s.get(s_cols["status"]),
                    "shared_by": s.get(s_cols["shared_by"]),
                    "email": s.get(s_cols["user_email"]),
                    "rfq_link": s.get(s_cols["rfq_link"]),
                    "shared_products": s.get(s_cols["shared_products"]) or [],
                    "shared_date": _to_ts(s.get(s_cols["shared_date"])),
                    "q_shared_date": _to_ts(s.get(s_cols["quotation_shared_date"])),
                    "q_received_by": s.get(s_cols["quotation_received_by"]),
                    "raw": s,
                    "source_updated_at": None,
                    "row_hash": row_hash,
                    "run_id": run_id,
                },
            )
            changed = cur.fetchone()
            if changed:
                stats.changed += 1
                changed_rfq_id = str(changed[0])
                stats.changed_rfq_ids.add(changed_rfq_id)
                _record_changed_rfq(cur, run_id, changed_rfq_id)
            else:
                stats.unchanged += 1

    return stats
