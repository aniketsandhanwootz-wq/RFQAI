# service/app/pipeline/nodes/upsert.py
from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime

from dateutil import parser as dtparser

from ...tools.db_tool import DB, tx
from ...tools.vector_tool import VectorWriter
from ..state import IngestState


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


def upsert_entities_node(state: IngestState, db: DB, glide_tables_cfg: Dict[str, Any]) -> IngestState:
    """
    Upsert 4 entity tables into rfq.*.
    Idempotent + cron-friendly (updates rows on conflict).
    """
    if not state.rfq_row:
        return state

    rfq_cols = glide_tables_cfg["all_rfq"]["columns"]
    prod_cols = glide_tables_cfg["all_products"]["columns"]
    q_cols = glide_tables_cfg["queries"]["columns"]
    s_cols = glide_tables_cfg["supplier_shares"]["columns"]

    rfq = state.rfq_row

    with tx(db) as cur:
        # -------------------
        # rfq.rfqs
        # -------------------
        cur.execute(
            """
            INSERT INTO rfq.rfqs (
              rfq_id, title, deadline, industry, geography, standard, customer_name,
              quotation_folder_link, screen_url, color_queries,
              current_status, team, required_by,
              archive, received_date, rfq_created_date,
              created_by, sales_por, shared_members, rfq_poc,
              last_status_updated_by, last_status_updated_at, last_status_comments, urgent,
              raw_glide, source_updated_at, ingested_at
            )
            VALUES (
              %(rfq_id)s, %(title)s, %(deadline)s, %(industry)s, %(geography)s, %(standard)s, %(customer)s,
              %(folder)s, %(screen)s, %(color)s,
              %(status)s, %(team)s, %(required_by)s,
              %(archive)s, %(received)s, %(created_date)s,
              %(created_by)s, %(sales_por)s, %(shared_members)s, %(rfq_poc)s,
              %(last_by)s, %(last_at)s, %(last_comments)s, %(urgent)s,
              %(raw)s, %(source_updated_at)s, now()
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
              ingested_at=now()
            ;
            """,
            {
                "rfq_id": state.rfq_id,
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
                "shared_members": rfq.get(rfq_cols["shared_members"]) or [],
                "rfq_poc": rfq.get(rfq_cols["rfq_poc"]),
                "last_by": rfq.get(rfq_cols["last_status_updated_by"]),
                "last_at": _to_ts(rfq.get(rfq_cols["last_status_updated_at"])),
                "last_comments": rfq.get(rfq_cols["last_status_comments"]),
                "urgent": _to_bool(rfq.get(rfq_cols["urgent"])),
                "raw": rfq,
                "source_updated_at": _to_ts(rfq.get(rfq_cols.get("last_updated_date", ""))),
            },
        )

        # -------------------
        # rfq.products
        # -------------------
        for p in state.products_rows:
            pid = _row_id(p)
            if not pid:
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

            cur.execute(
                """
                INSERT INTO rfq.products (
                  product_id, rfq_id,
                  name, qty, qty_raw, details,
                  target_price, target_price_raw,
                  dwg_link, rep_url,
                  addl_photos, addl_files, addl_files_internal, product_photo,
                  sr_no, choice_all, archive,
                  raw_glide, source_updated_at, ingested_at
                )
                VALUES (
                  %(product_id)s, %(rfq_id)s,
                  %(name)s, %(qty)s, %(qty_raw)s, %(details)s,
                  %(tp)s, %(tp_raw)s,
                  %(dwg)s, %(rep)s,
                  %(photos)s, %(files)s, %(files_internal)s, %(photo)s,
                  %(sr_no)s, %(choice_all)s, %(archive)s,
                  %(raw)s, %(source_updated_at)s, now()
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
                  ingested_at=now()
                ;
                """,
                {
                    "product_id": pid,
                    "rfq_id": state.rfq_id,
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
                },
            )

        # -------------------
        # rfq.queries
        # -------------------
        for q in state.queries_rows:
            qid = _row_id(q)
            if not qid:
                continue

            cur.execute(
                """
                INSERT INTO rfq.queries (
                  query_id, rfq_id,
                  thread_id, query_type, comment, "user",
                  time_added, status, show_upload,
                  images_attached, products_selected,
                  raw_glide, source_updated_at, ingested_at
                )
                VALUES (
                  %(query_id)s, %(rfq_id)s,
                  %(thread_id)s, %(query_type)s, %(comment)s, %(user)s,
                  %(time_added)s, %(status)s, %(show_upload)s,
                  %(images)s, %(products)s,
                  %(raw)s, %(source_updated_at)s, now()
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
                  ingested_at=now()
                ;
                """,
                {
                    "query_id": qid,
                    "rfq_id": state.rfq_id,
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
                },
            )

        # -------------------
        # rfq.supplier_shares
        # -------------------
        for s in state.shares_rows:
            sid = _row_id(s)
            if not sid:
                continue

            cur.execute(
                """
                INSERT INTO rfq.supplier_shares (
                  share_id, rfq_id,
                  supplier_name, status, shared_by, user_email, rfq_link,
                  shared_products, shared_date, quotation_shared_date, quotation_received_by,
                  raw_glide, source_updated_at, ingested_at
                )
                VALUES (
                  %(share_id)s, %(rfq_id)s,
                  %(supplier)s, %(status)s, %(shared_by)s, %(email)s, %(rfq_link)s,
                  %(shared_products)s, %(shared_date)s, %(q_shared_date)s, %(q_received_by)s,
                  %(raw)s, %(source_updated_at)s, now()
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
                  ingested_at=now()
                ;
                """,
                {
                    "share_id": sid,
                    "rfq_id": state.rfq_id,
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
                },
            )

    return state


def upsert_chunks_node(state: IngestState, vw: VectorWriter) -> IngestState:
    """
    Inserts embedded chunks into rfq.chunks (idempotent).
    """
    try:
        n = vw.upsert_chunks(state.chunks)
    except Exception as e:
        state.errors.append(f"upsert_chunks failed: {e}")
        return state

    state.warnings.append(f"vectors_inserted={n}")
    return state