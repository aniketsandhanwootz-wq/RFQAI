# service/app/pipeline/nodes/resolve_sources.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
import re

from ..state import IngestState


def _as_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    return [s] if s else []


def _is_drive_link(url: str) -> bool:
    u = (url or "").lower()
    return "drive.google.com" in u or "docs.google.com" in u


def resolve_sources_node(state: IngestState, glide_tables_cfg: Dict[str, Any]) -> IngestState:
    """
    Builds state.file_targets = list of {rfq_id, product_id?, query_id?, source_kind, url}
    """
    if not state.rfq_row:
        return state

    rfq_cols = glide_tables_cfg["all_rfq"]["columns"]
    prod_cols = glide_tables_cfg["all_products"]["columns"]
    q_cols = glide_tables_cfg["queries"]["columns"]

    targets: List[Dict[str, Any]] = []

    # RFQ root sources
    folder = (state.rfq_row.get(rfq_cols["quotation_folder_link"]) or "").strip()
    if folder:
        targets.append({"rfq_id": state.rfq_id, "source_kind": "RFQ_FOLDER", "url": folder})

    screen = (state.rfq_row.get(rfq_cols["screen_url"]) or "").strip()
    if screen:
        targets.append({"rfq_id": state.rfq_id, "source_kind": "DIRECT_URL", "url": screen})

    # Product sources
    for p in state.products_rows:
        pid = p.get("rowID") or p.get("RowID") or p.get("id")
        dwg = (p.get(prod_cols["dwg_link"]) or "").strip()
        rep = (p.get(prod_cols["rep_url"]) or "").strip()

        if dwg:
            targets.append({"rfq_id": state.rfq_id, "product_id": pid, "source_kind": "PRODUCT_LINK", "url": dwg})
        if rep:
            targets.append({"rfq_id": state.rfq_id, "product_id": pid, "source_kind": "PRODUCT_LINK", "url": rep})

        for u in _as_list(p.get(prod_cols["addl_photos"])):
            targets.append({"rfq_id": state.rfq_id, "product_id": pid, "source_kind": "PRODUCT_LINK", "url": u})
        for u in _as_list(p.get(prod_cols["addl_files"])):
            targets.append({"rfq_id": state.rfq_id, "product_id": pid, "source_kind": "PRODUCT_LINK", "url": u})

    # Query attachment sources
    for q in state.queries_rows:
        qid = q.get("rowID") or q.get("RowID") or q.get("id")
        for u in _as_list(q.get(q_cols["images_attached"])):
            targets.append({"rfq_id": state.rfq_id, "query_id": qid, "source_kind": "QUERY_ATTACHMENT", "url": u})

    # de-dupe by url+context
    seen = set()
    uniq = []
    for t in targets:
        key = (t.get("rfq_id"), t.get("product_id"), t.get("query_id"), t.get("url"))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(t)

    state.file_targets = uniq
    return state