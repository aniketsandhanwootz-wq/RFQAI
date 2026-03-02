# service/app/pipeline/nodes/resolve_sources.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from ..state import IngestState


def _as_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, dict):
        out = []
        for _, val in v.items():
            s = str(val).strip()
            if s:
                out.append(s)
        return out
    s = str(v).strip()
    return [s] if s else []


def _norm_url(u: str) -> str:
    u = (u or "").strip()
    return u[:-1] if u.endswith("/") else u


def resolve_sources_node(state: IngestState, glide_tables_cfg: Dict[str, Any]) -> IngestState:
    """
    Builds state.file_targets = list of:
      {rfq_id, product_id?, query_id?, source_kind, url}
    """
    if not state.rfq_row:
        return state

    rfq_cols = glide_tables_cfg["all_rfq"]["columns"]
    prod_cols = glide_tables_cfg["all_products"]["columns"]
    q_cols = glide_tables_cfg["queries"]["columns"]

    targets: List[Dict[str, Any]] = []

    # RFQ root sources
    folder = _norm_url(state.rfq_row.get(rfq_cols["quotation_folder_link"]) or "")
    if folder:
        targets.append({"rfq_id": state.rfq_id, "source_kind": "RFQ_FOLDER", "url": folder})

    screen = _norm_url(state.rfq_row.get(rfq_cols["screen_url"]) or "")
    if screen:
        targets.append({"rfq_id": state.rfq_id, "source_kind": "DIRECT_URL", "url": screen})

    # Product sources
    for p in state.products_rows:
        pid = p.get("$rowID") or p.get("rowID") or p.get("RowID") or p.get("id")

        dwg = _norm_url(p.get(prod_cols["dwg_link"]) or "")
        rep = _norm_url(p.get(prod_cols["rep_url"]) or "")

        if dwg:
            targets.append({"rfq_id": state.rfq_id, "product_id": pid, "source_kind": "PRODUCT_LINK", "url": dwg})
        if rep:
            targets.append({"rfq_id": state.rfq_id, "product_id": pid, "source_kind": "PRODUCT_LINK", "url": rep})

        for u in _as_list(p.get(prod_cols["addl_photos"])):
            u = _norm_url(u)
            if u:
                targets.append({"rfq_id": state.rfq_id, "product_id": pid, "source_kind": "PRODUCT_LINK", "url": u})

        for u in _as_list(p.get(prod_cols["addl_files"])):
            u = _norm_url(u)
            if u:
                targets.append({"rfq_id": state.rfq_id, "product_id": pid, "source_kind": "PRODUCT_LINK", "url": u})

        # Internal extra files (JR0Lx) - may include links
        if "addl_files_internal" in prod_cols:
            for u in _as_list(p.get(prod_cols["addl_files_internal"])):
                u = _norm_url(u)
                if u:
                    targets.append({"rfq_id": state.rfq_id, "product_id": pid, "source_kind": "PRODUCT_LINK", "url": u})

    # Query attachment sources
    for q in state.queries_rows:
        qid = q.get("$rowID") or q.get("rowID") or q.get("RowID") or q.get("id")
        for u in _as_list(q.get(q_cols["images_attached"])):
            u = _norm_url(u)
            if u:
                targets.append({"rfq_id": state.rfq_id, "query_id": qid, "source_kind": "QUERY_ATTACHMENT", "url": u})

    # de-dupe
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
