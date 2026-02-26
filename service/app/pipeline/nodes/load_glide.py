# service/app/pipeline/nodes/load_glide.py
from __future__ import annotations

from typing import Any, Dict, Optional

from ...integrations.glide_client import GlideClient
from ..state import IngestState


def _row_id(row: Dict[str, Any]) -> Optional[str]:
    # Glide row id key is usually "rowID" (sometimes "RowID")
    return row.get("rowID") or row.get("RowID") or row.get("id")


def load_glide_node(state: IngestState, glide: GlideClient) -> IngestState:
    """
    If state already has rfq_row (prefetched mode), do NOT call Glide again.
    Otherwise fetch all 4 tables once and filter by rfq_id.
    """
    if state.prefetched or state.rfq_row is not None:
        return state

    tables = glide.fetch_all_4_tables()
    rfq_id = state.rfq_id

    rfqs = tables["all_rfq"]
    products = tables["all_products"]
    queries = tables["queries"]
    shares = tables["supplier_shares"]

    rfq_row = None
    for r in rfqs:
        if _row_id(r) == rfq_id:
            rfq_row = r
            break
    if not rfq_row:
        state.errors.append(f"RFQ not found in ALL RFQ table for rfq_id={rfq_id}")
        return state

    prod_col_rfq = glide.tables["all_products"]["columns"]["rfq_id"]
    q_col_rfq = glide.tables["queries"]["columns"]["rfq"]
    s_col_rfq = glide.tables["supplier_shares"]["columns"]["rfq"]

    state.rfq_row = rfq_row
    state.products_rows = [p for p in products if str(p.get(prod_col_rfq, "")).strip() == rfq_id]
    state.queries_rows = [q for q in queries if str(q.get(q_col_rfq, "")).strip() == rfq_id]
    state.shares_rows = [s for s in shares if str(s.get(s_col_rfq, "")).strip() == rfq_id]
    return state