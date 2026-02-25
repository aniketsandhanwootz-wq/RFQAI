# service/app/pipeline/nodes/load_glide.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from ...integrations.glide_client import GlideClient
from ..state import IngestState


def _row_id(row: Dict[str, Any]) -> Optional[str]:
    # Glide row id key is usually "rowID" (sometimes "RowID")
    return row.get("rowID") or row.get("RowID") or row.get("id")


def load_glide_node(state: IngestState, glide: GlideClient) -> IngestState:
    """
    Fetch all 4 tables (bulk) then filter in Python by rfq_id (ALL RFQ RowID).
    This keeps Glide calls minimal.
    """
    tables = glide.fetch_all_4_tables()

    rfq_id = state.rfq_id

    rfqs = tables["all_rfq"]
    products = tables["all_products"]
    queries = tables["queries"]
    shares = tables["supplier_shares"]

    # find RFQ row by rowID
    rfq_row = None
    for r in rfqs:
        if _row_id(r) == rfq_id:
            rfq_row = r
            break
    if not rfq_row:
        state.errors.append(f"RFQ not found in ALL RFQ table for rfq_id={rfq_id}")
        return state

    # product rows: column 3E2xY stores rfq id
    prod_col_rfq = glide.tables["all_products"]["columns"]["rfq_id"]
    products_rows = [p for p in products if str(p.get(prod_col_rfq, "")).strip() == rfq_id]

    # queries rows: column iFLE0 stores RFQ relation/id
    q_col_rfq = glide.tables["queries"]["columns"]["rfq"]
    queries_rows = [q for q in queries if str(q.get(q_col_rfq, "")).strip() == rfq_id]

    # share rows: column fipwH stores RFQ
    s_col_rfq = glide.tables["supplier_shares"]["columns"]["rfq"]
    shares_rows = [s for s in shares if str(s.get(s_col_rfq, "")).strip() == rfq_id]

    state.rfq_row = rfq_row
    state.products_rows = products_rows
    state.queries_rows = queries_rows
    state.shares_rows = shares_rows
    return state