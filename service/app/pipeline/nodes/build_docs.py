# service/app/pipeline/nodes/build_docs.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..state import IngestState, TextDoc


def _row_id(row: Dict[str, Any]) -> Optional[str]:
    return row.get("$rowID") or row.get("rowID") or row.get("RowID") or row.get("id")


def _safe(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (list, dict)):
        return str(v)
    return str(v).strip()


def build_docs_node(state: IngestState, glide_tables_cfg: Dict[str, Any]) -> IngestState:
    """
    Builds in-memory TextDoc objects:
      RFQ_BRIEF (1)
      PRODUCT_CARD (N)
      THREAD_MESSAGE (N)

    These will become vectors in Phase 3.
    """
    if not state.rfq_row:
        return state

    docs: List[TextDoc] = []

    rfq_cols = glide_tables_cfg["all_rfq"]["columns"]
    rfq = state.rfq_row

    rfq_text = "\n".join(
        [
            f"RFQ_ID: {state.rfq_id}",
            f"TITLE: {_safe(rfq.get(rfq_cols['title']))}",
            f"CUSTOMER: {_safe(rfq.get(rfq_cols['customer_name']))}",
            f"INDUSTRY: {_safe(rfq.get(rfq_cols['industry']))}",
            f"GEOGRAPHY: {_safe(rfq.get(rfq_cols['geography']))}",
            f"STANDARD: {_safe(rfq.get(rfq_cols['standard']))}",
            f"DEADLINE: {_safe(rfq.get(rfq_cols['deadline']))}",
            f"CURRENT_STATUS: {_safe(rfq.get(rfq_cols['current_status']))}",
            f"COLOR_QUERIES: {_safe(rfq.get(rfq_cols['color_queries']))}",
            f"LAST_STATUS_COMMENTS: {_safe(rfq.get(rfq_cols['last_status_comments']))}",
            f"QUOTATION_FOLDER_LINK: {_safe(rfq.get(rfq_cols['quotation_folder_link']))}",
        ]
    )

    docs.append(
        TextDoc(
            doc_type="RFQ_BRIEF",
            rfq_id=state.rfq_id,
            title=_safe(rfq.get(rfq_cols["title"])) or f"RFQ {state.rfq_id}",
            text=rfq_text,
            meta={"source": "glide:ALL_RFQ", "rfq_id": state.rfq_id}
        )
    )

    # Products
    prod_cols = glide_tables_cfg["all_products"]["columns"]
    for p in state.products_rows:
        pid = _row_id(p) or ""
        p_text = "\n".join(
            [
                f"RFQ_ID: {state.rfq_id}",
                f"PRODUCT_ID: {pid}",
                f"NAME: {_safe(p.get(prod_cols['name']))}",
                f"QTY: {_safe(p.get(prod_cols['qty']))}",
                f"TARGET_PRICE: {_safe(p.get(prod_cols['target_price']))}",
                f"DETAILS: {_safe(p.get(prod_cols['details']))}",
                f"DWG_LINK: {_safe(p.get(prod_cols['dwg_link']))}",
                f"REP_URL: {_safe(p.get(prod_cols['rep_url']))}",
            ]
        )
        docs.append(
            TextDoc(
                doc_type="PRODUCT_CARD",
                rfq_id=state.rfq_id,
                product_id=pid or None,
                title=_safe(p.get(prod_cols["name"])) or f"Product {pid}",
                text=p_text,
                meta={"source": "glide:ALL_PRODUCTS", "rfq_id": state.rfq_id, "product_id": pid}
            )
        )

    # Queries/messages
    q_cols = glide_tables_cfg["queries"]["columns"]
    for q in state.queries_rows:
        qid = _row_id(q) or ""
        q_text = "\n".join(
            [
                f"RFQ_ID: {state.rfq_id}",
                f"QUERY_ID: {qid}",
                f"THREAD_ID: {_safe(q.get(q_cols['thread_id']))}",
                f"USER: {_safe(q.get(q_cols['user']))}",
                f"QUERY_TYPE: {_safe(q.get(q_cols['query_type']))}",
                f"STATUS: {_safe(q.get(q_cols['status']))}",
                f"TIME_ADDED: {_safe(q.get(q_cols['time_added']))}",
                f"PRODUCTS_SELECTED: {_safe(q.get(q_cols['products_selected']))}",
                f"COMMENT: {_safe(q.get(q_cols['comment']))}",
            ]
        )
        docs.append(
            TextDoc(
                doc_type="THREAD_MESSAGE",
                rfq_id=state.rfq_id,
                query_id=qid or None,
                title=f"Query {qid}",
                text=q_text,
                meta={"source": "glide:QUERIES", "rfq_id": state.rfq_id, "query_id": qid}
            )
        )

    state.docs = docs
    return state
