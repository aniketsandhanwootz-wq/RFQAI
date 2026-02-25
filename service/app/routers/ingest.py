# service/app/routers/ingest.py
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..config import Settings
from ..pipeline.ingest_graph import run_ingest_full

router = APIRouter(prefix="/ingest", tags=["ingest"])

@router.post("/rfq/{rfq_id}")
def ingest_rfq(rfq_id: str) -> dict:
    settings = Settings()
    settings.validate_runtime()

    st = run_ingest_full(rfq_id, settings)
    if st.errors:
        raise HTTPException(status_code=400, detail={"errors": st.errors, "warnings": st.warnings})

    return {
        "ok": True,
        "rfq_id": rfq_id,
        "products": len(st.products_rows),
        "queries": len(st.queries_rows),
        "supplier_shares": len(st.shares_rows),
        "docs": len(st.docs),
        "chunks": len(st.chunks),
        "warnings": st.warnings[:20],
    }