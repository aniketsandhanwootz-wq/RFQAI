# service/app/pipeline/ingest_graph.py
from __future__ import annotations

from typing import Any, Dict, List

import yaml
from langgraph.graph import StateGraph, END

from ..config import Settings
from ..integrations.glide_client import GlideClient
from ..integrations.drive_client import DriveClient
from ..integrations.fetch_client import FetchClient
from ..tools.db_tool import DB
from ..tools.embed_tool import Embedder
from ..tools.vector_tool import VectorWriter

from .state import IngestState
from .nodes.load_glide import load_glide_node
from .nodes.upsert import upsert_entities_node, upsert_chunks_node
from .nodes.build_docs import build_docs_node
from .nodes.resolve_sources import resolve_sources_node
from .nodes.extract_files import extract_files_node
from .nodes.chunk import chunk_node
from .nodes.embed import embed_node


def _load_glide_cfg(path: str = "packages/contracts/glide_tables.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["tables"]


def build_ingest_graph(settings: Settings) -> Any:
    glide = GlideClient(settings)
    db = DB(settings.database_url)

    drive = DriveClient(settings.gdrive_sa_json_path)
    fetcher = FetchClient(timeout_sec=settings.ingest_http_timeout_sec, max_mb=settings.ingest_file_max_mb)

    embedder = Embedder(
        api_key=settings.gemini_api_key,
        model=settings.gemini_embedding_model,
        output_dim=settings.embed_dim,
    )
    vw = VectorWriter(db=db)

    glide_tables_cfg = _load_glide_cfg()

    def n_load(state: IngestState) -> IngestState:
        return load_glide_node(state, glide)

    def n_upsert_entities(state: IngestState) -> IngestState:
        return upsert_entities_node(state, db, glide_tables_cfg)

    def n_build_docs(state: IngestState) -> IngestState:
        return build_docs_node(state, glide_tables_cfg)

    def n_resolve_sources(state: IngestState) -> IngestState:
        return resolve_sources_node(state, glide_tables_cfg)

    def n_extract_files(state: IngestState) -> IngestState:
        return extract_files_node(state, settings, db, drive, fetcher)

    def n_chunk(state: IngestState) -> IngestState:
        return chunk_node(state, chunk_size=settings.chunk_size, chunk_overlap=settings.chunk_overlap)

    def n_embed(state: IngestState) -> IngestState:
        return embed_node(state, embedder, batch_size=64)

    def n_upsert_chunks(state: IngestState) -> IngestState:
        return upsert_chunks_node(state, vw)

    g = StateGraph(IngestState)
    g.add_node("load_glide", n_load)
    g.add_node("upsert_entities", n_upsert_entities)
    g.add_node("build_docs", n_build_docs)
    g.add_node("resolve_sources", n_resolve_sources)
    g.add_node("extract_files", n_extract_files)
    g.add_node("chunk", n_chunk)
    g.add_node("embed", n_embed)
    g.add_node("upsert_chunks", n_upsert_chunks)

    g.set_entry_point("load_glide")
    g.add_edge("load_glide", "upsert_entities")
    g.add_edge("upsert_entities", "build_docs")
    g.add_edge("build_docs", "resolve_sources")
    g.add_edge("resolve_sources", "extract_files")
    g.add_edge("extract_files", "chunk")
    g.add_edge("chunk", "embed")
    g.add_edge("embed", "upsert_chunks")
    g.add_edge("upsert_chunks", END)

    return g.compile()


def run_ingest_full(rfq_id: str, settings: Settings) -> IngestState:
    graph = build_ingest_graph(settings)
    state = IngestState(rfq_id=rfq_id)
    out: IngestState = graph.invoke(state)
    return out


def run_ingest_full_prefetched(
    rfq_id: str,
    settings: Settings,
    *,
    prefetched_tables: Dict[str, List[Dict[str, Any]]],
) -> IngestState:
    """
    Same as run_ingest_full, but avoids extra Glide API calls.
    Used by cron/backfill to keep Glide costs minimal.
    """
    graph = build_ingest_graph(settings)

    # Seed state with prefetched data so load_glide_node can be skipped
    st = IngestState(rfq_id=rfq_id, prefetched=True)

    # We will invoke graph starting from upsert_entities by manually calling nodes.
    # This keeps changes minimal and avoids reworking LangGraph wiring.
    # (Phase-3 uses same nodes after load_glide.)
    from .nodes.load_glide import _row_id as _rid  # reuse helper
    glide_cfg = _load_glide_cfg()
    prod_col_rfq = glide_cfg["all_products"]["columns"]["rfq_id"]
    q_col_rfq = glide_cfg["queries"]["columns"]["rfq"]
    s_col_rfq = glide_cfg["supplier_shares"]["columns"]["rfq"]

    rfq_row = None
    for r in prefetched_tables["all_rfq"]:
        if _rid(r) == rfq_id:
            rfq_row = r
            break
    if not rfq_row:
        st.errors.append(f"RFQ not found in prefetched ALL RFQ table for rfq_id={rfq_id}")
        return st

    st.rfq_row = rfq_row
    st.products_rows = [p for p in prefetched_tables["all_products"] if str(p.get(prod_col_rfq, "")).strip() == rfq_id]
    st.queries_rows = [q for q in prefetched_tables["queries"] if str(q.get(q_col_rfq, "")).strip() == rfq_id]
    st.shares_rows = [s for s in prefetched_tables["supplier_shares"] if str(s.get(s_col_rfq, "")).strip() == rfq_id]

    # Now run the rest of the graph by invoking compiled graph with seeded state.
    # Because load_glide_node is first node, we will just call run_ingest_full on a graph variant later.
    # Minimal: call build_ingest_graph but override load_glide behavior by already having rfq_row.
    out: IngestState = graph.invoke(st)
    return out