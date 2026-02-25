# service/app/pipeline/ingest_graph.py
from __future__ import annotations

from typing import Any, Dict

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