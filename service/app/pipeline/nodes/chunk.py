# service/app/pipeline/nodes/chunk.py
from __future__ import annotations

from typing import List
import hashlib

from langchain_text_splitters import RecursiveCharacterTextSplitter

from ..state import IngestState, Chunk, TextDoc


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def chunk_node(state: IngestState, chunk_size: int = 1200, chunk_overlap: int = 150) -> IngestState:
    splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)

    chunks: List[Chunk] = []

    for d in state.docs:
        txt = (d.text or "").strip()
        if not txt:
            continue

        parts = splitter.split_text(txt)
        for i, part in enumerate(parts):
            part = part.strip()
            if not part:
                continue
            # idempotency hash: stable by doc_type + rfq_id + optional ids + chunk text
            base = f"{d.doc_type}|{d.rfq_id}|{d.product_id or ''}|{d.query_id or ''}|{d.title}|{i}|{part}"
            content_sha = _sha(base)

            chunks.append(
                Chunk(
                    rfq_id=d.rfq_id,
                    doc_type=d.doc_type,
                    chunk_idx=i,
                    content_text=part,
                    content_sha=content_sha,
                    product_id=d.product_id,
                    query_id=d.query_id,
                    file_id=d.file_id,
                    page_num=None,
                    meta=d.meta or {},
                )
            )

    state.chunks = chunks
    return state