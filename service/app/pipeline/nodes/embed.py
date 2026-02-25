# service/app/pipeline/nodes/embed.py
from __future__ import annotations

from typing import List

from ...tools.embed_tool import Embedder
from ..state import IngestState


def embed_node(state: IngestState, embedder: Embedder, batch_size: int = 64) -> IngestState:
    if not state.chunks:
        return state

    chunks = state.chunks
    i = 0
    while i < len(chunks):
        batch = chunks[i : i + batch_size]
        texts = [c.content_text for c in batch]
        vecs = embedder.embed_texts(texts)
        for c, v in zip(batch, vecs):
            c.embedding = v
        i += batch_size

    state.chunks = chunks
    return state