# service/app/tools/vector_tool.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .db_tool import DB, tx
from ..pipeline.state import Chunk


def _vector_literal(vec: Sequence[float]) -> str:
    # Safe: only floats; used as a SQL literal cast to vector.
    return "[" + ",".join(f"{float(x):.10g}" for x in vec) + "]"


@dataclass(frozen=True)
class VectorWriter:
    db: DB

    def upsert_chunks(self, chunks: List[Chunk]) -> int:
        """
        Insert vectors into rfq.chunks.
        Idempotent via UNIQUE(rfq_id, doc_type, content_sha).
        """
        if not chunks:
            return 0

        inserted = 0
        with tx(self.db) as cur:
            for c in chunks:
                if not c.embedding:
                    continue

                cur.execute(
                    """
                    INSERT INTO rfq.chunks (
                      rfq_id, doc_type,
                      product_id, query_id, file_id,
                      page_num, chunk_idx,
                      content_text, content_sha, meta, embedding,
                      created_at
                    )
                    VALUES (
                      %(rfq_id)s, %(doc_type)s,
                      %(product_id)s, %(query_id)s, %(file_id)s,
                      %(page_num)s, %(chunk_idx)s,
                      %(content_text)s, %(content_sha)s, %(meta)s, (%(embedding)s)::vector,
                      now()
                    )
                    ON CONFLICT (rfq_id, doc_type, content_sha) DO NOTHING
                    ;
                    """,
                    {
                        "rfq_id": c.rfq_id,
                        "doc_type": c.doc_type,
                        "product_id": c.product_id,
                        "query_id": c.query_id,
                        "file_id": c.file_id,
                        "page_num": c.page_num,
                        "chunk_idx": c.chunk_idx,
                        "content_text": c.content_text,
                        "content_sha": c.content_sha,
                        "meta": c.meta or {},
                        "embedding": _vector_literal(c.embedding),
                    },
                )
                try:
                    inserted += int(cur.rowcount or 0)
                except Exception:
                    pass

        return inserted


@dataclass(frozen=True)
class VectorRetriever:
    """
    Future retrieval helper. Always filter by rfq_id.
    Uses cosine distance operator via vector_cosine_ops index.
    """
    db: DB

    def search(
        self,
        *,
        rfq_id: str,
        query_embedding: Sequence[float],
        k: int = 10,
        doc_types: Optional[Sequence[str]] = None,
        meta_contains: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Returns list of chunks with distance (lower = more similar).

        meta_contains: jsonb containment filter (meta @> {...})
        """
        rfq_id = (rfq_id or "").strip()
        if not rfq_id:
            return []

        qvec = _vector_literal(query_embedding)

        where = ["rfq_id = %(rfq_id)s"]
        params: Dict[str, Any] = {"rfq_id": rfq_id, "qvec": qvec, "k": int(k)}

        if doc_types:
            where.append("doc_type = ANY(%(doc_types)s)")
            params["doc_types"] = list(doc_types)

        if meta_contains:
            where.append("meta @> %(meta_contains)s::jsonb")
            params["meta_contains"] = meta_contains

        where_sql = " AND ".join(where)

        sql = f"""
        SELECT
          chunk_id,
          rfq_id,
          doc_type,
          product_id,
          query_id,
          file_id,
          page_num,
          chunk_idx,
          content_text,
          content_sha,
          meta,
          (embedding <=> (%(qvec)s)::vector) AS distance
        FROM rfq.chunks
        WHERE {where_sql}
        ORDER BY embedding <=> (%(qvec)s)::vector
        LIMIT %(k)s
        ;
        """

        out: List[Dict[str, Any]] = []
        with tx(self.db) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]  # type: ignore[attr-defined]
            for r in rows:
                out.append(dict(zip(cols, r)))
        return out