from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence
import json

from .db_tool import DB, tx
from ..pipeline.state import Chunk


def _vector_literal(vec: Sequence[float]) -> str:
    return "[" + ",".join(f"{float(x):.10g}" for x in vec) + "]"


@dataclass(frozen=True)
class VectorWriter:
    db: DB

    def delete_scope(
        self,
        *,
        rfq_id: str,
        doc_type: Optional[str] = None,
        product_id: Optional[str] = None,
        query_id: Optional[str] = None,
        file_id: Optional[str] = None,
    ) -> int:
        """
        Deletes vectors for a given scope (cron-safe update).
        If only rfq_id provided => deletes all vectors for RFQ.
        """
        where = ["rfq_id = %(rfq_id)s"]
        params: Dict[str, Any] = {"rfq_id": rfq_id}

        if doc_type:
            where.append("doc_type = %(doc_type)s")
            params["doc_type"] = doc_type
        if product_id is not None:
            where.append("product_id IS NOT DISTINCT FROM %(product_id)s")
            params["product_id"] = product_id
        if query_id is not None:
            where.append("query_id IS NOT DISTINCT FROM %(query_id)s")
            params["query_id"] = query_id
        if file_id is not None:
            where.append("file_id IS NOT DISTINCT FROM %(file_id)s")
            params["file_id"] = file_id

        sql = f"DELETE FROM rfq.chunks WHERE {' AND '.join(where)};"
        with tx(self.db) as cur:
            cur.execute(sql, params)
            try:
                return int(cur.rowcount or 0)
            except Exception:
                return 0

    def upsert_chunks(self, chunks: List[Chunk]) -> int:
        """
        Cron-safe strategy:
        - Delete existing vectors for the same RFQ and doc scopes we are writing.
        - Insert all chunks (dedupe still supported by UNIQUE constraint).
        """
        if not chunks:
            return 0

        rfq_id = chunks[0].rfq_id

        # Determine which doc_types are present
        doc_types = sorted(set(c.doc_type for c in chunks))

        # 1) Structured docs: delete per product/query scope
        # 2) FILE_CHUNK: simplest safe behavior: delete all FILE_CHUNK for rfq_id
        with tx(self.db) as cur:
            if "FILE_CHUNK" in doc_types:
                cur.execute(
                    "DELETE FROM rfq.chunks WHERE rfq_id=%(rfq_id)s AND doc_type='FILE_CHUNK';",
                    {"rfq_id": rfq_id},
                )

            # For RFQ_BRIEF delete once
            if "RFQ_BRIEF" in doc_types:
                cur.execute(
                    "DELETE FROM rfq.chunks WHERE rfq_id=%(rfq_id)s AND doc_type='RFQ_BRIEF';",
                    {"rfq_id": rfq_id},
                )

            # For PRODUCT_CARD delete per product_id present
            if "PRODUCT_CARD" in doc_types:
                pids = sorted({c.product_id for c in chunks if c.doc_type == "PRODUCT_CARD"})
                for pid in pids:
                    cur.execute(
                        """
                        DELETE FROM rfq.chunks
                        WHERE rfq_id=%(rfq_id)s AND doc_type='PRODUCT_CARD'
                          AND product_id IS NOT DISTINCT FROM %(pid)s;
                        """,
                        {"rfq_id": rfq_id, "pid": pid},
                    )

            # For THREAD_MESSAGE delete per query_id present
            if "THREAD_MESSAGE" in doc_types:
                qids = sorted({c.query_id for c in chunks if c.doc_type == "THREAD_MESSAGE"})
                for qid in qids:
                    cur.execute(
                        """
                        DELETE FROM rfq.chunks
                        WHERE rfq_id=%(rfq_id)s AND doc_type='THREAD_MESSAGE'
                          AND query_id IS NOT DISTINCT FROM %(qid)s;
                        """,
                        {"rfq_id": rfq_id, "qid": qid},
                    )

            # Insert all
            inserted = 0
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
                      %(content_text)s, %(content_sha)s, %(meta)s::jsonb, (%(embedding)s)::vector,
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
                        "meta": json.dumps(c.meta or {}, ensure_ascii=True, separators=(",", ":"), sort_keys=True, default=str),
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

        sql = f"""
        SELECT
          chunk_id, rfq_id, doc_type, product_id, query_id, file_id,
          page_num, chunk_idx, content_text, content_sha, meta,
          (embedding <=> (%(qvec)s)::vector) AS distance
        FROM rfq.chunks
        WHERE {" AND ".join(where)}
        ORDER BY embedding <=> (%(qvec)s)::vector
        LIMIT %(k)s;
        """

        out: List[Dict[str, Any]] = []
        with tx(self.db) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]  # type: ignore[attr-defined]
            for r in rows:
                out.append(dict(zip(cols, r)))
        return out
