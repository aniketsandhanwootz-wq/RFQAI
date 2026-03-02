from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Dict, Iterator, List, Literal, Optional

import yaml

from ..config import Settings
from ..integrations.drive_client import DriveClient
from ..integrations.fetch_client import FetchClient
from ..integrations.glide_client import GlideClient
from ..tools.db_tool import DB, tx
from ..tools.embed_tool import Embedder
from ..tools.vector_tool import VectorWriter
from .state import IngestState
from .nodes.build_docs import build_docs_node
from .nodes.resolve_sources import resolve_sources_node
from .nodes.extract_files import extract_files_node
from .nodes.chunk import chunk_node
from .nodes.embed import embed_node
from .nodes.upsert import upsert_chunks_node
from .nodes.upsert_tables import (
    PageUpsertStats,
    upsert_products,
    upsert_queries,
    upsert_rfqs,
    upsert_supplier_shares,
)

IngestMode = Literal["backfill", "cron"]
TABLE_ORDER = ("all_rfq", "all_products", "queries", "supplier_shares")


@dataclass
class TableProgress:
    table_key: str
    table_name: str
    pages: int = 0
    rows_seen: int = 0
    rows_changed: int = 0
    rows_unchanged: int = 0
    rows_skipped: int = 0


@dataclass
class GlideIngestResult:
    run_id: str
    mode: IngestMode
    table_progress: Dict[str, TableProgress]
    changed_rfq_count: int


def _load_glide_cfg(path: str = "packages/contracts/glide_tables.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["tables"]


def _start_ingest_run(db: DB, mode: IngestMode) -> str:
    with tx(db) as cur:
        cur.execute(
            """
            INSERT INTO rfq.ingest_runs (mode, status, started_at)
            VALUES (%(mode)s, 'RUNNING', now())
            RETURNING run_id
            """,
            {"mode": mode},
        )
        row = cur.fetchone()
        return str(row[0])


def _finish_ingest_run(db: DB, run_id: str, *, status: str, summary: Dict[str, Any], error: Optional[str]) -> None:
    with tx(db) as cur:
        cur.execute(
            """
            UPDATE rfq.ingest_runs
            SET status=%(status)s,
                finished_at=now(),
                error=%(error)s,
                summary=%(summary)s::jsonb
            WHERE run_id=%(run_id)s::uuid
            """,
            {
                "status": status,
                "error": error,
                "summary": json.dumps(summary),
                "run_id": run_id,
            },
        )


def merge_run_summary(db: DB, run_id: str, patch: Dict[str, Any]) -> None:
    with tx(db) as cur:
        cur.execute(
            """
            UPDATE rfq.ingest_runs
            SET summary = COALESCE(summary, '{}'::jsonb) || %(patch)s::jsonb
            WHERE run_id=%(run_id)s::uuid
            """,
            {
                "patch": json.dumps(patch),
                "run_id": run_id,
            },
        )


def _upsert_run_table_progress(
    db: DB,
    *,
    run_id: str,
    progress: TableProgress,
    status: str,
    error: Optional[str] = None,
    last_token: Optional[str] = None,
    last_token_kind: Optional[str] = None,
) -> None:
    with tx(db) as cur:
        cur.execute(
            """
            INSERT INTO rfq.ingest_run_tables (
              run_id, table_key, table_name, status,
              pages, rows_seen, rows_changed, rows_unchanged, rows_skipped,
              last_token, last_token_kind, error, updated_at
            )
            VALUES (
              %(run_id)s::uuid, %(table_key)s, %(table_name)s, %(status)s,
              %(pages)s, %(rows_seen)s, %(rows_changed)s, %(rows_unchanged)s, %(rows_skipped)s,
              %(last_token)s, %(last_token_kind)s, %(error)s, now()
            )
            ON CONFLICT (run_id, table_key) DO UPDATE SET
              status=EXCLUDED.status,
              pages=EXCLUDED.pages,
              rows_seen=EXCLUDED.rows_seen,
              rows_changed=EXCLUDED.rows_changed,
              rows_unchanged=EXCLUDED.rows_unchanged,
              rows_skipped=EXCLUDED.rows_skipped,
              last_token=EXCLUDED.last_token,
              last_token_kind=EXCLUDED.last_token_kind,
              error=EXCLUDED.error,
              updated_at=now()
            """,
            {
                "run_id": run_id,
                "table_key": progress.table_key,
                "table_name": progress.table_name,
                "status": status,
                "pages": progress.pages,
                "rows_seen": progress.rows_seen,
                "rows_changed": progress.rows_changed,
                "rows_unchanged": progress.rows_unchanged,
                "rows_skipped": progress.rows_skipped,
                "last_token": last_token,
                "last_token_kind": last_token_kind,
                "error": error,
            },
        )


def _update_cursor_checkpoint(
    db: DB,
    *,
    table_key: str,
    table_name: str,
    run_id: str,
    next_token: Optional[str],
    token_kind: Optional[str],
) -> None:
    with tx(db) as cur:
        cur.execute(
            """
            INSERT INTO rfq.glide_cursors (table_key, table_name, next_token, token_kind, last_run_id, updated_at)
            VALUES (%(table_key)s, %(table_name)s, %(next_token)s, %(token_kind)s, %(run_id)s::uuid, now())
            ON CONFLICT (table_key) DO UPDATE SET
              table_name=EXCLUDED.table_name,
              next_token=EXCLUDED.next_token,
              token_kind=EXCLUDED.token_kind,
              last_run_id=EXCLUDED.last_run_id,
              updated_at=now()
            """,
            {
                "table_key": table_key,
                "table_name": table_name,
                "next_token": next_token,
                "token_kind": token_kind,
                "run_id": run_id,
            },
        )


def _count_changed_rfqs(db: DB, run_id: str) -> int:
    with tx(db) as cur:
        cur.execute(
            """
            SELECT count(*)
            FROM rfq.ingest_run_changed_rfqs
            WHERE run_id=%(run_id)s::uuid
            """,
            {"run_id": run_id},
        )
        row = cur.fetchone()
        return int(row[0] if row else 0)


def iter_changed_rfq_batches(
    db: DB,
    run_id: str,
    *,
    batch_size: int = 200,
    limit: int = 0,
) -> Iterator[List[str]]:
    """
    Keyset pagination over changed RFQs for this run to avoid loading all IDs in memory.
    """
    bs = max(1, int(batch_size))
    max_rows = max(0, int(limit))

    emitted = 0
    after: Optional[str] = None

    while True:
        with tx(db) as cur:
            if after is None:
                cur.execute(
                    """
                    SELECT rfq_id
                    FROM rfq.ingest_run_changed_rfqs
                    WHERE run_id=%(run_id)s::uuid
                    ORDER BY rfq_id
                    LIMIT %(lim)s
                    """,
                    {"run_id": run_id, "lim": bs},
                )
            else:
                cur.execute(
                    """
                    SELECT rfq_id
                    FROM rfq.ingest_run_changed_rfqs
                    WHERE run_id=%(run_id)s::uuid
                      AND rfq_id > %(after)s
                    ORDER BY rfq_id
                    LIMIT %(lim)s
                    """,
                    {"run_id": run_id, "after": after, "lim": bs},
                )

            rows = [str(r[0]) for r in cur.fetchall()]

        if not rows:
            break

        if max_rows > 0 and emitted + len(rows) > max_rows:
            rows = rows[: max_rows - emitted]

        if not rows:
            break

        yield rows
        emitted += len(rows)
        if max_rows > 0 and emitted >= max_rows:
            break

        after = rows[-1]


def _build_table_summary(progress: Dict[str, TableProgress]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, p in progress.items():
        out[key] = {
            "pages": p.pages,
            "rows_seen": p.rows_seen,
            "rows_changed": p.rows_changed,
            "rows_unchanged": p.rows_unchanged,
            "rows_skipped": p.rows_skipped,
        }
    return out


def _apply_table_page(
    *,
    table_key: str,
    rows_page: List[Dict[str, Any]],
    db: DB,
    table_cfg: Dict[str, Any],
    run_id: str,
) -> PageUpsertStats:
    cols = table_cfg["columns"]

    if table_key == "all_rfq":
        return upsert_rfqs(rows_page, db=db, rfq_cols=cols, run_id=run_id)
    if table_key == "all_products":
        return upsert_products(rows_page, db=db, prod_cols=cols, run_id=run_id)
    if table_key == "queries":
        return upsert_queries(rows_page, db=db, q_cols=cols, run_id=run_id)
    if table_key == "supplier_shares":
        return upsert_supplier_shares(rows_page, db=db, s_cols=cols, run_id=run_id)

    raise KeyError(f"Unsupported table_key: {table_key}")


def ingest_glide_tables(settings: Settings, *, mode: IngestMode) -> GlideIngestResult:
    """
    Table-by-table, page-by-page Glide ingestion.

    Memory profile is bounded by one Glide page + one DB transaction scope.
    """
    if mode not in ("backfill", "cron"):
        raise ValueError(f"Unsupported mode={mode}")

    db = DB(settings.database_url)
    glide = GlideClient(settings)

    run_id = _start_ingest_run(db, mode)
    table_progress: Dict[str, TableProgress] = {}

    try:
        page_limit = glide.max_allowed_limit()

        for table_key in TABLE_ORDER:
            table_cfg = glide.tables[table_key]
            table_name = table_cfg["table_name"]
            progress = TableProgress(table_key=table_key, table_name=table_name)

            _upsert_run_table_progress(db, run_id=run_id, progress=progress, status="RUNNING")

            try:
                for page in glide.fetch_table_rows_paginated(table_name, limit=page_limit):
                    progress.pages += 1
                    progress.rows_seen += len(page.rows)

                    page_stats = _apply_table_page(
                        table_key=table_key,
                        rows_page=page.rows,
                        db=db,
                        table_cfg=table_cfg,
                        run_id=run_id,
                    )
                    progress.rows_changed += page_stats.changed
                    progress.rows_unchanged += page_stats.unchanged
                    progress.rows_skipped += page_stats.skipped

                    _update_cursor_checkpoint(
                        db,
                        table_key=table_key,
                        table_name=table_name,
                        run_id=run_id,
                        next_token=page.next_token,
                        token_kind=page.token_kind,
                    )

                    _upsert_run_table_progress(
                        db,
                        run_id=run_id,
                        progress=progress,
                        status="RUNNING",
                        last_token=page.next_token,
                        last_token_kind=page.token_kind,
                    )

                _upsert_run_table_progress(db, run_id=run_id, progress=progress, status="SUCCESS")
                table_progress[table_key] = progress
            except Exception as e:
                _upsert_run_table_progress(
                    db,
                    run_id=run_id,
                    progress=progress,
                    status="FAILED",
                    error=str(e)[:2000],
                )
                raise

        changed_rfq_count = _count_changed_rfqs(db, run_id)
        summary = {
            "table_progress": _build_table_summary(table_progress),
            "changed_rfq_count": changed_rfq_count,
        }
        _finish_ingest_run(db, run_id, status="SUCCESS", summary=summary, error=None)

        return GlideIngestResult(
            run_id=run_id,
            mode=mode,
            table_progress=table_progress,
            changed_rfq_count=changed_rfq_count,
        )

    except Exception as e:
        summary = {
            "table_progress": _build_table_summary(table_progress),
            "error": str(e),
        }
        _finish_ingest_run(db, run_id, status="FAILED", summary=summary, error=str(e)[:2000])
        raise


def _load_prefetched_state_from_db(db: DB, rfq_id: str) -> IngestState:
    st = IngestState(rfq_id=rfq_id, prefetched=True)

    with tx(db) as cur:
        cur.execute(
            """
            SELECT raw_glide
            FROM rfq.rfqs
            WHERE rfq_id=%(rfq_id)s
            """,
            {"rfq_id": rfq_id},
        )
        rfq_row = cur.fetchone()
        if not rfq_row:
            st.errors.append(f"RFQ not found in DB for rfq_id={rfq_id}")
            return st

        st.rfq_row = rfq_row[0] or {}

        cur.execute(
            """
            SELECT raw_glide
            FROM rfq.products
            WHERE rfq_id=%(rfq_id)s
            ORDER BY product_id
            """,
            {"rfq_id": rfq_id},
        )
        st.products_rows = [(r[0] or {}) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT raw_glide
            FROM rfq.queries
            WHERE rfq_id=%(rfq_id)s
            ORDER BY query_id
            """,
            {"rfq_id": rfq_id},
        )
        st.queries_rows = [(r[0] or {}) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT raw_glide
            FROM rfq.supplier_shares
            WHERE rfq_id=%(rfq_id)s
            ORDER BY share_id
            """,
            {"rfq_id": rfq_id},
        )
        st.shares_rows = [(r[0] or {}) for r in cur.fetchall()]

    return st


def run_rfq_postprocess_from_db(rfq_id: str, settings: Settings) -> IngestState:
    """
    Run docs/files/chunks/vectors pipeline from DB-prefetched Glide rows.
    Avoids repeated Glide API calls per RFQ in cron/backfill stages.
    """
    db = DB(settings.database_url)
    st = _load_prefetched_state_from_db(db, rfq_id)
    if st.errors:
        return st

    glide_cfg = _load_glide_cfg()

    drive = DriveClient(settings.gdrive_sa_json_path)
    fetcher = FetchClient(timeout_sec=settings.ingest_http_timeout_sec, max_mb=settings.ingest_file_max_mb)

    embedder = Embedder(
        api_key=settings.gemini_api_key,
        model=settings.gemini_embedding_model,
        output_dim=settings.embed_dim,
    )
    vw = VectorWriter(db=db)

    st = build_docs_node(st, glide_cfg)
    st = resolve_sources_node(st, glide_cfg)
    st = extract_files_node(st, settings, db, drive, fetcher)
    st = chunk_node(st, chunk_size=settings.chunk_size, chunk_overlap=settings.chunk_overlap)
    st = embed_node(st, embedder, batch_size=64)
    st = upsert_chunks_node(st, vw)

    return st
