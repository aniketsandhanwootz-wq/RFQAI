from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from service.app.integrations.glide_client import GlidePage
from service.app.pipeline.nodes.upsert_tables import PageUpsertStats
from service.app.pipeline import table_ingest as ti


class _Settings:
    database_url = "postgresql://dry-run"
    glide_api_key = "x"
    glide_app_id = ""
    glide_max_rows_per_call = 500

    ingest_http_timeout_sec = 60
    ingest_file_max_mb = 40

    gemini_api_key = ""
    gemini_embedding_model = "gemini-embedding-001"
    embed_dim = 1536

    chunk_size = 1200
    chunk_overlap = 150
    gdrive_sa_json_path = ""


class _StubGlide:
    def __init__(self, settings: Any):
        self.settings = settings
        self.tables = {
            "all_rfq": {"table_name": "t_all_rfq", "columns": {}},
            "all_products": {"table_name": "t_products", "columns": {}},
            "queries": {"table_name": "t_queries", "columns": {}},
            "supplier_shares": {"table_name": "t_shares", "columns": {}},
        }

    def max_allowed_limit(self, requested: int | None = None) -> int:
        return 500

    def fetch_table_rows_paginated(self, table_name: str, *, limit: int | None = None):
        pages: Dict[str, List[GlidePage]] = {
            "t_all_rfq": [
                GlidePage(rows=[{"rowID": "r1"}, {"rowID": "r2"}], next_token="n1", token_kind="startAt"),
                GlidePage(rows=[{"rowID": "r3"}], next_token=None, token_kind=None),
            ],
            "t_products": [
                GlidePage(rows=[{"rowID": "p1"}], next_token="c1", token_kind="cursor"),
                GlidePage(rows=[{"rowID": "p2"}], next_token=None, token_kind=None),
            ],
            "t_queries": [GlidePage(rows=[{"rowID": "q1"}], next_token=None, token_kind=None)],
            "t_shares": [GlidePage(rows=[{"rowID": "s1"}], next_token=None, token_kind=None)],
        }
        for p in pages[table_name]:
            yield p


class _DummyDB:
    def __init__(self, database_url: str):
        self.database_url = database_url


def main() -> int:
    progress_updates: List[Dict[str, Any]] = []
    cursor_updates: List[Dict[str, Any]] = []
    finish_payload: Dict[str, Any] = {}

    orig = {
        "GlideClient": ti.GlideClient,
        "DB": ti.DB,
        "_start_ingest_run": ti._start_ingest_run,
        "_upsert_run_table_progress": ti._upsert_run_table_progress,
        "_update_cursor_checkpoint": ti._update_cursor_checkpoint,
        "_finish_ingest_run": ti._finish_ingest_run,
        "_count_changed_rfqs": ti._count_changed_rfqs,
        "_apply_table_page": ti._apply_table_page,
    }

    def _start_ingest_run(db: Any, mode: str) -> str:
        assert mode == "cron"
        return "00000000-0000-0000-0000-000000000001"

    def _upsert_run_table_progress(db: Any, **kwargs: Any) -> None:
        progress_updates.append(
            {
                "table_key": kwargs["progress"].table_key,
                "status": kwargs["status"],
                "pages": kwargs["progress"].pages,
                "rows_seen": kwargs["progress"].rows_seen,
            }
        )

    def _update_cursor_checkpoint(db: Any, **kwargs: Any) -> None:
        cursor_updates.append(
            {
                "table_key": kwargs["table_key"],
                "next_token": kwargs.get("next_token"),
                "token_kind": kwargs.get("token_kind"),
            }
        )

    def _finish_ingest_run(db: Any, run_id: str, *, status: str, summary: Dict[str, Any], error: str | None) -> None:
        finish_payload.update({"run_id": run_id, "status": status, "summary": summary, "error": error})

    def _count_changed_rfqs(db: Any, run_id: str) -> int:
        assert run_id == "00000000-0000-0000-0000-000000000001"
        return 7

    def _apply_table_page(**kwargs: Any) -> PageUpsertStats:
        rows_page = kwargs["rows_page"]
        stats = PageUpsertStats()
        stats.seen = len(rows_page)
        stats.changed = len(rows_page)
        return stats

    try:
        ti.GlideClient = _StubGlide
        ti.DB = _DummyDB
        ti._start_ingest_run = _start_ingest_run
        ti._upsert_run_table_progress = _upsert_run_table_progress
        ti._update_cursor_checkpoint = _update_cursor_checkpoint
        ti._finish_ingest_run = _finish_ingest_run
        ti._count_changed_rfqs = _count_changed_rfqs
        ti._apply_table_page = _apply_table_page

        result = ti.ingest_glide_tables(_Settings(), mode="cron")

        assert result.run_id == "00000000-0000-0000-0000-000000000001"
        assert result.changed_rfq_count == 7
        assert result.table_progress["all_rfq"].pages == 2
        assert result.table_progress["all_products"].pages == 2
        assert result.table_progress["queries"].pages == 1
        assert result.table_progress["supplier_shares"].pages == 1

        assert any(x["table_key"] == "all_rfq" and x["status"] == "SUCCESS" for x in progress_updates)
        assert any(x["table_key"] == "all_products" and x["status"] == "SUCCESS" for x in progress_updates)

        # Ensure token styles are preserved in progress/cursor updates
        assert any(x["token_kind"] == "startAt" for x in cursor_updates)
        assert any(x["token_kind"] == "cursor" for x in cursor_updates)

        assert finish_payload.get("status") == "SUCCESS"
        print("[OK] dry table_ingest smoke passed")
        return 0

    finally:
        ti.GlideClient = orig["GlideClient"]
        ti.DB = orig["DB"]
        ti._start_ingest_run = orig["_start_ingest_run"]
        ti._upsert_run_table_progress = orig["_upsert_run_table_progress"]
        ti._update_cursor_checkpoint = orig["_update_cursor_checkpoint"]
        ti._finish_ingest_run = orig["_finish_ingest_run"]
        ti._count_changed_rfqs = orig["_count_changed_rfqs"]
        ti._apply_table_page = orig["_apply_table_page"]


if __name__ == "__main__":
    raise SystemExit(main())
