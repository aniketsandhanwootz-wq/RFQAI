from __future__ import annotations

from typing import Any, Dict, List

from service.app.integrations.glide_client import GlideClient


class _Settings:
    glide_api_key = "test-key"
    glide_app_id = ""
    glide_max_rows_per_call = 10000


class _StubGlide(GlideClient):
    def __init__(self, responses: List[Any]):
        super().__init__(_Settings(), contracts_path="packages/contracts/glide_tables.yaml")
        self._responses = list(responses)

    def _post_with_retry(self, payload: Dict[str, Any], *, max_attempts: int = 5) -> Any:
        if self._responses:
            return self._responses.pop(0)
        return {"rows": []}


def test_paginated_dict_rows_next_shape() -> None:
    c = _StubGlide(
        [
            {"rows": [{"rowID": "r1"}], "next": "tok-1"},
            {"rows": [{"rowID": "r2"}]},
        ]
    )

    pages = list(c.fetch_table_rows_paginated("dummy"))
    ids = [r["rowID"] for p in pages for r in p.rows]

    assert ids == ["r1", "r2"]
    assert pages[0].token_kind == "startAt"
    assert pages[0].next_token == "tok-1"


def test_paginated_results_cursor_shape() -> None:
    c = _StubGlide(
        [
            {"results": [{"rows": [{"rowID": "r3"}], "cursor": "cur-1"}]},
            {"results": [{"rows": [{"rowID": "r4"}]}]},
        ]
    )

    pages = list(c.fetch_table_rows_paginated("dummy"))
    ids = [r["rowID"] for p in pages for r in p.rows]

    assert ids == ["r3", "r4"]
    assert pages[0].token_kind == "cursor"
    assert pages[0].next_token == "cur-1"


def test_paginated_top_level_list_wrappers() -> None:
    c = _StubGlide(
        [
            [{"rows": [{"rowID": "r5"}], "cursor": "cur-2"}],
            [{"results": [{"rows": [{"rowID": "r6"}]}]}],
        ]
    )

    pages = list(c.fetch_table_rows_paginated("dummy"))
    ids = [r["rowID"] for p in pages for r in p.rows]

    assert ids == ["r5", "r6"]


def test_limit_clamped_to_glide_hard_max() -> None:
    c = _StubGlide([])
    assert c.max_allowed_limit(500000) == 10000
    assert c.max_allowed_limit(0) == 1
