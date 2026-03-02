from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple
import json
import os
import time

import requests
import yaml

from ..config import Settings


@dataclass(frozen=True)
class GlideConfig:
    api_key: str
    app_id: str
    table_name: str


@dataclass(frozen=True)
class GlidePage:
    rows: List[Dict[str, Any]]
    next_token: Optional[str] = None
    token_kind: Optional[str] = None  # startAt | cursor


class GlideClient:
    """
    STRICT READ-ONLY Glide Tables API client.

    Allowed endpoint:
      POST https://api.glideapp.io/api/function/queryTables

    We NEVER call mutateTables (write).
    """

    READ_ONLY_URL = "https://api.glideapp.io/api/function/queryTables"
    BASE_URL = READ_ONLY_URL
    HARD_MAX_LIMIT_DEFAULT = 10000
    DEFAULT_PAGE_LIMIT = 1000  # conservative default

    def __init__(self, settings: Settings, contracts_path: str = "packages/contracts/glide_tables.yaml"):
        self.settings = settings
        self.contracts_path = contracts_path
        self._session = requests.Session()
        self.hard_max_limit = self._load_hard_max_limit()

        cfg = self._load_contracts()
        self.app_id = cfg["app"]["app_id"]
        self.tables = cfg["tables"]

        if self.settings.glide_app_id and self.settings.glide_app_id != self.app_id:
            self.app_id = self.settings.glide_app_id

        self._assert_read_only_endpoint()

    def _load_hard_max_limit(self) -> int:
        """
        Upper safety ceiling. Can be lowered via GLIDE_HARD_MAX_LIMIT.
        Never exceeds 10000.
        """
        raw = os.getenv("GLIDE_HARD_MAX_LIMIT", str(self.HARD_MAX_LIMIT_DEFAULT))
        try:
            v = int(raw)
        except Exception:
            v = self.HARD_MAX_LIMIT_DEFAULT
        return max(1, min(v, self.HARD_MAX_LIMIT_DEFAULT))

    def _assert_read_only_endpoint(self) -> None:
        actual = (self.BASE_URL or "").strip()
        if not actual:
            raise RuntimeError("GlideClient misconfigured: missing endpoint.")
        if "mutate" in actual.lower():
            raise RuntimeError("GlideClient misconfigured: mutate endpoint detected (write).")
        if actual != self.READ_ONLY_URL:
            raise RuntimeError(
                f"GlideClient misconfigured: only '{self.READ_ONLY_URL}' is allowed, got '{actual}'."
            )

    def _load_contracts(self) -> Dict[str, Any]:
        with open(self.contracts_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.settings.glide_api_key}",
        }

    def max_allowed_limit(self, requested: Optional[int] = None) -> int:
        want = requested
        if want is None:
            want = int(self.settings.glide_max_rows_per_call or self.DEFAULT_PAGE_LIMIT)
        if int(want) <= 0:
            want = self.DEFAULT_PAGE_LIMIT
        return max(1, min(int(want), self.hard_max_limit))

    def _post_with_retry(self, payload: Dict[str, Any], *, max_attempts: int = 5) -> Any:
        """
        Retry on transient errors (429/5xx).
        Return parsed JSON (can be dict or list depending on Glide).
        """
        self._assert_read_only_endpoint()

        if not isinstance(payload, dict) or "queries" not in payload:
            raise RuntimeError("Invalid Glide query payload: expected dict with 'queries'.")

        for attempt in range(1, max_attempts + 1):
            r = self._session.post(self.BASE_URL, headers=self._headers(), data=json.dumps(payload), timeout=60)

            if r.status_code < 400:
                return r.json()

            if r.status_code in (429, 500, 502, 503, 504):
                sleep_s = min(2.0 * attempt, 8.0)
                time.sleep(sleep_s)
                continue

            raise RuntimeError(f"Glide queryTables failed {r.status_code}: {r.text}")

        raise RuntimeError("Glide queryTables failed after retries.")

    @staticmethod
    def _normalize_top(data: Any) -> Dict[str, Any]:
        """
        Glide sometimes returns list at top-level.
        Normalize to a dict-like object (best effort).
        """
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    return item
                if isinstance(item, list):
                    for sub in item:
                        if isinstance(sub, dict):
                            return sub
        return {}

    @staticmethod
    def _extract_rows_and_token(data0: Any) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]:
        """
        Returns: (rows, next_token, cursor_token)
        Supports:
          - {"rows":[...], "next":"..."}   (Glide continuation)
          - {"rows":[...], "cursor":"..."} (cursor style)
          - {"results":[{"rows":[...], "next":"..."}]}
          - {"results":[{"rows":[...], "cursor":"..."}]}
        """
        if not isinstance(data0, dict):
            return ([], None, None)

        if "results" in data0 and isinstance(data0.get("results"), list) and data0["results"]:
            res0 = next((x for x in data0["results"] if isinstance(x, dict)), {}) or {}
            rows = res0.get("rows") or []
            rows_out = [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
            nxt = res0.get("next")
            cur = res0.get("cursor") or res0.get("nextCursor")
            return (rows_out, (str(nxt) if nxt else None), (str(cur) if cur else None))

        rows = data0.get("rows") or []
        rows_out = [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
        nxt = data0.get("next")
        cur = data0.get("cursor") or data0.get("nextCursor")
        return (rows_out, (str(nxt) if nxt else None), (str(cur) if cur else None))

    def fetch_table_rows_paginated(
        self,
        table_name: str,
        *,
        limit: Optional[int] = None,
        start_at: Optional[str] = None,
        cursor: Optional[str] = None,
        max_pages: int = 1000,
    ) -> Iterator[GlidePage]:
        """
        Streams one table page-by-page without accumulating all rows.
        """
        use_limit = self.max_allowed_limit(limit)

        next_start: Optional[str] = start_at
        # Use one pagination style at a time. startAt takes precedence.
        next_cursor: Optional[str] = None if next_start else cursor

        for _ in range(max_pages):
            q: Dict[str, Any] = {"tableName": table_name, "limit": use_limit}
            if next_start:
                q["startAt"] = next_start
            if next_cursor:
                q["cursor"] = next_cursor

            payload: Dict[str, Any] = {"appID": self.app_id, "queries": [q]}
            raw = self._post_with_retry(payload)
            data0 = self._normalize_top(raw)

            rows, nxt, cur = self._extract_rows_and_token(data0)
            if not rows:
                break

            token: Optional[str] = None
            kind: Optional[str] = None

            # Prefer Glide continuation style when both are present.
            if nxt:
                token = str(nxt)
                kind = "startAt"
                next_start = token
                next_cursor = None
            elif cur:
                token = str(cur)
                kind = "cursor"
                next_cursor = token
                next_start = None
            else:
                next_start = None
                next_cursor = None

            yield GlidePage(rows=rows, next_token=token, token_kind=kind)

            if not token:
                break

            time.sleep(0.05)

    def iter_table_rows(
        self,
        table_key: str,
        *,
        limit: Optional[int] = None,
        start_at: Optional[str] = None,
        cursor: Optional[str] = None,
        max_pages: int = 1000,
    ) -> Iterator[Dict[str, Any]]:
        """
        Yields rows for a configured table key without loading full table in memory.
        """
        if table_key not in self.tables:
            raise KeyError(f"Unknown Glide table key: {table_key}")

        table_name = self.tables[table_key]["table_name"]
        for page in self.fetch_table_rows_paginated(
            table_name,
            limit=limit,
            start_at=start_at,
            cursor=cursor,
            max_pages=max_pages,
        ):
            for row in page.rows:
                yield row

    def fetch_table_all_rows(self, table_name: str, *, max_pages: int = 1000) -> List[Dict[str, Any]]:
        """
        Backward-compatible helper that accumulates all table rows in memory.
        Prefer fetch_table_rows_paginated() for cron-safe streaming.
        """
        out: List[Dict[str, Any]] = []
        for page in self.fetch_table_rows_paginated(table_name, max_pages=max_pages):
            out.extend(page.rows)
        return out

    def fetch_all_4_tables(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Exactly 4 table fetches per run (but each may paginate internally).
        """
        return {
            "all_rfq": self.fetch_table_all_rows(self.tables["all_rfq"]["table_name"]),
            "all_products": self.fetch_table_all_rows(self.tables["all_products"]["table_name"]),
            "queries": self.fetch_table_all_rows(self.tables["queries"]["table_name"]),
            "supplier_shares": self.fetch_table_all_rows(self.tables["supplier_shares"]["table_name"]),
        }
