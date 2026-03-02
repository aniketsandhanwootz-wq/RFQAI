from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import json
import time

import requests
import yaml

from ..config import Settings


@dataclass(frozen=True)
class GlideConfig:
    api_key: str
    app_id: str
    table_name: str


class GlideClient:
    """
    STRICT READ-ONLY Glide Tables API client.

    We ONLY call:
      POST https://api.glideapp.io/api/function/queryTables

    We NEVER call mutateTables (write).

    Pagination in Glide commonly uses:
      - response: {"rows": [...], "next": "CONTINUATION_TOKEN"}
      - request:  {"startAt": "CONTINUATION_TOKEN"}  (inside the query object)

    Some responses also use cursor/nextCursor.
    This client supports BOTH styles.
    """

    BASE_URL = "https://api.glideapp.io/api/function/queryTables"
    HARD_MAX_LIMIT = 10000  # Glide max per response (practical cap)

    def __init__(self, settings: Settings, contracts_path: str = "packages/contracts/glide_tables.yaml"):
        self.settings = settings
        self.contracts_path = contracts_path
        self._session = requests.Session()

        cfg = self._load_contracts()
        self.app_id = cfg["app"]["app_id"]
        self.tables = cfg["tables"]

        if self.settings.glide_app_id and self.settings.glide_app_id != self.app_id:
            self.app_id = self.settings.glide_app_id

        if "mutate" in self.BASE_URL.lower():
            raise RuntimeError("GlideClient misconfigured: mutate endpoint detected (write).")

    def _load_contracts(self) -> Dict[str, Any]:
        with open(self.contracts_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.settings.glide_api_key}",
        }

    def _post_with_retry(self, payload: Dict[str, Any], *, max_attempts: int = 5) -> Any:
        """
        Retry on transient errors (429/5xx).
        Return parsed JSON (can be dict or list depending on Glide).
        """
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
        if isinstance(data, list):
            return data[0] if data else {}
        if isinstance(data, dict):
            return data
        return {}

    @staticmethod
    def _extract_rows_and_token(data0: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]:
        """
        Returns: (rows, next_token, cursor_token)
        Supports:
          - {"rows":[...], "next":"..."}   (Glide continuation)
          - {"rows":[...], "cursor":"..."} (cursor style)
          - {"results":[{"rows":[...], "next":"..."}]}
          - {"results":[{"rows":[...], "cursor":"..."}]}
        """
        # results wrapper
        if "results" in data0 and isinstance(data0.get("results"), list) and data0["results"]:
            res0 = data0["results"][0] or {}
            rows = res0.get("rows") or []
            nxt = res0.get("next")
            cur = res0.get("cursor") or res0.get("nextCursor")
            return (rows if isinstance(rows, list) else [], nxt, cur)

        rows = data0.get("rows") or []
        nxt = data0.get("next")
        cur = data0.get("cursor") or data0.get("nextCursor")
        return (rows if isinstance(rows, list) else [], nxt, cur)

    def fetch_table_all_rows(self, table_name: str, *, max_pages: int = 1000) -> List[Dict[str, Any]]:
        """
        Fetches all rows from a Glide table with pagination.
        Cost control:
          - uses the highest allowed limit (clamped to 10000)
          - paginates only when Glide returns next/cursor tokens
        """
        want = int(self.settings.glide_max_rows_per_call or self.HARD_MAX_LIMIT)
        limit = max(1, min(want, self.HARD_MAX_LIMIT))

        out: List[Dict[str, Any]] = []

        start_at: Optional[str] = None     # Glide continuation token ("next" -> "startAt")
        cursor: Optional[str] = None       # Alternate cursor style

        for _ in range(max_pages):
            q: Dict[str, Any] = {"tableName": table_name, "limit": limit}
            if start_at:
                q["startAt"] = start_at
            if cursor:
                q["cursor"] = cursor

            payload: Dict[str, Any] = {"appID": self.app_id, "queries": [q]}
            raw = self._post_with_retry(payload)
            data0 = self._normalize_top(raw)

            rows, nxt, cur = self._extract_rows_and_token(data0)
            if not rows:
                break

            out.extend(rows)

            # Prefer Glide official continuation if present
            if nxt:
                start_at = str(nxt)
                cursor = None
            elif cur:
                cursor = str(cur)
                start_at = None
            else:
                break

            time.sleep(0.05)

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