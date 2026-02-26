# service/app/integrations/glide_client.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
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
    """

    BASE_URL = "https://api.glideapp.io/api/function/queryTables"

    def __init__(self, settings: Settings, contracts_path: str = "packages/contracts/glide_tables.yaml"):
        self.settings = settings
        self.contracts_path = contracts_path
        self._session = requests.Session()

        cfg = self._load_contracts()
        self.app_id = cfg["app"]["app_id"]
        self.tables = cfg["tables"]

        if self.settings.glide_app_id and self.settings.glide_app_id != self.app_id:
            self.app_id = self.settings.glide_app_id

        # Hard guard: prevent accidental use if someone sets a mutate URL later
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

    def _post_with_retry(self, payload: Dict[str, Any], *, max_attempts: int = 5) -> Dict[str, Any]:
        """
        Retry on transient errors (429/5xx).
        """
        for attempt in range(1, max_attempts + 1):
            r = self._session.post(self.BASE_URL, headers=self._headers(), data=json.dumps(payload), timeout=60)

            if r.status_code < 400:
                return r.json()

            # Retry only for transient issues
            if r.status_code in (429, 500, 502, 503, 504):
                sleep_s = min(2.0 * attempt, 8.0)
                time.sleep(sleep_s)
                continue

            raise RuntimeError(f"Glide queryTables failed {r.status_code}: {r.text}")

        raise RuntimeError("Glide queryTables failed after retries.")

    def fetch_table_all_rows(self, table_name: str, *, max_pages: int = 200) -> List[Dict[str, Any]]:
        """
        Fetch all rows from a Glide table with pagination.

        We keep Glide calls minimal by using a high limit per page.
        """
        limit = int(self.settings.glide_max_rows_per_call or 5000)
        out: List[Dict[str, Any]] = []

        cursor: Optional[str] = None
        for _ in range(max_pages):
            payload: Dict[str, Any] = {
                "appID": self.app_id,
                "queries": [{"tableName": table_name, "limit": limit}],
            }
            if cursor:
                payload["queries"][0]["cursor"] = cursor

            data = self._post_with_retry(payload)

            rows = None
            next_cursor = None

            if isinstance(data, dict) and "results" in data:
                res0 = (data.get("results") or [{}])[0] or {}
                rows = res0.get("rows")
                next_cursor = res0.get("cursor") or res0.get("nextCursor")
            else:
                rows = data.get("rows")
                next_cursor = data.get("cursor") or data.get("nextCursor")

            if not rows:
                break

            out.extend(rows)
            cursor = next_cursor
            if not cursor:
                break

            time.sleep(0.05)

        return out

    def fetch_all_4_tables(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Exactly 4 table fetches per run.
        """
        return {
            "all_rfq": self.fetch_table_all_rows(self.tables["all_rfq"]["table_name"]),
            "all_products": self.fetch_table_all_rows(self.tables["all_products"]["table_name"]),
            "queries": self.fetch_table_all_rows(self.tables["queries"]["table_name"]),
            "supplier_shares": self.fetch_table_all_rows(self.tables["supplier_shares"]["table_name"]),
        }