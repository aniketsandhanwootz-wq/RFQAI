# service/app/integrations/fetch_client.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import requests


@dataclass(frozen=True)
class FetchResult:
    url: str
    status_code: int
    content_type: str
    filename: str
    content: bytes


class FetchClient:
    def __init__(self, timeout_sec: int = 60, max_mb: int = 40):
        self.timeout_sec = timeout_sec
        self.max_bytes = max_mb * 1024 * 1024
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "RFQAI/1.0"})

    def fetch(self, url: str) -> Optional[FetchResult]:
        url = (url or "").strip()
        if not url:
            return None

        try:
            r = self._session.get(url, timeout=self.timeout_sec, stream=True, allow_redirects=True)
        except Exception:
            return None

        ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()

        filename = ""
        cd = r.headers.get("content-disposition") or ""
        if "filename=" in cd:
            filename = cd.split("filename=")[-1].strip().strip('"')
        if not filename:
            filename = url.split("?")[0].split("/")[-1] or "download"

        if r.status_code >= 400:
            return FetchResult(url=url, status_code=r.status_code, content_type=ct, filename=filename, content=b"")

        chunks = []
        total = 0
        for part in r.iter_content(chunk_size=1024 * 64):
            if not part:
                continue
            chunks.append(part)
            total += len(part)
            if total > self.max_bytes:
                return FetchResult(url=url, status_code=413, content_type=ct, filename=filename, content=b"")

        return FetchResult(url=url, status_code=r.status_code, content_type=ct, filename=filename, content=b"".join(chunks))