# service/app/tools/embed_tool.py
from __future__ import annotations

from dataclasses import dataclass
import os
import time
from typing import List, Optional
import requests


_TRANSIENT_HTTP = {408, 429, 500, 502, 503, 504}


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return max(0.0, float(value.strip()))
    except Exception:
        return None


@dataclass(frozen=True)
class Embedder:
    api_key: str
    model: str = "gemini-embedding-001"
    output_dim: int = 1536

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """
        Calls Gemini embeddings endpoint. Hard-assert output dimension.
        """
        if not texts:
            return []

        if not self.api_key:
            raise RuntimeError("GEMINI_API_KEY is missing")

        max_retries = int(os.getenv("EMBED_MAX_RETRIES", "6"))
        base_sleep = float(os.getenv("EMBED_RETRY_BASE_SEC", "2"))
        max_sleep = float(os.getenv("EMBED_RETRY_MAX_SEC", "60"))
        timeout_sec = int(os.getenv("EMBED_TIMEOUT_SEC", "60"))

        model_name = (self.model or "").strip()
        if model_name.startswith("models/"):
            model_name = model_name.split("/", 1)[1]
        req_model = f"models/{model_name}"

        # Gemini API endpoint (v1beta)
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:batchEmbedContents?key={self.api_key}"

        # Each item: {content: {parts:[{text:"..."}]}}
        payload = {
            "requests": [
                {
                    "model": req_model,
                    "content": {"parts": [{"text": t}]},
                    "outputDimensionality": self.output_dim,
                }
                for t in texts
            ]
        }

        last_err = "unknown embedding error"
        r = None
        for attempt in range(max_retries + 1):
            try:
                r = requests.post(url, json=payload, timeout=timeout_sec)
            except requests.RequestException as e:
                last_err = f"request error: {e}"
                if attempt >= max_retries:
                    raise RuntimeError(f"Gemini embeddings failed: {last_err}")
                sleep_s = min(max_sleep, base_sleep * (2**attempt))
                time.sleep(sleep_s)
                continue

            if r.status_code < 400:
                break

            last_err = f"http {r.status_code}: {(r.text or '')[:800]}"
            if r.status_code in _TRANSIENT_HTTP and attempt < max_retries:
                retry_after = _parse_retry_after(r.headers.get("Retry-After"))
                sleep_s = retry_after if retry_after is not None else min(max_sleep, base_sleep * (2**attempt))
                time.sleep(sleep_s)
                continue

            raise RuntimeError(f"Gemini embeddings failed {last_err}")

        if r is None or r.status_code >= 400:
            raise RuntimeError(f"Gemini embeddings failed {last_err}")

        data = r.json()
        # response: { "embeddings": [ { "values": [...] }, ... ] }
        embs = data.get("embeddings") or []
        out: List[List[float]] = []
        for e in embs:
            vec = e.get("values") or []
            if len(vec) != self.output_dim:
                raise RuntimeError(f"Embedding dim mismatch: got {len(vec)} expected {self.output_dim}")
            out.append(vec)

        if len(out) != len(texts):
            raise RuntimeError(f"Embedding count mismatch: got {len(out)} expected {len(texts)}")
        return out
