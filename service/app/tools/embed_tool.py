# service/app/tools/embed_tool.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List
import requests


@dataclass(frozen=True)
class Embedder:
    api_key: str
    model: str = "gemini-embedding-001"
    output_dim: int = 1536

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """
        Calls Gemini embeddings endpoint. Hard-assert output dimension.
        """
        if not self.api_key:
            raise RuntimeError("GEMINI_API_KEY is missing")

        # Gemini API endpoint (v1beta)
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:batchEmbedContents?key={self.api_key}"

        # Each item: {content: {parts:[{text:"..."}]}}
        payload = {
            "requests": [
                {
                    "content": {"parts": [{"text": t}]},
                    "outputDimensionality": self.output_dim,
                }
                for t in texts
            ]
        }

        r = requests.post(url, json=payload, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"Gemini embeddings failed {r.status_code}: {r.text}")

        data = r.json()
        # response: { "embeddings": [ { "values": [...] }, ... ] }
        embs = data.get("embeddings") or []
        out: List[List[float]] = []
        for e in embs:
            vec = e.get("values") or []
            if len(vec) != self.output_dim:
                raise RuntimeError(f"Embedding dim mismatch: got {len(vec)} expected {self.output_dim}")
            out.append(vec)
        return out