from __future__ import annotations

import base64
from dataclasses import dataclass
from pathlib import Path
import requests


def repo_root() -> Path:
    # service/app/tools/vision_tool.py -> repo root = parents[3]
    return Path(__file__).resolve().parents[3]


def load_prompt(path: str) -> str:
    """
    Load prompt from repo, robust to current working directory.
    path is repo-relative, e.g. "packages/prompts/vision_extract_rich.md"
    """
    p = Path(path)
    if not p.is_absolute():
        p = repo_root() / p
    if not p.exists():
        raise FileNotFoundError(f"Prompt not found: {p}")
    return p.read_text(encoding="utf-8").strip()


@dataclass(frozen=True)
class GeminiVision:
    api_key: str
    model: str
    timeout_sec: int = 90

    def enabled(self) -> bool:
        return bool((self.api_key or "").strip())

    def analyze_image(self, *, prompt: str, image_bytes: bytes, mime: str = "image/png") -> str:
        if not self.enabled():
            return ""

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self.api_key}"
        b64 = base64.b64encode(image_bytes).decode("ascii")

        payload = {
            "contents": [
                {
                    "parts": [
                        {"text": prompt},
                        {"inline_data": {"mime_type": mime or "image/png", "data": b64}},
                    ]
                }
            ],
            "generationConfig": {"temperature": 0.2, "maxOutputTokens": 1024},
        }

        try:
            r = requests.post(url, json=payload, timeout=self.timeout_sec)
        except Exception:
            return ""

        if r.status_code >= 400:
            return ""

        data = r.json()
        try:
            return (data["candidates"][0]["content"]["parts"][0].get("text") or "").strip()
        except Exception:
            return ""