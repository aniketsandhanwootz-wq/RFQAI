from __future__ import annotations

from typing import Optional

from ..vision_tool import GeminiVision, load_prompt

PROMPT_PATH = "packages/prompts/vision_extract_rich.md"


def extract_image(content: bytes, *, mime: str, vision: Optional[GeminiVision]) -> str:
    if not vision or not vision.enabled():
        return ""
    prompt = load_prompt(PROMPT_PATH)
    return vision.analyze_image(prompt=prompt, image_bytes=content, mime=mime or "image/png").strip()