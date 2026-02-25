from __future__ import annotations

from typing import Optional

from ..vision_tool import GeminiVision, load_prompt

PROMPT_PATH = "packages/prompts/vision_extract_rich.md"


def extract_docx(content: bytes, *, vision: Optional[GeminiVision], limits: dict) -> str:
    try:
        import io
        from docx import Document
    except Exception:
        return ""

    prompt = load_prompt(PROMPT_PATH)
    max_imgs = int(limits.get("DOCX_VISION_MAX_IMAGES", 10))

    doc = Document(io.BytesIO(content))
    parts = []

    paras = [p.text.strip() for p in doc.paragraphs if p.text and p.text.strip()]
    if paras:
        parts.append("--- DOCX TEXT ---\n" + "\n".join(paras))

    if vision and vision.enabled() and max_imgs > 0:
        try:
            rels = doc.part.related_parts  # type: ignore[attr-defined]
            img_idx = 0
            for _, part in rels.items():
                if img_idx >= max_imgs:
                    break
                ct = getattr(part, "content_type", "") or ""
                if ct.startswith("image/"):
                    img_idx += 1
                    blob = part.blob
                    vis = vision.analyze_image(prompt=prompt, image_bytes=blob, mime=ct)
                    if vis:
                        parts.append(f"\n--- DOCX IMAGE {img_idx} VISION ---\n{vis}")
        except Exception:
            pass

    return "\n".join(parts).strip()