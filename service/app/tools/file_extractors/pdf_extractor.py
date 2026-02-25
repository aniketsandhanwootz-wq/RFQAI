from __future__ import annotations

from typing import Optional, List

from ..vision_tool import GeminiVision, load_prompt


PROMPT_PATH = "packages/prompts/vision_extract_rich.md"


def _pick_pages_for_vision(n_pages: int, max_pages: int) -> List[int]:
    """
    Sample pages deterministically: first 3, last 2, and evenly spaced in between.
    """
    if n_pages <= 0:
        return []
    if n_pages <= max_pages:
        return list(range(n_pages))

    picks = []
    picks += [0, 1, 2]
    picks += [n_pages - 2, n_pages - 1]

    remaining = max_pages - len(picks)
    if remaining > 0:
        step = max(1, n_pages // (remaining + 1))
        i = step
        while len(picks) < max_pages and i < n_pages - 2:
            picks.append(i)
            i += step

    # unique, sorted
    return sorted(set([p for p in picks if 0 <= p < n_pages]))[:max_pages]


def extract_pdf(content: bytes, *, vision: Optional[GeminiVision], limits: dict) -> str:
    """
    - Extract normal text for every page.
    - If page text is very low, use Gemini Vision on that page image.
    - Apply caps: PDF_MAX_PAGES (hard skip beyond), PDF_VISION_MAX_PAGES (budget).
    """
    try:
        import fitz  # PyMuPDF
    except Exception:
        return ""

    pdf_max_pages = int(limits.get("PDF_MAX_PAGES", 120))
    vision_max_pages = int(limits.get("PDF_VISION_MAX_PAGES", 12))
    text_threshold = int(limits.get("PDF_VISION_TEXT_THRESHOLD", 40))

    prompt = load_prompt(PROMPT_PATH)

    doc = fitz.open(stream=content, filetype="pdf")
    n = len(doc)

    if n > pdf_max_pages:
        # Too large -> keep only first pdf_max_pages text pages; still deterministic
        n_proc = pdf_max_pages
        suffix = f"\n\n[SKIPPED_PAGES] total_pages={n} processed_pages={n_proc}"
    else:
        n_proc = n
        suffix = ""

    parts = []
    low_text_pages = []

    for i in range(n_proc):
        page = doc[i]
        txt = (page.get_text("text") or "").strip()
        parts.append(f"\n\n--- PAGE {i+1} TEXT ---\n{txt}")
        if len(txt) < text_threshold:
            low_text_pages.append(i)

    # Vision budget: prioritize low-text pages; if too many, sample deterministically
    if vision and vision.enabled() and vision_max_pages > 0:
        if len(low_text_pages) > 0:
            pages_for_vision = low_text_pages[:vision_max_pages]
        else:
            pages_for_vision = _pick_pages_for_vision(n_proc, vision_max_pages)

        for i in pages_for_vision:
            try:
                page = doc[i]
                pix = page.get_pixmap(dpi=180)
                img = pix.tobytes("png")
                vis = vision.analyze_image(prompt=prompt, image_bytes=img, mime="image/png")
                if vis:
                    parts.append(f"\n--- PAGE {i+1} VISION ---\n{vis}")
            except Exception:
                continue

    doc.close()
    return ("\n".join(parts).strip() + suffix).strip()