from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..vision_tool import GeminiVision

from .pdf_extractor import extract_pdf
from .xlsx_extractor import extract_xlsx
from .csv_extractor import extract_csv_text
from .image_extractor import extract_image
from .pptx_extractor import extract_pptx
from .docx_extractor import extract_docx


@dataclass(frozen=True)
class Extracted:
    text: str
    mime: str


def route_extract(
    *,
    filename: str,
    mime: str,
    content: bytes,
    vision: Optional[GeminiVision],
    limits: dict,
) -> Optional[Extracted]:
    fn = (filename or "").lower()
    m = (mime or "").lower()

    # Skip Google native docs (need export flow; safe skip)
    if "application/vnd.google-apps" in m:
        return None

    if fn.endswith(".pdf") or m == "application/pdf":
        return Extracted(text=extract_pdf(content, vision=vision, limits=limits), mime=mime)

    if fn.endswith(".xlsx") or fn.endswith(".xls") or m in (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
    ):
        return Extracted(text=extract_xlsx(content, vision=vision, limits=limits), mime=mime)

    if fn.endswith(".pptx") or m == "application/vnd.openxmlformats-officedocument.presentationml.presentation":
        return Extracted(text=extract_pptx(content, vision=vision, limits=limits), mime=mime)

    if fn.endswith(".docx") or m == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        return Extracted(text=extract_docx(content, vision=vision, limits=limits), mime=mime)

    if fn.endswith(".csv") or m in ("text/csv", "application/csv"):
        return Extracted(text=extract_csv_text(content), mime=mime)

    if m.startswith("image/") or any(fn.endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"]):
        return Extracted(text=extract_image(content, mime=mime, vision=vision), mime=mime)

    if m.startswith("text/") or fn.endswith(".txt"):
        try:
            return Extracted(text=content.decode("utf-8", errors="ignore"), mime=mime)
        except Exception:
            return None

    return None