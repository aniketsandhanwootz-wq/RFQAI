from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import mimetypes

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


_IMAGE_EXTS = {
    ".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff",
    ".gif", ".jfif", ".pjpeg", ".pjp"
}


def _norm_filename(name: str) -> str:
    return (name or "").strip()


def _norm_mime(m: str) -> str:
    return (m or "").split(";")[0].strip().lower()


def _guess_mime(filename: str, mime: str) -> str:
    m = _norm_mime(mime)
    if m and m != "application/octet-stream":
        return m
    fn = _norm_filename(filename).lower()
    guess, _ = mimetypes.guess_type(fn)
    return (guess or m or "").lower()


def _is_image(filename: str, mime: str) -> bool:
    fn = _norm_filename(filename).lower()
    m = _norm_mime(mime)
    if m.startswith("image/"):
        return True
    for ext in _IMAGE_EXTS:
        if fn.endswith(ext):
            return True
    return False


def route_extract(
    *,
    filename: str,
    mime: str,
    content: bytes,
    vision: Optional[GeminiVision],
    limits: dict,
) -> Optional[Extracted]:
    fn = _norm_filename(filename).lower()
    m = _guess_mime(filename, mime)

    # Skip Google native docs (need export flow; safe skip)
    if "application/vnd.google-apps" in m:
        return None

    if fn.endswith(".pdf") or m == "application/pdf":
        return Extracted(text=extract_pdf(content, vision=vision, limits=limits), mime=m or "application/pdf")

    if fn.endswith(".xlsx") or fn.endswith(".xls") or m in (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
    ):
        return Extracted(text=extract_xlsx(content, vision=vision, limits=limits), mime=m)

    if fn.endswith(".pptx") or m == "application/vnd.openxmlformats-officedocument.presentationml.presentation":
        return Extracted(text=extract_pptx(content, vision=vision, limits=limits), mime=m)

    if fn.endswith(".docx") or m == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        return Extracted(text=extract_docx(content, vision=vision, limits=limits), mime=m)

    if fn.endswith(".csv") or m in ("text/csv", "application/csv"):
        return Extracted(text=extract_csv_text(content), mime=m or "text/csv")

    if _is_image(fn, m):
        use_mime = m if m else "image/png"
        return Extracted(text=extract_image(content, mime=use_mime, vision=vision), mime=use_mime)

    if m.startswith("text/") or fn.endswith(".txt"):
        try:
            return Extracted(text=content.decode("utf-8", errors="ignore"), mime=m or "text/plain")
        except Exception:
            return None

    return None