# service/app/tools/file_extractors/router.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import mimetypes

from ...integrations.document_ai_client import DocumentAIClient
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
    ".gif", ".jfif", ".pjpeg", ".pjp", ".heic", ".heif", ".avif", ".ico", ".svg"
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


def _sniff_image_mime(content: bytes) -> Optional[str]:
    # Keep this lightweight and deterministic for empty/missing MIME cases.
    sig = content[:16]
    if sig.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if sig.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if sig.startswith(b"GIF87a") or sig.startswith(b"GIF89a"):
        return "image/gif"
    if sig.startswith(b"BM"):
        return "image/bmp"
    if len(sig) >= 12 and sig[:4] == b"RIFF" and sig[8:12] == b"WEBP":
        return "image/webp"
    if sig.startswith(b"II*\x00") or sig.startswith(b"MM\x00*"):
        return "image/tiff"
    return None


def route_extract(
    *,
    filename: str,
    mime: str,
    content: bytes,
    vision: Optional[GeminiVision],
    limits: dict,
    docai: Optional[DocumentAIClient] = None,
) -> Optional[Extracted]:
    fn = _norm_filename(filename).lower()
    m = _guess_mime(filename, mime)
    sniffed_image_mime = _sniff_image_mime(content) if not m else None

    # Skip Google native docs (need export flow; safe skip)
    if "application/vnd.google-apps" in m:
        return None

    if fn.endswith(".pdf") or m == "application/pdf":
        return Extracted(
            text=extract_pdf(content, limits=limits, docai=docai),
            mime=m or "application/pdf",
        )

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

    if _is_image(fn, m) or (not m and sniffed_image_mime):
        use_mime = m if m else (sniffed_image_mime or "image/png")
        return Extracted(text=extract_image(content, mime=use_mime, vision=vision), mime=use_mime)

    if m.startswith("text/") or fn.endswith(".txt"):
        try:
            return Extracted(text=content.decode("utf-8", errors="ignore"), mime=m or "text/plain")
        except Exception:
            return None

    return None
