# service/app/tools/file_extractors/router.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from .pdf_extractor import extract_pdf_text
from .xlsx_extractor import extract_xlsx_text
from .csv_extractor import extract_csv_text
from .image_extractor import extract_image_text


@dataclass(frozen=True)
class Extracted:
    text: str
    mime: str
    notes: str = ""


def route_extract(filename: str, mime: str, content: bytes) -> Optional[Extracted]:
    """
    Returns extracted text or None (skip).
    """
    fn = (filename or "").lower()
    m = (mime or "").lower()

    # Skip Google native docs (need export flow)
    if "application/vnd.google-apps" in m:
        return None

    if fn.endswith(".pdf") or m == "application/pdf":
        return Extracted(text=extract_pdf_text(content), mime=mime)

    if fn.endswith(".xlsx") or fn.endswith(".xls") or "spreadsheet" in m or m in (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
    ):
        return Extracted(text=extract_xlsx_text(content), mime=mime)

    if fn.endswith(".csv") or m in ("text/csv", "application/csv"):
        return Extracted(text=extract_csv_text(content), mime=mime)

    if any(fn.endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"]) or m.startswith("image/"):
        return Extracted(text=extract_image_text(content), mime=mime)

    # DOC/PPT: safe skip now, add later if needed
    if any(fn.endswith(ext) for ext in [".doc", ".docx", ".ppt", ".pptx"]):
        return None

    # plain text fallback
    if m.startswith("text/") or fn.endswith(".txt"):
        try:
            return Extracted(text=content.decode("utf-8", errors="ignore"), mime=mime)
        except Exception:
            return None

    return None