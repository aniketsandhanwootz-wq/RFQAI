# service/app/tools/file_extractors/pdf_extractor.py
from __future__ import annotations

from typing import List, Optional

from ...integrations.document_ai_client import DocumentAIClient


def extract_pdf(
    content: bytes,
    *,
    limits: dict,
    docai: Optional[DocumentAIClient],
) -> str:
    """
    Policy:
      - NO Gemini Vision for PDFs.
      - Use PyMuPDF text extraction per page.
      - If a page has very low extracted text, run Document AI OCR (once per PDF) and
        substitute OCR text for those low-text pages (mixed PDFs supported).
    """
    try:
        import fitz  # PyMuPDF
    except Exception:
        return ""

    pdf_max_pages = int(limits.get("PDF_MAX_PAGES", 120))
    text_threshold = int(limits.get("PDF_TEXT_THRESHOLD", 40))

    # DocAI controls
    docai_max_pages = int(limits.get("PDF_DOCAI_MAX_PAGES", 200))  # allow bigger than PDF_MAX_PAGES if you want
    docai_enabled = bool(docai and docai.enabled())

    doc = fitz.open(stream=content, filetype="pdf")
    n_total = len(doc)

    n_proc = min(n_total, pdf_max_pages)
    suffix = ""
    if n_total > pdf_max_pages:
        suffix = f"\n\n[SKIPPED_PAGES] total_pages={n_total} processed_pages={n_proc}"

    parts: List[str] = []
    low_pages: List[int] = []

    # 1) Extract selectable text per page
    page_texts: List[str] = []
    for i in range(n_proc):
        page = doc[i]
        txt = (page.get_text("text") or "").strip()
        page_texts.append(txt)
        if len(txt) < text_threshold:
            low_pages.append(i)

    # 2) If needed, run DocAI OCR once and map page OCR back
    ocr_pages: List[str] = []
    if docai_enabled and low_pages and n_total <= docai_max_pages:
        try:
            ocr_pages = docai.ocr_pdf_pages(content)
        except Exception:
            ocr_pages = []

    # 3) Merge output
    for i in range(n_proc):
        txt = page_texts[i]
        parts.append(f"\n\n--- PAGE {i+1} TEXT ---\n{txt}")

        # only add OCR when page is low-text and OCR exists
        if i in low_pages and ocr_pages and i < len(ocr_pages):
            ocr_txt = (ocr_pages[i] or "").strip()
            if ocr_txt:
                parts.append(f"\n--- PAGE {i+1} OCR_DOCAI ---\n{ocr_txt}")

    doc.close()
    return ("\n".join(parts).strip() + suffix).strip()