# service/app/tools/file_extractors/pdf_extractor.py
from __future__ import annotations

def extract_pdf_text(content: bytes) -> str:
    """
    Best-effort PDF text extraction with multiple fallbacks.
    """
    # 1) PyMuPDF
    try:
        import fitz  # type: ignore

        doc = fitz.open(stream=content, filetype="pdf")
        parts = []
        for i in range(len(doc)):
            page = doc[i]
            txt = page.get_text("text") or ""
            if txt.strip():
                parts.append(f"\n\n--- PAGE {i+1} ---\n{txt}")
        doc.close()
        return "\n".join(parts).strip()
    except Exception:
        pass

    # 2) pdfplumber
    try:
        import pdfplumber  # type: ignore
        import io

        parts = []
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for i, page in enumerate(pdf.pages):
                txt = page.extract_text() or ""
                if txt.strip():
                    parts.append(f"\n\n--- PAGE {i+1} ---\n{txt}")
        return "\n".join(parts).strip()
    except Exception:
        pass

    # 3) pypdf
    try:
        from pypdf import PdfReader  # type: ignore
        import io

        reader = PdfReader(io.BytesIO(content))
        parts = []
        for i, page in enumerate(reader.pages):
            txt = page.extract_text() or ""
            if txt.strip():
                parts.append(f"\n\n--- PAGE {i+1} ---\n{txt}")
        return "\n".join(parts).strip()
    except Exception:
        pass

    return ""