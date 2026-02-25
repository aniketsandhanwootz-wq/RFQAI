# service/app/tools/file_extractors/xlsx_extractor.py
from __future__ import annotations

def extract_xlsx_text(content: bytes) -> str:
    """
    Extract visible cell text from XLSX (no embedded images for now).
    """
    try:
        import io
        from openpyxl import load_workbook  # type: ignore

        wb = load_workbook(io.BytesIO(content), data_only=True, read_only=True)
        parts = []
        for ws in wb.worksheets:
            lines = []
            for row in ws.iter_rows(values_only=True):
                vals = [str(v).strip() for v in row if v is not None and str(v).strip() != ""]
                if vals:
                    lines.append(" | ".join(vals))
            if lines:
                parts.append(f"\n\n--- SHEET {ws.title} ---\n" + "\n".join(lines))
        return "\n".join(parts).strip()
    except Exception:
        return ""