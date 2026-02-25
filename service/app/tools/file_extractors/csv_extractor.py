# service/app/tools/file_extractors/csv_extractor.py
from __future__ import annotations

import csv
import io

def extract_csv_text(content: bytes) -> str:
    try:
        text = content.decode("utf-8", errors="ignore")
        reader = csv.reader(io.StringIO(text))
        out = []
        for i, row in enumerate(reader):
            if not row:
                continue
            out.append(" | ".join([c.strip() for c in row if c is not None]))
            if i > 5000:
                break
        return "\n".join(out).strip()
    except Exception:
        return ""