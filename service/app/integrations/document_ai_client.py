# service/app/integrations/document_ai_client.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List

try:
    # google-cloud-documentai
    from google.cloud import documentai  # type: ignore
except Exception:  # pragma: no cover - optional dependency at runtime
    documentai = None  # type: ignore[assignment]


@dataclass(frozen=True)
class DocAIConfig:
    project_id: str
    location: str
    processor_id: str
    processor_version: str = ""  # optional


class DocumentAIClient:
    """
    Google Document AI OCR for PDFs (scanned or low-text pages).
    We run it on the full PDF once (when needed), then return per-page extracted text.
    """

    def __init__(self, cfg: DocAIConfig):
        self.cfg = cfg

    def enabled(self) -> bool:
        return bool(
            documentai is not None
            and self.cfg.project_id
            and self.cfg.location
            and self.cfg.processor_id
        )

    @staticmethod
    def _client() -> Any:
        if documentai is None:
            raise RuntimeError("google-cloud-documentai is not installed")
        return documentai.DocumentProcessorServiceClient()

    def _processor_name(self) -> str:
        client = self._client()
        if self.cfg.processor_version:
            return client.processor_version_path(
                self.cfg.project_id, self.cfg.location, self.cfg.processor_id, self.cfg.processor_version
            )
        return client.processor_path(self.cfg.project_id, self.cfg.location, self.cfg.processor_id)

    @staticmethod
    def _page_text(doc: Any, page: Any) -> str:
        """
        Extract page text using text anchors into doc.text.
        """
        if not doc.text:
            return ""
        out = []
        anchors = getattr(page.layout, "text_anchor", None)
        if not anchors or not anchors.text_segments:
            return ""
        for seg in anchors.text_segments:
            start = int(getattr(seg, "start_index", 0) or 0)
            end = int(getattr(seg, "end_index", 0) or 0)
            if end > start:
                out.append(doc.text[start:end])
        return "".join(out).strip()

    def ocr_pdf_pages(self, pdf_bytes: bytes, mime: str = "application/pdf") -> List[str]:
        """
        Returns per-page OCR text (index aligned with PDF pages).
        """
        if not self.enabled():
            return []

        if documentai is None:
            return []

        client = self._client()
        name = self._processor_name()

        raw_document = documentai.RawDocument(content=pdf_bytes, mime_type=mime)
        req = documentai.ProcessRequest(name=name, raw_document=raw_document)

        result = client.process_document(request=req)
        doc = result.document

        pages = doc.pages or []
        per_page = []
        for p in pages:
            per_page.append(self._page_text(doc, p))
        return per_page
