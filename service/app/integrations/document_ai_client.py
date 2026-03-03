# service/app/integrations/document_ai_client.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List
import base64
import json
import os
from pathlib import Path

from google.oauth2.service_account import Credentials as SACredentials
from google.api_core.client_options import ClientOptions

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
    # Optional credentials source: file path, raw JSON, or base64 JSON.
    credentials: str = ""


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
    def _safe_path_exists(p: str) -> bool:
        if not p:
            return False
        try:
            return Path(p).exists()
        except Exception:
            return False

    @staticmethod
    def _parse_service_account_info(src: str) -> Any:
        s = (src or "").strip()
        if not s:
            return None

        # Raw JSON string
        try:
            data = json.loads(s)
            if isinstance(data, dict) and data.get("type") == "service_account":
                return data
        except Exception:
            pass

        # Base64 encoded JSON
        try:
            decoded = base64.b64decode(s, validate=False).decode("utf-8")
            data = json.loads(decoded)
            if isinstance(data, dict) and data.get("type") == "service_account":
                return data
        except Exception:
            pass

        return None

    def _client(self) -> Any:
        if documentai is None:
            raise RuntimeError("google-cloud-documentai is not installed")

        # Priority:
        # 1) cfg.credentials (path/raw-json/base64)
        # 2) GOOGLE_APPLICATION_CREDENTIALS env value (path/raw-json/base64)
        # 3) default ADC resolution by google client libs
        src = (self.cfg.credentials or "").strip() or (os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")).strip()
        endpoint = f"{self.cfg.location}-documentai.googleapis.com" if self.cfg.location else None
        client_options = ClientOptions(api_endpoint=endpoint) if endpoint else None

        if src:
            if self._safe_path_exists(src):
                creds = SACredentials.from_service_account_file(src)
            else:
                info = self._parse_service_account_info(src)
                if not info:
                    raise RuntimeError(
                        "DocAI credentials must be a valid file path, raw service-account JSON, or base64 JSON."
                    )
                creds = SACredentials.from_service_account_info(info)
            return documentai.DocumentProcessorServiceClient(credentials=creds, client_options=client_options)

        return documentai.DocumentProcessorServiceClient(client_options=client_options)

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
