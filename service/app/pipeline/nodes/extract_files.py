# service/app/pipeline/nodes/extract_files.py
from __future__ import annotations

from typing import Any, Dict, List
import hashlib
import os

from ...config import Settings
from ...integrations.drive_client import DriveClient
from ...integrations.fetch_client import FetchClient
from ...tools.db_tool import DB, tx
from ...tools.vision_tool import GeminiVision
from ...tools.file_extractors.router import route_extract
from ..state import IngestState, TextDoc


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def extract_files_node(
    state: IngestState,
    settings: Settings,
    db: DB,
    drive: DriveClient,
    fetcher: FetchClient,
) -> IngestState:
    """
    For each source target:
      - if Drive folder -> crawl recursively -> create rfq.files entries for files
      - download accessible files
      - extract text with router (text + vision)
      - emit TextDoc(doc_type=FILE_CHUNK) as a raw extracted doc (chunking later)
    """
    if not state.file_targets:
        return state

    # limits from env (safe defaults)
    limits = {
        "MAX_FILE_MB": int(os.getenv("MAX_FILE_MB", str(settings.ingest_file_max_mb))),
        "PDF_MAX_PAGES": int(os.getenv("PDF_MAX_PAGES", "120")),
        "PDF_VISION_MAX_PAGES": int(os.getenv("PDF_VISION_MAX_PAGES", "12")),
        "PDF_VISION_TEXT_THRESHOLD": int(os.getenv("PDF_VISION_TEXT_THRESHOLD", "40")),
        "XLSX_VISION_MAX_IMAGES": int(os.getenv("XLSX_VISION_MAX_IMAGES", "10")),
        "PPTX_VISION_MAX_IMAGES": int(os.getenv("PPTX_VISION_MAX_IMAGES", "10")),
        "DOCX_VISION_MAX_IMAGES": int(os.getenv("DOCX_VISION_MAX_IMAGES", "10")),
    }

    vision = GeminiVision(
        api_key=settings.gemini_api_key,
        model=os.getenv("GEMINI_VISION_MODEL", "gemini-1.5-flash"),
        timeout_sec=int(os.getenv("VISION_TIMEOUT_SEC", "90")),
    )

    max_mb = settings.ingest_file_max_mb
    docs: List[TextDoc] = list(state.docs)

    for t in state.file_targets:
        url = (t.get("url") or "").strip()
        if not url:
            continue

        product_id = t.get("product_id")
        query_id = t.get("query_id")
        source_kind = t.get("source_kind") or "DIRECT_URL"

        # ---- Drive handling (deep folder traversal) ----
        root_id = drive.resolve_root(url) if drive.enabled() else None

        if root_id and drive.enabled():
            try:
                items = drive.list_recursive(root_id, max_items=5000)
            except Exception as e:
                state.warnings.append(f"Drive crawl failed for url={url}: {e}")
                continue

            for it in items:
                # inventory row
                with tx(db) as cur:
                    cur.execute(
                        """
                        INSERT INTO rfq.files (
                          rfq_id, product_id, query_id,
                          source_kind, root_url,
                          provider, provider_id,
                          is_folder, parent_provider_id, path, name, mime,
                          size_bytes, modified_at,
                          fetch_status, parse_status, ingested_at
                        )
                        VALUES (
                          %(rfq_id)s, %(product_id)s, %(query_id)s,
                          %(source_kind)s, %(root_url)s,
                          'gdrive', %(provider_id)s,
                          %(is_folder)s, %(parent_provider_id)s, %(path)s, %(name)s, %(mime)s,
                          %(size_bytes)s, %(modified_at)s,
                          'PENDING', 'PENDING', now()
                        )
                        ON CONFLICT DO NOTHING
                        ;
                        """,
                        {
                            "rfq_id": state.rfq_id,
                            "product_id": product_id,
                            "query_id": query_id,
                            "source_kind": source_kind,
                            "root_url": url,
                            "provider_id": it.provider_id,
                            "is_folder": it.is_folder,
                            "parent_provider_id": it.parent_provider_id,
                            "path": it.path,
                            "name": it.name,
                            "mime": it.mime,
                            "size_bytes": it.size_bytes,
                            "modified_at": it.modified_at,
                        },
                    )

                if it.is_folder:
                    continue

                # download
                try:
                    content = drive.download(it.provider_id, max_mb=max_mb)
                except Exception as e:
                    state.warnings.append(f"Drive download failed {it.path}: {e}")
                    continue

                if not content:
                    continue

                checksum = _sha256(content)

                extracted = route_extract(
                    filename=it.name,
                    mime=it.mime,
                    content=content,
                    vision=vision,
                    limits=limits,
                )
                if not extracted or not extracted.text.strip():
                    continue

                docs.append(
                    TextDoc(
                        doc_type="FILE_CHUNK",
                        rfq_id=state.rfq_id,
                        product_id=product_id,
                        query_id=query_id,
                        title=it.path,
                        text=extracted.text,
                        meta={
                            "provider": "gdrive",
                            "provider_id": it.provider_id,
                            "path": it.path,
                            "mime": it.mime,
                            "checksum_sha256": checksum,
                            "source_kind": source_kind,
                            "root_url": url,
                        },
                    )
                )
            continue

        # ---- Generic HTTP fetch ----
        fr = fetcher.fetch(url)
        if not fr or fr.status_code >= 400 or not fr.content:
            with tx(db) as cur:
                cur.execute(
                    """
                    INSERT INTO rfq.files (
                      rfq_id, product_id, query_id,
                      source_kind, root_url,
                      provider, provider_id,
                      is_folder, name, mime,
                      fetch_status, parse_status, error, ingested_at
                    )
                    VALUES (
                      %(rfq_id)s, %(product_id)s, %(query_id)s,
                      %(source_kind)s, %(root_url)s,
                      'http', %(provider_id)s,
                      false, %(name)s, %(mime)s,
                      %(fetch_status)s, 'SKIPPED', %(error)s, now()
                    )
                    ;
                    """,
                    {
                        "rfq_id": state.rfq_id,
                        "product_id": product_id,
                        "query_id": query_id,
                        "source_kind": source_kind,
                        "root_url": url,
                        "provider_id": url,
                        "name": (fr.filename if fr else ""),
                        "mime": (fr.content_type if fr else ""),
                        "fetch_status": "FAILED",
                        "error": f"http status {fr.status_code}" if fr else "fetch error",
                    },
                )
            continue

        checksum = _sha256(fr.content)

        extracted = route_extract(
            filename=fr.filename,
            mime=fr.content_type,
            content=fr.content,
            vision=vision,
            limits=limits,
        )
        if extracted and extracted.text.strip():
            docs.append(
                TextDoc(
                    doc_type="FILE_CHUNK",
                    rfq_id=state.rfq_id,
                    product_id=product_id,
                    query_id=query_id,
                    title=fr.filename,
                    text=extracted.text,
                    meta={
                        "provider": "http",
                        "provider_id": url,
                        "mime": fr.content_type,
                        "checksum_sha256": checksum,
                        "source_kind": source_kind,
                        "root_url": url,
                    },
                )
            )

    state.docs = docs
    return state