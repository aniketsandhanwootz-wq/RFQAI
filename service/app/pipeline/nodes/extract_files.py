# service/app/pipeline/nodes/extract_files.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
import hashlib
import os
from ...integrations.document_ai_client import DocumentAIClient, DocAIConfig
from ...config import Settings
from ...integrations.drive_client import DriveClient
from ...integrations.fetch_client import FetchClient, FetchResult
from ...tools.db_tool import DB, tx
from ...tools.vision_tool import GeminiVision
from ...tools.file_extractors.router import route_extract
from ..state import IngestState, TextDoc


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _upsert_file_row(
    *,
    db: DB,
    rfq_id: str,
    product_id: Optional[str],
    query_id: Optional[str],
    source_kind: str,
    root_url: str,
    provider: str,
    provider_id: str,
    is_folder: bool,
    parent_provider_id: Optional[str],
    path: str,
    name: str,
    mime: str,
    size_bytes: Optional[int],
    modified_at: Any,
    checksum_sha256: Optional[str],
    fetch_status: str,
    parse_status: str,
    error: Optional[str],
) -> None:
    """
    Idempotent insert/update into rfq.files using unique key:
    (rfq_id, provider, provider_id, is_folder, path)
    """
    with tx(db) as cur:
        cur.execute(
            """
            INSERT INTO rfq.files (
              rfq_id, product_id, query_id,
              source_kind, root_url,
              provider, provider_id,
              is_folder, parent_provider_id, path, name, mime,
              size_bytes, modified_at,
              checksum_sha256,
              fetch_status, parse_status, error,
              ingested_at
            )
            VALUES (
              %(rfq_id)s, %(product_id)s, %(query_id)s,
              %(source_kind)s, %(root_url)s,
              %(provider)s, %(provider_id)s,
              %(is_folder)s, %(parent_provider_id)s, %(path)s, %(name)s, %(mime)s,
              %(size_bytes)s, %(modified_at)s,
              %(checksum_sha256)s,
              %(fetch_status)s, %(parse_status)s, %(error)s,
              now()
            )
            ON CONFLICT (rfq_id, provider, provider_id, is_folder, path) DO UPDATE SET
              product_id=EXCLUDED.product_id,
              query_id=EXCLUDED.query_id,
              source_kind=EXCLUDED.source_kind,
              root_url=EXCLUDED.root_url,
              parent_provider_id=EXCLUDED.parent_provider_id,
              name=EXCLUDED.name,
              mime=EXCLUDED.mime,
              size_bytes=EXCLUDED.size_bytes,
              modified_at=EXCLUDED.modified_at,
              checksum_sha256=COALESCE(EXCLUDED.checksum_sha256, rfq.files.checksum_sha256),
              fetch_status=EXCLUDED.fetch_status,
              parse_status=EXCLUDED.parse_status,
              error=EXCLUDED.error,
              ingested_at=now()
            ;
            """,
            {
                "rfq_id": rfq_id,
                "product_id": product_id,
                "query_id": query_id,
                "source_kind": source_kind,
                "root_url": root_url,
                "provider": provider,
                "provider_id": provider_id,
                "is_folder": is_folder,
                "parent_provider_id": parent_provider_id,
                "path": path or "",
                "name": name,
                "mime": mime,
                "size_bytes": size_bytes,
                "modified_at": modified_at,
                "checksum_sha256": checksum_sha256,
                "fetch_status": fetch_status,
                "parse_status": parse_status,
                "error": error,
            },
        )


def extract_files_node(
    state: IngestState,
    settings: Settings,
    db: DB,
    drive: DriveClient,
    fetcher: FetchClient,
) -> IngestState:
    """
    Cron-safe behavior:
    - Always record a file row (even if inaccessible) with FAILED/SKIPPED status.
    - On later runs, if it becomes accessible, the same row is updated to FETCHED/PARSED + checksum.
    """
    if not state.file_targets:
        return state

    limits = {
        "MAX_FILE_MB": int(os.getenv("MAX_FILE_MB", str(settings.ingest_file_max_mb))),
        "PDF_MAX_PAGES": int(os.getenv("PDF_MAX_PAGES", "120")),
        "PDF_TEXT_THRESHOLD": int(os.getenv("PDF_TEXT_THRESHOLD", "40")),
        "PDF_DOCAI_MAX_PAGES": int(os.getenv("PDF_DOCAI_MAX_PAGES", "200")),
        "XLSX_VISION_MAX_IMAGES": int(os.getenv("XLSX_VISION_MAX_IMAGES", "10")),
        "XLSX_MAX_CELL_LINES": int(os.getenv("XLSX_MAX_CELL_LINES", "5000")),
        "PPTX_VISION_MAX_IMAGES": int(os.getenv("PPTX_VISION_MAX_IMAGES", "10")),
        "DOCX_VISION_MAX_IMAGES": int(os.getenv("DOCX_VISION_MAX_IMAGES", "10")),
    }


    docai = None
    if os.getenv("DOCAI_PROJECT_ID") and os.getenv("DOCAI_LOCATION") and os.getenv("DOCAI_PROCESSOR_ID"):
        docai = DocumentAIClient(
            DocAIConfig(
                project_id=os.getenv("DOCAI_PROJECT_ID", ""),
                location=os.getenv("DOCAI_LOCATION", ""),
                processor_id=os.getenv("DOCAI_PROCESSOR_ID", ""),
                processor_version=os.getenv("DOCAI_PROCESSOR_VERSION", ""),
            )
        )

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
                # record failure for the root link itself (so cron can retry later)
                _upsert_file_row(
                    db=db,
                    rfq_id=state.rfq_id,
                    product_id=product_id,
                    query_id=query_id,
                    source_kind=source_kind,
                    root_url=url,
                    provider="gdrive",
                    provider_id=root_id,
                    is_folder=True,
                    parent_provider_id=None,
                    path="",
                    name="",
                    mime="",
                    size_bytes=None,
                    modified_at=None,
                    checksum_sha256=None,
                    fetch_status="FAILED",
                    parse_status="SKIPPED",
                    error=str(e)[:500],
                )
                continue

            for it in items:
                # Always upsert inventory row
                _upsert_file_row(
                    db=db,
                    rfq_id=state.rfq_id,
                    product_id=product_id,
                    query_id=query_id,
                    source_kind=source_kind,
                    root_url=url,
                    provider="gdrive",
                    provider_id=it.provider_id,
                    is_folder=it.is_folder,
                    parent_provider_id=it.parent_provider_id,
                    path=it.path or "",
                    name=it.name or "",
                    mime=it.mime or "",
                    size_bytes=it.size_bytes,
                    modified_at=it.modified_at,
                    checksum_sha256=None,
                    fetch_status="PENDING",
                    parse_status="PENDING",
                    error=None,
                )

                if it.is_folder:
                    continue

                # Download
                try:
                    content = drive.download(it.provider_id, max_mb=max_mb)
                except Exception as e:
                    state.warnings.append(f"Drive download failed {it.path}: {e}")
                    _upsert_file_row(
                        db=db,
                        rfq_id=state.rfq_id,
                        product_id=product_id,
                        query_id=query_id,
                        source_kind=source_kind,
                        root_url=url,
                        provider="gdrive",
                        provider_id=it.provider_id,
                        is_folder=False,
                        parent_provider_id=it.parent_provider_id,
                        path=it.path or "",
                        name=it.name or "",
                        mime=it.mime or "",
                        size_bytes=it.size_bytes,
                        modified_at=it.modified_at,
                        checksum_sha256=None,
                        fetch_status="FAILED",
                        parse_status="SKIPPED",
                        error=str(e)[:500],
                    )
                    continue

                if not content:
                    _upsert_file_row(
                        db=db,
                        rfq_id=state.rfq_id,
                        product_id=product_id,
                        query_id=query_id,
                        source_kind=source_kind,
                        root_url=url,
                        provider="gdrive",
                        provider_id=it.provider_id,
                        is_folder=False,
                        parent_provider_id=it.parent_provider_id,
                        path=it.path or "",
                        name=it.name or "",
                        mime=it.mime or "",
                        size_bytes=it.size_bytes,
                        modified_at=it.modified_at,
                        checksum_sha256=None,
                        fetch_status="FAILED",
                        parse_status="SKIPPED",
                        error="empty content",
                    )
                    continue

                checksum = _sha256(content)

                extracted = route_extract(
                    filename=it.name or "",
                    mime=it.mime or "",
                    content=content,
                    vision=vision,
                    limits=limits,
                    docai=docai,
                )

                # Update file row status with checksum even if text extraction returns empty
                _upsert_file_row(
                    db=db,
                    rfq_id=state.rfq_id,
                    product_id=product_id,
                    query_id=query_id,
                    source_kind=source_kind,
                    root_url=url,
                    provider="gdrive",
                    provider_id=it.provider_id,
                    is_folder=False,
                    parent_provider_id=it.parent_provider_id,
                    path=it.path or "",
                    name=it.name or "",
                    mime=it.mime or "",
                    size_bytes=it.size_bytes,
                    modified_at=it.modified_at,
                    checksum_sha256=checksum,
                    fetch_status="FETCHED",
                    parse_status="PARSED" if (extracted and extracted.text.strip()) else "SKIPPED",
                    error=None,
                )

                if extracted and extracted.text.strip():
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
        fr = None
        try:
            fr = fetcher.fetch(url)
        except Exception as e:
            fr = None
            state.warnings.append(f"HTTP fetch exception url={url}: {e}")

        if not fr or fr.status_code >= 400 or not fr.content:
            _upsert_file_row(
                db=db,
                rfq_id=state.rfq_id,
                product_id=product_id,
                query_id=query_id,
                source_kind=source_kind,
                root_url=url,
                provider="http",
                provider_id=url,
                is_folder=False,
                parent_provider_id=None,
                path=url,
                name=(fr.filename if fr else ""),
                mime=(fr.content_type if fr else ""),
                size_bytes=None,
                modified_at=None,
                checksum_sha256=None,
                fetch_status="FAILED",
                parse_status="SKIPPED",
                error=(f"http status {fr.status_code}" if fr else "fetch error")[:500],
            )
            continue

        checksum = _sha256(fr.content)

        extracted = route_extract(
            filename=fr.filename,
            mime=fr.content_type,
            content=fr.content,
            vision=vision,
            limits=limits,
            docai=docai,
        )

        _upsert_file_row(
            db=db,
            rfq_id=state.rfq_id,
            product_id=product_id,
            query_id=query_id,
            source_kind=source_kind,
            root_url=url,
            provider="http",
            provider_id=url,
            is_folder=False,
            parent_provider_id=None,
            path=url,
            name=fr.filename,
            mime=fr.content_type,
            size_bytes=len(fr.content),
            modified_at=None,
            checksum_sha256=checksum,
            fetch_status="FETCHED",
            parse_status="PARSED" if (extracted and extracted.text.strip()) else "SKIPPED",
            error=None,
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
