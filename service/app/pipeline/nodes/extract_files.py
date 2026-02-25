# service/app/pipeline/nodes/extract_files.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import hashlib

from ...config import Settings
from ...integrations.drive_client import DriveClient
from ...integrations.fetch_client import FetchClient
from ...tools.db_tool import DB, tx
from ...tools.file_extractors.router import route_extract
from ..state import IngestState, TextDoc


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def extract_files_node(state: IngestState, settings: Settings, db: DB, drive: DriveClient, fetcher: FetchClient) -> IngestState:
    """
    For each source target:
      - if Drive folder -> crawl recursively -> create rfq.files entries for files
      - download accessible files
      - extract text with router
      - emit TextDoc(doc_type=FILE_CHUNK) as a raw extracted doc (chunking later)
    """
    if not state.file_targets:
        return state

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
        if drive.enabled():
            root_id = drive.resolve_root(url)
        else:
            root_id = None

        if root_id and drive.enabled():
            try:
                items = drive.list_recursive(root_id, max_items=5000)
            except Exception as e:
                state.warnings.append(f"Drive crawl failed for url={url}: {e}")
                continue

            # Insert folder/file inventory (skip folders in extraction stage)
            for it in items:
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

                # Download file
                try:
                    content = drive.download(it.provider_id, max_mb=max_mb)
                except Exception as e:
                    state.warnings.append(f"Drive download failed {it.path}: {e}")
                    continue

                if not content:
                    continue

                checksum = _sha256(content)
                extracted = route_extract(it.name, it.mime, content)
                if not extracted or not extracted.text.strip():
                    # no extractable text is fine (images w/out OCR etc.)
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
            # store as skipped (not fatal)
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
                        "fetch_status": "FAILED" if fr else "FAILED",
                        "error": f"http status {fr.status_code}" if fr else "fetch error",
                    },
                )
            continue

        checksum = _sha256(fr.content)

        extracted = route_extract(fr.filename, fr.content_type, fr.content)
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