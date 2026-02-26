from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Literal


DocType = Literal["RFQ_BRIEF", "PRODUCT_CARD", "THREAD_MESSAGE", "FILE_CHUNK"]


@dataclass
class TextDoc:
    doc_type: DocType
    rfq_id: str
    product_id: Optional[str] = None
    query_id: Optional[str] = None
    file_id: Optional[str] = None
    title: str = ""
    text: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Chunk:
    rfq_id: str
    doc_type: DocType
    chunk_idx: int
    content_text: str
    content_sha: str
    embedding: Optional[List[float]] = None

    product_id: Optional[str] = None
    query_id: Optional[str] = None
    file_id: Optional[str] = None
    page_num: Optional[int] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IngestState:
    rfq_id: str

    # If True, load_glide_node must NOT call Glide
    prefetched: bool = False

    rfq_row: Optional[Dict[str, Any]] = None
    products_rows: List[Dict[str, Any]] = field(default_factory=list)
    queries_rows: List[Dict[str, Any]] = field(default_factory=list)
    shares_rows: List[Dict[str, Any]] = field(default_factory=list)

    file_targets: List[Dict[str, Any]] = field(default_factory=list)

    docs: List[TextDoc] = field(default_factory=list)
    chunks: List[Chunk] = field(default_factory=list)

    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)