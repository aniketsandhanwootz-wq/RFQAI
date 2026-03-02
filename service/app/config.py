# service/app/config.py
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    RFQAI settings. Keep everything env-driven so Cron/CLI/API all behave the same.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---- DB ----
    database_url: str = Field(..., alias="DATABASE_URL")
    db_schema: str = Field("rfq", alias="DB_SCHEMA")

    # ---- Embeddings (Gemini) ----
    gemini_api_key: str = Field("", alias="GEMINI_API_KEY")
    gemini_embedding_model: str = Field("gemini-embedding-001", alias="GEMINI_EMBED_MODEL")
    embed_dim: int = Field(1536, alias="EMBED_DIM")

    # ---- Ingestion controls ----
    ingest_http_timeout_sec: int = Field(60, alias="INGEST_HTTP_TIMEOUT_SEC")
    ingest_file_max_mb: int = Field(40, alias="INGEST_FILE_MAX_MB")

    # Chunking defaults (used in Phase 3)
    chunk_size: int = Field(1200, alias="CHUNK_SIZE")
    chunk_overlap: int = Field(150, alias="CHUNK_OVERLAP")

    # ---- Glide (Phase 2) ----
    glide_api_key: str = Field("", alias="GLIDE_API_KEY")
    glide_app_id: str = Field("", alias="GLIDE_APP_ID")
    # Conservative default; can be raised via env if needed.
    glide_max_rows_per_call: int = Field(1000, alias="GLIDE_MAX_ROWS_PER_CALL")
    # ---- Drive (Phase 3) ----
    gdrive_sa_json_path: str = Field("", alias="GDRIVE_SA_JSON_PATH")
    # ---- Document AI (PDF OCR) ----
    docai_project_id: str = Field("", alias="DOCAI_PROJECT_ID")
    docai_location: str = Field("", alias="DOCAI_LOCATION")
    docai_processor_id: str = Field("", alias="DOCAI_PROCESSOR_ID")
    docai_processor_version: str = Field("", alias="DOCAI_PROCESSOR_VERSION")
    # ---- Logging ----
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    def validate_runtime(self) -> None:
        if self.embed_dim != 1536:
            raise ValueError(f"EMBED_DIM must be 1536 for current pgvector schema; got {self.embed_dim}")
