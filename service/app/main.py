# service/app/main.py
from __future__ import annotations

from fastapi import FastAPI

from .routers.health import router as health_router
from .routers.ingest import router as ingest_router

app = FastAPI(title="RFQAI", version="0.1.0")

app.include_router(health_router)
app.include_router(ingest_router)