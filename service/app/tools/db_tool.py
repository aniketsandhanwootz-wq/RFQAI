# service/app/tools/db_tool.py
from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional, Sequence

# Prefer psycopg (v3), fallback to psycopg2.
try:
    import psycopg  # type: ignore
    _PSYCOPG_V3 = True
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore
    _PSYCOPG_V3 = False

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore


@dataclass(frozen=True)
class DB:
    database_url: str

    def connect(self):
        if _PSYCOPG_V3:
            return psycopg.connect(self.database_url)
        if psycopg2 is not None:
            return psycopg2.connect(self.database_url)
        raise RuntimeError("Neither psycopg (v3) nor psycopg2 is installed.")


@contextmanager
def tx(db: DB) -> Iterator[object]:
    """
    Transaction context manager.
    """
    conn = db.connect()
    try:
        if _PSYCOPG_V3:
            with conn:
                with conn.cursor() as cur:
                    yield cur
        else:
            conn.autocommit = False
            cur = conn.cursor()
            try:
                yield cur
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
    finally:
        conn.close()


def exec_sql(cur: object, sql: str) -> None:
    """
    Execute SQL (single statement or multi-statement).
    psycopg2 cannot run multiple statements in one execute reliably, so we split naïvely.
    """
    sql = (sql or "").strip()
    if not sql:
        return

    if _PSYCOPG_V3:
        cur.execute(sql)  # type: ignore[attr-defined]
        return

    # psycopg2: simple split on ';' for migration files.
    # Keep it basic; our migration files are straightforward.
    parts = [p.strip() for p in sql.split(";") if p.strip()]
    for p in parts:
        cur.execute(p + ";")  # type: ignore[attr-defined]


def apply_migrations(db: DB, migrations_dir: str | Path) -> None:
    """
    Apply all *.sql migrations in lexical order.
    """
    migrations_dir = Path(migrations_dir)
    files = sorted(migrations_dir.glob("*.sql"))
    if not files:
        raise FileNotFoundError(f"No .sql migrations found in {migrations_dir}")

    with tx(db) as cur:
        for fp in files:
            sql = fp.read_text(encoding="utf-8")
            exec_sql(cur, sql)


def ping(db: DB) -> None:
    """
    Simple connectivity check.
    """
    with tx(db) as cur:
        exec_sql(cur, "SELECT 1;")