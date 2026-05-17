"""AstroGrid dependency container.

AstroGrid keeps its own singleton registry even when it points at the same
underlying database. This prevents AstroGrid runtime wiring from depending on
the main GRID app's dependency module.
"""

from __future__ import annotations

import os

from loguru import logger as log
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from config import settings
from store.astrogrid import AstroGridStore

_astrogrid_db_engine: Engine | None = None
_astrogrid_store: AstroGridStore | None = None


def get_db_engine() -> Engine:
    global _astrogrid_db_engine
    if _astrogrid_db_engine is None:
        pool_size = int(os.getenv("ASTROGRID_DB_POOL_SIZE", "5"))
        max_overflow = int(os.getenv("ASTROGRID_DB_MAX_OVERFLOW", "5"))
        statement_timeout_ms = int(os.getenv("ASTROGRID_DB_STATEMENT_TIMEOUT_MS", "30000"))
        log.info(
            "Creating AstroGrid SQLAlchemy engine — {url} pool_size={ps} max_overflow={mo}",
            url=settings.DB_URL.replace(settings.DB_PASSWORD, "***"),
            ps=pool_size,
            mo=max_overflow,
        )
        _astrogrid_db_engine = create_engine(
            settings.DB_URL,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=10,
            pool_pre_ping=True,
            pool_recycle=1800,
            connect_args={
                "options": (
                    f"-c statement_timeout={statement_timeout_ms} "
                    "-c application_name=astrogrid-api"
                ),
            },
        )
    return _astrogrid_db_engine


def get_astrogrid_store() -> AstroGridStore:
    global _astrogrid_store
    if _astrogrid_store is None:
        _astrogrid_store = AstroGridStore(get_db_engine())
    return _astrogrid_store


def clear_singletons() -> None:
    global _astrogrid_db_engine, _astrogrid_store
    if _astrogrid_db_engine is not None:
        _astrogrid_db_engine.dispose()
    _astrogrid_db_engine = None
    _astrogrid_store = None
