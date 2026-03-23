"""
Shared FastAPI dependencies.

Provides database engine, PIT store, and other shared resources.
"""

from __future__ import annotations

from functools import lru_cache

from sqlalchemy.engine import Engine

from db import get_engine
from governance.registry import ModelRegistry
from journal.log import DecisionJournal
from store.pit import PITStore

# Module-level singletons (clearable, unlike lru_cache)
_db_engine: Engine | None = None


def get_db_engine() -> Engine:
    """Return the shared database engine.

    Uses a module-level singleton instead of lru_cache to allow
    explicit cache clearing if config changes at runtime.
    """
    global _db_engine
    if _db_engine is None:
        _db_engine = get_engine()
    return _db_engine


@lru_cache()
def get_pit_store() -> PITStore:
    """Return the shared PIT store."""
    return PITStore(get_db_engine())


@lru_cache()
def get_journal() -> DecisionJournal:
    """Return the shared decision journal."""
    return DecisionJournal(get_db_engine())


@lru_cache()
def get_model_registry() -> ModelRegistry:
    """Return the shared model registry."""
    return ModelRegistry(get_db_engine())
