"""
Shared FastAPI dependencies.

Provides database engine, PIT store, and other shared resources.
All singletons use module-level variables so they can be cleared
at runtime (e.g. when config changes) without a full restart.
"""

from __future__ import annotations

from sqlalchemy.engine import Engine

from db import get_engine
from governance.registry import ModelRegistry
from journal.log import DecisionJournal
from store.pit import PITStore

# Module-level singletons (clearable, unlike lru_cache)
_db_engine: Engine | None = None
_pit_store: PITStore | None = None
_journal: DecisionJournal | None = None
_model_registry: ModelRegistry | None = None


def get_db_engine() -> Engine:
    """Return the shared database engine."""
    global _db_engine
    if _db_engine is None:
        _db_engine = get_engine()
    return _db_engine


def get_pit_store() -> PITStore:
    """Return the shared PIT store."""
    global _pit_store
    if _pit_store is None:
        _pit_store = PITStore(get_db_engine())
    return _pit_store


def get_journal() -> DecisionJournal:
    """Return the shared decision journal."""
    global _journal
    if _journal is None:
        _journal = DecisionJournal(get_db_engine())
    return _journal


def get_model_registry() -> ModelRegistry:
    """Return the shared model registry."""
    global _model_registry
    if _model_registry is None:
        _model_registry = ModelRegistry(get_db_engine())
    return _model_registry


def clear_singletons() -> None:
    """Clear all cached singletons so they are re-created on next access.

    Call this when configuration changes at runtime (e.g. database URL).
    """
    global _db_engine, _pit_store, _journal, _model_registry
    if _db_engine is not None:
        _db_engine.dispose()
    _db_engine = None
    _pit_store = None
    _journal = None
    _model_registry = None
