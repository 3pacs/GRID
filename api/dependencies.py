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


@lru_cache()
def get_db_engine() -> Engine:
    """Return the shared database engine."""
    return get_engine()


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
