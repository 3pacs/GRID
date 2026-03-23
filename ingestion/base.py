"""Base class for all GRID data ingestion pullers.

Provides the ``_resolve_source_id()`` and ``_row_exists()`` methods that
were previously copy-pasted across 33+ puller modules.  New pullers should
subclass ``BasePuller`` and set ``SOURCE_NAME`` to their source_catalog name.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


class BasePuller:
    """Common base for all ingestion pullers.

    Subclasses must set ``SOURCE_NAME`` to the string matching their
    ``source_catalog.name`` row (e.g. ``"FRED"``, ``"BLS"``, ``"yfinance"``).

    Attributes:
        engine: SQLAlchemy engine for database access.
        source_id: Resolved ``source_catalog.id`` for this source.
    """

    SOURCE_NAME: str = ""  # Override in subclass

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        if self.SOURCE_NAME:
            self.source_id = self._resolve_source_id()

    def _resolve_source_id(self) -> int:
        """Look up the source_catalog id for this puller's SOURCE_NAME.

        Returns:
            The ``source_catalog.id`` integer.

        Raises:
            RuntimeError: If the source is not found in source_catalog.
        """
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"),
                {"name": self.SOURCE_NAME},
            ).fetchone()
        if row is None:
            raise RuntimeError(
                f"{self.SOURCE_NAME} source not found in source_catalog. "
                "Run schema.sql first."
            )
        return row[0]

    def _row_exists(
        self,
        series_id: str,
        obs_date: Any,
        conn: Any,
        window_hours: int = 1,
    ) -> bool:
        """Check whether a duplicate row already exists within a time window.

        Parameters:
            series_id: Series identifier string.
            obs_date: Observation date.
            conn: Active SQLAlchemy connection (must be passed in by caller).
            window_hours: Deduplication window in hours (default: 1).

        Returns:
            True if a matching row was inserted within the window.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
        result = conn.execute(
            text(
                "SELECT 1 FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src "
                "AND obs_date = :od AND pull_timestamp >= :ts "
                "LIMIT 1"
            ),
            {
                "sid": series_id,
                "src": self.source_id,
                "od": obs_date,
                "ts": cutoff,
            },
        ).fetchone()
        return result is not None
