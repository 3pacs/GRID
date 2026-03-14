"""
GRID entity mapping module.

Maps raw series identifiers (e.g. FRED codes, yfinance ticker fields) to
canonical feature names in the ``feature_registry``.  Provides fuzzy matching
to suggest mappings for unmapped series.
"""

from __future__ import annotations

import difflib
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Hardcoded seed mappings: raw series_id -> feature_registry.name
SEED_MAPPINGS: dict[str, str] = {
    "T10Y2Y": "yld_curve_2s10s",
    "T10Y3M": "yld_curve_3m10y",
    "DFF": "fed_funds_rate",
    "VIXCLS": "vix_spot",
    "USSLIND": "conf_board_lei",
    "CPIAUCSL": "cpi_yoy",
    "YF:^GSPC:close": "sp500_close",
    "YF:^VIX:close": "vix_spot_yf",
    "YF:HG=F:close": "copper_futures_close",
    "YF:GC=F:close": "gold_futures_close",
    "YF:HYG:close": "hyg_close",
    "YF:LQD:close": "lqd_close",
    "YF:TLT:close": "tlt_close",
    "YF:UUP:close": "dxy_proxy_close",
}


class EntityMap:
    """Maps raw series identifiers to feature registry entries.

    Uses the hardcoded SEED_MAPPINGS as a base and resolves
    feature_registry IDs from the database.

    Attributes:
        engine: SQLAlchemy engine for database lookups.
        _feature_cache: Cached mapping of feature name -> feature_registry.id.
    """

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the entity mapper.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.engine = db_engine
        self._feature_cache: dict[str, int] = {}
        self._load_feature_cache()
        log.info(
            "EntityMap initialised — {n} features cached, {m} seed mappings",
            n=len(self._feature_cache),
            m=len(SEED_MAPPINGS),
        )

    def _load_feature_cache(self) -> None:
        """Load all feature_registry entries into an in-memory cache.

        Populates ``_feature_cache`` with {name: id} pairs.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT id, name FROM feature_registry")
            ).fetchall()
        self._feature_cache = {row[1]: row[0] for row in rows}
        log.debug("Feature cache loaded: {n} entries", n=len(self._feature_cache))

    def get_feature_id(self, series_id: str) -> int | None:
        """Resolve a raw series_id to a feature_registry.id.

        Parameters:
            series_id: Raw series identifier (e.g. 'T10Y2Y', 'YF:^GSPC:close').

        Returns:
            int: The feature_registry.id if a mapping exists, None otherwise.
        """
        feature_name = SEED_MAPPINGS.get(series_id)
        if feature_name is None:
            log.debug("No mapping found for series_id={sid}", sid=series_id)
            return None

        feature_id = self._feature_cache.get(feature_name)
        if feature_id is None:
            # Refresh cache in case new features were added
            self._load_feature_cache()
            feature_id = self._feature_cache.get(feature_name)

        if feature_id is None:
            log.warning(
                "Mapping exists ({sid} -> {fn}) but feature not in registry",
                sid=series_id,
                fn=feature_name,
            )
        return feature_id

    def get_all_mappings(self) -> dict[str, str]:
        """Return the full mapping dictionary.

        Returns:
            dict: Mapping of raw series_id -> feature name.
        """
        return dict(SEED_MAPPINGS)

    def suggest_mapping(self, series_id: str) -> list[str]:
        """Suggest possible feature names for an unmapped series_id.

        Uses ``difflib.SequenceMatcher`` to find the top 3 closest matches
        among registered feature names.

        Parameters:
            series_id: Raw series identifier to find matches for.

        Returns:
            list[str]: Up to 3 feature name suggestions, ordered by similarity.
        """
        all_names = list(self._feature_cache.keys())
        if not all_names:
            return []

        # Normalise the query for better matching
        query = series_id.lower().replace(":", "_").replace("^", "").replace("=", "")

        scored: list[tuple[float, str]] = []
        for name in all_names:
            ratio = difflib.SequenceMatcher(None, query, name.lower()).ratio()
            scored.append((ratio, name))

        scored.sort(key=lambda x: x[0], reverse=True)
        suggestions = [name for _, name in scored[:3]]

        log.debug(
            "Suggestions for '{sid}': {s}",
            sid=series_id,
            s=suggestions,
        )
        return suggestions


if __name__ == "__main__":
    from db import get_engine

    em = EntityMap(db_engine=get_engine())
    print("All mappings:")
    for sid, fname in em.get_all_mappings().items():
        fid = em.get_feature_id(sid)
        print(f"  {sid:25s} -> {fname:25s} (id={fid})")

    print("\nSuggestions for 'SP500':")
    for s in em.suggest_mapping("SP500"):
        print(f"  {s}")
