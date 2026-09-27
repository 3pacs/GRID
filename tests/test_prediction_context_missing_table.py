"""oracle.prediction_context._latest_feature_value must not silently swallow a
missing-table error (e.g. resolved_series_retractions absent because the
migration did not run), while the expected-absent legacy feature_catalog
registry stays a quiet debug message."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from loguru import logger

from oracle import prediction_context as pc


class _PgError(Exception):
    def __init__(self, message: str, pgcode: str | None) -> None:
        super().__init__(message)
        self.pgcode = pgcode


class _Wrapped(Exception):
    """Stands in for sqlalchemy.exc.ProgrammingError (carries ``.orig``)."""

    def __init__(self, orig: _PgError) -> None:
        super().__init__(str(orig))
        self.orig = orig


def _engine_raising(*errors: Exception) -> MagicMock:
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.side_effect = list(errors)
    return engine


def _capture(level: str):
    messages: list[str] = []
    sink = logger.add(lambda m: messages.append(m.record["message"]), level=level,
                      filter=lambda r: r["level"].name == level)
    return messages, sink


def test_missing_retractions_table_is_logged_as_warning():
    retractions_missing = _Wrapped(_PgError(
        'relation "resolved_series_retractions" does not exist', "42P01"))
    catalog_missing = _Wrapped(_PgError(
        'relation "feature_catalog" does not exist', "42P01"))
    engine = _engine_raising(retractions_missing, catalog_missing)
    warnings, sink = _capture("WARNING")
    try:
        assert pc._latest_feature_value(engine, ["vix_spot"], date(2026, 9, 27)) is None
    finally:
        logger.remove(sink)
    assert len(warnings) == 1
    assert "resolved_series_retractions" in warnings[0]
    assert "feature_registry" in warnings[0]


def test_expected_missing_legacy_registry_and_other_errors_stay_quiet():
    catalog_missing = _Wrapped(_PgError(
        'relation "feature_catalog" does not exist', "42P01"))
    other = _Wrapped(_PgError("canceling statement due to statement timeout", "57014"))
    engine = _engine_raising(other, catalog_missing)
    warnings, sink = _capture("WARNING")
    try:
        assert pc._latest_feature_value(engine, ["vix_spot"], date(2026, 9, 27)) is None
    finally:
        logger.remove(sink)
    assert warnings == []


def test_classifier():
    f = pc._is_unexpected_missing_table
    assert f(_Wrapped(_PgError('relation "resolved_series_retractions" does not exist', "42P01")),
             "feature_registry")
    assert f(_Wrapped(_PgError('relation "resolved_series" does not exist', "42P01")),
             "feature_catalog")
    assert not f(_Wrapped(_PgError('relation "feature_catalog" does not exist', "42P01")),
                 "feature_catalog")
    assert not f(RuntimeError("connection refused"), "feature_registry")
