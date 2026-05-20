from __future__ import annotations

import inspect

from scripts import hermes_health


def test_check_db_health_avoids_unbounded_raw_series_scans() -> None:
    source = inspect.getsource(hermes_health.check_db_health)

    assert "SET LOCAL statement_timeout" in source
    assert "reltuples" in source
    assert 'text("SELECT COUNT(*) FROM raw_series")' not in source
    assert "LEFT JOIN raw_series" not in source


def test_resolve_source_issues_marks_unresolved_severe_rows() -> None:
    source = inspect.getsource(hermes_health.resolve_source_issues)

    assert "resolved_at = NOW()" in source
    assert "severity IN ('WARNING', 'ERROR', 'CRITICAL')" in source
    assert "source = :source" in source
