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


def test_operator_state_persists_digest_timestamps() -> None:
    source = inspect.getsource(hermes_health.OperatorState.to_dict)
    hydrate_source = inspect.getsource(hermes_health.OperatorState.hydrate_from_snapshot)

    assert "last_daily_digest" in source
    assert "last_100x_digest" in source
    assert "last_daily_digest" in hydrate_source
    assert "last_100x_digest" in hydrate_source


def test_log_issue_deduplicates_recent_unresolved_noise() -> None:
    source = inspect.getsource(hermes_health.log_issue)

    assert "OPERATOR_ISSUE_DEDUPE_HOURS" in source
    assert "IS NOT DISTINCT FROM :src" in source
    assert "duplicate issue suppressed" in source
