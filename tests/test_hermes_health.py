from __future__ import annotations

import inspect

from scripts import hermes_health


def test_check_db_health_avoids_unbounded_raw_series_scans() -> None:
    source = inspect.getsource(hermes_health.check_db_health)

    assert "SET LOCAL statement_timeout" in source
    assert "reltuples" in source
    assert 'text("SELECT COUNT(*) FROM raw_series")' not in source
    assert "LEFT JOIN raw_series" not in source


def test_check_db_health_stale_sources_is_cadence_aware_and_index_backed() -> None:
    """GRID-STALE-SOURCES-AUDIT-20260929.md: the freshness read must (a) use
    the per-source cadence map instead of one flat window, (b) take the
    newer of source_catalog.last_pull_at and the latest raw_series SUCCESS
    pull, via the audit's cited index, and (c) never aggregate/group over
    raw_series (which would defeat the point of using that index)."""
    source = inspect.getsource(hermes_health.check_db_health)

    assert "from alerts.cadence import" in inspect.getsource(hermes_health)
    assert "cadence_for_source(" in source
    assert "is_stale(" in source
    assert "is_excluded(" in source
    # Index-backed per-source lookup: a LATERAL top-1, not a GROUP BY over
    # the whole table.
    assert "LATERAL" in source
    assert "idx_raw_series_status_source_pull" in source
    assert "LIMIT 1" in source
    assert "GROUP BY" not in source
    assert "sc.active = TRUE" in source


def test_source_catalog_column_exists_uses_information_schema_not_a_query_error() -> None:
    """Schema-drift-safe: update_frequency isn't in schema.sql, so reading
    it must be gated on an existence check, not a bare SELECT that would
    throw on a DB where the column doesn't exist. Uses to_regclass rather
    than a hardcoded 'public' schema so the check is also correct inside a
    throwaway test schema (search_path-scoped), not just production."""
    source = inspect.getsource(hermes_health._source_catalog_column_exists)

    assert "to_regclass(" in source
    assert "source_catalog" in source
    assert "table_schema = 'public'" not in source


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


def test_not_configured_reason_markers():
    from scripts.hermes_health import is_not_configured_reason

    assert is_not_configured_reason("PullerReportedFailure: no_api_key")
    assert is_not_configured_reason("SKIPPED: all 3 items skipped: not_configured")
    assert is_not_configured_reason("SKIPPED: ['no CF Radar token configured']")
    assert is_not_configured_reason("pmxt SDK not installed")
    assert not is_not_configured_reason("PullerReportedFailure: puller did not report committed write count")
    assert not is_not_configured_reason("NO_NEW_DATA: run completed, 0 rows written")
    assert not is_not_configured_reason(None)


def test_redact_reason_strips_credentials():
    from scripts.hermes_health import redact_reason

    r = redact_reason(
        "429 Client Error: Too Many Requests for url: "
        "https://api.stlouisfed.org/fred/series/observations?series_id=SOFR&api_key=abcd1234efgh&file_type=json"
    )
    assert "abcd1234efgh" not in r and "series_id" not in r
    assert "https://api.stlouisfed.org/fred/series/observations?[REDACTED]" in r
    assert "hunter2" not in redact_reason("connect https://grid:hunter2@db.example/x failed")
    assert "s3cr3tvalue" not in redact_reason("bad config token=s3cr3tvalue retry")
    assert "abcdefghijkl" not in redact_reason("Authorization: Bearer abcdefghijkl")
    assert redact_reason("puller did not report committed write count") == (
        "puller did not report committed write count"
    )
    assert redact_reason(None) is None
