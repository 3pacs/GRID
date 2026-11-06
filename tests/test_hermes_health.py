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
    assert "abcd1234efgh" not in r and "SOFR" not in r
    assert ("https://api.stlouisfed.org/fred/series/observations?series_id=[REDACTED]"
            "&api_key=[REDACTED]&file_type=[REDACTED]") in r
    assert "hunter2" not in redact_reason("connect https://grid:hunter2@db.example/x failed")
    assert "s3cr3tvalue" not in redact_reason("bad config token=s3cr3tvalue retry")
    assert "abcdefghijkl" not in redact_reason("Authorization: Bearer abcdefghijkl")
    assert redact_reason("puller did not report committed write count") == (
        "puller did not report committed write count"
    )
    assert redact_reason(None) is None


def test_redact_reason_dsn_and_json_forms():
    from scripts.hermes_health import redact_reason

    r = redact_reason("could not connect to postgresql://grid:p@ss@word@db.internal:5432/griddb")
    assert "p@ss" not in r and "word@db" not in r
    assert "postgresql://[REDACTED]@db.internal:5432/griddb" in r
    r = redact_reason('payload {"api_key": "abc def 123", "series": "SOFR"}')
    assert "abc def 123" not in r and '"series": "SOFR"' in r
    assert "zzz999" not in redact_reason("{'token': 'zzz999'}")
    assert "qqq777" not in redact_reason("access_token=qqq777&x=1")


def test_redact_reason_unterminated_quote():
    from scripts.hermes_health import redact_reason

    assert "hunter2" not in redact_reason('bad login password="hunter2')
    assert "hunter2 and more" not in redact_reason("cfg {'api_key': 'hunter2 and more")
    assert redact_reason('password="x" ok').endswith(" ok")


# ── check_sky_snapshot_freshness (issue #500) ────────────────────────────────


class _SkyConn:
    def __init__(self, *, exists: bool, row, raise_on_select: bool = False) -> None:
        self.exists, self.row, self.raise_on_select = exists, row, raise_on_select
        self.params: list = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, query, params=None):
        from unittest.mock import MagicMock

        sql = str(query)
        self.params.append((sql, params))
        res = MagicMock()
        if "to_regclass" in sql:
            res.scalar.return_value = self.exists
        elif "FROM astrogrid.sky_snapshot" in sql:
            if self.raise_on_select:
                raise RuntimeError("statement timeout")
            res.fetchone.return_value = self.row
        return res


class _SkyEngine:
    def __init__(self, conn: _SkyConn) -> None:
        self.conn = conn

    def connect(self):
        return self.conn


def test_sky_snapshot_present_today() -> None:
    from datetime import date, datetime, timezone

    created = datetime(2026, 11, 6, 0, 4, tzinfo=timezone.utc)
    conn = _SkyConn(exists=True, row=(date(2026, 11, 6), created))
    out = hermes_health.check_sky_snapshot_freshness(
        _SkyEngine(conn), now=datetime(2026, 11, 6, 3, 0, tzinfo=timezone.utc)
    )
    assert out["checked"] is True and out["present_today"] is True and out["due"] is True
    assert out["latest_snapshot_date"] == "2026-11-06"
    # Bound parameter, no lookahead past today's UTC date.
    select = [p for s, p in conn.params if "FROM astrogrid.sky_snapshot" in s]
    assert select == [{"today": date(2026, 11, 6)}]


def test_sky_snapshot_missing_today_after_due_hour() -> None:
    from datetime import date, datetime, timezone

    conn = _SkyConn(exists=True, row=(date(2026, 11, 5), datetime(2026, 11, 5, 0, 4, tzinfo=timezone.utc)))
    out = hermes_health.check_sky_snapshot_freshness(
        _SkyEngine(conn), now=datetime(2026, 11, 6, 2, 0, tzinfo=timezone.utc)
    )
    assert out["checked"] is True and out["present_today"] is False and out["due"] is True
    assert out["latest_snapshot_date"] == "2026-11-05"


def test_sky_snapshot_not_due_before_0200z() -> None:
    from datetime import datetime, timezone

    conn = _SkyConn(exists=True, row=None)
    out = hermes_health.check_sky_snapshot_freshness(
        _SkyEngine(conn), now=datetime(2026, 11, 6, 1, 59, tzinfo=timezone.utc)
    )
    assert out["due"] is False and out["present_today"] is False


def test_sky_snapshot_relation_missing_or_error_is_unchecked() -> None:
    from datetime import datetime, timezone

    now = datetime(2026, 11, 6, 3, 0, tzinfo=timezone.utc)
    out = hermes_health.check_sky_snapshot_freshness(_SkyEngine(_SkyConn(exists=False, row=None)), now=now)
    assert out["checked"] is False and out["error"] == "relation missing"
    out = hermes_health.check_sky_snapshot_freshness(
        _SkyEngine(_SkyConn(exists=True, row=None, raise_on_select=True)), now=now
    )
    assert out["checked"] is False and "timeout" in out["error"]


def test_check_system_health_carries_sky_snapshot_block() -> None:
    src = inspect.getsource(hermes_health.check_system_health)
    assert '"astrogrid": {"sky_snapshot": check_sky_snapshot_freshness(engine)}' in src
