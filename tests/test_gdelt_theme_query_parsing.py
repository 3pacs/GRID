"""Regression test for the GDELT theme-query timeline parsing bug.

Root cause (diagnosed 2026-09-26): the GDELT DOC 2.0 API always wraps
timeline points one level deeper than ``GDELTPuller.pull_recent``'s theme
query loop assumed. A real response looks like::

    {"timeline": [{"series": "Average Tone", "data": [{"date": ..., "value": ...}]}]}

i.e. ``timeline`` is a list of *per-series* objects, each carrying its
points in a nested ``data`` list — not a flat list of ``{"date", "value"}``
points. The old loop did ``for point in timeline: point.get("date")``,
which always looked at the outer ``{"series", "data"}`` dict and found no
"date" key, so every point was silently skipped. Confirmed live: querying
the real API from grid-svr returns HTTP 200 with this exact nested shape,
and pull_log shows ``puller_name='GDELT'`` runs going back to March 2026 —
every single one recording ``rows_inserted=0``, even though the API itself
was healthy. ``_pull_actor_tones``/``_pull_tension_scores`` already unwrap
``data`` correctly; this fix makes the theme-query loop do the same.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ingestion.altdata.gdelt import GDELTPuller


def _mock_engine(source_id: int = 42) -> tuple[MagicMock, MagicMock]:
    """Mock engine whose source_catalog lookup resolves to ``source_id``,
    and whose raw_series ``_row_exists`` lookups report "no existing row"
    (so inserts proceed) — everything else defaults to empty/None."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    def _execute(clause, *args, **kwargs):
        sql = str(clause)
        result = MagicMock()
        if "FROM source_catalog" in sql or "INTO source_catalog" in sql:
            result.fetchone.return_value = (source_id,)
        else:
            # _row_exists' "SELECT 1 FROM raw_series ... LIMIT 1" and any
            # other lookup: report nothing found, so inserts proceed.
            result.fetchone.return_value = None
        result.fetchall.return_value = []
        return result

    conn.execute.side_effect = _execute
    return engine, conn


@pytest.fixture
def puller() -> tuple[GDELTPuller, MagicMock, MagicMock]:
    engine, conn = _mock_engine()
    p = GDELTPuller(db_engine=engine)
    return p, engine, conn


# The exact nested shape the live GDELT DOC API returns for a
# mode=timelinetone / timelinevol query (verified against the real
# endpoint from grid-svr on 2026-09-26).
_REAL_API_SHAPE = {
    "query_details": {"title": "economy recession", "date_resolution": "15m"},
    "timeline": [
        {
            "series": "Average Tone",
            "data": [
                {"date": "20260924123000", "value": -1.5},
                {"date": "20260925123000", "value": -1.2},
            ],
        }
    ],
}


class TestThemeQueryTimelineParsing:
    def test_real_api_shape_inserts_rows(self, puller) -> None:
        """The nested {series, data:[...]} shape must yield inserted rows.

        Before the fix this returned total_rows == 0 no matter what the
        API sent back — reproducing the "SUCCESS, 0 rows" pattern seen in
        production pull_log for every GDELT run since March 2026.
        """
        p, _engine, conn = puller
        p._fetch_gdelt_api = MagicMock(return_value=_REAL_API_SHAPE)

        result = p.pull_recent(
            days_back=1,
            max_theme_queries=1,
            include_actor_tones=False,
            include_tensions=False,
            include_signals=False,
        )

        assert result["total_rows"] == 2
        assert result["errors"] == []

        insert_calls = [
            call
            for call in conn.execute.call_args_list
            if "INSERT INTO raw_series" in str(call.args[0])
        ]
        assert len(insert_calls) == 2
        inserted_values = {c.args[1]["val"] for c in insert_calls}
        assert inserted_values == {-1.5, -1.2}

    def test_empty_data_array_inserts_nothing(self, puller) -> None:
        """A series with no data points yet must not raise or fabricate rows."""
        p, _engine, _conn = puller
        p._fetch_gdelt_api = MagicMock(
            return_value={
                "query_details": {"title": "x"},
                "timeline": [{"series": "Average Tone", "data": []}],
            }
        )

        result = p.pull_recent(
            days_back=1,
            max_theme_queries=1,
            include_actor_tones=False,
            include_tensions=False,
            include_signals=False,
        )

        assert result["total_rows"] == 0
        assert result["errors"] == []

    def test_flat_point_list_still_supported(self, puller) -> None:
        """Defensive: a hypothetical flat (non-nested) response still parses.

        ``series_data.get("data", [series_data])`` falls back to treating
        the entry itself as the point when there is no "data" key, so this
        keeps working even if the API (or a future test double) sends the
        old assumed shape.
        """
        p, _engine, conn = puller
        p._fetch_gdelt_api = MagicMock(
            return_value={
                "timeline": [{"date": "20260925123000", "value": 0.75}],
            }
        )

        result = p.pull_recent(
            days_back=1,
            max_theme_queries=1,
            include_actor_tones=False,
            include_tensions=False,
            include_signals=False,
        )

        assert result["total_rows"] == 1
        insert_calls = [
            call
            for call in conn.execute.call_args_list
            if "INSERT INTO raw_series" in str(call.args[0])
        ]
        assert insert_calls[0].args[1]["val"] == 0.75
