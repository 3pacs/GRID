"""Unit tests for ingestion/altdata/taiwan_strait_osint.py (CAT-91)."""

from __future__ import annotations

import dataclasses
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.taiwan_strait_osint import (
    ALL_SERIES,
    KNOWN_PLA_EXERCISES,
    SERIES_ADIZ,
    SERIES_AIRCRAFT,
    SERIES_EXERCISE_FLAG,
    SERIES_VESSEL,
    TaiwanStraitPuller,
    TaiwanStraitSnapshot,
    _parse_mnd_html,
    is_exercise_active,
    run_taiwan_strait_puller,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_engine():
    """SQLAlchemy engine mock that yields a source_id of 77."""
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = ctx
    engine.begin.return_value = ctx

    row_mock = MagicMock()
    row_mock.__getitem__ = lambda self, idx: 77
    conn.execute.return_value.fetchone.return_value = row_mock
    conn.execute.return_value.fetchall.return_value = []

    return engine


@pytest.fixture
def puller(mock_engine):
    return TaiwanStraitPuller(mock_engine)


# ---------------------------------------------------------------------------
# Hard-coded calendar invariants
# ---------------------------------------------------------------------------


class TestKnownExercises:
    def test_non_empty(self):
        assert len(KNOWN_PLA_EXERCISES) >= 6

    def test_dates_in_range(self):
        for d in KNOWN_PLA_EXERCISES:
            assert 2022 <= d.year <= 2026, f"{d} out of range"

    def test_dates_sorted_ascending(self):
        dates = list(KNOWN_PLA_EXERCISES.keys())
        assert dates == sorted(dates), "KNOWN_PLA_EXERCISES should be date-sorted"

    def test_names_non_empty(self):
        for name in KNOWN_PLA_EXERCISES.values():
            assert isinstance(name, str) and name.strip()

    def test_includes_pelosi(self):
        pelosi = date(2022, 8, 4)
        assert pelosi in KNOWN_PLA_EXERCISES
        assert "pelosi" in KNOWN_PLA_EXERCISES[pelosi].lower()


# ---------------------------------------------------------------------------
# Dataclass contract
# ---------------------------------------------------------------------------


class TestTaiwanStraitSnapshot:
    def test_frozen(self):
        snap = TaiwanStraitSnapshot(
            date=date(2026, 4, 13),
            aircraft_count=24,
            adiz_crossing_count=15,
            vessel_count=7,
            exercise_announced=False,
            exercise_name=None,
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            snap.aircraft_count = 99  # type: ignore[misc]

    def test_exercise_flag_true(self):
        snap = TaiwanStraitSnapshot(
            date=date(2026, 4, 13),
            aircraft_count=0,
            adiz_crossing_count=0,
            vessel_count=0,
            exercise_announced=True,
            exercise_name="Joint Sword 2024-B",
        )
        assert snap.exercise_flag == 1
        assert isinstance(snap.exercise_flag, int)

    def test_exercise_flag_false(self):
        snap = TaiwanStraitSnapshot(
            date=date(2026, 4, 13),
            aircraft_count=1,
            adiz_crossing_count=0,
            vessel_count=0,
            exercise_announced=False,
            exercise_name=None,
        )
        assert snap.exercise_flag == 0


# ---------------------------------------------------------------------------
# is_exercise_active
# ---------------------------------------------------------------------------


class TestIsExerciseActive:
    def test_exact_match(self):
        cal = {date(2024, 5, 23): "Joint Sword 2024-A"}
        active, name = is_exercise_active(date(2024, 5, 23), cal)
        assert active is True
        assert name == "Joint Sword 2024-A"

    def test_within_window(self):
        cal = {date(2024, 5, 23): "Joint Sword 2024-A"}
        active, name = is_exercise_active(date(2024, 5, 28), cal, window_days=7)
        assert active is True
        assert name == "Joint Sword 2024-A"

    def test_outside_window(self):
        cal = {date(2024, 5, 23): "Joint Sword 2024-A"}
        active, name = is_exercise_active(date(2024, 6, 10), cal, window_days=7)
        assert active is False
        assert name is None

    def test_empty_calendar(self):
        active, name = is_exercise_active(date(2024, 5, 23), {})
        assert active is False
        assert name is None

    def test_closest_picked(self):
        cal = {
            date(2024, 5, 1): "Exercise A",
            date(2024, 5, 20): "Exercise B",
        }
        # May 18 → distance 17 vs 2 → picks B
        active, name = is_exercise_active(date(2024, 5, 18), cal, window_days=30)
        assert active is True
        assert name == "Exercise B"


# ---------------------------------------------------------------------------
# _parse_mnd_html
# ---------------------------------------------------------------------------


SINGLE_RELEASE_HTML = """
<html><body>
<div class="press-release">
  <time datetime="2026-04-13">2026/04/13</time>
  <h3>PLA Activity Update</h3>
  <p>Since 6 a.m., 24 PLA aircraft and 7 PLAN vessels operating around Taiwan
  have been detected. 15 of the aircraft crossed the median line of the Taiwan
  Strait and entered Taiwan's northern, southwestern and eastern ADIZ.</p>
</div>
</body></html>
"""


MULTI_RELEASE_HTML = """
<html><body>
<div class="press-release">
  <time datetime="2026-04-13">2026/04/13</time>
  <p>24 PLA aircraft and 7 PLAN vessels operating around Taiwan have been
  detected. 15 of them crossed the median line.</p>
</div>
<div class="press-release">
  <time datetime="2026-04-12">2026/04/12</time>
  <p>18 PLA aircraft and 5 PLAN vessels. 9 of them crossed the median line.</p>
</div>
<div class="press-release">
  <time datetime="2026-04-11">2026/04/11</time>
  <p>32 PLA aircraft and 11 PLAN vessels. 21 of them crossed the ADIZ.</p>
</div>
</body></html>
"""


MALFORMED_HTML = """
<html><body>
<div class="press-release">
  <time datetime="2026-04-13">2026/04/13</time>
  <p>N/A PLA aircraft and N/A PLAN vessels were detected. No ADIZ crossings
  reported due to weather.</p>
</div>
<div class="press-release">
  <!-- no date at all -->
  <p>12 PLA aircraft, 3 PLAN vessels, 4 crossed the median line.</p>
</div>
</body></html>
"""


class TestParseMndHtml:
    def test_single_release(self):
        snaps = _parse_mnd_html(SINGLE_RELEASE_HTML)
        assert len(snaps) == 1
        s = snaps[0]
        assert s.date == date(2026, 4, 13)
        assert s.aircraft_count == 24
        assert s.vessel_count == 7
        assert s.adiz_crossing_count == 15

    def test_multiple_releases_sorted_desc(self):
        snaps = _parse_mnd_html(MULTI_RELEASE_HTML)
        assert len(snaps) == 3
        assert snaps[0].date == date(2026, 4, 13)
        assert snaps[1].date == date(2026, 4, 12)
        assert snaps[2].date == date(2026, 4, 11)
        assert snaps[0].aircraft_count == 24
        assert snaps[1].aircraft_count == 18
        assert snaps[2].adiz_crossing_count == 21

    def test_malformed_graceful(self):
        snaps = _parse_mnd_html(MALFORMED_HTML)
        # First release has N/A → zero counts. Second has no date → skipped.
        assert len(snaps) == 1
        s = snaps[0]
        assert s.date == date(2026, 4, 13)
        assert s.aircraft_count == 0
        assert s.vessel_count == 0
        assert s.adiz_crossing_count == 0

    def test_empty_html(self):
        assert _parse_mnd_html("") == []
        assert _parse_mnd_html("<html></html>") == []


# ---------------------------------------------------------------------------
# run_taiwan_strait_puller — full path
# ---------------------------------------------------------------------------


class TestRunTaiwanStraitPuller:
    def test_happy_path_mnd_html(self, mock_engine):
        """MND returns valid HTML → snapshots parsed, source=mnd_html."""
        with patch(
            "ingestion.altdata.taiwan_strait_osint.requests.get"
        ) as mock_get, patch.object(
            TaiwanStraitPuller, "_get_existing_dates", return_value=set()
        ), patch.object(
            TaiwanStraitPuller, "_insert_raw"
        ) as mock_insert:
            mock_resp = MagicMock()
            mock_resp.text = SINGLE_RELEASE_HTML
            mock_resp.raise_for_status.return_value = None
            mock_get.return_value = mock_resp

            result = run_taiwan_strait_puller(mock_engine)

        assert result["source"] == "mnd_html"
        assert result["fetched"] == 1
        # 1 snapshot × 4 series
        assert result["inserted"] == 4
        assert result["latest_aircraft_count"] == 24
        assert mock_insert.call_count == 4

    def test_fallback_to_seed(self, mock_engine):
        """MND 500s → fallback seed row written, source=seed."""
        import requests

        with patch(
            "ingestion.altdata.taiwan_strait_osint.requests.get",
            side_effect=requests.RequestException("boom"),
        ), patch.object(
            TaiwanStraitPuller, "_get_existing_dates", return_value=set()
        ), patch.object(
            TaiwanStraitPuller, "_insert_raw"
        ) as mock_insert:
            result = run_taiwan_strait_puller(mock_engine)

        assert result["source"] == "seed"
        assert result["fetched"] == 1
        assert result["inserted"] == 4  # still writes the seed row × 4 series
        assert result["latest_aircraft_count"] == 0
        assert mock_insert.call_count == 4

    def test_idempotent_rerun_same_date(self, mock_engine):
        """Re-run with same date already in raw_series → zero new inserts."""
        existing = {date(2026, 4, 13)}
        with patch(
            "ingestion.altdata.taiwan_strait_osint.requests.get"
        ) as mock_get, patch.object(
            TaiwanStraitPuller, "_get_existing_dates", return_value=existing
        ), patch.object(
            TaiwanStraitPuller, "_insert_raw"
        ) as mock_insert:
            mock_resp = MagicMock()
            mock_resp.text = SINGLE_RELEASE_HTML
            mock_resp.raise_for_status.return_value = None
            mock_get.return_value = mock_resp

            result = run_taiwan_strait_puller(mock_engine)

        assert result["fetched"] == 1
        assert result["inserted"] == 0
        mock_insert.assert_not_called()

    def test_namespace_contract(self):
        """Ensure all 4 series namespaces are exported and unique."""
        assert SERIES_AIRCRAFT == "taiwan_strait:aircraft_count"
        assert SERIES_ADIZ == "taiwan_strait:adiz_crossing_count"
        assert SERIES_VESSEL == "taiwan_strait:vessel_count"
        assert SERIES_EXERCISE_FLAG == "taiwan_strait:exercise_flag"
        assert set(ALL_SERIES) == {
            SERIES_AIRCRAFT,
            SERIES_ADIZ,
            SERIES_VESSEL,
            SERIES_EXERCISE_FLAG,
        }
        assert len(ALL_SERIES) == 4

    def test_save_to_db_uses_all_namespaces(self, puller):
        """Saving one snapshot must hit all four series_id namespaces."""
        snap = TaiwanStraitSnapshot(
            date=date(2026, 4, 13),
            aircraft_count=24,
            adiz_crossing_count=15,
            vessel_count=7,
            exercise_announced=True,
            exercise_name="Joint Sword 2024-B",
        )
        with patch.object(
            TaiwanStraitPuller, "_get_existing_dates", return_value=set()
        ), patch.object(
            TaiwanStraitPuller, "_insert_raw"
        ) as mock_insert:
            inserted = puller.save_to_db([snap])

        assert inserted == 4
        called_series = {call.kwargs["series_id"] for call in mock_insert.call_args_list}
        assert called_series == set(ALL_SERIES)

        # And the exercise flag row should carry value=1.0
        flag_call = next(
            c for c in mock_insert.call_args_list
            if c.kwargs["series_id"] == SERIES_EXERCISE_FLAG
        )
        assert flag_call.kwargs["value"] == 1.0
