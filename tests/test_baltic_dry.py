from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest

from ingestion.altdata import baltic_dry
from ingestion.altdata.baltic_dry import BalticDryPuller


def _mock_engine(source_id: int = 13) -> tuple[MagicMock, MagicMock]:
    engine = MagicMock()
    conn = MagicMock()

    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    conn.execute.return_value.fetchone.return_value = (source_id,)
    conn.execute.return_value.fetchall.return_value = []
    return engine, conn


_FRESH = date.today() - timedelta(days=1)


@pytest.fixture(autouse=True)
def _reset_process_state():
    baltic_dry._KNOWN_DEAD_FRED_IDS.clear()
    baltic_dry._STALE_SNAPSHOT_REPORTED.clear()
    yield
    baltic_dry._KNOWN_DEAD_FRED_IDS.clear()
    baltic_dry._STALE_SNAPSHOT_REPORTED.clear()


def _normalized_snapshot(obs_date: date | None = None) -> dict:
    return {
        "date": obs_date or _FRESH,
        "updated": "21:51 UTC",
        "source": "Baltic Exchange",
        "values": {
            "bdi": {"value": 3054.0, "prev": 3151, "change": -97, "pct": -3.08},
            "bci": {"value": 4949.0, "prev": 5173, "change": -224, "pct": -4.33},
            "bpi": {"value": 2459.0, "prev": 2521, "change": -62, "pct": -2.46},
            "bsi": {"value": 1568.0, "prev": 1565, "change": 3, "pct": 0.19},
            "bhsi": {"value": 850.0, "prev": 850, "change": 0, "pct": 0.0},
        },
        "stats": {},
    }


def test_parse_public_snapshot_maps_baltic_indices() -> None:
    parsed = baltic_dry._parse_public_snapshot(
        {
            "date": "2026-05-19",
            "updated": "21:51 UTC",
            "source": "Baltic Exchange",
            "bdi": {"value": 3054, "prev": 3151, "change": -97, "pct": -3.08},
            "bci": {"value": 4949, "prev": 5173, "change": -224, "pct": -4.33},
            "bpi": {"value": 2459, "prev": 2521, "change": -62, "pct": -2.46},
            "bsi": {"value": 1568, "prev": 1565, "change": 3, "pct": 0.19},
            "bhsi": {"value": 850, "prev": 850, "change": 0, "pct": 0.0},
        }
    )

    assert parsed["date"] == date(2026, 5, 19)
    assert parsed["values"]["bdi"]["value"] == 3054.0
    assert parsed["values"]["bhsi"]["value"] == 850.0


def test_known_dead_detector_reads_wrapped_retry_response_body() -> None:
    class FakeResponse:
        text = '{"error_message":"Bad Request.  The series does not exist."}'

    class FakeHttpError(Exception):
        response = FakeResponse()

    class FakeFuture:
        def exception(self):
            return FakeHttpError("400 Bad Request")

    class FakeRetryError(Exception):
        last_attempt = FakeFuture()

    assert baltic_dry._fred_series_is_known_dead(FakeRetryError("RetryError"))


def test_known_dead_fred_series_uses_public_snapshot_without_failed_row(monkeypatch) -> None:
    _engine, conn = _mock_engine()
    puller = BalticDryPuller(api_key="test", db_engine=_engine)

    puller.fred.get_series_observations = MagicMock(
        side_effect=RuntimeError("Bad Request: The series does not exist.")
    )
    monkeypatch.setattr(puller, "_get_public_snapshot", _normalized_snapshot)

    result = puller.pull_series("baltic.bdi", start_date=(_FRESH - timedelta(days=30)).isoformat())

    assert result["status"] == "SUCCESS"
    assert result["source"] == "public_snapshot"
    assert result["rows_inserted"] == 1
    assert not any("'FAILED'" in str(call.args[0]) for call in conn.execute.call_args_list)
    assert any(
        "Baltic_Exchange_public_snapshot" in str(call.kwargs or call.args)
        for call in conn.execute.call_args_list
    )


def test_public_snapshot_outside_requested_range_is_partial_without_insert(monkeypatch) -> None:
    _engine, conn = _mock_engine()
    puller = BalticDryPuller(api_key="test", db_engine=_engine)
    monkeypatch.setattr(puller, "_get_public_snapshot", _normalized_snapshot)

    result = puller._pull_public_snapshot_series(
        "baltic.bdi",
        start_date=(_FRESH + timedelta(days=1)).isoformat(),
    )

    assert result["status"] == "PARTIAL"
    assert result["rows_inserted"] == 0
    assert not any("INSERT INTO raw_series" in str(call.args[0]) for call in conn.execute.call_args_list)


def test_frozen_public_snapshot_is_reported_dead_once(monkeypatch) -> None:
    """The balticdryindex snapshot froze at 2026-05-20: FAILED with a truthful
    reason, one warning per process, never an insert."""
    _engine, conn = _mock_engine()
    puller = BalticDryPuller(api_key="test", db_engine=_engine)
    monkeypatch.setattr(
        puller, "_get_public_snapshot", lambda: _normalized_snapshot(date(2026, 5, 20))
    )
    warnings: list[str] = []
    monkeypatch.setattr(baltic_dry.log, "warning", lambda msg, **kw: warnings.append(kw.get("r", msg)))

    results = [
        puller._pull_public_snapshot_series(gsid, start_date="2026-01-01")
        for gsid in baltic_dry.BALTIC_SERIES
    ]

    assert all(r["status"] == "FAILED" for r in results)
    assert all("Baltic source dead" in r["errors"][0] and "2026-05-20" in r["errors"][0] for r in results)
    assert len(warnings) == 1
    assert not any("INSERT INTO raw_series" in str(call.args[0]) for call in conn.execute.call_args_list)


def test_known_dead_fred_id_is_not_requested_again(monkeypatch) -> None:
    _engine, _conn = _mock_engine()
    puller = BalticDryPuller(api_key="test", db_engine=_engine)
    puller.fred.get_series_observations = MagicMock(
        side_effect=RuntimeError("Bad Request: The series does not exist.")
    )
    monkeypatch.setattr(puller, "_get_public_snapshot", _normalized_snapshot)
    start = (_FRESH - timedelta(days=30)).isoformat()

    puller.pull_series("baltic.bdi", start_date=start)
    puller.pull_series("baltic.bdi", start_date=start)

    assert puller.fred.get_series_observations.call_count == 1
