"""Tests for ``scripts/probe_fred_series.py``.

Every FRED interaction is mocked — these tests never touch
``api.stlouisfed.org``. The behaviours that matter:

* a 400 is classified ``dead`` (FRED's not-found), and only a 400 is;
* a series that resolves but stopped updating is ``stale``, not ``live``;
* transport and non-200 failures are ``unverified``, never a pass;
* a missing API key warns and returns cleanly instead of crashing.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest
import requests

from scripts.probe_fred_series import (
    DEFAULT_STALE_DAYS,
    ProbeResult,
    _newest_observation,
    collect_fred_series_ids,
    main,
    probe_all,
    probe_series,
)

TODAY = date(2026, 9, 11)


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Minimal stand-in for ``requests.Response``."""

    def __init__(self, status_code: int, payload: Any = None, *, bad_json: bool = False):
        self.status_code = status_code
        self._payload = payload
        self._bad_json = bad_json

    def json(self) -> Any:
        if self._bad_json:
            raise ValueError("not json")
        return self._payload


class _FakeSession:
    """Routes ``/series`` and ``/series/observations`` to canned responses."""

    def __init__(
        self,
        series_response: _FakeResponse | Exception,
        observations_response: _FakeResponse | Exception | None = None,
    ):
        self._series = series_response
        self._observations = observations_response
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, params: dict[str, Any], timeout: float) -> _FakeResponse:
        self.calls.append((url, params))
        target = self._observations if url.endswith("/observations") else self._series
        if isinstance(target, Exception):
            raise target
        assert target is not None, f"no canned response for {url}"
        return target


def _series_ok(title: str = "A Series") -> _FakeResponse:
    return _FakeResponse(200, {"seriess": [{"id": "X", "title": title}]})


def _observations(*rows: tuple[str, str]) -> _FakeResponse:
    return _FakeResponse(
        200, {"observations": [{"date": d, "value": v} for d, v in rows]}
    )


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def test_recent_series_is_live() -> None:
    recent = (TODAY - timedelta(days=5)).isoformat()
    session = _FakeSession(_series_ok("Federal Debt"), _observations((recent, "35.5")))

    result = probe_series("GFDEBTN", "key", session=session, today=TODAY)

    assert result.status == "live"
    assert result.http_status == 200
    assert result.title == "Federal Debt"
    assert result.last_observation == recent


def test_http_400_is_dead() -> None:
    """FRED answers 400 — not 404 — for an id it does not carry."""
    session = _FakeSession(_FakeResponse(400, {"error_message": "Bad Request"}))

    result = probe_series("NCBBCCB1Q027S", "key", session=session, today=TODAY)

    assert result.status == "dead"
    assert result.http_status == 400
    assert "400" in result.detail
    # A dead id must not cost a second call.
    assert len(session.calls) == 1


def test_series_that_resolves_but_stopped_updating_is_stale() -> None:
    """Resolving is not enough — a series frozen in 2019 is not a fix."""
    session = _FakeSession(_series_ok(), _observations(("2019-03-01", "101.2")))

    result = probe_series("FROZEN", "key", session=session, today=TODAY)

    assert result.status == "stale"
    assert result.last_observation == "2019-03-01"
    assert str(DEFAULT_STALE_DAYS) in result.detail


def test_stale_threshold_boundary_is_respected() -> None:
    """An observation just inside the window is live; just outside is stale."""
    inside = (TODAY - timedelta(days=10)).isoformat()
    outside = (TODAY - timedelta(days=40)).isoformat()

    live = probe_series(
        "X",
        "key",
        session=_FakeSession(_series_ok(), _observations((inside, "1"))),
        stale_after_days=30,
        today=TODAY,
    )
    stale = probe_series(
        "X",
        "key",
        session=_FakeSession(_series_ok(), _observations((outside, "1"))),
        stale_after_days=30,
        today=TODAY,
    )

    assert live.status == "live"
    assert stale.status == "stale"


def test_series_with_only_missing_values_is_stale() -> None:
    """FRED encodes a missing value as '.' — padding is not freshness."""
    recent = (TODAY - timedelta(days=2)).isoformat()
    session = _FakeSession(_series_ok(), _observations((recent, ".")))

    result = probe_series("PADDED", "key", session=session, today=TODAY)

    assert result.status == "stale"
    assert result.last_observation is None
    assert "no non-missing observations" in result.detail


def test_newest_observation_skips_missing_and_unparseable() -> None:
    payload = {
        "observations": [
            {"date": "2026-09-01", "value": "."},
            {"date": "not-a-date", "value": "5"},
            {"date": "2026-06-01", "value": "4.2"},
            {"date": "2026-05-01", "value": "4.1"},
        ]
    }
    assert _newest_observation(payload) == date(2026, 6, 1)


# ---------------------------------------------------------------------------
# Failures must never read as a pass
# ---------------------------------------------------------------------------


def test_transport_error_is_unverified_not_dead() -> None:
    session = _FakeSession(requests.ConnectionError("proxy refused CONNECT"))

    result = probe_series("ANYTHING", "key", session=session, today=TODAY)

    assert result.status == "unverified"
    assert result.status != "dead"
    assert "transport error" in result.detail


@pytest.mark.parametrize("status_code", [401, 403, 429, 500, 503])
def test_non_400_http_errors_are_unverified(status_code: int) -> None:
    """Only 400 means 'not in the catalog'. Everything else is unknown."""
    session = _FakeSession(_FakeResponse(status_code, {}))

    result = probe_series("X", "key", session=session, today=TODAY)

    assert result.status == "unverified"
    assert result.http_status == status_code


def test_observations_failure_after_resolution_is_unverified() -> None:
    session = _FakeSession(_series_ok(), _FakeResponse(500, {}))

    result = probe_series("X", "key", session=session, today=TODAY)

    assert result.status == "unverified"
    assert "observations returned HTTP 500" in result.detail


def test_unparseable_series_json_is_unverified() -> None:
    session = _FakeSession(_FakeResponse(200, bad_json=True))

    result = probe_series("X", "key", session=session, today=TODAY)

    assert result.status == "unverified"
    assert "unparseable JSON" in result.detail


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------


def test_collect_finds_ids_and_records_call_sites() -> None:
    collected = collect_fred_series_ids()

    # Ids the repo demonstrably references.
    assert "GFDEBTN" in collected
    assert "VIXCLS" in collected
    # Repointed in PR #425 — the new ids, not the retired ones.
    assert "NCBCEBQ027S" in collected
    assert "NCBBCCB1Q027S" not in collected
    assert any("ingestion/fred.py:" in site for site in collected["VIXCLS"])


def test_collect_excludes_non_fred_enum_literals() -> None:
    """Module-scope restriction plus the container denylist keeps prose out."""
    collected = collect_fred_series_ids()

    for noise in ("SUCCESS", "FAILED", "PARTIAL", "OVERWRITE", "TRUST", "OFFICIAL"):
        assert noise not in collected


def test_collect_includes_three_character_ids() -> None:
    """A 4-char floor silently drops real ids — the exact failure mode here."""
    collected = collect_fred_series_ids()

    for short_id in ("DFF", "TCU", "M2V", "BSI"):
        assert short_id in collected, f"{short_id} must not be dropped for length"


def test_collect_excludes_currency_codes() -> None:
    """ISO 4217 codes are id-shaped but are not FRED series."""
    collected = collect_fred_series_ids()

    for currency in ("USD", "EUR", "GBP", "JPY", "CHF", "AUD", "ZAR"):
        assert currency not in collected
    # …while the DEX* ids in the same module are still collected.
    assert "DEXUSEU" in collected
    assert "DEXJPUS" in collected


def test_id_shaped_dict_key_wins_over_its_value(tmp_path: Path) -> None:
    """In a mapping, an id-shaped key is the series id; its value is metadata."""
    pkg = tmp_path / "ingestion"
    pkg.mkdir()
    (pkg / "fx.py").write_text(
        "# FRED\n"
        '_FRED_FX_SERIES = {"DEXUSEU": ("EUR", "usd_per_ccy")}\n'
        'H8_SERIES = {"H8B1023NCBCMG": "ci_loans"}\n'
    )

    collected = collect_fred_series_ids(tmp_path)

    assert "DEXUSEU" in collected
    assert "H8B1023NCBCMG" in collected
    assert "EUR" not in collected


def test_non_id_shaped_dict_key_descends_into_value(tmp_path: Path) -> None:
    """The other container layout: the id lives inside the value."""
    pkg = tmp_path / "ingestion"
    pkg.mkdir()
    (pkg / "yc.py").write_text(
        "# FRED\n"
        'YC_SERIES = {"yc_1y": {"fred_id": "DGS1", "description": "1y"}}\n'
    )

    assert "DGS1" in collect_fred_series_ids(tmp_path)


def test_collect_ignores_unparseable_module(tmp_path: Path) -> None:
    pkg = tmp_path / "ingestion"
    pkg.mkdir()
    (pkg / "broken.py").write_text("FRED_SERIES = [ this is not python\n")
    (pkg / "good.py").write_text('FRED_SERIES_LIST = ["UNRATE"]\n')

    collected = collect_fred_series_ids(tmp_path)

    assert "UNRATE" in collected


def test_collect_skips_files_that_never_mention_fred(tmp_path: Path) -> None:
    pkg = tmp_path / "ingestion"
    pkg.mkdir()
    (pkg / "unrelated.py").write_text('CODES = ["ABCDE"]\n')

    assert collect_fred_series_ids(tmp_path) == {}


# ---------------------------------------------------------------------------
# Batch + CLI
# ---------------------------------------------------------------------------


def test_probe_all_attaches_references_and_does_not_sleep_between_none() -> None:
    session = _FakeSession(_FakeResponse(400, {}))

    results = probe_all(
        ["DEADONE"],
        "key",
        references={"DEADONE": ["ingestion/fred.py:10"]},
        session=session,
        delay=0,
    )

    assert [r.status for r in results] == ["dead"]
    assert results[0].references == ["ingestion/fred.py:10"]


def test_missing_api_key_warns_and_exits_cleanly(monkeypatch, capsys) -> None:
    """Graceful degradation: no key is a degraded run, never a crash."""
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    monkeypatch.setattr(
        "scripts.probe_fred_series._resolve_api_key", lambda: "", raising=True
    )

    exit_code = main([])

    assert exit_code == 0
    assert "nothing probed" in capsys.readouterr().out


def test_list_only_never_contacts_fred(monkeypatch, capsys) -> None:
    def _explode(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("--list-only must not contact FRED")

    monkeypatch.setattr("scripts.probe_fred_series.probe_all", _explode)

    assert main(["--list-only"]) == 0
    assert "ids collected" in capsys.readouterr().out


def test_dead_result_sets_failing_exit_code(monkeypatch, tmp_path, capsys) -> None:
    monkeypatch.setattr("scripts.probe_fred_series._resolve_api_key", lambda: "key")
    monkeypatch.setattr(
        "scripts.probe_fred_series.probe_all",
        lambda *a, **k: [ProbeResult(series_id="DEADONE", status="dead", detail="gone")],
    )
    out_path = tmp_path / "results.json"

    exit_code = main(["--ids", "DEADONE", "--json", str(out_path)])

    assert exit_code == 1
    assert "DEADONE" in capsys.readouterr().out
    assert json.loads(out_path.read_text())[0]["status"] == "dead"


def test_all_live_sets_success_exit_code(monkeypatch) -> None:
    monkeypatch.setattr("scripts.probe_fred_series._resolve_api_key", lambda: "key")
    monkeypatch.setattr(
        "scripts.probe_fred_series.probe_all",
        lambda *a, **k: [ProbeResult(series_id="UNRATE", status="live")],
    )

    assert main(["--ids", "UNRATE"]) == 0
