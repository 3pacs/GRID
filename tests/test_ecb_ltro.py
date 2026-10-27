"""Tests for ingestion/altdata/ecb_ltro.py.

Fixtures in tests/fixtures/ecb/ are real ECB Data Portal responses recorded
from grid-svr on 2026-09-29:
  * ilm_ltro_last6_20260929.json -- ILM.W.U2.C.A050200.U2.EUR, last 6 weeks
  * ilm_old_tltro_key_404_20260929.json -- the 404 body the old
    ecb_tltro key (ILM/M.U2.C.LT3.U2.EUR) gets
No network is used.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from ingestion.altdata.ecb_ltro import (
    ECB_LTRO_URL,
    SERIES_LTRO_OUTSTANDING,
    ECBLtroPuller,
    parse_ecb_sdmx_json,
)

FIXTURES = Path(__file__).parent / "fixtures" / "ecb"
LTRO_PAYLOAD = json.loads((FIXTURES / "ilm_ltro_last6_20260929.json").read_text(encoding="utf-8"))
OLD_KEY_404 = (FIXTURES / "ilm_old_tltro_key_404_20260929.json").read_text(encoding="utf-8")


def test_parse_uses_period_end_and_scales_millions_to_billions() -> None:
    rows = parse_ecb_sdmx_json(LTRO_PAYLOAD)
    assert [r[2] for r in rows] == [
        "2026-W33", "2026-W34", "2026-W35", "2026-W36", "2026-W37", "2026-W38",
    ]
    # 2026-W38 ends Sunday 2026-09-20 per the ECB's own period metadata.
    assert rows[-1][0] == date(2026, 9, 20)
    # 14305 EUR millions -> 14.305 EUR bn
    assert rows[-1][1] == pytest.approx(14.305)
    assert rows[0][1] == pytest.approx(14.230)
    assert all(a[0] < b[0] for a, b in zip(rows, rows[1:]))


def test_parse_rejects_unexpected_shape() -> None:
    with pytest.raises(ValueError):
        parse_ecb_sdmx_json({"type": "x", "status": 404})


def test_parse_rejects_non_eur_unit() -> None:
    payload = json.loads(json.dumps(LTRO_PAYLOAD))
    for attr in payload["structure"]["attributes"]["series"]:
        if attr["id"] == "UNIT":
            attr["values"] = [{"id": "USD", "name": "US dollar"}]
    with pytest.raises(ValueError):
        parse_ecb_sdmx_json(payload)


class _Resp:
    def __init__(self, body: str | dict, status: int = 200) -> None:
        self._body = body
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error")

    def json(self) -> dict:
        return self._body if isinstance(self._body, dict) else json.loads(self._body)


class _Session:
    def __init__(self, resp: _Resp) -> None:
        self.resp = resp
        self.headers: dict[str, str] = {}
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict | None = None, timeout: int = 0) -> _Resp:  # noqa: ARG002
        self.calls.append((url, dict(params or {})))
        return self.resp


def _engine(latest: date | None = None, existing: list[date] | None = None) -> MagicMock:
    engine = MagicMock()
    cconn = MagicMock()

    def _cexec(stmt, params=None):  # noqa: ANN001, ARG001
        res = MagicMock()
        res.fetchone.return_value = (latest,) if "MAX(obs_date)" in str(stmt) else (11,)
        return res

    cconn.execute.side_effect = _cexec
    engine.connect.return_value.__enter__ = MagicMock(return_value=cconn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    bconn = MagicMock()

    def _bexec(stmt, params=None):  # noqa: ANN001, ARG001
        res = MagicMock()
        res.fetchall.return_value = [(d,) for d in (existing or [])]
        res.fetchone.return_value = None
        return res

    bconn.execute.side_effect = _bexec
    engine.begin.return_value.__enter__ = MagicMock(return_value=bconn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine._bconn = bconn
    return engine


def _inserts(engine: MagicMock) -> list[dict]:
    return [
        c.args[1]
        for c in engine._bconn.execute.call_args_list
        if "INSERT INTO raw_series" in str(c.args[0])
    ]


def test_first_run_loads_history_with_default_pull_timestamp() -> None:
    engine = _engine(latest=None)
    session = _Session(_Resp(LTRO_PAYLOAD))
    result = ECBLtroPuller(engine, session=session).pull()

    assert session.calls[0][0] == ECB_LTRO_URL
    assert session.calls[0][1]["startPeriod"] == "2019-01-01"
    assert result["status"] == "SUCCESS"
    rows = _inserts(engine)
    assert result["rows_inserted"] == 6 == len(rows)
    for r in rows:
        assert r["sid"] == SERIES_LTRO_OUTSTANDING
        assert r["status"] == "SUCCESS"
        assert r["src"] == 11
        assert "pull_timestamp" not in r  # column default = fetch time


def test_incremental_run_only_inserts_new_weeks() -> None:
    engine = _engine(
        latest=date(2026, 9, 13),
        existing=[date(2026, 8, 16), date(2026, 8, 23), date(2026, 8, 30),
                  date(2026, 9, 6), date(2026, 9, 13)],
    )
    session = _Session(_Resp(LTRO_PAYLOAD))
    result = ECBLtroPuller(engine, session=session).pull()
    assert session.calls[0][1]["lastNObservations"] == 8
    assert result["status"] == "SUCCESS"
    rows = _inserts(engine)
    assert [r["od"] for r in rows] == [date(2026, 9, 20)]


def test_old_tltro_key_404_is_failed_and_writes_nothing() -> None:
    engine = _engine()
    session = _Session(_Resp(OLD_KEY_404, status=404))
    result = ECBLtroPuller(engine, session=session).pull()
    assert result["status"] == "FAILED"
    assert result["rows_inserted"] == 0
    assert _inserts(engine) == []


def test_empty_series_is_failed() -> None:
    payload = json.loads(json.dumps(LTRO_PAYLOAD))
    series = next(iter(payload["dataSets"][0]["series"].values()))
    series["observations"] = {}
    engine = _engine()
    result = ECBLtroPuller(engine, session=_Session(_Resp(payload))).pull()
    assert result["status"] == "FAILED"
    assert _inserts(engine) == []


def test_own_source_identity() -> None:
    assert ECBLtroPuller.SOURCE_NAME == "ecb_ilm_ltro"
    assert ECBLtroPuller.SOURCE_NAME != "ecb_tltro"
