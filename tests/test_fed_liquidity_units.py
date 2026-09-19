"""
Tests for the RRPONTSYD units fix (billions -> millions) in the Fed net
liquidity equation.

FRED publishes WALCL and WTREGEN in MILLIONS of USD but RRPONTSYD in
BILLIONS of USD. Combining them without converting RRP to millions first
understates the RRP drain by 1000x. These tests pin the corrected behavior
across all four call sites: ingestion/altdata/fed_liquidity.py,
analysis/money_flow.py, analysis/flow_thesis_data.py, and
analysis/thesis_scorer.py.

Uses unittest.mock to avoid real API/DB calls, following the pattern in
tests/test_ingestion.py.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from ingestion.altdata.fed_liquidity import (
    RRPONTSYD_TO_MILLIONS,
    FedLiquidityPuller,
    net_liquidity_millions,
)

# Realistic 2024-era magnitudes.
_WALCL_MILLIONS = 7_000_000.0  # $7.0T
_WTREGEN_MILLIONS = 750_000.0  # $750B
_RRPONTSYD_BILLIONS = 450.0  # $450B
_CORRECT_NET_LIQ = 5_800_000.0  # $5.8T, in millions
_BUGGY_NET_LIQ = 6_249_550.0  # what you get if RRP is not scaled


# ══════════════════════════════════════════════════════════════════════════
# (a) Pure helper
# ══════════════════════════════════════════════════════════════════════════


def test_net_liquidity_millions_scales_rrp():
    value = net_liquidity_millions(
        _WALCL_MILLIONS, _WTREGEN_MILLIONS, _RRPONTSYD_BILLIONS
    )

    assert value == _CORRECT_NET_LIQ
    # Plausible band: $3T-$9T, expressed in millions USD.
    assert 3_000_000 <= value <= 9_000_000
    assert value != _BUGGY_NET_LIQ


# ══════════════════════════════════════════════════════════════════════════
# (b) FedLiquidityPuller._compute_derived
# ══════════════════════════════════════════════════════════════════════════


def test_compute_derived_stores_net_liquidity_in_millions():
    puller = FedLiquidityPuller.__new__(FedLiquidityPuller)

    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    puller.engine = mock_engine
    puller.source_id = 1

    series_data = {
        "WALCL": {date(2024, 6, 5): _WALCL_MILLIONS},
        "WTREGEN": {date(2024, 6, 5): _WTREGEN_MILLIONS},
        "RRPONTSYD": {
            date(2024, 6, 5): _RRPONTSYD_BILLIONS,
            date(2024, 6, 6): 400.0,
        },
    }

    def fake_load(series_id, start_date=None, end_date=None):
        return dict(series_data[series_id])

    puller._load_series_from_db = MagicMock(side_effect=fake_load)
    puller._row_exists = MagicMock(return_value=False)
    puller._insert_raw = MagicMock()

    puller._compute_derived(date(2024, 6, 1), date(2024, 6, 30))

    nl_calls = [
        c
        for c in puller._insert_raw.call_args_list
        if c.kwargs.get("series_id") == "COMPUTED:fed_net_liquidity"
    ]
    assert len(nl_calls) == 2

    by_date = {c.kwargs["obs_date"]: c.kwargs for c in nl_calls}
    assert by_date[date(2024, 6, 5)]["value"] == pytest.approx(5_800_000.0)
    assert by_date[date(2024, 6, 6)]["value"] == pytest.approx(5_850_000.0)

    for kwargs in by_date.values():
        assert 3_000_000 <= kwargs["value"] <= 9_000_000
        payload = kwargs["raw_payload"]
        assert "rrpontsyd_billions" in payload
        assert payload["rrpontsyd_millions"] == pytest.approx(
            payload["rrpontsyd_billions"] * RRPONTSYD_TO_MILLIONS
        )


# ══════════════════════════════════════════════════════════════════════════
# Shared fakes for (c) — flow_thesis_data / thesis_scorer read raw_series
# via plain (non-bound-param) SQL through `with engine.connect() as conn:`.
# ══════════════════════════════════════════════════════════════════════════


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConn:
    """Routes queries by which series name and date-window appear in the SQL."""

    def __init__(self, current: dict, past: dict):
        self.current = current
        self.past = past

    def execute(self, stmt, *args, **kwargs):
        sql = str(stmt)
        source = self.past if "CURRENT_DATE - 30" in sql else self.current

        if "WALCL" in sql:
            if "pull_timestamp" in sql:
                row = (source["WALCL"], datetime.now(timezone.utc))
            else:
                row = (source["WALCL"],)
        elif "RRPONTSYD" in sql:
            row = (source["RRPONTSYD"],)
        elif "WTREGEN" in sql:
            row = (source["WTREGEN"],)
        else:
            row = None
        return _FakeResult(row)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class _FakeEngine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn


_CURRENT = {"WALCL": 7_000_000.0, "RRPONTSYD": 450.0, "WTREGEN": 750_000.0}
_PAST_30D = {"WALCL": 7_050_000.0, "RRPONTSYD": 500.0, "WTREGEN": 700_000.0}
_CORRECT_CURRENT_NL = 5_800_000.0  # 7,000,000 - 450*1000 - 750,000
_CORRECT_PAST_NL = 5_850_000.0  # 7,050,000 - 500*1000 - 700,000
_CORRECT_CHANGE = _CORRECT_CURRENT_NL - _CORRECT_PAST_NL  # -50,000


def test_flow_thesis_fed_liquidity_state_scales_rrp():
    from analysis.flow_thesis_data import _get_fed_liquidity_state

    engine = _FakeEngine(_FakeConn(_CURRENT, _PAST_30D))
    result = _get_fed_liquidity_state(engine)

    assert result["value"] == pytest.approx(_CORRECT_CURRENT_NL)
    assert result["change_30d"] == pytest.approx(_CORRECT_CHANGE)

    buggy_current = _CURRENT["WALCL"] - _CURRENT["RRPONTSYD"] - _CURRENT["WTREGEN"]
    buggy_past = _PAST_30D["WALCL"] - _PAST_30D["RRPONTSYD"] - _PAST_30D["WTREGEN"]
    buggy_change = buggy_current - buggy_past
    assert result["change_30d"] != pytest.approx(buggy_change)


def test_thesis_scorer_fed_liquidity_scales_rrp():
    from analysis.thesis_scorer import _score_fed_liquidity

    engine = _FakeEngine(_FakeConn(_CURRENT, _PAST_30D))
    result = _score_fed_liquidity(engine, accuracy=0.5)

    # Values/changes here surface only through the summary string.
    assert "5,800,000" in result["data_point"]

    buggy_current = _CURRENT["WALCL"] - _CURRENT["RRPONTSYD"] - _CURRENT["WTREGEN"]
    assert f"{buggy_current:,.0f}" not in result["data_point"]


# ══════════════════════════════════════════════════════════════════════════
# (d) money_flow._build_central_banks_layer
# ══════════════════════════════════════════════════════════════════════════


def test_money_flow_fed_net_liquidity_scales_rrp(monkeypatch):
    from analysis import money_flow

    def fake_bs(engine, cb_id, cb_config, as_of):
        if cb_id == "fed":
            return _WALCL_MILLIONS, "confirmed"
        return None, "estimated"

    def fake_bs_change(engine, cb_config, as_of, days=30):
        return None, "estimated"

    def fake_rate(engine, cb_id, cb_config, as_of):
        return None, "estimated"

    def fake_stance(engine, cb_id, cb_config, as_of):
        return "unknown", 0.0

    def fake_fred_latest(engine, series_id, as_of=None):
        if series_id == "RRPONTSYD":
            return _RRPONTSYD_BILLIONS
        if series_id == "WTREGEN":
            return _WTREGEN_MILLIONS
        return None

    def fake_fred_value_at(engine, series_id, target_date):
        return None

    monkeypatch.setattr(money_flow, "_resolve_cb_balance_sheet", fake_bs)
    monkeypatch.setattr(money_flow, "_compute_bs_change", fake_bs_change)
    monkeypatch.setattr(money_flow, "_resolve_cb_rate", fake_rate)
    monkeypatch.setattr(money_flow, "_infer_policy_stance", fake_stance)
    monkeypatch.setattr(money_flow, "_get_fred_latest", fake_fred_latest)
    monkeypatch.setattr(money_flow, "_get_fred_value_at", fake_fred_value_at)

    result = money_flow._build_central_banks_layer(MagicMock(), date(2024, 6, 5))

    fed_node = next(n for n in result["nodes"] if n["id"] == "fed")
    assert fed_node["metrics"]["net_liquidity"] == pytest.approx(_CORRECT_NET_LIQ)
