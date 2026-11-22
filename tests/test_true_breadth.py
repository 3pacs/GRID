"""True S&P 500 breadth: dated membership, coverage, UNKNOWN, and known-at rows only.

Pure computation plus reads of local membership files (the repo's
``config/sp500_membership.json`` and small files under ``tmp_path``); sockets
are refused, so nothing reaches the network.
"""

from __future__ import annotations

import json
import socket
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from features.true_breadth import (
    BREADTH_COLUMNS,
    MIN_MEMBER_COVERAGE,
    STATUS_OK,
    STATUS_PARTIAL,
    STATUS_UNKNOWN,
    UnknownMembership,
    compute_sector_dispersion_iqr,
    compute_true_breadth_panel,
    sp500_members_on,
)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    calls: list[object] = []

    def _refuse(*args, **kwargs):
        calls.append(args)
        raise AssertionError("network access attempted in an offline test")

    monkeypatch.setattr(socket.socket, "connect", _refuse)
    monkeypatch.setattr(socket, "create_connection", _refuse)
    monkeypatch.setattr(socket, "getaddrinfo", _refuse)
    yield
    assert calls == []


def _prices(n_tickers: int = 60, n_days: int = 230, seed: int = 42, start: str = "2025-01-01") -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=n_days)
    rng = np.random.default_rng(seed)
    data = {f"T{i}": 100.0 + np.cumsum(rng.normal(0.05, 1.0, n_days)) for i in range(n_tickers)}
    return pd.DataFrame(data, index=dates)


def _all_columns(px: pd.DataFrame):
    members = frozenset(px.columns)
    return lambda _d: members


def test_true_breadth_full_coverage_and_sector_iqr() -> None:
    px = _prices()
    panel = compute_true_breadth_panel(px, members_on=_all_columns(px))
    assert list(panel.columns) == BREADTH_COLUMNS and len(panel) == len(px)
    last = panel.iloc[-1]
    assert last["status"] == STATUS_OK and last["members"] == 60
    assert last["coverage_200ma"] == pytest.approx(1.0)
    assert 0.0 <= last["pct_above_200ma"] <= 1.0
    assert np.isfinite(last["mcclellan_osc"])
    # Too little history for a 200-session mean: that statistic is UNKNOWN, not 0.
    early = panel.iloc[100]
    assert early["status"] == STATUS_PARTIAL and np.isnan(early["pct_above_200ma"])
    assert np.isfinite(early["pct_above_50ma"])

    rng = np.random.default_rng(7)
    sec = pd.DataFrame(
        {s: 100.0 + np.cumsum(rng.normal(0.0, 0.8 + i * 0.1, 30))
         for i, s in enumerate(["XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLU"])},
        index=pd.bdate_range("2026-10-01", periods=30),
    )
    iqr = compute_sector_dispersion_iqr(sec, horizon_days=5)
    assert len(iqr) == 25
    assert float(iqr.iloc[-1]) >= 0.0


def test_fifty_observed_names_are_not_the_index() -> None:
    px = _prices(n_tickers=60)
    index_members = frozenset(f"T{i}" for i in range(500))  # the panel holds 60 of 500
    panel = compute_true_breadth_panel(px, members_on=lambda _d: index_members)
    last = panel.iloc[-1]
    assert last["members"] == 500 and last["observed_ret1"] == 60
    assert last["coverage_ret1"] == pytest.approx(0.12)
    assert last["status"] == STATUS_UNKNOWN
    assert panel[["pct_above_50ma", "pct_above_200ma", "net_adv_ratio", "mcclellan_osc"]].isna().all().all()
    assert "net_adv_ratio coverage 0.120 below 0.95" in last["reason"]


def test_coverage_threshold_boundary() -> None:
    assert MIN_MEMBER_COVERAGE == 0.95
    members = frozenset(f"T{i}" for i in range(100))
    at = compute_true_breadth_panel(_prices(n_tickers=95, n_days=3), members_on=lambda _d: members)
    below = compute_true_breadth_panel(_prices(n_tickers=94, n_days=3), members_on=lambda _d: members)
    assert at.iloc[-1]["coverage_ret1"] == pytest.approx(0.95) and np.isfinite(at.iloc[-1]["net_adv_ratio"])
    assert at.iloc[-1]["status"] == STATUS_PARTIAL  # 3 sessions: the moving averages are UNKNOWN
    assert np.isnan(below.iloc[-1]["net_adv_ratio"]) and below.iloc[-1]["status"] == STATUS_UNKNOWN


def test_membership_is_required() -> None:
    with pytest.raises(TypeError):
        compute_true_breadth_panel(_prices())  # type: ignore[call-arg]


def test_a_ticker_counts_only_from_its_join_date() -> None:
    dates = pd.bdate_range("2026-01-05", periods=10)
    rising = np.arange(10, dtype=float) + 100.0  # every base member rises daily
    px = pd.DataFrame({f"T{i}": rising for i in range(51)}, index=dates)
    px["LATE"] = 1.0  # flat; any day it counts it is a non-advancer
    join = px.index[5].date()
    base = frozenset(f"T{i}" for i in range(51))

    def members_on(d: date) -> frozenset[str]:
        return base | {"LATE"} if d >= join else base

    panel = compute_true_breadth_panel(px, members_on=members_on)
    before = panel.loc[[d for d in panel.index if d < join], "net_adv_ratio"].dropna()
    after = panel.loc[[d for d in panel.index if d >= join], "net_adv_ratio"]
    assert len(before) == 4 and (before == 1.0).all()   # LATE not counted before joining
    assert len(after) == 5 and after.tolist() == pytest.approx([51 / 52] * 5)

    # Today's list applied to history (LATE always a member) changes the past rows.
    today_list = compute_true_breadth_panel(px, members_on=lambda _d: base | {"LATE"})
    assert today_list.loc[before.index, "net_adv_ratio"].tolist() == pytest.approx([51 / 52] * 4)


def test_unknown_membership_rows_are_unknown_and_restart_the_oscillator() -> None:
    px = _prices(n_tickers=60, n_days=120)
    members = frozenset(px.columns)
    gap = px.index[60].date()

    def members_on(d: date):
        return UnknownMembership("membership file gap") if d == gap else members

    panel = compute_true_breadth_panel(px, members_on=members_on)
    row = panel.loc[gap]
    assert row["status"] == STATUS_UNKNOWN and row["reason"] == "membership file gap"
    osc = panel["mcclellan_osc"]
    assert np.isfinite(osc.iloc[59])                 # 59 known rows before the gap
    assert osc.iloc[60:99].isna().all()              # gap, then 38 rows of warm-up
    assert np.isfinite(osc.iloc[99])                 # 39th known row after the gap


def _membership_file(tmp_path: Path) -> Path:
    doc = {
        "version": "sp500-membership/1",
        "base_date": "2026-01-02",
        "base_members": [f"T{i}" for i in range(20)],
        "changes": [{"date": "2026-01-07", "add": ["NEW"], "remove": ["T0"]}],
        "covers_to": "2026-01-07",
        "last_count": 20,
    }
    path = tmp_path / "membership.json"
    path.write_text(json.dumps(doc))
    return path


def test_members_on_follows_the_dated_membership_file() -> None:
    members_on = sp500_members_on()
    # config/sp500_membership.json change of 2026-08-18: +RDDT +VMRK, -AVB -EQR.
    before, after = members_on(date(2026, 8, 17)), members_on(date(2026, 8, 18))
    assert "AVB" in before and "RDDT" not in before
    assert "RDDT" in after and "AVB" not in after
    early = members_on(date(2010, 1, 4))
    assert isinstance(early, UnknownMembership) and "before membership base_date" in early.reason


def test_dates_after_covers_to_are_unknown_not_carried_forward(tmp_path: Path) -> None:
    members_on = sp500_members_on(_membership_file(tmp_path))
    late = members_on(date(2026, 1, 8))
    assert isinstance(late, UnknownMembership)
    assert late.reason == "after membership covers_to 2026-01-07"

    cols = [f"T{i}" for i in range(20)] + ["NEW"]
    px = pd.DataFrame({c: np.arange(6, dtype=float) + 100.0 for c in cols},
                      index=pd.bdate_range("2026-01-02", periods=6))  # 01-02 .. 01-09
    panel = compute_true_breadth_panel(px, members_on=members_on)
    assert panel.loc[date(2026, 1, 7), "status"] == STATUS_PARTIAL and panel.loc[date(2026, 1, 7), "members"] == 20
    for d in (date(2026, 1, 8), date(2026, 1, 9)):
        assert panel.loc[d, "status"] == STATUS_UNKNOWN
        assert panel.loc[d, "reason"] == "after membership covers_to 2026-01-07"
        assert np.isnan(panel.loc[d, "net_adv_ratio"])


def test_future_rows_have_no_effect_on_past_rows() -> None:
    # Known-at control: row d may use only rows at or before d.
    px = _prices(n_tickers=60, n_days=230)
    members_on = _all_columns(px)
    cut = px.index[180].date()
    base = compute_true_breadth_panel(px, members_on=members_on)

    altered = px.copy()
    altered.iloc[181:] = altered.iloc[181:] * 50.0 - 7.0          # rewrite every later row
    extra = pd.DataFrame(1e6, index=pd.bdate_range(px.index[-1] + pd.Timedelta(days=1), periods=5),
                         columns=px.columns)
    altered = pd.concat([altered, extra])
    changed = compute_true_breadth_panel(altered, members_on=members_on)
    past = [d for d in base.index if d <= cut]
    pd.testing.assert_frame_equal(base.loc[past], changed.loc[past])

    as_of = compute_true_breadth_panel(altered, members_on=members_on, as_of=cut)
    assert as_of.index[-1] == cut
    pd.testing.assert_frame_equal(as_of, base.loc[past])


def test_duplicate_session_dates_are_refused() -> None:
    px = _prices(n_tickers=5, n_days=3)
    with pytest.raises(ValueError, match="duplicate"):
        compute_true_breadth_panel(pd.concat([px, px.iloc[[-1]]]), members_on=_all_columns(px))
