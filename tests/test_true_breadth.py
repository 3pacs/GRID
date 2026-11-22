"""True S&P 500 breadth counts only the members of each date, from dated membership.

Pure computation plus a read of the local ``config/sp500_membership.json``;
sockets are refused, so nothing reaches the network.
"""

from __future__ import annotations

import socket
from datetime import date

import numpy as np
import pandas as pd
import pytest

from features.true_breadth import (
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


def _prices(n_tickers: int = 60, n_days: int = 230, seed: int = 42) -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-01", periods=n_days)
    rng = np.random.default_rng(seed)
    data = {f"T{i}": 100.0 + np.cumsum(rng.normal(0.05, 1.0, n_days)) for i in range(n_tickers)}
    return pd.DataFrame(data, index=dates)


def test_true_breadth_pit_membership_and_sector_iqr() -> None:
    px = _prices()
    allowed = frozenset(f"T{i}" for i in range(55))
    panel = compute_true_breadth_panel(px, members_on=lambda _d: allowed, min_constituents=50)
    assert not panel.empty
    assert 0.0 <= panel["pct_above_200ma"].dropna().iloc[-1] <= 1.0
    assert np.isfinite(panel["mcclellan_osc"].dropna().iloc[-1])

    rng = np.random.default_rng(7)
    sec = pd.DataFrame(
        {s: 100.0 + np.cumsum(rng.normal(0.0, 0.8 + i * 0.1, 30))
         for i, s in enumerate(["XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLU"])},
        index=pd.bdate_range("2026-10-01", periods=30),
    )
    iqr = compute_sector_dispersion_iqr(sec, horizon_days=5)
    assert len(iqr) == 25
    assert float(iqr.iloc[-1]) >= 0.0


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

    panel = compute_true_breadth_panel(px, members_on=members_on, min_constituents=50)
    before = panel.loc[[d for d in panel.index if d < join], "net_adv_ratio"].dropna()
    after = panel.loc[[d for d in panel.index if d >= join], "net_adv_ratio"]
    assert len(before) == 4 and (before == 1.0).all()   # LATE not counted before joining
    assert len(after) == 5 and after.tolist() == pytest.approx([51 / 52] * 5)

    # Today's list applied to history (LATE always a member) changes the past rows.
    today_list = compute_true_breadth_panel(px, members_on=lambda _d: base | {"LATE"}, min_constituents=50)
    assert today_list.loc[before.index, "net_adv_ratio"].tolist() == pytest.approx([51 / 52] * 4)


def test_members_on_follows_the_dated_membership_file() -> None:
    members_on = sp500_members_on()
    # config/sp500_membership.json change of 2026-08-18: +RDDT +VMRK, -AVB -EQR.
    before, after = members_on(date(2026, 8, 17)), members_on(date(2026, 8, 18))
    assert "AVB" in before and "RDDT" not in before
    assert "RDDT" in after and "AVB" not in after
    # Before the file's base date there is no known membership: nothing counts.
    assert members_on(date(2010, 1, 4)) == frozenset()
