"""True S&P 500 breadth: dated membership, coverage, UNKNOWN, and known-at rows only.

Pure computation plus reads of local membership files (the repo's
``config/sp500_membership.json`` and small files under ``tmp_path``); sockets
are refused, so nothing reaches the network.
"""

from __future__ import annotations

import json
import socket
from datetime import date, datetime
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


# ── Non-finite and out-of-range inputs (Codex 6011729288) ──────────────────

_STAT_COLUMNS = ["pct_above_50ma", "pct_above_200ma", "net_adv_ratio", "mcclellan_osc",
                 "coverage_50ma", "coverage_200ma", "coverage_ret1"]


def _assert_finite_or_unknown(panel: pd.DataFrame) -> None:
    values = panel[_STAT_COLUMNS].astype(float).to_numpy()
    assert not np.isinf(values).any()                       # never +-inf
    for _, row in panel.iterrows():
        if row["status"] == STATUS_OK:
            assert np.isfinite([row["pct_above_50ma"], row["pct_above_200ma"], row["net_adv_ratio"]]).all()
        for stat in ("pct_above_50ma", "pct_above_200ma", "net_adv_ratio"):
            if np.isnan(row[stat]):
                assert row["status"] in (STATUS_PARTIAL, STATUS_UNKNOWN) and row["reason"]
            else:
                assert -1.0 <= row[stat] <= 1.0


def test_inf_zero_and_negative_prices_are_unobserved_and_counted() -> None:
    px = _prices(n_tickers=60, n_days=230)
    d = px.index[-1]
    for i, bad in enumerate([np.inf, -np.inf, 0.0, -5.0]):
        px.loc[d, f"T{i}"] = bad
    panel = compute_true_breadth_panel(px, members_on=_all_columns(px), min_coverage=0.9)
    last = panel.iloc[-1]
    assert last["invalid_prices"] == 4
    assert last["observed_ret1"] == 56 and last["coverage_ret1"] == pytest.approx(56 / 60)
    assert "4 member price(s) non-finite or <= 0" in last["reason"]
    _assert_finite_or_unknown(panel)
    # At the default 0.95 the same row is UNKNOWN: 56 of 60 is below it.
    strict = compute_true_breadth_panel(px, members_on=_all_columns(px))
    assert np.isnan(strict.iloc[-1]["net_adv_ratio"]) and strict.iloc[-1]["status"] == STATUS_UNKNOWN


def test_a_zero_previous_close_never_yields_an_infinite_return() -> None:
    px = _prices(n_tickers=20, n_days=5)
    px.iloc[2, 0] = 0.0                                      # T0 closes at 0, then recovers
    panel = compute_true_breadth_panel(px, members_on=_all_columns(px), min_coverage=0.9)
    assert panel.iloc[2]["invalid_prices"] == 1
    assert panel.iloc[3]["observed_ret1"] == 19              # T0's next return has no valid base
    _assert_finite_or_unknown(panel)


def test_randomly_corrupted_panels_only_emit_finite_or_unknown_values() -> None:
    rng = np.random.default_rng(11)
    px = _prices(n_tickers=80, n_days=260, seed=3)
    values = px.to_numpy(copy=True)  # writable under copy-on-write (pandas 3)
    for bad in (np.nan, np.inf, -np.inf, 0.0, -1.0):
        mask = rng.random(values.shape) < 0.01
        values[mask] = bad
    corrupt = pd.DataFrame(values, index=px.index, columns=px.columns)
    for min_cov in (0.5, 0.9, MIN_MEMBER_COVERAGE):
        panel = compute_true_breadth_panel(corrupt, members_on=_all_columns(corrupt), min_coverage=min_cov)
        _assert_finite_or_unknown(panel)
        assert (panel["invalid_prices"] >= 0).all()


def test_string_inf_prices_parse_and_are_treated_as_invalid() -> None:
    px = _prices(n_tickers=20, n_days=5).astype(object)
    px.iloc[-1, 0] = "inf"
    panel = compute_true_breadth_panel(px, members_on=_all_columns(px), min_coverage=0.9)
    assert panel.iloc[-1]["invalid_prices"] == 1
    _assert_finite_or_unknown(panel)


def test_invalid_panels_and_arguments_raise() -> None:
    px = _prices(n_tickers=5, n_days=4)
    members_on = _all_columns(px)
    text = px.astype(object)
    text.iloc[0, 0] = "abc"
    with pytest.raises(ValueError, match="numeric"):
        compute_true_breadth_panel(text, members_on=members_on)
    with pytest.raises(ValueError, match="NaT"):
        compute_true_breadth_panel(px.set_axis([pd.NaT, *px.index[1:]]), members_on=members_on)
    with pytest.raises(ValueError, match="session dates"):
        compute_true_breadth_panel(px.set_axis(["a", "b", "c", "d"]), members_on=members_on)
    same_day = px.set_axis([pd.Timestamp("2026-01-05 09:30"), pd.Timestamp("2026-01-05 16:00"),
                            pd.Timestamp("2026-01-06"), pd.Timestamp("2026-01-07")])
    with pytest.raises(ValueError, match="duplicate"):
        compute_true_breadth_panel(same_day, members_on=members_on)
    for bad_cov in (0.0, 1.5, float("nan"), True):
        with pytest.raises(ValueError, match="min_coverage"):
            compute_true_breadth_panel(px, members_on=members_on, min_coverage=bad_cov)
    with pytest.raises(ValueError, match="as_of"):
        compute_true_breadth_panel(px, members_on=members_on, as_of=datetime(2025, 1, 3, 16))
    for bad_members in (None, "T0", 5):
        with pytest.raises(ValueError, match="members_on"):
            compute_true_breadth_panel(px, members_on=lambda _d, m=bad_members: m)


def test_sector_iqr_ignores_bad_prices_and_validates_arguments() -> None:
    rng = np.random.default_rng(5)
    sec = pd.DataFrame({s: 100.0 + np.cumsum(rng.normal(0.0, 1.0, 20)) for s in
                        ["XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLU", "XLB"]},
                       index=pd.bdate_range("2026-10-01", periods=20))
    sec.iloc[10, 0], sec.iloc[11, 1], sec.iloc[12, 2] = np.inf, 0.0, -3.0
    iqr = compute_sector_dispersion_iqr(sec, horizon_days=5)
    assert len(iqr) > 0 and np.isfinite(iqr.to_numpy()).all() and (iqr >= 0).all()
    for kwargs in ({"horizon_days": 0}, {"horizon_days": True}, {"min_sectors": 1}):
        with pytest.raises(ValueError):
            compute_sector_dispersion_iqr(sec, **kwargs)


# ── Unique normalized tickers and naive session dates (Codex, 393f1e29) ────

def _dup_frame(columns: list[str], n_days: int = 3) -> pd.DataFrame:
    rising = np.arange(n_days, dtype=float) + 100.0
    return pd.DataFrame(np.column_stack([rising] * len(columns)), columns=columns,
                        index=pd.bdate_range("2026-01-05", periods=n_days))


@pytest.mark.parametrize("columns,named", [
    (["T0", "T1", "T0"], "T0 <- 'T0', 'T0'"),
    (["AAPL", "aapl", "MSFT"], "AAPL <- 'AAPL', 'aapl'"),
    (["AAPL", " AAPL ", "MSFT"], "AAPL <- ' AAPL ', 'AAPL'"),
    (["brk.b", "BRK.B"], "BRK.B <- 'BRK.B', 'brk.b'"),
])
def test_duplicate_ticker_columns_are_refused_by_name(columns: list[str], named: str) -> None:
    px = _dup_frame(columns)
    with pytest.raises(ValueError, match="duplicate tickers") as exc:
        compute_true_breadth_panel(px, members_on=lambda _d: frozenset(c.strip().upper() for c in columns))
    assert named in str(exc.value)
    with pytest.raises(ValueError, match="duplicate tickers"):
        compute_sector_dispersion_iqr(px)


def test_duplicate_members_are_refused_by_name() -> None:
    px = _dup_frame(["AAPL", "MSFT"])
    with pytest.raises(ValueError, match=r"members_on\(2026-01-05\) has duplicate tickers.*AAPL"):
        compute_true_breadth_panel(px, members_on=lambda _d: ["AAPL", " aapl", "MSFT"])
    with pytest.raises(ValueError, match="members_on"):
        compute_true_breadth_panel(px, members_on=lambda _d: frozenset({"AAPL", "", "MSFT"}))


def test_tickers_match_across_case_and_whitespace() -> None:
    # Columns as a lower-case feed spells them; members as the membership file does.
    cols = [f" t{i} " for i in range(20)]
    panel = compute_true_breadth_panel(_dup_frame(cols), members_on=lambda _d: frozenset(f"T{i}" for i in range(20)))
    assert panel.iloc[-1]["observed_ret1"] == 20 and panel.iloc[-1]["coverage_ret1"] == pytest.approx(1.0)


def test_duplicates_cannot_push_coverage_past_one_or_the_threshold() -> None:
    # 90 real names plus 10 re-spelled copies against 100 members: counted naively
    # that is 100 observations, exactly the 95% threshold passed. It is refused.
    members = frozenset(f"T{i}" for i in range(100))
    cols = [f"T{i}" for i in range(90)]
    for copies in ([f"T{i}" for i in range(10)], [f"t{i}" for i in range(10)], [f" T{i}" for i in range(10)]):
        with pytest.raises(ValueError, match="duplicate tickers"):
            compute_true_breadth_panel(_dup_frame(cols + copies), members_on=lambda _d: members)
    # Without the copies the same date is UNKNOWN: 90 of 100 is below 0.95.
    panel = compute_true_breadth_panel(_dup_frame(cols), members_on=lambda _d: members)
    assert panel.iloc[-1]["coverage_ret1"] == pytest.approx(0.90)
    assert panel.iloc[-1]["status"] == STATUS_UNKNOWN and np.isnan(panel.iloc[-1]["net_adv_ratio"])


def test_coverage_is_always_within_zero_and_one() -> None:
    rng = np.random.default_rng(23)
    for trial in range(20):
        n_cols = int(rng.integers(1, 40))
        members = frozenset(f"T{i}" for i in rng.choice(60, size=int(rng.integers(1, 60)), replace=False))
        px = _prices(n_tickers=n_cols, n_days=60, seed=trial)
        px = px.mask(rng.random(px.shape) < 0.1)
        panel = compute_true_breadth_panel(px, members_on=lambda _d, m=members: m, min_coverage=0.3)
        cov = panel[["coverage_50ma", "coverage_200ma", "coverage_ret1"]].astype(float).to_numpy()
        cov = cov[~np.isnan(cov)]
        assert ((cov >= 0.0) & (cov <= 1.0)).all()


def _closes_at(zone: str) -> pd.DatetimeIndex:
    # The same instants: 16:00 New York closes on three sessions.
    local = pd.DatetimeIndex(["2026-01-05 16:00", "2026-01-06 16:00", "2026-01-07 16:00"],
                             tz="America/New_York")
    return local if zone == "America/New_York" else local.tz_convert(zone)


@pytest.mark.parametrize("zone", ["America/New_York", "UTC", "Asia/Tokyo"])
def test_timezone_aware_indexes_are_refused_alike(zone: str) -> None:
    px = _dup_frame([f"T{i}" for i in range(20)]).set_axis(_closes_at(zone))
    members = frozenset(f"T{i}" for i in range(20))
    with pytest.raises(ValueError, match="timezone-aware"):
        compute_true_breadth_panel(px, members_on=lambda _d: members)
    with pytest.raises(ValueError, match="timezone-aware"):
        compute_sector_dispersion_iqr(px)
    # Offset-bearing strings are instants too.
    as_text = px.set_axis([ts.isoformat() for ts in px.index])
    with pytest.raises(ValueError, match="naive session dates|timezone-aware"):
        compute_true_breadth_panel(as_text, members_on=lambda _d: members)


def test_naive_labels_are_session_dates_whatever_the_time_of_day() -> None:
    members = frozenset(f"T{i}" for i in range(20))
    base = _dup_frame(sorted(members))
    stamped = base.set_axis([pd.Timestamp("2026-01-05 16:00"), pd.Timestamp("2026-01-06 09:30"),
                             pd.Timestamp("2026-01-07 23:59")])
    a = compute_true_breadth_panel(base, members_on=lambda _d: members)
    b = compute_true_breadth_panel(stamped, members_on=lambda _d: members)
    assert list(a.index) == list(b.index) == [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
    pd.testing.assert_frame_equal(a, b)
