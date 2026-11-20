"""One price basis per ``{ticker}_full`` feature (2026-11-08 panel corruption).

What went wrong (docs/reports/HIGH-SHARPE-INVENTORY-20261108.md §3): the
entity map sent both ``YF:<T>:adj_close`` and ``YF:<T>:close`` into the same
``<t>_full`` feature. The resolver resolves per series_id, so the two fields
produced two candidates for the same (feature, obs_date, vintage) and
``ON CONFLICT DO NOTHING`` kept whichever insert ran first; on days only one
field was pulled it won alone. Vintages of one obs_date therefore alternated
between raw and dividend/split-adjusted closes (NVDA 2021-07-19: 751.19 vs
18.74). ``build_price_panel`` read the latest vintage per date, saw the
flips as -97 % days, and the universal split adjuster compounded every one
of them into history: XLP first-to-last ratio 8e201, GLD 2,071x, NVDA 366x.

These tests pin the fix at each layer:

1. entity map  -- ``adj_close`` is the only field that reaches a ``_full``
   feature (raw close only for RAW_CLOSE_FULL_FEATURES); the pattern
   fallback obeys the same rule; ``price_basis_conflicts()`` is empty.
2. resolver    -- with the policy, an alternating-vintage raw feed yields
   resolved rows from one basis only; with the old double mapping the
   collision is reproduced and the run-time tripwire fires.
3. panel       -- ``build_price_panel`` no longer infers splits from price
   jumps (a transient glitch leaves history untouched) and re-bases each
   vintage with recorded split events, idempotently.
4. adjuster    -- ``detect_splits`` refuses a drop that reverts.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest
from loguru import logger as log

from alpha_research.data.panel_builder import (
    build_price_panel,
    rebase_vintages_to_as_of,
)
from alpha_research.data.split_adjuster import adjust_splits, detect_splits
from normalization.entity_map import (
    FULL_PRICE_BASIS_FIELD,
    NEW_MAPPINGS_V2,
    RAW_CLOSE_FULL_FEATURES,
    SEED_MAPPINGS,
    _YF_FIELD_SUFFIXES,
    EntityMap,
    price_basis_conflicts,
    yf_candidate_feature_names,
)
from normalization.resolver import Resolver
from store.observations import SplitEvent

UTC = timezone.utc


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _registry_engine(features: list[tuple[int, str]]) -> MagicMock:
    """Mock engine whose feature_registry holds exactly ``features``."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    conn.execute.return_value.fetchall.return_value = list(features)
    return engine


def _mappings() -> dict[str, str]:
    return {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}


# ---------------------------------------------------------------------------
# 1. entity map
# ---------------------------------------------------------------------------

class TestEntityMapBasisPolicy:
    def test_static_mappings_have_no_basis_conflicts(self) -> None:
        assert price_basis_conflicts() == {}

    def test_full_basis_is_adj_close(self) -> None:
        assert FULL_PRICE_BASIS_FIELD == "adj_close"

    def test_every_raw_close_full_target_is_allowlisted(self) -> None:
        offenders = {
            sid: feat for sid, feat in _mappings().items()
            if sid.startswith("YF:") and sid.endswith(":close")
            and feat.endswith("_full") and feat not in RAW_CLOSE_FULL_FEATURES
        }
        assert offenders == {}

    def test_allowlisted_features_are_not_also_fed_by_adj_close(self) -> None:
        for sid, feat in _mappings().items():
            if sid.startswith("YF:") and sid.endswith(":adj_close"):
                assert feat not in RAW_CLOSE_FULL_FEATURES, sid

    @pytest.mark.parametrize("ticker", ["XLP", "GLD", "NVDA", "XLB", "XLRE", "QQQ", "TLT", "HYG", "AAPL"])
    def test_equity_full_is_fed_by_adj_close_only(self, ticker: str) -> None:
        feat = f"{ticker.lower()}_full"
        mapper = EntityMap(db_engine=_registry_engine([(7, feat)]))
        assert mapper.get_feature_id(f"YF:{ticker}:adj_close") == 7
        assert mapper.get_feature_id(f"YF:{ticker}:close") is None

    def test_spy_full_is_the_receipted_raw_close_by_contract(self) -> None:
        from price_close_contract import SPY_CLOSE_FEATURE, SPY_CLOSE_SERIES

        assert SPY_CLOSE_FEATURE == "spy_full"
        # YF:SPY:close lives in NEW_MAPPINGS_V2, merged into SEED_MAPPINGS only
        # once an EntityMap is built in the process (the parametrized test above
        # does that), so under xdist this test must read the effective mapping.
        assert _mappings()[SPY_CLOSE_SERIES] == "spy_full"
        assert "YF:SPY:adj_close" not in _mappings()
        assert "spy_full" in RAW_CLOSE_FULL_FEATURES

    def test_detects_a_mixed_basis_mapping(self) -> None:
        bad = {"YF:XLP:adj_close": "xlp_full", "YF:XLP:close": "xlp_full"}
        assert price_basis_conflicts(bad) == {
            "xlp_full": {"YF:XLP:adj_close": "adj_close", "YF:XLP:close": "close"},
        }

    def test_detects_a_raw_close_full_outside_the_allowlist(self) -> None:
        assert price_basis_conflicts({"YF:XLP:close": "xlp_full"}) == {
            "xlp_full": {"YF:XLP:close": "close"},
        }

    def test_allowlisted_raw_close_full_is_not_a_conflict(self) -> None:
        assert price_basis_conflicts({"YF:^GSPC:close": "sp500_full"}) == {}

    def test_pattern_fallback_never_crosses_bases(self) -> None:
        assert "_full" not in _YF_FIELD_SUFFIXES["close"]
        assert "_close" not in _YF_FIELD_SUFFIXES["adj_close"]
        assert "ftnt_full" in yf_candidate_feature_names("YF:FTNT:adj_close")
        assert "ftnt_full" not in yf_candidate_feature_names("YF:FTNT:close")
        assert "ftnt_close" in yf_candidate_feature_names("YF:FTNT:close")
        assert "ftnt_close" not in yf_candidate_feature_names("YF:FTNT:adj_close")

    def test_instance_report_is_empty(self) -> None:
        mapper = EntityMap(db_engine=_registry_engine([(1, "xlp_full")]))
        assert mapper.mixed_basis_features() == {}


# ---------------------------------------------------------------------------
# 2. resolver
# ---------------------------------------------------------------------------

XLP_FID = 7
OBS = date(2016, 1, 4)
# Three pull-date vintages. adj_close is re-adjusted at each pull (a dividend
# between pulls moves it a little); the raw close is the as-traded print.
RAW_ROWS = [
    # series_id, obs_date, value, source_id, pull_timestamp, priority_rank, source_name, raw_id, payload
    ("YF:XLP:adj_close", OBS, 60.10, 3, datetime(2026, 4, 24, 5, 0, tzinfo=UTC), 1, "TIINGO", 1, None),
    ("YF:XLP:close", OBS, 70.00, 3, datetime(2026, 4, 24, 5, 0, tzinfo=UTC), 1, "TIINGO", 2, None),
    ("YF:XLP:close", OBS, 70.00, 2, datetime(2026, 4, 29, 5, 0, tzinfo=UTC), 2, "yfinance", 3, None),
    ("YF:XLP:adj_close", OBS, 59.95, 2, datetime(2026, 5, 2, 5, 0, tzinfo=UTC), 2, "yfinance", 4, None),
    ("YF:XLP:close", OBS, 70.00, 2, datetime(2026, 5, 2, 5, 0, tzinfo=UTC), 2, "yfinance", 5, None),
]


class _RecordingEngine:
    """Engine double: a feature_registry with xlp_full, the raw rows above,
    and a recorder for every executemany INSERT the resolver issues."""

    def __init__(self, raw_rows: list[tuple]) -> None:
        self.raw_rows = raw_rows
        self.inserts: list[dict] = []

    def _conn(self):
        engine = self

        class _Result:
            def __init__(self, rows, rowcount=-1):
                self._rows, self.rowcount = rows, rowcount

            def fetchall(self):
                return self._rows

            def fetchone(self):
                return self._rows[0] if self._rows else None

            def scalar(self):
                return self._rows[0][0] if self._rows else None

        class _Conn:
            def execute(self, statement, params=None):
                sql = str(statement)
                if isinstance(params, list):
                    engine.inserts.extend(params)
                    return _Result([], rowcount=len(params))
                if "SELECT DISTINCT" in sql:
                    return _Result([(sid,) for sid in sorted({r[0] for r in engine.raw_rows})])
                if "FROM raw_series rs" in sql:
                    sids = set(params.get("sids", [])) if params else set()
                    return _Result([r for r in engine.raw_rows if r[0] in sids])
                if "feature_registry" in sql:
                    if "family" in sql:
                        return _Result([(XLP_FID, "equity")])
                    return _Result([(XLP_FID, "xlp_full")])
                return _Result([])

        class _Ctx:
            def __enter__(self_inner):
                return _Conn()

            def __exit__(self_inner, *_exc):
                return False

        return _Ctx()

    def begin(self):
        return self._conn()

    def connect(self):
        return self._conn()


@pytest.fixture
def warnings_captured():
    records: list[str] = []
    sink = log.add(lambda m: records.append(str(m)), level="WARNING")
    yield records
    log.remove(sink)


class TestResolverOneBasisPerFeature:
    def _run(self, monkeypatch) -> _RecordingEngine:
        engine = _RecordingEngine(RAW_ROWS)
        # The real EntityMap over a registry that holds xlp_full only.
        monkeypatch.setattr(
            "normalization.resolver.EntityMap",
            lambda _engine: EntityMap(db_engine=_registry_engine([(XLP_FID, "xlp_full")])),
        )
        Resolver(db_engine=engine).resolve_pending(lookback_days=30, workers=1)
        return engine

    def test_alternating_raw_feed_resolves_to_one_basis(self, monkeypatch, warnings_captured) -> None:
        engine = self._run(monkeypatch)
        rows = [r for r in engine.inserts if r["fid"] == XLP_FID]
        assert rows, "xlp_full received no rows"
        values = {r["val"] for r in rows}
        assert values == {60.10, 59.95}, "a raw close (70.00) reached xlp_full"
        # One candidate per vintage, no two rows competing for one key.
        keys = [(r["fid"], r["od"], r["vd"]) for r in rows]
        assert len(keys) == len(set(keys))
        assert {r["vd"] for r in rows} == {date(2026, 4, 24), date(2026, 5, 2)}
        assert not [w for w in warnings_captured if "more than one price basis" in w]

    def test_old_double_mapping_reproduces_the_collision_and_trips_the_wire(
        self, monkeypatch, warnings_captured,
    ) -> None:
        # The pre-2026-11-08 state: both fields onto xlp_full.
        monkeypatch.setitem(SEED_MAPPINGS, "YF:XLP:close", "xlp_full")
        engine = self._run(monkeypatch)
        rows = [r for r in engine.inserts if r["fid"] == XLP_FID]
        by_key: dict[tuple, set[float]] = {}
        for r in rows:
            by_key.setdefault((r["od"], r["vd"]), set()).add(r["val"])
        # 2026-04-24: 60.10 (adj) and 70.00 (raw) race for one unique key, and
        # 2026-04-29 is a raw-only vintage -- the alternation.
        assert by_key[(OBS, date(2026, 4, 24))] == {60.10, 70.00}
        assert by_key[(OBS, date(2026, 4, 29))] == {70.00}
        assert [w for w in warnings_captured if "more than one price basis" in w and "xlp_full" in w]


# ---------------------------------------------------------------------------
# 3. panel builder
# ---------------------------------------------------------------------------

class _PanelResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _PanelEngine:
    """feature_registry + resolved_series (PIT vintages) + raw split events."""

    def __init__(self, registry, resolved, splits=None):
        self.registry = registry
        self.resolved = resolved
        self.splits = splits or []   # (symbol, effective_date, ratio, pull_ts)

    def connect(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, params=None):
        params = params or {}
        sql = " ".join(str(stmt).split())
        if "FROM feature_registry" in sql:
            names = set(params.get("names", []))
            return _PanelResult([(fid, nm) for fid, nm in self.registry if nm in names])
        if "FROM resolved_series" in sql:
            fids, aod = set(params["fids"]), params["aod"]
            best: dict[tuple, dict] = {}
            for r in self.resolved:
                if r["fid"] in fids and r["obs"] <= aod and r["release"] <= aod:
                    cur = best.get((r["fid"], r["obs"]))
                    if cur is None or r["vintage"] > cur["vintage"]:
                        best[(r["fid"], r["obs"])] = r
            return _PanelResult([
                (r["fid"], r["obs"], r["value"], r["release"], r["vintage"]) for r in best.values()
            ])
        if "FROM raw_series r" in sql:
            sid = params["sid"]
            sym = sid.split(":")[1]
            as_of_ts = params.get("as_of_ts")
            rows = [
                (eff, ratio, ts, "TIINGO") for s, eff, ratio, ts in self.splits
                if s == sym and (as_of_ts is None or ts <= as_of_ts)
            ]
            return _PanelResult(rows)
        return _PanelResult([])


def _res(fid, obs, value, vintage):
    return {"fid": fid, "obs": obs, "value": value, "release": vintage, "vintage": vintage}


def _bdays(start: date, n: int) -> list[date]:
    return [d.date() for d in pd.bdate_range(start, periods=n)]


class TestBuildPricePanelNoHeuristicSplits:
    def test_transient_glitch_no_longer_rescales_history(self) -> None:
        """A holiday junk print (0.6 on 2024-01-15 for XLP) reverting next
        day used to be read as a ~130:1 split and divide all earlier history
        by 130 -- 101 of those gave XLP its 8e201 ratio. The panel must
        serve the stored values as they are."""
        days = _bdays(date(2024, 1, 2), 30)
        rows = []
        for i, d in enumerate(days):
            v = 0.6 if d == date(2024, 1, 15) else 75.0 + i * 0.1
            rows.append(_res(1, d, v, d))
        engine = _PanelEngine([(1, "xlp_full")], rows)
        as_of = days[-1]
        panel = build_price_panel(engine, tickers=["XLP"], start_date=days[0], end_date=as_of, as_of_date=as_of)
        assert panel.loc[pd.Timestamp(days[0]), "XLP"] == pytest.approx(75.0)
        assert panel.loc[pd.Timestamp(date(2024, 1, 15)), "XLP"] == pytest.approx(0.6)
        ratio = panel["XLP"].iloc[-1] / panel["XLP"].iloc[0]
        assert 1.0 < ratio < 1.1

    def test_latest_vintage_still_wins(self) -> None:
        d = date(2025, 11, 28)
        rows = [_res(1, d, 679.52, d), _res(1, d, 683.39, date(2025, 11, 29))]
        rows += [_res(1, dd, 680.0, dd) for dd in _bdays(date(2025, 11, 3), 15) if dd < d]
        engine = _PanelEngine([(1, "spy_full")], rows)
        panel = build_price_panel(engine, tickers=["SPY"], start_date=date(2025, 11, 1),
                                  end_date=date(2025, 12, 1), as_of_date=date(2025, 12, 1))
        assert panel.loc[pd.Timestamp(d), "SPY"] == pytest.approx(683.39)

    def test_recorded_split_rebases_only_pre_split_vintages(self) -> None:
        """NVDA 10:1 effective 2024-06-10. Cells written from pulls before
        that date are in the pre-split share basis and are divided by 10;
        cells from later pulls already carry the split and are untouched.
        Applying the panel twice changes nothing (idempotent per vintage)."""
        pre_days = _bdays(date(2024, 5, 20), 10)      # pulled 2024-06-01 (pre-split basis)
        post_days = _bdays(date(2024, 6, 10), 10)     # pulled next day each (post-split)
        rows = [_res(1, d, 1200.0, date(2024, 6, 1)) for d in pre_days]
        rows += [_res(1, d, 121.0, d) for d in post_days]
        split_ts = datetime(2024, 6, 10, 22, 0, tzinfo=UTC)
        engine = _PanelEngine([(1, "nvda_full")], rows, splits=[("NVDA", date(2024, 6, 10), 10.0, split_ts)])
        as_of = post_days[-1]
        panel = build_price_panel(engine, tickers=["NVDA"], start_date=pre_days[0], end_date=as_of, as_of_date=as_of)
        assert panel.loc[pd.Timestamp(pre_days[0]), "NVDA"] == pytest.approx(120.0)
        assert panel.loc[pd.Timestamp(post_days[0]), "NVDA"] == pytest.approx(121.0)
        again = build_price_panel(engine, tickers=["NVDA"], start_date=pre_days[0], end_date=as_of, as_of_date=as_of)
        pd.testing.assert_frame_equal(panel, again)

    def test_split_not_yet_known_as_of_is_not_applied(self) -> None:
        """PIT: a split row pulled after ``as_of_date`` is invisible, so the
        panel as of the day before the split shows the pre-split basis."""
        pre_days = _bdays(date(2024, 5, 20), 10)
        rows = [_res(1, d, 1200.0, date(2024, 6, 1)) for d in pre_days]
        split_ts = datetime(2024, 6, 10, 22, 0, tzinfo=UTC)
        engine = _PanelEngine([(1, "nvda_full")], rows, splits=[("NVDA", date(2024, 6, 10), 10.0, split_ts)])
        as_of = date(2024, 6, 7)
        panel = build_price_panel(engine, tickers=["NVDA"], start_date=pre_days[0], end_date=as_of, as_of_date=as_of)
        assert panel.loc[pd.Timestamp(pre_days[0]), "NVDA"] == pytest.approx(1200.0)

    def test_split_rebase_can_be_disabled(self) -> None:
        pre_days = _bdays(date(2024, 5, 20), 5)
        rows = [_res(1, d, 1200.0, date(2024, 6, 1)) for d in pre_days]
        split_ts = datetime(2024, 6, 10, 22, 0, tzinfo=UTC)
        engine = _PanelEngine([(1, "nvda_full")], rows, splits=[("NVDA", date(2024, 6, 10), 10.0, split_ts)])
        as_of = date(2024, 6, 28)
        panel = build_price_panel(engine, tickers=["NVDA"], start_date=pre_days[0], end_date=as_of,
                                  as_of_date=as_of, split_rebase=False)
        assert panel.loc[pd.Timestamp(pre_days[0]), "NVDA"] == pytest.approx(1200.0)


class TestRebaseVintagesToAsOf:
    SPLITS = [SplitEvent("NVDA", date(2024, 6, 10), 10.0), SplitEvent("NVDA", date(2021, 7, 20), 4.0)]

    def test_no_splits_is_identity(self) -> None:
        idx = pd.DatetimeIndex(["2024-01-02", "2024-01-03"])
        vals = pd.Series([1.0, 2.0], index=idx)
        vint = pd.Series([date(2024, 1, 2), date(2024, 1, 3)], index=idx)
        out = rebase_vintages_to_as_of(vals, vint, [], date(2024, 12, 31))
        pd.testing.assert_series_equal(out, vals)

    def test_divides_by_splits_between_vintage_and_as_of(self) -> None:
        idx = pd.DatetimeIndex(["2021-01-04", "2022-01-04", "2024-07-01"])
        vals = pd.Series([520.0, 130.0, 125.0], index=idx)
        vint = pd.Series([date(2021, 1, 5), date(2022, 1, 5), date(2024, 7, 2)], index=idx)
        out = rebase_vintages_to_as_of(vals, vint, self.SPLITS, date(2024, 12, 31))
        assert out.iloc[0] == pytest.approx(520.0 / 40.0)   # before both splits
        assert out.iloc[1] == pytest.approx(130.0 / 10.0)   # between them
        assert out.iloc[2] == pytest.approx(125.0)          # after both

    def test_split_after_as_of_is_ignored(self) -> None:
        idx = pd.DatetimeIndex(["2024-05-01"])
        out = rebase_vintages_to_as_of(
            pd.Series([1200.0], index=idx), pd.Series([date(2024, 5, 2)], index=idx),
            self.SPLITS, date(2024, 6, 7),
        )
        assert out.iloc[0] == pytest.approx(1200.0)


# ---------------------------------------------------------------------------
# 4. split adjuster reversion guard
# ---------------------------------------------------------------------------

def _series(dates: list[str], values: list[float], name: str = "T") -> pd.Series:
    return pd.Series(values, index=pd.DatetimeIndex(dates), name=name, dtype=float)


class TestDetectSplitsRefusesRevertingDrops:
    def test_holiday_junk_print_is_not_a_split(self, warnings_captured) -> None:
        prices = _series(["2024-01-12", "2024-01-15", "2024-01-16", "2024-01-17"], [78.0, 0.6, 78.2, 78.1], "XLP")
        assert detect_splits(prices) == []
        pd.testing.assert_series_equal(adjust_splits(prices), prices)
        assert any("reverts" in w and "XLP" in w for w in warnings_captured)

    def test_raw_adjusted_flip_lasting_days_is_not_a_split(self) -> None:
        dates = ["2021-07-16", "2021-07-19", "2021-07-20", "2021-07-21", "2021-07-22", "2021-07-23"]
        prices = _series(dates, [751.0, 18.74, 18.90, 752.0, 753.0, 751.5], "NVDA")
        assert detect_splits(prices) == []

    def test_persisting_drop_is_still_a_split(self) -> None:
        prices = _series(["2024-06-07", "2024-06-10", "2024-06-11", "2024-06-12"], [1208.0, 121.8, 120.9, 125.2], "NVDA")
        splits = detect_splits(prices)
        assert [s["ratio"] for s in splits] == [10]
        adjusted = adjust_splits(prices)
        assert adjusted.iloc[0] == pytest.approx(121.8)  # pre-split print rescaled to the post-split level

    def test_drop_on_the_last_bar_is_accepted_as_before(self) -> None:
        prices = _series(["2024-06-07", "2024-06-10"], [1208.0, 121.8])
        assert [s["ratio"] for s in detect_splits(prices)] == [10]

    def test_bars_outside_the_window_carry_no_evidence(self) -> None:
        # A genuine rally two years later is not a reversion.
        prices = _series(["2020-08-28", "2020-08-31", "2022-08-24"], [500.0, 100.0, 900.0])
        assert [s["ratio"] for s in detect_splits(prices)] == [5]

    def test_non_datetime_index_uses_bar_window(self) -> None:
        prices = pd.Series([100.0, 2.0, 100.0, 101.0], dtype=float)
        assert detect_splits(prices) == []
        assert [s["ratio"] for s in detect_splits(pd.Series([100.0, 50.0, 51.0]))] == [2]


def test_rebase_accepts_timestamp_vintages() -> None:
    """A PITWindowCache or a fake may hand back Timestamp vintages (a ``date``
    subclass); they must be normalised before the split-event comparison."""
    idx = pd.DatetimeIndex(["2024-05-01"])
    out = rebase_vintages_to_as_of(
        pd.Series([1200.0], index=idx), pd.Series([pd.Timestamp("2024-05-02")], index=idx),
        [SplitEvent("NVDA", date(2024, 6, 10), 10.0)], date(2024, 6, 28),
    )
    assert out.iloc[0] == pytest.approx(120.0)
