"""S16: trust scores on corrected outcomes, and the prune_rule_v1 filter.

Fake engines (no database). The flag defaults are pinned, the legacy SQL is
unchanged with the flags off, NO_MOVE / INELIGIBLE count as neither, and a
pruned unit gets weight 0 in detect_convergence, get_trusted_sources and the
convergence scanner.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from intelligence import trust_scorer as ts


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)
        self.rowcount = len(self._rows)

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self, handler):
        self.handler = handler
        self.calls: list[tuple[str, dict]] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def commit(self):
        pass

    def rollback(self):
        pass

    def execute(self, statement, params=None):
        sql = str(statement)
        self.calls.append((sql, dict(params or {})))
        return _Rows(self.handler(sql, params or {}))


class _Engine:
    def __init__(self, handler):
        self.conn = _Conn(handler)

    def connect(self):
        return self.conn

    def begin(self):
        return self.conn


def _settings(monkeypatch, **flags):
    import config

    base = {"GRID_TRUST_USE_CORRECTED_OUTCOMES": False, "GRID_TRUST_PRUNE_NO_EDGE": False,
            "GRID_TRUST_PRUNE_LIST_PATH": ""}
    base.update(flags)
    monkeypatch.setattr(config, "settings", SimpleNamespace(**base))


TODAY = date(2026, 11, 6)


# ---------------------------------------------------------------- flag defaults


def test_flags_default_off():
    from config import Settings

    fields = Settings.model_fields
    assert fields["GRID_TRUST_USE_CORRECTED_OUTCOMES"].default is False
    assert fields["GRID_TRUST_PRUNE_NO_EDGE"].default is False
    assert fields["GRID_TRUST_PRUNE_LIST_PATH"].default == ""


def test_blank_env_flag_reads_off(monkeypatch):
    from config import Settings

    monkeypatch.setenv("GRID_TRUST_USE_CORRECTED_OUTCOMES", "")
    monkeypatch.setenv("GRID_TRUST_PRUNE_NO_EDGE", " ")
    s = Settings()
    assert s.GRID_TRUST_USE_CORRECTED_OUTCOMES is False and s.GRID_TRUST_PRUNE_NO_EDGE is False


# ------------------------------------------------------------ compute (legacy)


def test_legacy_basis_reads_signal_sources_outcome(monkeypatch):
    _settings(monkeypatch)

    def handler(sql, params):
        assert "signal_source_outcomes" not in sql
        if "FROM signal_sources" in sql and "outcome IN ('CORRECT', 'WRONG')" in sql:
            return [("insider", "a", "CORRECT", 2.0, TODAY, "NVDA"),
                    ("insider", "a", "WRONG", 0.3, TODAY, "NVDA")]
        return []

    result = ts.compute_trust_scores(_Engine(handler), today=TODAY)
    assert result["outcome_basis"] == ts.OUTCOME_BASIS_LEGACY
    (src,) = result["sources"]
    assert (src["hit_count"], src["miss_count"]) == (1, 1)
    assert "no_move_count" not in src  # legacy breakdown shape unchanged


# --------------------------------------------------------- compute (corrected)


def _corrected_handler(scored, neutral):
    def handler(sql, params):
        if "FROM signal_source_outcomes o" in sql and "IN ('CORRECT', 'WRONG')" in sql:
            assert params["version"] == "sig-eval-4-sessions"
            return scored
        if "FROM signal_source_outcomes o" in sql and "IN ('NO_MOVE', 'INELIGIBLE')" in sql:
            return neutral
        raise AssertionError(f"unexpected SQL: {sql}")
    return handler


def test_corrected_basis_no_move_and_ineligible_count_as_neither(monkeypatch):
    _settings(monkeypatch, GRID_TRUST_USE_CORRECTED_OUTCOMES=True)
    scored = [("insider", "a", "CORRECT", 2.5, TODAY, "NVDA"),
              ("insider", "a", "WRONG", -1.5, TODAY, "NVDA")]
    neutral = [("insider", "a", "NO_MOVE", 8, TODAY),
               ("insider", "a", "INELIGIBLE", 3, TODAY - timedelta(days=2))]
    result = ts.compute_trust_scores(_Engine(_corrected_handler(scored, neutral)), today=TODAY)
    (src,) = result["sources"]
    assert result["outcome_basis"] == "corrected:sig-eval-4-sessions"
    assert (src["hit_count"], src["miss_count"]) == (1, 1)  # 8 NO_MOVE + 3 INELIGIBLE are neither
    assert src["no_move_count"] == 8 and src["ineligible_count"] == 3
    assert src["win_rate"] == 0.5
    prior = ts._source_prior("insider")
    expected = (1 + prior * 5) / (2 + 5)
    assert src["trust_score"] == pytest.approx(round(expected, 4))


def test_corrected_basis_neutral_only_source_falls_back_to_prior(monkeypatch):
    _settings(monkeypatch, GRID_TRUST_USE_CORRECTED_OUTCOMES=True)
    neutral = [("quiverquant", "qq_offexchange:SPY", "INELIGIBLE", 344_000, TODAY),
               ("quiverquant", "qq_offexchange:QQQ", "INELIGIBLE", 10, TODAY)]
    result = ts.compute_trust_scores(_Engine(_corrected_handler([], neutral)), today=TODAY)
    (src,) = result["sources"]  # QuiverQuant acts collapse to one feed
    assert src["source_id"] == "qq_offexchange"
    assert src["hit_count"] == src["miss_count"] == 0
    assert src["ineligible_count"] == 344_010
    assert src["trust_score"] == pytest.approx(round(ts._source_prior("quiverquant"), 4))


def test_explicit_use_corrected_overrides_flag(monkeypatch):
    _settings(monkeypatch, GRID_TRUST_USE_CORRECTED_OUTCOMES=False)
    scored = [("social", "x", "CORRECT", 1.5, TODAY, "AAPL")]
    result = ts.compute_trust_scores(_Engine(_corrected_handler(scored, [])), use_corrected=True, today=TODAY)
    assert result["sources"][0]["hit_count"] == 1


def test_update_trust_scores_propagates_corrected_aggregates(monkeypatch):
    _settings(monkeypatch, GRID_TRUST_USE_CORRECTED_OUTCOMES=True)
    monkeypatch.setattr(ts, "_ensure_tables", lambda engine: None)
    updates = []
    base = _corrected_handler([("insider", "a", "CORRECT", 2.0, TODAY, "NVDA")],
                              [("insider", "a", "NO_MOVE", 4, TODAY)])

    def handler(sql, params):
        if "SELECT id FROM signal_sources" in sql:
            return [(1,)] if params["after_id"] == 0 else []
        if "UPDATE signal_sources" in sql:
            updates.append(params)
            return [1]
        return base(sql, params)

    engine = _Engine(handler)
    result = ts.update_trust_scores(engine)
    assert result["outcome_basis"] == "corrected:sig-eval-4-sessions"
    assert len(updates) == 1 and (updates[0]["hc"], updates[0]["mc"]) == (1, 0)
    assert result["sources"][0]["propagated_rows"] == 1


# ----------------------------------------------------- readers, corrected mode


def test_get_trusted_sources_corrected_filter(monkeypatch):
    _settings(monkeypatch, GRID_TRUST_USE_CORRECTED_OUTCOMES=True)
    monkeypatch.setattr(ts, "_ensure_tables", lambda engine: None)
    seen = {}

    def handler(sql, params):
        seen["sql"], seen["params"] = sql, params
        return [("insider", "a", 0.7, 7, 3, 84.0, TODAY, 10)]

    out = ts.get_trusted_sources(_Engine(handler))
    assert "signal_source_outcomes" in seen["sql"] and "outcome IN ('CORRECT', 'WRONG')" in seen["sql"]
    assert "WHERE outcome IN" not in seen["sql"]
    assert seen["params"]["version"] == "sig-eval-4-sessions"
    assert out[0].win_rate == 0.7


def test_get_trusted_sources_legacy_sql_unchanged(monkeypatch):
    _settings(monkeypatch)
    monkeypatch.setattr(ts, "_ensure_tables", lambda engine: None)
    seen = {}

    def handler(sql, params):
        seen["sql"], seen["params"] = sql, params
        return []

    ts.get_trusted_sources(_Engine(handler))
    assert "WHERE outcome IN ('CORRECT', 'WRONG')" in seen["sql"]
    assert "signal_source_outcomes" not in seen["sql"] and "version" not in seen["params"]


def _recent(source_type, source_id, signal_type="BUY", ticker="NVDA", trust=0.6):
    return (ticker, source_type, source_id, signal_type, TODAY, trust)


def test_detect_convergence_corrected_excludes_only_corrected_wrong(monkeypatch):
    _settings(monkeypatch, GRID_TRUST_USE_CORRECTED_OUTCOMES=True)
    seen = {}

    def handler(sql, params):
        seen["sql"], seen["params"] = sql, params
        return [_recent("insider", "a"), _recent("congressional", "b"), _recent("darkpool", "c")]

    events = ts.detect_convergence(_Engine(handler), ticker="NVDA")
    assert "NOT EXISTS" in seen["sql"] and "o.outcome = 'WRONG'" in seen["sql"]
    assert "'PENDING'" not in seen["sql"] and "s.ticker = :ticker" in seen["sql"]
    assert seen["params"]["ticker"] == "NVDA" and seen["params"]["version"] == "sig-eval-4-sessions"
    assert len(events) == 1 and events[0]["source_count"] == 3


def test_detect_convergence_legacy_sql_unchanged(monkeypatch):
    _settings(monkeypatch)
    seen = {}

    def handler(sql, params):
        seen["sql"] = sql
        return []

    ts.detect_convergence(_Engine(handler))
    assert "outcome IN ('PENDING', 'CORRECT')" in seen["sql"]
    assert "signal_source_outcomes" not in seen["sql"]


# ------------------------------------------------------------------ prune list


def _prune_doc(**over):
    doc = {
        "rule_version": "prune_rule_v1",
        "units": [
            {"family": "signal_source_type", "unit": "darkpool", "source_type": "darkpool", "verdict": "NO_EDGE"},
            {"family": "signal_feed", "unit": "insider / a", "source_type": "insider", "source_id": "a",
             "verdict": "ANTI_EDGE"},
            {"family": "signal_source_type", "unit": "social", "source_type": "social", "verdict": "EDGE"},
            {"family": "signal_source_type", "unit": "x", "source_type": "x", "verdict": "INSUFFICIENT"},
            {"family": "oracle_model", "unit": "m1", "verdict": "NO_EDGE"},
            {"family": "astrogrid_model", "unit": "grid-crypto-v1", "verdict": "NO_EDGE"},
        ],
    }
    doc.update(over)
    return doc


def test_parse_prune_list_keeps_only_no_edge_and_anti_edge():
    prune = ts.parse_prune_list(_prune_doc())
    assert prune.source_types == frozenset({"darkpool"})
    assert prune.feeds == frozenset({("insider", "a")})
    assert prune.oracle_models == frozenset({"m1"})
    assert prune.astrogrid_models == frozenset({"grid-crypto-v1"})
    assert prune.is_source_pruned("darkpool", "anything")
    assert prune.is_source_pruned("insider", "a") and not prune.is_source_pruned("insider", "b")
    assert not prune.is_source_pruned("social") and not prune.is_source_pruned("x")
    assert not prune.is_source_pruned(None)


@pytest.mark.parametrize("bad", [[], {"rule_version": "prune_rule_v2", "units": []}, {"rule_version": "prune_rule_v1"}])
def test_parse_prune_list_rejects_wrong_shape(bad):
    with pytest.raises((ValueError, TypeError)):
        ts.parse_prune_list(bad)


def test_active_prune_set_off_by_default_and_fails_open(monkeypatch, tmp_path):
    _settings(monkeypatch)
    assert ts.active_prune_set() is None
    _settings(monkeypatch, GRID_TRUST_PRUNE_NO_EDGE=True, GRID_TRUST_PRUNE_LIST_PATH="")
    assert ts.active_prune_set() is None
    _settings(monkeypatch, GRID_TRUST_PRUNE_NO_EDGE=True, GRID_TRUST_PRUNE_LIST_PATH=str(tmp_path / "missing.json"))
    assert ts.active_prune_set() is None
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    _settings(monkeypatch, GRID_TRUST_PRUNE_NO_EDGE=True, GRID_TRUST_PRUNE_LIST_PATH=str(bad))
    assert ts.active_prune_set() is None
    good = tmp_path / "prune.json"
    good.write_text(json.dumps(_prune_doc()), encoding="utf-8")
    _settings(monkeypatch, GRID_TRUST_PRUNE_NO_EDGE=True, GRID_TRUST_PRUNE_LIST_PATH=str(good))
    prune = ts.active_prune_set()
    assert prune is not None and prune.source_types == frozenset({"darkpool"})
    # Flag off again: the same valid file is ignored.
    _settings(monkeypatch, GRID_TRUST_PRUNE_NO_EDGE=False, GRID_TRUST_PRUNE_LIST_PATH=str(good))
    assert ts.active_prune_set() is None


@pytest.fixture()
def active_prune(monkeypatch, tmp_path):
    path = tmp_path / "prune.json"
    path.write_text(json.dumps(_prune_doc()), encoding="utf-8")
    _settings(monkeypatch, GRID_TRUST_PRUNE_NO_EDGE=True, GRID_TRUST_PRUNE_LIST_PATH=str(path))
    return path


def test_detect_convergence_pruned_sources_carry_no_weight(active_prune):
    rows = [_recent("insider", "b"), _recent("congressional", "c"), _recent("darkpool", "d"),
            _recent("insider", "a", ticker="AMD"), _recent("congressional", "e", ticker="AMD"),
            _recent("social", "f", ticker="AMD")]
    events = ts.detect_convergence(_Engine(lambda sql, params: rows))
    # NVDA had 3 types but darkpool is pruned -> 2 -> no event. AMD: insider/a is
    # a pruned feed, but insider/b is not there -> 2 types -> no event.
    assert events == []


def test_detect_convergence_unpruned_still_converges(active_prune):
    rows = [_recent("insider", "b"), _recent("congressional", "c"), _recent("social", "d")]
    (event,) = ts.detect_convergence(_Engine(lambda sql, params: rows))
    assert event["source_count"] == 3


def test_get_trusted_sources_drops_pruned(active_prune, monkeypatch):
    monkeypatch.setattr(ts, "_ensure_tables", lambda engine: None)
    rows = [("darkpool", "z", 0.9, 9, 1, 60.0, TODAY, 10),
            ("insider", "a", 0.8, 8, 2, 84.0, TODAY, 10),
            ("insider", "b", 0.7, 7, 3, 84.0, TODAY, 10)]
    out = ts.get_trusted_sources(_Engine(lambda sql, params: rows))
    assert [(s.source_type, s.source_id, s.rank) for s in out] == [("insider", "b", 1)]


def test_convergence_scanner_drops_pruned_streams(active_prune):
    from intelligence import signal_convergence_scanner as scs

    signals = [
        scs.StreamSignal("darkpool", 0.9, "bullish", 0.8, "2026-11-05  [darkpool] x"),
        scs.StreamSignal("insider", 0.9, "bullish", 0.8, "2026-11-05  [insider] y"),
        scs.StreamSignal("13f", 0.5, "bullish", 0.5, "2026-11-05  [13f] z"),
    ]
    kept, pruned = scs._drop_pruned_streams(signals)
    assert [s.stream_name for s in kept] == ["insider", "13f"] and pruned == 1


def test_convergence_scanner_keeps_everything_when_off(monkeypatch):
    from intelligence import signal_convergence_scanner as scs

    _settings(monkeypatch)
    signals = [scs.StreamSignal("darkpool", 0.9, "bullish", 0.8, "2026-11-05  [darkpool] x")]
    assert scs._drop_pruned_streams(signals) == (signals, 0)


def test_scan_convergence_counts_pruned_stream_as_missing(active_prune, monkeypatch):
    from intelligence import signal_convergence_scanner as scs

    def _extractor(name, direction):
        def _fn(engine, ticker, as_of, window_days, cache):
            return scs.StreamSignal(name, 1.0, direction, 0.8, f"2026-11-05  [{name}] e")
        return _fn

    extractors = [("darkpool", _extractor("darkpool", "bullish")),
                  ("insider", _extractor("insider", "bullish"))]
    extractors += [(f"none{i}", lambda *a, **k: None) for i in range(6)]
    monkeypatch.setattr(scs, "_STREAM_EXTRACTORS", extractors)
    report = scs.scan_convergence(object(), ticker="NVDA", as_of=TODAY, target_direction="bullish")
    assert report.n_active_streams == 1 and report.missing_stream_count == 7
    assert [s.stream_name for s in report.stream_signals] == ["insider"]
