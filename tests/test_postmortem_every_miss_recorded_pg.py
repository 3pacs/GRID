"""PostgreSQL proof for "every miss gets a record; narration upgrades the newest" (2026-11-27).

Proves on a real server what the unit tests can only mock: the one-record-per-miss store under
concurrent writers (the spring bulk run left 76,710 predictions with 2 to 34 rows), the narration
UPDATE touching only rule-based rows, every reader windowing by the miss's own date (a record
written today for a May miss is not a recent lesson), the coverage counts, the nightly batch's two
phases, and the backfill CLI's keyset selection.
"""
from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

import intelligence.postmortem as pm_mod
from intelligence.postmortem import ANALYSIS_LLM, ANALYSIS_RULES, LEGACY_LLM_FALLBACK_TEXT, PostMortem

NOW = datetime.now(timezone.utc)
RECENT = NOW - timedelta(days=2)
OLD = datetime(2026, 5, 20, 21, 0, tzinfo=timezone.utc)


@pytest.fixture
def pg():
    db_url = os.environ.get("GRID_TEST_DB_URL")
    if not db_url:
        if os.environ.get("REQUIRE_POSTMORTEM_PG") == "1":
            pytest.fail("GRID_TEST_DB_URL is required for the postmortem PostgreSQL proof")
        pytest.skip("GRID_TEST_DB_URL is not configured")
    admin = create_engine(db_url, pool_pre_ping=True)
    schema = f"pmrec_{uuid.uuid4().hex[:12]}"
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(
        admin.url, pool_size=10, max_overflow=0,
        connect_args={"options": f"-csearch_path={schema}"},
    )
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "CREATE TABLE oracle_predictions (id text PRIMARY KEY, verdict text, "
                "scored_at timestamptz, pnl_pct double precision, flow_context jsonb DEFAULT '{}'::jsonb, "
                "ticker text, direction text, expiry date, model_name text)"
            ))
            conn.execute(text(
                "CREATE TABLE options_recommendations (id integer PRIMARY KEY, outcome text, "
                "closed_at timestamptz, actual_return numeric, signals jsonb, opposing_signals jsonb)"
            ))
        pm_mod._ensure_tables(engine)   # the real trade_postmortems DDL
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def _miss(engine, pid, scored_at, pnl=-4.0, ticker="SPY", direction="CALL", expiry="2026-11-20", model="m"):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO oracle_predictions (id, verdict, scored_at, pnl_pct, ticker, direction, expiry, model_name)"
            " VALUES (:i, 'miss', :s, :p, :t, :d, :e, :m)"
        ), {"i": pid, "s": scored_at, "p": pnl, "t": ticker, "d": direction, "e": expiry, "m": model})


def _trade(engine, tid, closed_at, ret=-0.4, outcome="LOSS"):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO options_recommendations (id, outcome, closed_at, actual_return) VALUES (:i, :o, :c, :r)"
        ), {"i": tid, "o": outcome, "c": closed_at, "r": ret})


def _pm(method=ANALYSIS_RULES, what="rule text", category="wrong_signal") -> PostMortem:
    return PostMortem(
        trade_id=0, ticker="SPY", direction="CALL", outcome="miss", actual_return=-0.04,
        data_at_decision={}, thesis_at_decision="t", sanity_results_at_decision={},
        what_actually_happened="w", price_path=[], failure_category=category, root_cause="rc",
        which_signals_were_wrong=["iv_skew"], which_signals_were_right=[], what_we_missed=what,
        recommended_fix="rc", confidence_in_analysis=None if method == ANALYSIS_RULES else 0.7,
        generated_at=NOW.isoformat(), analysis_method=method,
    )


def _rows(engine, where="TRUE"):
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT id, prediction_id, trade_id, what_we_missed, confidence, "
            "full_analysis->>'analysis_method' FROM trade_postmortems WHERE " + where + " ORDER BY id"
        )).fetchall()


# ── one record per miss ─────────────────────────────────────────────────────────────────────────────


def test_a_second_record_for_the_same_miss_is_refused(pg):
    assert pm_mod._store_postmortem(pg, _pm(), trade_id=None, prediction_id="p1") is True
    assert pm_mod._store_postmortem(pg, _pm(), trade_id=None, prediction_id="p1") is False
    assert pm_mod._store_postmortem(pg, _pm(), trade_id=5, prediction_id=None) is True
    assert pm_mod._store_postmortem(pg, _pm(), trade_id=5, prediction_id=None) is False
    rows = _rows(pg)
    assert len(rows) == 2
    assert all(r[5] == ANALYSIS_RULES and r[4] is None for r in rows)   # JSONB marker, NULL confidence


def test_concurrent_writers_for_one_miss_leave_exactly_one_record(pg):
    # Widen the check-then-insert window so the race is certain without the advisory lock: each
    # insert sleeps 0.2 s before it lands, so unlocked writers all pass the existence check first.
    # (Removing the lock from _store_postmortem makes this test fail with 8 rows.)
    with pg.begin() as conn:
        conn.execute(text(
            "CREATE FUNCTION slow_insert() RETURNS trigger LANGUAGE plpgsql AS "
            "$$ BEGIN PERFORM pg_sleep(0.2); RETURN NEW; END $$"
        ))
        conn.execute(text(
            "CREATE TRIGGER slow_insert BEFORE INSERT ON trade_postmortems "
            "FOR EACH ROW EXECUTE FUNCTION slow_insert()"
        ))
    barrier = threading.Barrier(8)
    results: list = []

    def _write():
        barrier.wait()
        results.append(pm_mod._store_postmortem(pg, _pm(), trade_id=None, prediction_id="race"))

    threads = [threading.Thread(target=_write) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results) == [False] * 7 + [True]
    assert len(_rows(pg, "prediction_id = 'race'")) == 1


# ── narration in place ──────────────────────────────────────────────────────────────────────────────


def _full(engine, row_id):
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT failure_category, root_cause, signals_wrong, generated_at, what_we_missed, confidence,"
            " full_analysis FROM trade_postmortems WHERE id = :i"), {"i": row_id}).fetchone()


def test_narration_keeps_the_rule_baseline_and_the_replaced_narrative(pg, monkeypatch):
    # Codex HOLD 6093994627: one stable classification per miss, the narration kept separately.
    _miss(pg, "rules", RECENT)
    pm_mod._store_postmortem(pg, _pm(ANALYSIS_RULES), trade_id=None, prediction_id="rules")
    row_id = _rows(pg)[0][0]
    before = _full(pg, row_id)
    calls: list = []
    monkeypatch.setattr(pm_mod, "_get_llm_postmortem",
                        lambda **k: calls.append(k) or {"what_we_missed": "FOMC", "recommended_fix": "wait", "confidence": 0.7})
    monkeypatch.setattr(pm_mod, "_utcnow", lambda: datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc))
    narrated = pm_mod.narrate_postmortem(pg, row_id)
    assert narrated is not None and calls[0]["category"] == "wrong_signal"
    after = _full(pg, row_id)
    assert after[:4] == before[:4]                     # category, root cause, signals, generated_at: untouched
    assert (after[4], float(after[5])) == ("FOMC", 0.7)
    full = after[6]
    assert full["analysis_method"] == ANALYSIS_LLM and full["classification_method"] == ANALYSIS_RULES
    assert full["failure_category"] == "wrong_signal" and full["root_cause"] == "rc"   # baseline keys kept
    # the narrative keys agree with the columns (hydration reads the keys first; partner review)
    assert (full["what_we_missed"], full["recommended_fix"], float(full["confidence_in_analysis"])) == ("FOMC", "wait", 0.7)
    assert full["rule_narrative"] == {"what_we_missed": "rule text", "recommended_fix": "rc", "confidence": None}
    assert full["narration"]["narrated_at"] == "2026-11-27T15:00:00+00:00"
    # Read back the way the Lessons synthesis reads it: the narration, labelled as such.
    loaded = [r for r in pm_mod.load_postmortems(pg, days=30) if r["prediction_id"] == "rules"]
    assert loaded[0]["analysis_method"] == ANALYSIS_LLM and loaded[0]["what_we_missed"] == "FOMC"
    hydrated = pm_mod.postmortems_from_records(loaded).postmortems[0]
    assert (hydrated.what_we_missed, hydrated.recommended_fix, hydrated.confidence_in_analysis,
            hydrated.analysis_method, hydrated.failure_category) == ("FOMC", "wait", 0.7, ANALYSIS_LLM, "wrong_signal")
    assert pm_mod.narrate_postmortem(pg, row_id) is None          # once only
    assert len(calls) == 1


def test_narration_update_touches_only_rule_based_and_placeholder_rows(pg):
    pm_mod._store_postmortem(pg, _pm(ANALYSIS_RULES), trade_id=None, prediction_id="rules")
    pm_mod._store_postmortem(pg, _pm(ANALYSIS_LLM, what="already narrated"), trade_id=None, prediction_id="llm")
    pm_mod._store_postmortem(pg, _pm(None, what=LEGACY_LLM_FALLBACK_TEXT), trade_id=None, prediction_id="legacy")
    ids = {r[1]: r[0] for r in _rows(pg)}
    narration = {"what_we_missed": "FOMC", "recommended_fix": "wait", "confidence": 0.7,
                 "narrated_at": NOW.isoformat(), "method": ANALYSIS_LLM}
    assert pm_mod._update_postmortem_narration(pg, ids["rules"], narration) is True
    assert pm_mod._update_postmortem_narration(pg, ids["legacy"], narration) is True
    assert pm_mod._update_postmortem_narration(pg, ids["llm"], narration) is False
    assert pm_mod._update_postmortem_narration(pg, ids["rules"], narration) is False   # once only
    by_pid = {r[1]: r for r in _rows(pg)}
    assert by_pid["rules"][3] == "FOMC" and by_pid["legacy"][3] == "FOMC"
    assert by_pid["llm"][3] == "already narrated"
    assert _full(pg, ids["legacy"])[6]["rule_narrative"]["what_we_missed"] == LEGACY_LLM_FALLBACK_TEXT


# ── readers window on the miss's own date ───────────────────────────────────────────────────────────


@pytest.fixture
def mixed_window(pg):
    """Records all written now: a recent miss, a May miss (backfilled), a recent trade, an old trade."""
    _miss(pg, "recent", RECENT)
    _miss(pg, "may", OLD)
    _trade(pg, 1, RECENT)
    _trade(pg, 2, OLD)
    pm_mod._store_postmortem(pg, _pm(), trade_id=None, prediction_id="recent")
    pm_mod._store_postmortem(pg, _pm(category="external_shock"), trade_id=None, prediction_id="may")
    pm_mod._store_postmortem(pg, _pm(), trade_id=1, prediction_id=None)
    pm_mod._store_postmortem(pg, _pm(category="external_shock"), trade_id=2, prediction_id=None)
    return pg


def test_the_postmortem_readers_exclude_a_record_of_an_old_miss(mixed_window, monkeypatch):
    pg = mixed_window
    loaded = pm_mod.load_postmortems(pg, days=30)
    assert {(r["prediction_id"], r["trade_id"]) for r in loaded} == {(None, 1), ("recent", None)}
    assert pm_mod.count_postmortems(pg, days=30) == 2
    assert len(pm_mod.load_postmortems_top_n(pg, n=5, days=30)) == 2
    assert len(pm_mod.load_postmortems(pg, days=30, ticker="SPY", category="wrong_signal")) == 2
    assert pm_mod.count_postmortems(pg, days=365) == 4


def test_trace_evolver_loads_only_recent_misses(mixed_window):
    from oracle.trace_evolver import TraceAnalyzer

    loaded = TraceAnalyzer(mixed_window)._load_postmortems(NOW - timedelta(days=30))
    assert sorted(str(r["prediction_id"]) for r in loaded) == ["None", "recent"]


def test_auto_improve_counts_each_logical_miss_once(pg, monkeypatch):
    # Codex HOLD 6093994627: 115 models making one call, or one prediction with 34 rows, are one miss.
    import scripts.auto_improve_from_postmortems as ai

    for i in range(4):   # four models, one call (SPY CALL 2026-11-20); one model saw it as a shock
        _miss(pg, f"clone{i}", RECENT, model=f"m{i}")
        pm_mod._store_postmortem(pg, _pm(category="external_shock" if i == 3 else "wrong_signal"),
                                 trade_id=None, prediction_id=f"clone{i}")
    _miss(pg, "other", RECENT, ticker="QQQ")
    _legacy_row(pg, "other", "x")
    _legacy_row(pg, "other", "x")      # a duplicate row of the same prediction
    with pg.connect() as conn:
        # each call weighs 1, split across its models: SPY 0.75 + 0.25, QQQ 1 (its duplicate row dropped)
        assert ai._failure_categories(conn) == {"wrong_signal": 1.75, "external_shock": 0.25}
        assert ai._anti_signal_override_clusters(conn)["total_all_time"] == 0
        assert ai._wrong_direction_tickers(conn) == []          # 1 miss per ticker, minimum 10
        monkeypatch.setattr(ai, "_MIN_OCCURRENCES_FOR_SIGNAL", 1)
        monkeypatch.setattr(ai, "_POSTMORTEM_CORPUS_CUTOFF", (RECENT - timedelta(days=1)).date().isoformat())
        ratios = {r["signal"]: r for r in ai._signal_ratios(conn)}
    assert ratios["iv_skew"]["wrong_count"] == 1.0     # the SPY call once (4 x 0.25), not four times


def test_a_ticker_of_trade_losses_only_is_not_listed_first(pg):
    # Partner review: SUM ... FILTER is NULL (not 0) for a ticker with no 'miss' rows, and DESC put it first.
    import dataclasses

    import scripts.auto_improve_from_postmortems as ai

    for i in range(10):
        _miss(pg, f"real{i}", RECENT, ticker="REAL", expiry=f"2026-11-{10 + i:02d}")   # ten distinct calls
        pm_mod._store_postmortem(pg, dataclasses.replace(_pm(), ticker="REAL"), trade_id=None, prediction_id=f"real{i}")
        _trade(pg, 100 + i, RECENT)
        pm_mod._store_postmortem(pg, dataclasses.replace(_pm(), ticker="LOSSONLY", outcome="LOSS"),
                                 trade_id=100 + i, prediction_id=None)
    with pg.connect() as conn:
        clusters = ai._wrong_direction_tickers(conn)
    assert [c["ticker"] for c in clusters] == ["REAL", "LOSSONLY"]
    assert (clusters[0]["fail_rate"], clusters[1]["wrong_n"], clusters[1]["fail_rate"]) == (1.0, 0, 0.0)


def test_auto_improve_queries_run_on_postgresql_and_count_only_recent_misses(mixed_window, monkeypatch):
    import scripts.auto_improve_from_postmortems as ai

    with mixed_window.connect() as conn:
        assert ai._failure_categories(conn) == {"wrong_signal": 2}   # the two May records are not "30 days"
        assert ai._anti_signal_override_clusters(conn)["last_7d"] == 0
        ai._wrong_direction_tickers(conn)
        monkeypatch.setattr(ai, "_POSTMORTEM_CORPUS_CUTOFF", RECENT.date().isoformat())
        ai._signal_ratios(conn)


def test_coverage_counts_recorded_narrated_and_not_analysed_in_the_window(pg):
    _miss(pg, "a", RECENT)
    _miss(pg, "b", RECENT)
    _miss(pg, "c", RECENT)
    _miss(pg, "nan", RECENT, pnl=float("nan"))    # not eligible: the writer refuses it
    _miss(pg, "may", OLD)                          # outside the window
    _trade(pg, 1, RECENT)
    _miss(pg, "d", RECENT)
    pm_mod._store_postmortem(pg, _pm(ANALYSIS_RULES), trade_id=None, prediction_id="a")
    pm_mod._store_postmortem(pg, _pm(ANALYSIS_LLM, what="x"), trade_id=None, prediction_id="b")
    _legacy_row(pg, "d", "LEVER: a narrated row written before the marker existed")
    cov = pm_mod.postmortem_coverage(pg, days=30)
    # "d" has no analysis_method key at all: it is narrated, not dropped by a NULL (partner review B)
    assert cov["predictions"] == {"eligible": 4, "recorded": 3, "narrated": 2, "not_analysed": 1}
    assert cov["trades"] == {"eligible": 1, "recorded": 0, "narrated": 0, "not_analysed": 1}
    assert (cov["eligible"], cov["recorded"], cov["narrated"], cov["not_analysed"]) == (5, 3, 2, 2)


def _legacy_row(engine, pid, what, generated_at=None, unit=None):
    """A row as the pre-2026-11-27 writer stored it: no analysis_method key; bypasses the store guard.
    ``unit``: the return-unit marker rows carry from 2026-11-26 (None: an older row)."""
    full = {"root_cause": "rc", "ticker": "SPY", "direction": "CALL", "outcome": "miss",
            "actual_return": -4.0 if unit is None else -0.04, "failure_category": "wrong_signal",
            "what_we_missed": what, "recommended_fix": "rc"}
    if unit is not None:
        full["actual_return_unit"] = unit
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO trade_postmortems (prediction_id, ticker, outcome, failure_category, root_cause,"
            " what_we_missed, recommended_fix, full_analysis, confidence, generated_at)"
            " VALUES (:p, 'SPY', 'miss', 'wrong_signal', 'rc', :w, 'rc', CAST(:f AS jsonb), 0.5,"
            " COALESCE(:g, now()))"
        ), {"p": pid, "w": what, "g": generated_at, "f": json.dumps(full)})


def test_narration_takes_only_the_newest_copy_of_a_duplicated_miss(pg, monkeypatch):
    narrated = _patch_writers(monkeypatch)
    _miss(pg, "dup", RECENT)
    for _ in range(3):   # duplicates: three placeholder copies of one miss, written with the unit marker
        _legacy_row(pg, "dup", LEGACY_LLM_FALLBACK_TEXT, unit="fraction")
    _miss(pg, "old-unit", RECENT)
    _legacy_row(pg, "old-unit", LEGACY_LLM_FALLBACK_TEXT)   # no marker: prose withheld, never narrated
    newest = max(r[0] for r in _rows(pg, "prediction_id = 'dup'"))
    pm_mod.batch_postmortem(pg, days=30, limit=10)
    assert narrated == [newest]


# ── the nightly batch on PostgreSQL ─────────────────────────────────────────────────────────────────


def _patch_writers(monkeypatch):
    def _record(engine, ident, *, narrate=True, update_row_id=None):
        is_trade = isinstance(ident, int)
        ok = pm_mod._store_postmortem(
            engine, _pm(), trade_id=ident if is_trade else None, prediction_id=None if is_trade else ident)
        return _pm() if ok else None

    monkeypatch.setattr(pm_mod, "generate_prediction_postmortem", _record)
    monkeypatch.setattr(pm_mod, "generate_postmortem", _record)
    narrated: list = []
    monkeypatch.setattr(pm_mod, "narrate_postmortem", lambda _e, row_id: narrated.append(row_id) or _pm(ANALYSIS_LLM))
    return narrated


def test_the_batch_records_the_window_then_narrates_newest_first_and_leaves_old_misses_alone(pg, monkeypatch):
    narrated = _patch_writers(monkeypatch)
    for i in range(5):
        _miss(pg, f"p{i}", NOW - timedelta(days=i + 1))   # p0 newest
    _miss(pg, "may", OLD)
    _trade(pg, 9, NOW - timedelta(hours=30))
    result = pm_mod.batch_postmortem(pg, days=30, limit=3)
    recorded = {r[1] or r[2] for r in _rows(pg)}
    assert recorded == {"p0", "p1", "p2", "p3", "p4", 9}     # the May miss is the backfill's, not the batch's
    ids = {(r[1] or r[2]): r[0] for r in _rows(pg)}
    assert narrated == [ids["p0"], ids[9], ids["p1"]]        # newest miss first, three of them
    assert len(result) == 9
    narrated.clear()
    pm_mod.batch_postmortem(pg, days=30, limit=3)
    assert len(_rows(pg)) == 6                               # nothing recorded twice


# ── the backfill CLI on PostgreSQL ──────────────────────────────────────────────────────────────────


def test_the_backfill_records_every_old_miss_once_across_pages(pg, monkeypatch):
    import scripts.postmortem_backfill as bf

    _patch_writers(monkeypatch)
    for i in range(5):
        _miss(pg, f"old{i}", OLD - timedelta(days=i))
    _miss(pg, "done", OLD)
    _miss(pg, "nan", OLD, pnl=float("nan"))
    _miss(pg, "undated", None)
    _trade(pg, 3, OLD)
    _trade(pg, 4, OLD, outcome="WIN")
    pm_mod._store_postmortem(pg, _pm(), trade_id=None, prediction_id="done")
    counts = bf.count_unrecorded(pg)
    assert counts["predictions"]["by_month"] == {"2026-05": 5, "no date": 1}
    assert counts["trades"]["total"] == 1
    assert list(bf.iter_unrecorded(pg, "predictions", page_size=2)) == ["old0", "old1", "old2", "old3", "old4"]
    stats = bf.run_backfill(pg, page_size=2)
    assert stats == {"selected": 6, "written": 6, "skipped": 0, "errors": 0, "stopped": 0}
    assert bf.run_backfill(pg, page_size=2)["selected"] == 0
    assert bf.count_unrecorded(pg)["predictions"]["by_month"] == {"no date": 1}   # reported, never selected
    limited = bf.count_unrecorded(pg, scored_before=OLD.date())
    assert limited["predictions"]["total"] == 0
