"""PostgreSQL proof for "every miss gets a record; narration upgrades the newest" (2026-11-27).

Proves on a real server what the unit tests can only mock: the one-record-per-miss store under
concurrent writers (the spring bulk run left 76,710 predictions with 2 to 34 rows), the narration
UPDATE touching only rule-based rows, every reader windowing by the miss's own date (a record
written today for a May miss is not a recent lesson), the coverage counts, the nightly batch's two
phases, and the backfill CLI's keyset selection.
"""
from __future__ import annotations

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
                "scored_at timestamptz, pnl_pct double precision, flow_context jsonb DEFAULT '{}'::jsonb)"
            ))
            conn.execute(text(
                "CREATE TABLE options_recommendations (id integer PRIMARY KEY, outcome text, "
                "closed_at timestamptz, actual_return numeric)"
            ))
        pm_mod._ensure_tables(engine)   # the real trade_postmortems DDL
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def _miss(engine, pid, scored_at, pnl=-4.0):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO oracle_predictions (id, verdict, scored_at, pnl_pct) VALUES (:i, 'miss', :s, :p)"
        ), {"i": pid, "s": scored_at, "p": pnl})


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


def test_narration_rewrites_only_rule_based_rows(pg):
    pm_mod._store_postmortem(pg, _pm(ANALYSIS_RULES), trade_id=None, prediction_id="rules")
    pm_mod._store_postmortem(pg, _pm(ANALYSIS_LLM, what="already narrated"), trade_id=None, prediction_id="llm")
    # a pre-2026-11-27 row: no marker, the old placeholder text
    pm_mod._store_postmortem(pg, _pm(None, what=LEGACY_LLM_FALLBACK_TEXT), trade_id=None, prediction_id="legacy")
    ids = {r[1]: r[0] for r in _rows(pg)}
    narrated = _pm(ANALYSIS_LLM, what="FOMC")
    assert pm_mod._update_postmortem_narration(pg, ids["rules"], narrated) is True
    assert pm_mod._update_postmortem_narration(pg, ids["legacy"], narrated) is True
    assert pm_mod._update_postmortem_narration(pg, ids["llm"], narrated) is False
    assert pm_mod._update_postmortem_narration(pg, ids["rules"], narrated) is False   # once only
    by_pid = {r[1]: r for r in _rows(pg)}
    assert by_pid["rules"][3] == "FOMC" and by_pid["rules"][5] == ANALYSIS_LLM and float(by_pid["rules"][4]) == 0.7
    assert by_pid["legacy"][3] == "FOMC"
    assert by_pid["llm"][3] == "already narrated"


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


def _legacy_row(engine, pid, what, generated_at=None):
    """A row as the pre-2026-11-27 writer stored it: no analysis_method key; bypasses the store guard."""
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO trade_postmortems (prediction_id, ticker, outcome, failure_category, root_cause,"
            " what_we_missed, recommended_fix, full_analysis, confidence, generated_at)"
            " VALUES (:p, 'SPY', 'miss', 'wrong_signal', 'rc', :w, 'rc', '{\"root_cause\": \"rc\"}'::jsonb, 0.5,"
            " COALESCE(:g, now()))"
        ), {"p": pid, "w": what, "g": generated_at})


def test_narration_takes_only_the_newest_copy_of_a_duplicated_miss(pg, monkeypatch):
    narrated = _patch_writers(monkeypatch)
    _miss(pg, "dup", RECENT)
    for _ in range(3):   # the spring run's duplicates: three placeholder copies of one miss
        _legacy_row(pg, "dup", LEGACY_LLM_FALLBACK_TEXT)
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
    assert stats == {"selected": 6, "written": 6, "skipped": 0, "errors": 0}
    assert bf.run_backfill(pg, page_size=2)["selected"] == 0
    assert bf.count_unrecorded(pg)["predictions"]["by_month"] == {"no date": 1}   # reported, never selected
    limited = bf.count_unrecorded(pg, scored_before=OLD.date())
    assert limited["predictions"]["total"] == 0
