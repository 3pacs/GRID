"""Sleuth intersection leads and their declared outcomes (2026-11-07 rebuild).

* A lead needs two independent evidence families agreeing on one ticker's
  direction; one source alone is never a lead.
* Anomalies are robust (median/MAD on returns) and refuse step functions:
  the old generator called a flat Selic rate's 0.0551 -> 0.0543 step
  "43 trillion sigma".
* Evidence is what was recorded by ``as_of``.
* Every investigation ends in ``hypothesis_declared`` (fixed criteria the v2
  trial scorer can plan) or ``dead_end``; the LLM cannot change the test.
* Chat questions are stored with ``origin='chat'`` and never queued.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from intelligence import sleuth as sleuth_mod
from intelligence import sleuth_intersections as si
from intelligence.hypothesis_trials import plan_trial

AS_OF = datetime(2026, 11, 7, 2, 0, tzinfo=timezone.utc)
END = AS_OF.date()
START = END - timedelta(days=si.WINDOW_DAYS)


def _obs(family, ticker="NVDA", direction="up", d=END, **cite):
    return si.Observation(family=family, ticker=ticker, direction=direction,
                          obs_date=d, citation={"table": "signal_sources", **cite})


# ── classify_signal ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(("source", "stype", "value", "expected"), [
    ("options_flow", "UNUSUAL_OPTIONS", {"direction": "CALL"}, ("options_flow", "up")),
    ("options_flow", "UNUSUAL_OPTIONS", json.dumps({"direction": "PUT"}), ("options_flow", "down")),
    ("options_flow", "UNUSUAL_OPTIONS", {"direction": "STRADDLE"}, None),
    ("insider", "BUY", {}, ("insider", "up")),
    ("insider", "UNUSUAL_SELL", {}, ("insider", "down")),
    ("insider", "CLUSTER_BUY", {}, None),  # summary of buys that already vote
    ("quiverquant:insider", "insider_buy", {"TransactionCode": "P"}, ("insider", "up")),
    ("congressional", "SELL", {}, ("congress", "down")),
    ("gov_contract", "CONTRACT_AWARD", {"amount": 1e6}, ("gov_contract", "up")),
    ("quiverquant:wsb", "wsb_bearish", {}, None),  # frozen snapshot: no vote
    ("quiverquant:wsb", "wsb_bullish", {}, None),
    ("quiverquant:wsb", "wsb_neutral", {}, None),
    ("smart_money", "NET_POSITION_DELTA", {"direction": "BULLISH"}, ("retail_sentiment", "up")),
    ("quiverquant:lobbying", "lobbying", {}, None),
    ("quiverquant:offexchange", "off_exchange", {"DPI": 0.6}, None),
    ("crucix_idea", "trade_idea_long", {}, None),
])
def test_classify_signal(source, stype, value, expected):
    assert si.classify_signal(source, stype, value) == expected


def test_frozen_sources_are_not_queried_or_classified():
    assert "quiverquant:wsb" in si._FROZEN_SOURCES
    assert not si._FROZEN_SOURCES & set(si._FAMILY_BY_SOURCE)


# ── robust anomalies ────────────────────────────────────────────────────────


def test_step_function_is_not_an_anomaly():
    # The Selic case: a rate held flat for months, then one small step.
    history = [0.0] * 300 + [0.0008]
    assert si.robust_z(history, -0.0008) is None


def test_constant_history_has_no_z():
    assert si.robust_z([0.01] * 300, 0.5) is None


def test_robust_z_is_capped():
    history = [(-1) ** i * 0.01 * (1 + i % 7) for i in range(300)]
    assert si.robust_z(history, 100.0) == si.ROBUST_Z_CAP
    assert abs(si.robust_z(history, 0.0)) < 1


def test_implausible_jump_is_a_data_error_not_a_move():
    days = [date(2026, 1, 1) + timedelta(days=i) for i in range(3)]
    closes = {days[0]: 100.0, days[1]: 10.0, days[2]: 10.5}
    market = {d: 500.0 for d in days}
    rets = si.excess_log_returns(closes, market)
    assert [d for d, _ in rets] == [days[2]]


def _returns(n_hist, window_moves):
    out = []
    d0 = START - timedelta(days=n_hist)
    for i in range(n_hist):
        out.append((d0 + timedelta(days=i), (-1) ** i * 0.004 * (1 + i % 5)))
    for i, r in enumerate(window_moves):
        out.append((START + timedelta(days=i), r))
    return out


def test_price_observation_flags_a_large_window_move_with_its_sign():
    obs = si.price_observation("NVDA", _returns(300, [0.001, -0.09, 0.002]), START, END)
    assert obs is not None and obs.family == "price_move" and obs.direction == "down"
    assert obs.magnitude <= -si.PRICE_Z_THRESHOLD
    assert obs.citation["series_id"] == "YF:NVDA:close"


def test_price_observation_ignores_ordinary_days_and_short_history():
    assert si.price_observation("NVDA", _returns(300, [0.001, 0.003]), START, END) is None
    assert si.price_observation("NVDA", _returns(50, [0.2]), START, END) is None


# ── intersections ───────────────────────────────────────────────────────────


def test_one_family_is_never_a_lead_however_many_rows():
    obs = [_obs("insider", id=i) for i in range(10)]
    assert si.find_intersections(obs, END) == []


def test_two_agreeing_families_make_one_lead():
    obs = [_obs("insider", id=1), _obs("options_flow", id=2), _obs("insider", id=3)]
    (lead,) = si.find_intersections(obs, END)
    assert (lead.ticker, lead.direction, lead.families) == ("NVDA", "up", ["insider", "options_flow"])
    assert lead.lead_id == si.find_intersections(obs, END)[0].lead_id  # deterministic
    assert lead.lead_id.startswith("LEAD-INTX-")
    ev = lead.evidence()
    assert [(e["family"], e["up"]) for e in ev if e["role"] == "agrees"] == [("insider", 2), ("options_flow", 1)]
    assert sorted(e["id"] for e in ev if e["role"] == "cites") == [1, 2, 3]


def test_two_sided_flow_casts_no_vote():
    # Mega-cap options flow runs both ways; a family that is not >= 2/3 one
    # way is split and supports nothing.
    obs = [_obs("options_flow", direction="up", id=i) for i in range(5)]
    obs += [_obs("options_flow", direction="down", id=10 + i) for i in range(4)]
    obs += [_obs("insider", direction="down", id=20)]
    assert si.find_intersections(obs, END) == []
    (vote,) = [v for v in si.family_votes(obs) if v.family == "options_flow"]
    assert vote.direction is None and (vote.up, vote.down) == (5, 4)


def test_equal_dissent_cancels_the_lead():
    obs = [_obs("insider"), _obs("options_flow"),
           _obs("congress", direction="down"), _obs("retail_sentiment", direction="down")]
    assert si.find_intersections(obs, END) == []


def test_minority_dissent_is_kept_as_evidence():
    obs = [_obs("insider"), _obs("options_flow"), _obs("gov_contract"),
           _obs("retail_sentiment", direction="down")]
    (lead,) = si.find_intersections(obs, END)
    assert [e["family"] for e in lead.evidence() if e["role"] == "dissents"] == ["retail_sentiment"]


def test_citations_are_capped_per_family():
    obs = [_obs("options_flow", id=i) for i in range(40)] + [_obs("insider", id=99)]
    (lead,) = si.find_intersections(obs, END)
    cites = [e for e in lead.evidence() if e["role"] == "cites"]
    assert sum(e["family"] == "options_flow" for e in cites) == si.MAX_CITATIONS_PER_FAMILY
    assert next(e for e in lead.evidence() if e["role"] == "agrees")["up"] in (1, 40)


def test_price_candidates_need_a_family_vote_and_a_testable_ticker():
    obs = [_obs("insider", "AAA"), _obs("options_flow", "AAA"),
           _obs("insider", "BBB"),
           _obs("options_flow", "CCC", direction="up"), _obs("options_flow", "CCC", direction="down"),
           _obs("insider", "BTC-USD")]
    assert si.price_candidates(obs) == ["AAA", "BBB"]


# ── data access is point-in-time and bound ──────────────────────────────────


def test_signal_query_reads_only_rows_recorded_by_as_of():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        (7, "insider", "CEO", "nvda", END, "BUY", {}),
        (8, "quiverquant:lobbying", "x", "NVDA", END, "lobbying", {}),
    ]
    obs = si.collect_signal_observations(conn, START, END, AS_OF)
    sql, params = str(conn.execute.call_args.args[0]), conn.execute.call_args.args[1]
    assert "created_at <= :as_of_ts" in sql and "signal_date <= :end" in sql
    assert params["as_of_ts"] == AS_OF and params["end"] == END
    assert [(o.family, o.ticker, o.citation["id"]) for o in obs] == [("insider", "NVDA", 7)]


# ── generation ──────────────────────────────────────────────────────────────


@pytest.fixture
def no_tables(monkeypatch):
    monkeypatch.setattr(sleuth_mod, "_tables_ensured", True)


def _engine_with_recent(recent_keys):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    rows = [(json.dumps([{"role": "agrees", "ticker": t, "direction": d}]),) for t, d in recent_keys]
    conn.execute.return_value.fetchall.return_value = rows
    return engine


def test_generate_saves_new_leads_once_per_cooldown_and_caps(monkeypatch, no_tables):
    signals = [_obs("insider", t, id=i) for i, t in enumerate(["AAA", "BBB", "CCC"])]
    signals += [_obs("options_flow", t, id=10 + i) for i, t in enumerate(["AAA", "BBB", "CCC"])]
    monkeypatch.setattr(si, "collect_signal_observations", lambda *a: signals)
    monkeypatch.setattr(si, "collect_price_observations", lambda *a: [])
    saved = []
    monkeypatch.setattr(sleuth_mod.Sleuth, "_save_lead", lambda self, lead: saved.append(lead))

    ids = si.generate_intersection_leads(_engine_with_recent([("BBB", "up")]), AS_OF, max_new=1)
    assert len(ids) == 1 and len(saved) == 1
    assert saved[0].origin == si.ORIGIN and saved[0].status == "new"
    assert "AAA" in saved[0].question  # BBB is in cooldown; the cap stops at one


# ── baseline: every lead declared without review (generator v2) ─────────────


def _baseline_db(leads):
    from sqlalchemy import create_engine, event

    engine = create_engine("sqlite://")

    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def _jsonb_as_text(conn, cursor, statement, params, context, executemany):
        # SQLite gives a JSONB cast numeric affinity; Postgres stores the JSON.
        return statement.replace("AS JSONB", "AS TEXT"), params

    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE investigation_leads (id TEXT, question TEXT, "
                             "evidence TEXT, origin TEXT, created_at TIMESTAMP)")
        conn.exec_driver_sql("CREATE TABLE discovered_hypotheses (id TEXT PRIMARY KEY, thesis TEXT, "
                             "pattern_type TEXT, evidence TEXT, test_criteria TEXT, invalidation TEXT, "
                             "confidence REAL, status TEXT, role TEXT, pair_id TEXT)")
        for lead_id, ticker, direction, origin, ts in leads:
            ev = [{"role": "agrees", "ticker": ticker, "direction": direction, "family": f}
                  for f in ("insider", "options_flow")]
            conn.exec_driver_sql("INSERT INTO investigation_leads VALUES (?, ?, ?, ?, ?)",
                                 (lead_id, f"sources point {direction} on {ticker}",
                                  json.dumps(ev), origin, ts))
    return engine


def _hyps(engine):
    with engine.connect() as conn:
        return {r[0]: r for r in conn.exec_driver_sql(
            "SELECT id, pattern_type, test_criteria, status, role FROM discovered_hypotheses")}


def test_every_recent_lead_gets_the_sleuth_test_without_review():
    since = "2026-11-06 00:00:00"
    engine = _baseline_db([
        ("LEAD-INTX-aaa", "NVDA", "up", si.ORIGIN, "2026-11-07 02:00:00"),
        ("LEAD-INTX-bbb", "AAPL", "down", si.ORIGIN, "2026-11-06 12:00:00"),
        ("LEAD-INTX-old", "MSFT", "up", si.ORIGIN, "2026-11-01 02:00:00"),     # before since
        ("LEAD-INTX-btc", "BTC-USD", "up", si.ORIGIN, "2026-11-07 02:00:00"),  # not testable
        ("LEAD-CHAT-1", "TSLA", "up", "chat", "2026-11-07 02:00:00"),          # not a lead
    ])
    ids = si.declare_missing_baselines(engine, since=since)
    assert ids == ["hyp_base_bbb", "hyp_base_aaa"]
    rows = _hyps(engine)
    assert set(rows) == {"hyp_base_aaa", "hyp_base_bbb"}
    _, ptype, criteria, status, role = rows["hyp_base_aaa"]
    criteria = json.loads(criteria)
    assert (ptype, status, role) == (si.BASELINE_PATTERN, "active", "thesis")
    # The same test a Sleuth declaration gets; only the source differs.
    assert criteria == {"ticker": "NVDA", "window_days": si.HYPOTHESIS_WINDOW_DAYS,
                        "min_move_pct": si.HYPOTHESIS_MIN_MOVE_PCT, "expected_direction": "up",
                        "source": si.BASELINE_PATTERN, "lead_id": "LEAD-INTX-aaa"}
    plan = plan_trial("hyp_base_aaa", ptype, criteria, AS_OF)
    assert plan.test_kind == "ticker_move" and plan.status is None
    # One baseline per lead, however often it runs.
    assert si.declare_missing_baselines(engine, since=since) == []
    assert len(_hyps(engine)) == 2


def test_ambiguous_evidence_declares_no_baseline():
    conn = MagicMock()
    ev = [{"role": "agrees", "ticker": "NVDA", "direction": "up"},
          {"role": "agrees", "ticker": "NVDA", "direction": "down"}]
    assert si.declare_baseline(conn, "LEAD-INTX-x", "q", ev) is None
    conn.execute.assert_not_called()


def test_a_failed_baseline_still_queues_the_investigations(monkeypatch, no_tables):
    from sqlalchemy.exc import OperationalError

    signals = [_obs("insider", "AAA", id=1), _obs("options_flow", "AAA", id=2)]
    monkeypatch.setattr(si, "collect_signal_observations", lambda *a: signals)
    monkeypatch.setattr(si, "collect_price_observations", lambda *a: [])
    monkeypatch.setattr(sleuth_mod.Sleuth, "_save_lead", lambda self, lead: None)

    def boom(engine, since):
        raise OperationalError("INSERT", {}, Exception("db down"))

    monkeypatch.setattr(si, "declare_missing_baselines", boom)
    assert len(si.generate_intersection_leads(_engine_with_recent([]), AS_OF)) == 1


def test_generate_declares_baselines_for_the_catchup_window(monkeypatch, no_tables):
    monkeypatch.setattr(si, "collect_signal_observations", lambda *a: [])
    monkeypatch.setattr(si, "collect_price_observations", lambda *a: [])
    seen = []
    monkeypatch.setattr(si, "declare_missing_baselines",
                        lambda engine, since: seen.append(since) or ["hyp_base_x"])
    si.generate_intersection_leads(_engine_with_recent([]), AS_OF)
    assert seen == [AS_OF - timedelta(days=si.BASELINE_CATCHUP_DAYS)]


def test_generator_v2_runs_no_legacy_detector(monkeypatch):
    from intelligence import hypothesis_engine as he

    def legacy(*a, **k):
        raise AssertionError("legacy detector ran")

    monkeypatch.setattr(he, "ensure_tables", lambda engine: None)
    monkeypatch.setattr(he.TemporalPatternDetector, "discover_patterns", legacy)
    monkeypatch.setattr(he.AnomalyHunter, "scan_convergence", legacy)
    monkeypatch.setattr(he, "score_due_active_hypotheses", legacy)
    monkeypatch.setattr(si, "declare_missing_baselines", lambda engine, since: ["hyp_base_x"])
    out = he.HypothesisGenerator(MagicMock()).auto_discover()
    assert out == [{"id": "hyp_base_x", "pattern_type": si.BASELINE_PATTERN}]


# ── investigation: declared outcomes ────────────────────────────────────────


def _lead(ticker="NVDA", origin=si.ORIGIN, status="new", direction="up"):
    lead = sleuth_mod.Lead(
        id="LEAD-INTX-abc123def456", question="2 independent sources point up on NVDA",
        category="connection_found", priority=0.8, origin=origin, status=status,
        evidence=[{"role": "agrees", "ticker": ticker, "direction": direction, "family": "insider"},
                  {"role": "agrees", "ticker": ticker, "direction": direction, "family": "options_flow"}],
    )
    return lead


@pytest.fixture
def leads(monkeypatch, no_tables):
    store: dict[str, sleuth_mod.Lead] = {}
    monkeypatch.setattr(sleuth_mod.Sleuth, "_load_lead", lambda self, i: store.get(i))
    monkeypatch.setattr(sleuth_mod.Sleuth, "_save_lead", lambda self, lead: store.__setitem__(lead.id, lead))
    return store


def _llm(answer):
    return SimpleNamespace(chat=lambda *a, **k: answer)


def _engine_recording():
    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value
    return engine, conn


def test_declare_inserts_fixed_criteria_the_v2_scorer_can_plan(leads):
    leads["LEAD-INTX-abc123def456"] = _lead()
    engine, conn = _engine_recording()
    # The LLM tries to change the test; it cannot.
    answer = ('Sure. {"verdict": "declare", "reason": "insider buying alongside call sweeps", '
              '"common_cause": "product cycle", "invalidation": "no move", '
              '"direction": "down", "window_days": 90}')
    out = si.investigate_intersection_lead(engine, "LEAD-INTX-abc123def456", llm=_llm(answer))
    assert out["outcome"] == si.OUTCOME_DECLARED and out["hypothesis_id"] == "hyp_sleuth_abc123def456"

    params = conn.execute.call_args.args[1]
    criteria = json.loads(params["criteria"])
    assert criteria == {"ticker": "NVDA", "window_days": si.HYPOTHESIS_WINDOW_DAYS,
                        "min_move_pct": si.HYPOTHESIS_MIN_MOVE_PCT, "expected_direction": "up",
                        "source": "sleuth", "lead_id": "LEAD-INTX-abc123def456"}
    assert "ON CONFLICT (id) DO NOTHING" in str(conn.execute.call_args.args[0])
    plan = plan_trial(params["id"], params["ptype"], criteria, AS_OF)
    assert plan.test_kind == "ticker_move" and plan.params["direction"] == "up"
    assert plan.status != "untestable"

    saved = leads["LEAD-INTX-abc123def456"]
    assert (saved.status, saved.outcome, saved.declared_hypothesis_id) == (
        "resolved", si.OUTCOME_DECLARED, "hyp_sleuth_abc123def456")


def test_dead_end_keeps_its_reason_and_declares_nothing(leads):
    leads["LEAD-INTX-abc123def456"] = _lead()
    engine, conn = _engine_recording()
    answer = '{"verdict": "dead_end", "reason": "same 8-K reported twice", "common_cause": "", "invalidation": ""}'
    out = si.investigate_intersection_lead(engine, "LEAD-INTX-abc123def456", llm=_llm(answer))
    assert out["outcome"] == si.OUTCOME_DEAD_END and out["reason"] == "same 8-K reported twice"
    conn.execute.assert_not_called()
    assert leads["LEAD-INTX-abc123def456"].status == "dead_end"


@pytest.mark.parametrize("answer", [None, "", "further investigation needed",
                                    '{"verdict": "maybe"}'])
def test_no_usable_answer_raises_and_the_lead_stays_new(leads, answer):
    leads["LEAD-INTX-abc123def456"] = _lead()
    engine, conn = _engine_recording()
    with pytest.raises(RuntimeError):
        si.investigate_intersection_lead(engine, "LEAD-INTX-abc123def456", llm=_llm(answer))
    assert leads["LEAD-INTX-abc123def456"].status == "new"
    conn.execute.assert_not_called()


def test_unsupported_instrument_is_a_dead_end_without_the_llm(leads):
    leads["LEAD-INTX-abc123def456"] = _lead(ticker="BTC-USD")
    engine, _ = _engine_recording()
    llm = MagicMock()
    out = si.investigate_intersection_lead(engine, "LEAD-INTX-abc123def456", llm=llm)
    assert (out["outcome"], out["reason"]) == (si.OUTCOME_DEAD_END, "UNSUPPORTED_INSTRUMENT")
    llm.chat.assert_not_called()


@pytest.mark.parametrize("kwargs", [{"origin": "chat"}, {"origin": "legacy"}, {"status": "resolved"}])
def test_only_open_intersection_leads_are_investigated(leads, kwargs):
    leads["LEAD-INTX-abc123def456"] = _lead(**kwargs)
    engine, _ = _engine_recording()
    llm = MagicMock()
    out = si.investigate_intersection_lead(engine, "LEAD-INTX-abc123def456", llm=llm)
    assert out["skipped"] is True
    llm.chat.assert_not_called()


# ── wiring ──────────────────────────────────────────────────────────────────


def test_goal_worker_routes_sleuth_investigate(monkeypatch):
    from scripts import goal_worker

    seen = []
    monkeypatch.setattr(si, "investigate_intersection_lead",
                        lambda engine, lead_id: seen.append(lead_id) or {"lead_id": lead_id})
    goal = SimpleNamespace(target_id="LEAD-INTX-1", allow_cloud=False)
    assert goal_worker.HANDLERS["sleuth_investigate"](MagicMock(), goal) == {"lead_id": "LEAD-INTX-1"}
    assert seen == ["LEAD-INTX-1"]


def test_hermes_sleuth_dispatch_is_enabled_and_queues_one_goal_per_lead(monkeypatch):
    from intelligence import goal_queue
    from scripts import hermes_operator as ho

    assert "sleuth_dispatch" in ho.DAILY_INTEL_INITIAL_ALLOWLIST
    assert "sleuth_dispatch" not in ho.DAILY_INTEL_HOLD_REASONS
    monkeypatch.setattr(si, "generate_intersection_leads", lambda engine, as_of: ["L1", "L2"])
    queued = []
    monkeypatch.setattr(goal_queue, "enqueue_goal",
                        lambda engine, **kw: queued.append(kw) or len(queued))
    results: dict = {}
    ho._daily_intel_sleuth_dispatch(MagicMock(), MagicMock(), AS_OF, results)
    assert results["sleuth_dispatch"] == {"leads": 2, "queued": 2}
    assert [q["target_id"] for q in queued] == ["L1", "L2"]
    assert all(q["goal_type"] == "sleuth_investigate" and q["allow_cloud"] is False for q in queued)


def test_chat_questions_are_stored_as_chat_leads():
    source = (Path(__file__).resolve().parents[1] / "api" / "routers" / "chat.py").read_text()
    assert source.count('origin="chat"') == 2


# ── Research-only canonical evidence candidate; never alters v2 ─────────────


def _event_version(**overrides):
    row = dict(id=1, channel="form4", dedup_key="NVDA|TEST OWNER|2026-10-01|P|10",
               content_hash="a" * 64, actor_id="TEST OWNER", actor_id_basis="normalized_name",
               entity_ticker="NVDA", event_time=AS_OF - timedelta(days=30),
               known_at=AS_OF - timedelta(days=1), known_at_basis="first_seen",
               clock_contract="people-events-post-commit-receipt-v1", end_history_valid=True,
               receipt_id="SYNTHETIC-POST-COMMIT-CAPTURE", receipt_sha256="c" * 64,
               direction="buy", transaction_code="P", echo_of=None,
               superseded_at=None, retracted_at=None)
    row.update(overrides)
    return row


def _qualify(*rows, as_of=AS_OF):
    return si.qualify_people_event_versions(list(rows), as_of=as_of)


def test_evidence_candidate_uses_receipt_clock_and_keeps_exact_version():
    result = _qualify(_event_version())
    (record,) = result.records
    assert result.status == "RESEARCH_ONLY_NOT_ACTIVATED"
    assert result.contract == "people-event-evidence-v1-20261111"
    assert record["event_time"] == (AS_OF - timedelta(days=30)).isoformat()
    assert record["known_at"] == (AS_OF - timedelta(days=1)).isoformat()
    assert record["id"] == 1 and record["content_hash"] == "a" * 64
    assert record["table"] == "people_events"
    assert result.exclusions == {}
    assert not isinstance(record, si.Observation)


def test_evidence_candidate_ignores_current_source_counts_and_later_metadata():
    original = _event_version()
    mutated = {**original, "n_sources": 999, "source_refs": ["future-reporter"],
               "provenance": {"near_duplicate": True, "later_secret": "ignored"},
               "source": "future-authoritative-source", "created_at": AS_OF - timedelta(days=90)}
    assert _qualify(original) == _qualify(mutated)


def test_evidence_candidate_never_uses_old_creation_time_for_revised_payload():
    result = _qualify(_event_version(known_at=AS_OF + timedelta(seconds=1),
                                    created_at=AS_OF - timedelta(days=90)))
    assert not result.records
    assert result.exclusions == {"not_yet_known": 1}


@pytest.mark.parametrize("end_field", ["superseded_at", "retracted_at"])
def test_evidence_candidate_end_boundary_is_exclusive(end_field):
    row = _event_version(**{end_field: AS_OF})
    assert _qualify(row, as_of=AS_OF - timedelta(microseconds=1)).records
    assert not _qualify(row).records
    assert _qualify(row).exclusions == {"superseded_or_retracted": 1}


def test_evidence_candidate_correction_selects_one_version_at_boundary():
    old = _event_version(superseded_at=AS_OF)
    new = _event_version(id=2, content_hash="b" * 64, known_at=AS_OF,
                         direction="sell", transaction_code="S")
    before = _qualify(old, new, as_of=AS_OF - timedelta(microseconds=1))
    after = _qualify(old, new)
    assert [r["id"] for r in before.records] == [1]
    assert [r["id"] for r in after.records] == [2]
    assert after.records[0]["direction"] == "sell"


def test_evidence_candidate_future_append_does_not_change_earlier_records():
    base = _event_version()
    future = _event_version(id=2, content_hash="b" * 64, known_at=AS_OF + timedelta(days=1))
    assert _qualify(base).records == _qualify(base, future).records


@pytest.mark.parametrize("delta,accepted", [(timedelta(0), True),
                                             (timedelta(days=7), False),
                                             (timedelta(days=7, microseconds=-1), True)])
def test_evidence_candidate_receipt_window_is_exact(delta, accepted):
    assert bool(_qualify(_event_version(known_at=AS_OF - delta)).records) is accepted


@pytest.mark.parametrize("overrides,reason", [
    ({"channel": "gov_contract_qq_aggregate"}, "unsupported_or_aggregate_channel"),
    ({"channel": "news"}, "unsupported_or_aggregate_channel"),
    ({"clock_contract": "public-clock"}, "unqualified_receipt_history"),
    ({"end_history_valid": False}, "unqualified_receipt_history"),
    ({"known_at_basis": "filing"}, "unqualified_clock_basis"),
    ({"known_at_basis": "statutory_bound"}, "unqualified_clock_basis"),
    ({"known_at_basis": "qq_last_modified"}, "unqualified_clock_basis"),
    ({"entity_ticker": "NONE"}, "unqualified_ticker"),
    ({"entity_ticker": "BTC-USD"}, "unqualified_ticker"),
    ({"transaction_code": "A"}, "not_qualified_directional_act"),
    ({"transaction_code": "P", "direction": "sell"}, "not_qualified_directional_act"),
    ({"known_at": AS_OF.replace(tzinfo=None)}, "invalid_clock"),
    ({"known_at": "2026-11-07"}, "invalid_clock"),
    ({"event_time": AS_OF}, "invalid_event_clock"),
    ({"event_time": AS_OF.replace(tzinfo=None)}, "invalid_event_clock"),
    ({"id": None}, "missing_version_identity"),
    ({"content_hash": "unknown"}, "missing_version_identity"),
])
def test_evidence_candidate_refuses_unqualified_rows(overrides, reason):
    result = _qualify(_event_version(**overrides))
    assert not result.records
    assert result.exclusions == {reason: 1}


@pytest.mark.parametrize("channel,direction,code,family", [
    ("form4", "sell", "S", "insider"), ("congress", "buy", None, "congress"),
    ("congress", "sell", None, "congress"), ("gov_contract", "award", None, "gov_contract"),
])
def test_evidence_candidate_admits_only_documented_directional_channels(channel, direction, code, family):
    result = _qualify(_event_version(channel=channel, direction=direction, transaction_code=code))
    assert result.records[0]["family"] == family


def test_evidence_candidate_repeated_version_is_not_extra_evidence():
    row = _event_version()
    result = _qualify(row, dict(row))
    assert len(result.records) == 1
    assert result.exclusions == {"duplicate_version": 1}


@pytest.mark.parametrize("second", [
    {"id": 2, "content_hash": "b" * 64},
    {"id": 2, "direction": "neutral", "transaction_code": "A"},
    {"id": 2, "known_at_basis": "statutory_bound"},
    {"direction": "sell", "transaction_code": "S"},
    {"dedup_key": "DIFFERENT-ACT"},
])
def test_evidence_candidate_conflicting_visible_versions_withhold_whole_act(second):
    result = _qualify(_event_version(), _event_version(**second))
    assert not result.records
    assert result.exclusions == {("conflicting_visible_version" if second.get("id") == 2
                                  else "conflicting_version_identity"): 2}


def test_evidence_candidate_empty_and_order_independent():
    assert _qualify().records == ()
    first = _event_version()
    second = _event_version(id=2, dedup_key="SECOND-ACT", content_hash="b" * 64)
    assert _qualify(first, second) == _qualify(second, first)


def test_evidence_candidate_requires_aware_decision_before_read(monkeypatch):
    reader = MagicMock()
    monkeypatch.setattr("store.people_events.read_event_versions", reader)
    with pytest.raises(ValueError, match="timezone-aware"):
        si.collect_people_event_evidence(MagicMock(), entity_ticker="NVDA", as_of=AS_OF.replace(tzinfo=None))
    reader.assert_not_called()


def test_evidence_candidate_withholds_without_post_commit_proof_before_any_io(monkeypatch):
    reader = MagicMock(side_effect=AssertionError("public-clock read is not qualified"))
    monkeypatch.setattr("store.people_events.read_event_versions", reader)
    engine = MagicMock()
    result = si.collect_people_event_evidence(engine, entity_ticker="NVDA", as_of=AS_OF)
    assert not result.records
    assert result.exclusions == {"commit_visibility_unproven": 1}
    reader.assert_not_called()
    assert engine.mock_calls == []


@pytest.mark.parametrize("ticker", [None, "", "*", "NONE", "NVDA,SPY"])
def test_evidence_candidate_reader_requires_one_ticker(ticker):
    with pytest.raises(ValueError, match="one explicit"):
        si.collect_people_event_evidence(MagicMock(), entity_ticker=ticker, as_of=AS_OF)


def test_evidence_candidate_does_not_change_registered_runtime_or_construction():
    import inspect
    assert si.CONSTRUCTION == "v2"
    assert "collect_signal_observations" in inspect.getsource(si.generate_intersection_leads)
    assert "collect_people_event_evidence" not in inspect.getsource(si.generate_intersection_leads)


def test_evidence_candidate_later_identity_echo_or_public_clock_cannot_rewrite_receipt():
    row = _event_version()
    enriched = {**row, "actor_id": "0000000001", "actor_id_basis": "owner_cik", "echo_of": 99,
                "entity_cik": 1, "security_id": "late-resolution", "public_known_at": AS_OF - timedelta(days=50)}
    assert _qualify(row) == _qualify(enriched)
    assert not {"actor_id", "actor_id_basis", "echo_of", "security_id", "entity_cik"} & set(_qualify(row).records[0])


@pytest.mark.parametrize("changes", [
    {"superseded_at": AS_OF}, {"retracted_at": "invalid"},
    {"direction": "sell", "transaction_code": "S", "superseded_at": AS_OF},
])
def test_evidence_candidate_corrupt_same_id_cannot_hide_behind_visibility(changes):
    result = _qualify(_event_version(), _event_version(**changes))
    assert not result.records
    assert result.exclusions == {"conflicting_version_identity": 2}


def test_evidence_candidate_bad_clock_version_quarantines_identifiable_act():
    result = _qualify(_event_version(), _event_version(id=2, known_at="invalid"))
    assert not result.records
    assert result.exclusions == {"conflicting_act_history": 1, "invalid_clock": 1}


@pytest.mark.parametrize("clock", ["people-events-system-receipt-v1", "transaction_start",
                                    "public_filing", None])
def test_evidence_candidate_does_not_relabel_existing_clocks_as_post_commit(clock):
    result = _qualify(_event_version(clock_contract=clock))
    assert not result.records
    assert result.exclusions == {"unqualified_receipt_history": 1}


@pytest.mark.parametrize("changes", [{"receipt_id": None}, {"receipt_sha256": None},
                                      {"receipt_sha256": "not-a-receipt-hash"}])
def test_evidence_candidate_requires_explicit_receipt_identity(changes):
    result = _qualify(_event_version(**changes))
    assert not result.records
    assert result.exclusions == {"missing_version_identity": 1}
