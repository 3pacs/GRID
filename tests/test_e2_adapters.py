"""E2 stream adapters for the two live streams: GEX-levels v1 and the S10 forward log.

Both logs are written here by the streams' OWN writers
(``paper_log.gex_levels.storage.PaperLogStore`` and
``analysis.research_forward_log.ForwardLog``), so the adapters are tested
against the real on-disk formats, plus a byte copy of the real GEX-levels
log as of 2026-09-30 (``tests/fixtures/e2/gex_levels_v1.jsonl``).
"""

from __future__ import annotations

import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from evals.e2 import board, scoring
from evals.e2.adapters.gex_levels import GexLevelsAdapter
from evals.e2.adapters.s10 import S10Adapter
from evals.e2.chain import Ledger
from tests import e2_support as S

FIXTURE = Path(__file__).parent / "fixtures" / "e2" / "gex_levels_v1.jsonl"


def _rules():
    r = scoring.load_json("rules.json")
    r["registered_at"] = "2026-09-01T00:00:00+00:00"
    return r


def _run(board_dir, adapters, now, rules=None):
    return board.run(board_dir, adapters, now, rules=rules or _rules(), cost_model=S.cost_model(),
                     manifest_info=S.MANIFEST_INFO, code_sha=S.CODE_SHA)


def _scores(board_dir):
    return {r["unit"]: r for r in Ledger(board_dir, "e2-v1").read_all() if r["kind"] == "score"}


def _resolutions(board_dir):
    return {r["prediction_id"]: r for r in Ledger(board_dir, "e2-v1").read_all() if r["kind"] == "resolution"}


# --- GEX-levels v1 ---------------------------------------------------------------------------


def _preopen(session, run_at, *, excluded=None, regime="LONG_GAMMA"):
    return {
        "kind": "preopen", "session_date": session, "run_at": run_at, "code_sha": "07e1fc16" + "0" * 32,
        "excluded": excluded is not None, "exclusion_reason": excluded,
        "engine": {"available": True, "regime": regime, "spot": 100.0},
        "p0": {"as_of_date": "prev", "fetched_at": run_at, "price": 100.0},
        "levels": {
            "real": {"call_wall": 110.0, "call_wall_missing": False, "put_wall": 90.0, "put_wall_missing": False,
                     "gamma_flip": 100.0, "gamma_flip_missing": False},
            "placebo": {"call_wall": {"value": 95.0, "dropped": False, "name": "call_wall", "collided_with": None},
                        "put_wall": {"value": 105.0, "dropped": False, "name": "put_wall", "collided_with": None},
                        "gamma_flip": {"value": 100.0, "dropped": True, "name": "gamma_flip",
                                       "collided_with": "gamma_flip"}},
        },
    }


def _postclose(session, run_at, *, fetched=None, excluded=None):
    fetched = fetched or run_at
    none = {"bar_time": None, "held": None, "side": "below", "status": "none"}
    untriggered = {"triggered": False, "direction": None, "raw_entry_price": None, "raw_exit_price": None,
                   "regime_rule": "fade", "return_pct": None}
    return {
        "kind": "postclose", "session_date": session, "run_at": run_at, "code_sha": "07e1fc16" + "0" * 32,
        "excluded": excluded is not None, "exclusion_reason": excluded,
        "bars": {"early_close": False, "expected": 78, "present": 78, "missing_pct": 0.0, "fetched_at": fetched},
        "session_ohlc": {"open": 101.0, "high": 110.5, "low": 99.5, "close": 108.0, "fetched_at": fetched},
        "reaches": {
            "real": {"call_wall": {"bar_time": "t", "held": True, "side": "above", "status": "reached"},
                     "put_wall": none, "gamma_flip": {"bar_time": None, "held": None, "side": "below",
                                                      "status": "gap_through"}},
            "placebo": {"call_wall": {"bar_time": "t", "held": False, "side": "below", "status": "reached"},
                        "put_wall": none, "gamma_flip": None},
        },
        "h3_trade": {
            "real": {"triggered": True, "direction": "short", "raw_entry_price": 110.0, "raw_exit_price": 108.0,
                     "regime_rule": "fade", "trigger_level_name": "call_wall", "trigger_time": "t",
                     "return_pct": 0.0179},
            "placebo": untriggered,
        },
    }


def _gex_log(log_dir: Path, records: list[dict]) -> None:
    from paper_log.gex_levels.storage import PaperLogStore

    store = PaperLogStore(log_dir)
    for record in records:
        store.append(record)


def _gex(log_dir):
    return GexLevelsAdapter(log_dir, _rules()["streams"]["gex_levels_v1"])


def test_gex_session_normalizes_resolves_and_scores(tmp_path):
    logs = tmp_path / "gex"
    _gex_log(logs, [
        _preopen("2026-10-01", "2026-10-01T12:45:00+00:00"),
        _postclose("2026-10-01", "2026-10-01T20:30:00+00:00"),
        _preopen("2026-10-02", "2026-10-02T13:31:00+00:00"),            # after 09:30 ET: late
        _postclose("2026-10-02", "2026-10-02T20:30:00+00:00"),
        _preopen("2026-10-05", "2026-10-05T12:45:00+00:00", excluded="stale_chain"),
    ])
    # 16:00Z: the pre-open is logged, the session has not closed -> witnessed before the outcome
    first = _run(tmp_path / "board", [_gex(logs)], S.utc(2026, 10, 1, 16))
    ledger = Ledger(tmp_path / "board", "e2-v1").read_all()
    preds = [r for r in ledger if r["kind"] == "prediction"]
    assert len(preds) == 7  # real: 3 levels + H3; placebo: 2 levels (flip dropped) + H3
    assert {r["ingest_timing"] for r in preds} == {"before_outcome"}
    assert first["snapshot"]["counts"]["pending"] == 7
    receipt = preds[0]["prediction"]["log_receipt"]
    assert receipt["log"] == "gex_levels_v1.jsonl" and receipt["line_index"] == 0 and receipt["prev_sha256"] is None

    snap = _run(tmp_path / "board", [_gex(logs)], S.utc(2026, 10, 6, 1))["snapshot"]
    res = _resolutions(tmp_path / "board")
    sid = "gex_levels_v1:2026-10-01"
    assert res[f"{sid}:real:hold:call_wall"]["outcome"]["held"] is True
    assert res[f"{sid}:real:hold:put_wall"]["reason"] == "not_reached"
    assert res[f"{sid}:real:hold:gamma_flip"]["reason"] == "gap_through"
    assert res[f"{sid}:placebo:h3"]["reason"] == "no_trigger"
    assert res[f"{sid}:real:hold:call_wall"]["receipt"]["bars_fetched_at"] == "2026-10-01T20:30:00+00:00"
    scores = _scores(tmp_path / "board")
    assert scores[f"{sid}:real:hold:call_wall"]["metrics"] == {"hit": 1}
    assert scores[f"{sid}:placebo:hold:call_wall"]["metrics"] == {"hit": 0}
    assert scores[f"{sid}:placebo:hold:call_wall"]["role"] == "control"
    # short at 110, out at 108: +1.8182% gross, minus 2 x 3 bp (us_equity_etf_large) = +1.7582%
    assert scores[f"{sid}:real:h3"]["metrics"] == {"net_return": pytest.approx(2 / 110 - 0.0006), "hit": 1}
    activity = snap["streams"]["gex_levels_v1"]["activity"]
    assert activity["valid_sessions"] == 1
    assert activity["excluded_sessions"] == {"late_preopen": 1, "preopen_excluded:stale_chain": 1}
    assert activity["interim"] is True
    assert all(r["interim"] for r in snap["aggregates"])
    # candidate and control arms are never pooled at stream level
    stream_rows = [r for r in snap["aggregates"] if set(r["group"]) == {"stream", "role"}]
    assert {r["group"]["role"] for r in stream_rows} == {"candidate", "control"}


def test_gex_postclose_fetched_before_the_close_is_look_ahead(tmp_path):
    logs = tmp_path / "gex"
    _gex_log(logs, [
        _preopen("2026-10-01", "2026-10-01T12:45:00+00:00"),
        _postclose("2026-10-01", "2026-10-01T20:30:00+00:00", fetched="2026-10-01T19:59:00+00:00"),
    ])
    snap = _run(tmp_path / "board", [_gex(logs)], S.utc(2026, 10, 2))["snapshot"]
    assert snap["streams"]["gex_levels_v1"]["ok"] is True
    assert snap["counts"]["void_by_reason"] == {"lookahead_refused": 7}
    assert snap["counts"]["scores"] == 0
    assert snap["counts"]["integrity_alerts"] == 1  # one alert for the one bad post-close record
    refusal = next(iter(_resolutions(tmp_path / "board").values()))["receipt"]["refusal"]
    assert "precedes the session close" in refusal


def test_gex_postclose_not_yet_written_stays_pending_then_voids_after_grace(tmp_path):
    logs = tmp_path / "gex"
    _gex_log(logs, [_preopen("2026-10-01", "2026-10-01T12:45:00+00:00"),
                    _postclose("2026-10-01", "2026-10-01T20:30:00+00:00")])
    # 20:15Z: session closed but the post-close record (20:30Z) does not exist yet at this instant
    snap = _run(tmp_path / "board", [_gex(logs)], S.utc(2026, 10, 1, 20, 15))["snapshot"]
    assert snap["counts"]["resolutions"] == 0 and snap["counts"]["pending"] == 7
    logs2 = tmp_path / "gex2"
    _gex_log(logs2, [_preopen("2026-10-01", "2026-10-01T12:45:00+00:00")])
    snap = _run(tmp_path / "board2", [_gex(logs2)], S.utc(2026, 10, 7))["snapshot"]
    assert snap["counts"]["void_by_reason"] == {"no_postclose": 7}


def test_real_gex_log_copy_dry_run(tmp_path):
    """The real grid-svr log (4 sessions, 2026-09-25..30), scored under the pinned rules."""
    logs = tmp_path / "gex"
    logs.mkdir()
    shutil.copyfile(FIXTURE, logs / "gex_levels_v1.jsonl")
    rules = scoring.load_json("rules.json")
    adapter = GexLevelsAdapter(logs, rules["streams"]["gex_levels_v1"])
    snap = _run(tmp_path / "board", [adapter], S.utc(2026, 10, 1, 5, 30), rules=rules)["snapshot"]
    assert snap["streams"]["gex_levels_v1"]["source_head_sha256"] == (
        "69ad15bbf9751f327cd96adbcf2464622230edb24c6eccb39849581fc5d1b152")
    assert snap["counts"]["predictions"] == 26 and snap["counts"]["pending"] == 0
    assert snap["counts"]["void_by_reason"] == {"gap_through": 1, "no_trigger": 6, "not_reached": 15}
    assert snap["counts"]["scores"] == 4 and snap["counts"]["official_scores"] == 0  # before registration
    assert snap["streams"]["gex_levels_v1"]["activity"]["valid_sessions"] == 4


def test_gex_log_with_wrong_prereg_is_not_read(tmp_path):
    logs = tmp_path / "gex"
    logs.mkdir()
    S.write_chain(logs / "gex_levels_v1.jsonl", [{**_preopen("2026-10-01", "2026-10-01T12:45:00+00:00"),
                                                  "prereg_sha256": "0" * 64}])
    snap = _run(tmp_path / "board", [_gex(logs)], S.utc(2026, 10, 2))["snapshot"]
    assert snap["streams"]["gex_levels_v1"]["ok"] is False
    assert snap["counts"]["predictions"] == 0


# --- S10 hypothesis forward log -----------------------------------------------------------------


D0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
X = [1.0, 2.0, 3.0, 4.0, 5.0]
Y = [0.1, 0.3, 0.2, 0.4, 0.0]


def _s10_events(*, verdict: bool, bad_outcome: bool = False) -> list[dict]:
    cid = "cand1"
    plan = {"family": "DGS10_fwd5", "direction": -1, "statistic": "spearman", "min_n": 4, "max_decisions": 8,
            "horizon_sessions": 5, "target": {"series_id": "DGS10", "label": "change"}}
    events = [{"kind": "admission", "run_at": "2026-09-29T11:33:00+00:00", "candidate_id": cid, "plan": plan,
               "plan_sha256": "p" * 64, "promotion_allowed": False}]
    for k in range(5):
        decided = D0 + timedelta(days=k)
        known = decided + timedelta(days=8)
        common = {"candidate_id": cid, "k": k, "decision_at": decided.isoformat(),
                  "label_end": (decided + timedelta(days=7)).isoformat(), "label_known_at": known.isoformat()}
        excluded = k == 2
        events.append({**common, "kind": "prediction", "run_at": (decided + timedelta(hours=11)).isoformat(),
                       "excluded": excluded, "exclusion_reason": "feature_abstained" if excluded else None,
                       "feature": {"name": "f", "value": None if excluded else X[k],
                                   "known_at": None if excluded else (decided - timedelta(days=1)).isoformat()},
                       "receipt": "r"})
        if not excluded:
            read_at = known + timedelta(hours=11) if not (bad_outcome and k == 0) else known - timedelta(hours=1)
            events.append({**common, "kind": "outcome", "run_at": read_at.isoformat(), "target_start": 4.0,
                           "target_end": 4.0 + Y[k], "label": Y[k], "excluded": False, "exclusion_reason": None,
                           "receipt": "panel-receipt"})
    if verdict:
        events.append({"kind": "verdict", "run_at": "2026-10-14T11:33:00+00:00", "candidate_id": cid,
                       "state": "FORWARD_FAILED", "n": 4, "rho": -0.2, "p_one_sided": 0.6,
                       "promotion_allowed": False})
    return sorted(events, key=lambda e: e["run_at"])


def _s10_log(log_dir: Path, events: list[dict]) -> None:
    from analysis import research_forward_log as fl

    log = fl.ForwardLog(log_dir)
    log.append([fl.header_record(datetime(2026, 9, 29, 11, 0, tzinfo=timezone.utc), "2" * 40)])
    for event in events:
        log.append([event])


def _s10(log_dir):
    r = _rules()
    return S10Adapter(log_dir, r["streams"]["s10_hypothesis_forward_v1"], r["rules"]["s10.ts_ic.v1"])


def test_s10_scores_are_sealed_until_the_verdict_then_match_it(tmp_path):
    logs = tmp_path / "s10"
    _s10_log(logs, _s10_events(verdict=False))
    snap = _run(tmp_path / "board", [_s10(logs)], S.utc(2026, 10, 13, 12))["snapshot"]
    assert snap["counts"]["predictions"] == 4          # the excluded decision is not a prediction
    assert snap["counts"]["resolutions"] == 4
    assert snap["counts"]["scores"] == 0               # sealed: no verdict yet
    assert snap["aggregates"] == []
    assert snap["streams"]["s10_hypothesis_forward_v1"]["activity"]["sealed_candidates"] == 1
    assert snap["streams"]["s10_hypothesis_forward_v1"]["activity"]["prediction_exclusions"] == {
        "feature_abstained": 1}

    shutil.rmtree(logs)
    _s10_log(logs, _s10_events(verdict=True))           # the same log, now carrying its verdict
    snap = _run(tmp_path / "board", [_s10(logs)], S.utc(2026, 10, 14, 12))["snapshot"]
    score = _scores(tmp_path / "board")["s10_hypothesis_forward_v1:cand1"]
    # pairs k=0,1,3,4: x ranks [1,2,3,4], y ranks [2,3,4,1] -> rho = 1 - 6*12/(4*15) = -0.2; direction -1
    assert score["metrics"] == {"signed_ic": pytest.approx(0.2)}
    assert score["detail"]["matches_verdict"] is True and score["detail"]["n_pairs"] == 4
    assert score["detail"]["verdict_state"] == "FORWARD_FAILED"
    assert snap["counts"]["scores"] == 1


def test_s10_outcome_read_before_publication_is_look_ahead(tmp_path):
    logs = tmp_path / "s10"
    _s10_log(logs, _s10_events(verdict=False, bad_outcome=True))
    snap = _run(tmp_path / "board", [_s10(logs)], S.utc(2026, 10, 13, 12))["snapshot"]
    assert snap["streams"]["s10_hypothesis_forward_v1"]["ok"] is True
    res = _resolutions(tmp_path / "board")
    bad = res["s10_hypothesis_forward_v1:cand1:k0"]
    assert bad["reason"] == "lookahead_refused" and "before its label was published" in bad["receipt"]["refusal"]
    assert sum(r["status"] == "resolved" for r in res.values()) == 3   # k=1,3,4 still resolve
    assert snap["counts"]["integrity_alerts"] == 1


def test_real_s10_log_copy_header_only(tmp_path):
    """The real grid-svr S10 log on 2026-09-30 holds its header only: zero candidates, nothing to score."""
    from analysis import research_forward_log as fl

    logs = tmp_path / "s10"
    log = fl.ForwardLog(logs)
    log.append([fl.header_record(datetime(2026, 9, 29, 11, 33, tzinfo=timezone.utc), "6" * 40)])
    snap = _run(tmp_path / "board", [_s10(logs)], S.utc(2026, 10, 1, 5, 30))["snapshot"]
    assert snap["streams"]["s10_hypothesis_forward_v1"]["ok"] is True
    assert snap["streams"]["s10_hypothesis_forward_v1"]["activity"]["candidates"] == 0
    assert snap["counts"]["predictions"] == 0


def test_s10_malformed_admission_plan_is_quarantined_not_fatal(tmp_path):
    logs = tmp_path / "s10"
    events = _s10_events(verdict=True)
    events[0] = {**events[0], "plan": {k: v for k, v in events[0]["plan"].items() if k != "min_n"}}
    _s10_log(logs, events)
    snap = _run(tmp_path / "board", [_s10(logs)], S.utc(2026, 10, 14, 12))["snapshot"]
    assert snap["streams"]["s10_hypothesis_forward_v1"]["ok"] is True
    assert snap["counts"]["scores"] == 0 and snap["counts"]["integrity_alerts"] == 1
    again = _run(tmp_path / "board", [_s10(logs)], S.utc(2026, 10, 15, 12))["snapshot"]
    assert again["counts"]["integrity_alerts"] == 1  # not repeated


def test_s10_prediction_with_a_malformed_label_end_is_quarantined(tmp_path):
    logs = tmp_path / "s10"
    events = _s10_events(verdict=False)
    first = next(i for i, e in enumerate(events) if e["kind"] == "prediction")
    events[first] = {**events[first], "label_end": "not-a-date"}
    _s10_log(logs, events)
    snap = _run(tmp_path / "board", [_s10(logs)], S.utc(2026, 10, 13, 12))["snapshot"]
    assert snap["streams"]["s10_hypothesis_forward_v1"]["ok"] is True
    assert snap["counts"]["predictions"] == 3 and snap["counts"]["integrity_alerts"] == 1


def test_gex_postclose_fetched_after_the_run_voids_not_refuses_the_stream(tmp_path):
    logs = tmp_path / "gex"
    _gex_log(logs, [
        _preopen("2026-10-01", "2026-10-01T12:45:00+00:00"),
        _postclose("2026-10-01", "2026-10-01T20:30:00+00:00", fetched="2027-01-01T00:00:00+00:00"),
    ])
    snap = _run(tmp_path / "board", [_gex(logs)], S.utc(2026, 10, 2))["snapshot"]
    assert snap["streams"]["gex_levels_v1"]["ok"] is True
    assert snap["counts"]["void_by_reason"] == {"invalid_resolution": 7}
    again = _run(tmp_path / "board", [_gex(logs)], S.utc(2026, 10, 3))["snapshot"]
    assert again["counts"]["integrity_alerts"] == snap["counts"]["integrity_alerts"]


def test_s10_odd_upstream_values_do_not_abort_activity(tmp_path):
    logs = tmp_path / "s10"
    events = _s10_events(verdict=True)
    pred = next(i for i, e in enumerate(events) if e["kind"] == "prediction" and e["excluded"])
    events[pred] = {**events[pred], "exclusion_reason": 7}
    verdict = next(i for i, e in enumerate(events) if e["kind"] == "verdict")
    events[verdict] = {**events[verdict], "state": ["odd"]}
    _s10_log(logs, events)
    snap = _run(tmp_path / "board", [_s10(logs)], S.utc(2026, 10, 14, 12))["snapshot"]
    activity = snap["streams"]["s10_hypothesis_forward_v1"]["activity"]
    assert activity["prediction_exclusions"] == {"7": 1} and activity["verdicts"] == {"['odd']": 1}
