"""E2 forward scoreboard: exact scoring, look-ahead refusal, determinism, append-only, versioning.

A synthetic ``e2-stream-v1`` stream with known closes is scored end to end
through ``evals.e2.board.run`` and every number is checked against a hand
computation (the literals below were worked out by hand from the closes in
``tests/e2_support.py``; they are not read back from the code under test).
"""

from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path

import pytest

from evals.e2 import board, scoring
from evals.e2.chain import ChainError, Ledger, raw_lines
from evals.e2.resolve import PriceObs
from tests import e2_support as S

AFTER = S.utc(2026, 10, 2, 21, 30)   # both closes observable (available 21:00Z)


def _run(tmp_path, now, *, price_source=None, rules=None, log=None, board_dir=None):
    rules = rules or S.rules()
    log = log or tmp_path / "stream.jsonl"
    if not log.exists():
        S.write_chain(log, S.stream_records())
    adapter = S.stream_adapter(log, rules, price_source if price_source is not None else S.prices())
    return board.run(board_dir or tmp_path / "board", [adapter], now, rules=rules, cost_model=S.cost_model(),
                     manifest_info=S.MANIFEST_INFO, code_sha=S.CODE_SHA)


def _row(snapshot, rule_id, metric, group, window="all", bucket="official"):
    rows = [r for r in snapshot["aggregates"] if r["rule_id"] == rule_id and r["metric"] == metric
            and r["group"] == group and r["window"] == window and r["bucket"] == bucket]
    assert len(rows) == 1, rows
    return rows[0]


def _scores(board_dir):
    return {r["unit"]: r for r in Ledger(board_dir, "e2-v1").read_all() if r["kind"] == "score"}


# --- scoring rules (unit) ------------------------------------------------------------


def test_scoring_primitives_match_hand_values():
    assert scoring.direction_hit(1, 0.1) == 1 and scoring.direction_hit(-1, 0.1) == 0
    assert scoring.direction_hit(1, 0.0) is None
    assert scoring.brier(0.8, 1) == pytest.approx(0.04)
    assert scoring.log_loss(0.8, 1, 1e-6) == pytest.approx(0.2231435513142097)
    assert scoring.log_loss(0.0, 1, 1e-6) == pytest.approx(-math.log(1e-6))  # clipped, finite
    assert scoring.net_return(1, 100, 110, "us_equity_large_cap", S.cost_model()) == pytest.approx(0.0984)
    assert scoring.net_return(-1, 110, 108, "us_equity_etf_large", S.cost_model()) == pytest.approx(
        -(108 / 110 - 1) - 2 * 3.0 / 1e4)
    assert scoring.net_return(1, 1, 2, "not_tradable", S.cost_model()) is None
    assert scoring.spearman([5, 1, 3, 2, 4], [0.10, -0.05, 0.02, -0.05, 0.05]) == pytest.approx(0.9746794344808963)
    lo, hi = scoring.wilson_interval(1, 3)
    assert (lo, hi) == (pytest.approx(0.06149194472039621), pytest.approx(0.7923403991979522))
    with pytest.raises(ValueError):
        scoring.brier(1.2, 1)
    with pytest.raises(KeyError):
        scoring.cost_per_side_bps("unknown_class", S.cost_model())


def test_bootstrap_is_deterministic_and_seed_sensitive():
    values = [0.1, -0.2, 0.05, 0.3, -0.1, 0.0, 0.2]
    a = scoring.bootstrap_mean_ci(values, seed=scoring.seed_for("x"), n_boot=500, ci=0.95)
    b = scoring.bootstrap_mean_ci(values, seed=scoring.seed_for("x"), n_boot=500, ci=0.95)
    c = scoring.bootstrap_mean_ci(values, seed=scoring.seed_for("y"), n_boot=500, ci=0.95)
    assert a == b and a != c
    assert a[0] <= sum(values) / len(values) <= a[1]
    # chunked generation gives the same draws as one pass (memory bound does not change results)
    assert list(scoring.splitmix64(7, 10)) == list(scoring.splitmix64(7, 4)) + list(scoring.splitmix64(7, 6, offset=4))


# --- the synthetic stream scores exactly as hand-computed ------------------------------


def test_synthetic_stream_scores_exactly_as_hand_computed(tmp_path):
    result = _run(tmp_path, AFTER)
    snap = result["snapshot"]
    scores = _scores(tmp_path / "board")
    pid = f"{S.STREAM}:"
    assert scores[pid + "d1"]["metrics"] == {"hit": 1, "net_return": pytest.approx(0.0984)}
    assert scores[pid + "d2"]["metrics"] == {"hit": 0, "net_return": pytest.approx(-0.0516)}
    assert scores[pid + "d3"]["metrics"] == {"hit": 0, "net_return": pytest.approx(-0.0216)}
    assert scores[pid + "p1"]["metrics"] == {"brier": pytest.approx(0.04), "log_loss": pytest.approx(0.2231435513142097)}
    assert scores[pid + "p2"]["metrics"] == {"brier": pytest.approx(0.09), "log_loss": pytest.approx(0.35667494393873245)}
    rank_unit = f"{S.STREAM}:rank:1d:2026-10-01"
    assert scores[rank_unit]["metrics"] == {"rank_ic": pytest.approx(0.9746794344808963)}
    assert scores[rank_unit]["detail"] == {"names": 5, "resolved_names": 5, "min_names": 5}

    hit = _row(snap, "e2.direction.v1", "hit", {"stream": S.STREAM, "family": "dir"})
    assert (hit["n"], hit["hits"], hit["mean"]) == (3, 1, pytest.approx(1 / 3))
    assert hit["wilson_ci"] == [pytest.approx(0.06149194472039621), pytest.approx(0.7923403991979522)]
    pnl = _row(snap, "e2.direction.v1", "net_return", {"stream": S.STREAM, "family": "dir"})
    assert (pnl["n"], pnl["mean"], pnl["sum"]) == (3, pytest.approx(0.0084), pytest.approx(0.0252))
    assert pnl["bootstrap_ci"][0] <= pnl["mean"] <= pnl["bootstrap_ci"][1]
    brier = _row(snap, "e2.probability.v1", "brier", {"stream": S.STREAM, "family": "prob"})
    assert brier["mean"] == pytest.approx(0.065)
    logloss = _row(snap, "e2.probability.v1", "log_loss", {"stream": S.STREAM, "family": "prob"})
    assert logloss["mean"] == pytest.approx(0.2899092476264711)
    ic = _row(snap, "e2.rank_ic.v1", "rank_ic", {"stream": S.STREAM, "role": "candidate"})
    assert (ic["n"], ic["mean"], ic["bootstrap_ci"]) == (1, pytest.approx(0.9746794344808963), None)
    assert snap["counts"]["pending"] == 0 and snap["counts"]["official_scores"] == 6
    # rolling windows are reported only once full
    assert not [r for r in snap["aggregates"] if r["window"] != "all"]


def test_resolution_receipt_states_price_source_and_vintage(tmp_path):
    _run(tmp_path, AFTER)
    res = {r["prediction_id"]: r for r in Ledger(tmp_path / "board", "e2-v1").read_all() if r["kind"] == "resolution"}
    receipt = res[f"{S.STREAM}:d1"]["receipt"]
    assert receipt["price_source"] == "static"
    assert receipt["exit"] == {"instrument": "AAA", "obs_date": "2026-10-02", "value": 110.0,
                               "available_at": "2026-10-02T21:00:00+00:00", "source": "static",
                               "series_id": "STATIC:AAA:close", "vintage": "vAAAx"}
    assert res[f"{S.STREAM}:d1"]["available_at"] == "2026-10-02T21:00:00+00:00"


# --- look-ahead refusal -------------------------------------------------------------------


def test_outcome_not_yet_available_does_not_resolve(tmp_path):
    # 20:30Z: the exit session has closed (20:00Z) but no close is observable until 21:00Z.
    first = _run(tmp_path, S.utc(2026, 10, 2, 20, 30))
    assert first["snapshot"]["counts"]["resolutions"] == 0
    assert first["snapshot"]["counts"]["pending"] == 10
    assert first["snapshot"]["counts"]["scores"] == 0
    # before the exit session even closes nothing is asked of the price source at all
    calls = []

    class Spy:
        name = "spy"

        def close(self, *a):
            calls.append(a)
            return None

    _run(tmp_path, S.utc(2026, 10, 2, 15), price_source=Spy(), board_dir=tmp_path / "board2")
    assert calls == []
    later = _run(tmp_path, AFTER)
    assert later["snapshot"]["counts"]["resolutions"] == 10


class _Hostile:
    """A price source that offers a close as observable before its session closed."""

    name = "hostile"

    def __init__(self, available_at):
        self.available_at = available_at

    def close(self, instrument, obs_date, now):
        value = S.CLOSES[instrument][0 if obs_date == date(2026, 10, 1) else 1]
        return PriceObs(instrument, obs_date, value, self.available_at, "hostile", "X", "v")


def _ledger(tmp_path):
    return Ledger(tmp_path / "board", "e2-v1").read_all()


def test_price_offered_before_the_close_is_refused_pending_then_void_after_grace(tmp_path):
    hostile = _Hostile(S.utc(2026, 10, 1, 12))
    snap = _run(tmp_path, AFTER, price_source=hostile)["snapshot"]
    assert snap["streams"][S.STREAM]["ok"] is True
    assert snap["counts"]["resolutions"] == 0 and snap["counts"]["pending"] == 10
    assert snap["counts"]["integrity_alerts"] == 10
    alert = [r for r in _ledger(tmp_path) if r["kind"] == "integrity_alert"][0]
    assert "before the session closed" in alert["detail"]
    # still inside the grace period (exit close 10-02 20:00Z + 5 days): pending, no repeated alerts
    again = _run(tmp_path, S.utc(2026, 10, 7, 19), price_source=hostile)["snapshot"]
    assert again["counts"]["pending"] == 10 and again["counts"]["integrity_alerts"] == 10
    late = _run(tmp_path, S.utc(2026, 10, 7, 21), price_source=hostile)["snapshot"]
    assert late["counts"]["void_by_reason"] == {"lookahead_refused": 10}
    assert late["counts"]["integrity_alerts"] == 10
    assert all(not r["metrics"] for r in _ledger(tmp_path) if r["kind"] == "score")  # nothing was scored


def test_price_offered_from_the_future_is_refused(tmp_path):
    snap = _run(tmp_path, AFTER, price_source=_Hostile(S.utc(2026, 10, 3, 12)))["snapshot"]
    assert snap["counts"]["resolutions"] == 0 and snap["counts"]["pending"] == 10
    details = [r["detail"] for r in _ledger(tmp_path) if r["kind"] == "integrity_alert"]
    assert len(details) == 10 and all("not observable until" in d for d in details)


class _HostileFor:
    """Honest closes, except one instrument's exit close is offered before its session closed."""

    name = "partly_hostile"

    def __init__(self, bad):
        self.bad, self.honest = bad, S.prices()

    def close(self, instrument, obs_date, now):
        obs = self.honest.close(instrument, obs_date, now)
        if obs is not None and instrument == self.bad and obs_date == date(2026, 10, 2):
            return PriceObs(instrument, obs_date, obs.value, S.utc(2026, 10, 2, 12), "x", "X", "v")
        return obs


def test_one_look_ahead_does_not_block_the_rest_of_the_stream(tmp_path):
    snap = _run(tmp_path, AFTER, price_source=_HostileFor("AAA"))["snapshot"]
    assert snap["streams"][S.STREAM]["ok"] is True
    assert snap["counts"]["pending"] == 3 and snap["counts"]["integrity_alerts"] == 3  # d1, p1, r_AAA
    scores = _scores(tmp_path / "board")
    assert scores[f"{S.STREAM}:d2"]["metrics"]["hit"] == 0 and scores[f"{S.STREAM}:p2"]["metrics"]["brier"] == 0.09
    again = _run(tmp_path, S.utc(2026, 10, 3, 23), price_source=_HostileFor("AAA"))["snapshot"]
    assert again["counts"]["integrity_alerts"] == 3


def _with(records, index, **changes):
    out = list(records)
    rec = {**out[index], **changes}
    for key, value in changes.items():
        if value is None:
            rec.pop(key)
    out[index] = rec
    return out


# (line index in the stream log, changes) -- each breaks exactly one record
MALFORMED = {
    "missing_family": (2, {"family": None}),
    "exit_before_issue": (2, {"horizon": {"label": "1d", "entry_date": "2026-10-01", "exit_date": "2026-09-30"}}),
    "bad_date": (2, {"horizon": {"label": "1d", "entry_date": "2026-10-01", "exit_date": "2026-13-45"}}),
    "side_2": (1, {"call": {"kind": "direction", "side": 2}}),
    "side_true": (1, {"call": {"kind": "direction", "side": True}}),
    "p_1.5": (4, {"call": {"kind": "probability", "event": "up", "p": 1.5}}),
    "p_nan": (4, {"call": {"kind": "probability", "event": "up", "p": float("nan")}}),
    "score_abc": (6, {"call": {"kind": "rank_score", "score": "abc"}}),
    "score_inf": (6, {"call": {"kind": "rank_score", "score": float("inf")}}),
    "family_list": (1, {"family": ["x"]}),
    "family_number": (1, {"family": 7}),
    "sector_object": (1, {"sector": {"a": 1}}),
    "label_list": (1, {"horizon": {"label": [1], "entry_date": "2026-10-01", "exit_date": "2026-10-02"}}),
    "instrument_list": (1, {"target": {"instrument": ["AAA"], "instrument_class": "us_equity_large_cap"}}),
}


@pytest.mark.parametrize("broken", sorted(MALFORMED))
def test_a_malformed_record_is_quarantined_not_fatal(tmp_path, broken):
    index, changes = MALFORMED[broken]
    records = _with(S.stream_records(), index, **changes)
    log = tmp_path / "stream.jsonl"
    S.write_chain(log, records, allow_nan=True)
    _run(tmp_path, S.utc(2026, 10, 1, 15), log=log)  # before the horizon: ingestion only
    snap = _run(tmp_path, S.utc(2026, 10, 9), log=log)["snapshot"]  # well past every horizon
    assert snap["streams"][S.STREAM]["ok"] is True
    assert snap["counts"]["predictions"] == 9 and snap["counts"]["pending"] == 0
    alerts = [r for r in _ledger(tmp_path) if r["kind"] == "integrity_alert"]
    assert len(alerts) == 1 and alerts[0]["observed_sha256"] == "quarantine"
    assert f"line {index}" in alerts[0]["detail"]
    _run(tmp_path, S.utc(2026, 10, 10), log=log)
    assert len([r for r in _ledger(tmp_path) if r["kind"] == "integrity_alert"]) == 1


class _BrokenAdapter:
    """A second stream whose only record breaks the E2 contract."""

    stream = "broken_v1"

    def load(self, now):
        from evals.e2.adapters import SourceView

        return SourceView("broken_v1", Path("broken.jsonl"), [], [], [], 0, None)

    def predictions(self, view):
        return [{"stream": "broken_v1", "prediction_id": "broken_v1:x", "rule_id": "nope.v1"}]

    def resolve(self, view, pred, now):
        return None

    def unit_scores(self, view, state, now):
        return []

    def activity(self, view):
        return {}


@pytest.mark.parametrize("broken_first", [True, False])
def test_a_broken_stream_never_affects_another(tmp_path, broken_first):
    log = tmp_path / "stream.jsonl"
    S.write_chain(log, S.stream_records())
    rules = S.rules()
    adapters = [S.stream_adapter(log, rules, S.prices()), _BrokenAdapter()]
    if broken_first:
        adapters.reverse()
    snap = board.run(tmp_path / "board", adapters, AFTER, rules=rules, cost_model=S.cost_model(),
                     manifest_info=S.MANIFEST_INFO, code_sha=S.CODE_SHA)["snapshot"]
    assert snap["streams"][S.STREAM]["ok"] is True
    assert (snap["counts"]["predictions"], snap["counts"]["pending"], snap["counts"]["scores"]) == (10, 0, 6)
    assert snap["counts"]["integrity_alerts"] == 1  # the broken record, quarantined
    assert not [r for r in _ledger(tmp_path) if "broken_v1:x" in json.dumps(r.get("prediction") or {})]


def test_prediction_whose_entry_close_precedes_it_is_never_ingested(tmp_path):
    records = S.stream_records()
    records[1] = {**records[1], "run_at": "2026-10-01T20:30:00+00:00"}  # after the 10-01 close (20:00Z)
    log = tmp_path / "late.jsonl"
    S.write_chain(log, records)
    snap = _run(tmp_path, AFTER, log=log)["snapshot"]
    assert snap["counts"]["predictions"] == 9
    assert snap["streams"][S.STREAM]["activity"]["late_entry_refused"] == 1


def test_records_logged_after_the_run_instant_are_invisible(tmp_path):
    records = S.stream_records()
    records.append({**records[1], "prediction_id": f"{S.STREAM}:future", "run_at": "2026-10-05T14:00:00+00:00"})
    log = tmp_path / "future.jsonl"
    S.write_chain(log, records)
    snap = _run(tmp_path, AFTER, log=log)["snapshot"]
    assert snap["counts"]["predictions"] == 10
    assert snap["streams"][S.STREAM]["source_records_seen"] == 11
    assert snap["streams"][S.STREAM]["source_records_total"] == 12


# --- determinism ---------------------------------------------------------------------------


def test_same_inputs_give_byte_identical_ledgers(tmp_path):
    for name in ("a", "b"):
        _run(tmp_path, S.utc(2026, 10, 2, 15), board_dir=tmp_path / name)
        _run(tmp_path, AFTER, board_dir=tmp_path / name)
    for suffix in (".jsonl", ".anchors.jsonl"):
        a = (tmp_path / "a" / f"e2_scoreboard_e2-v1{suffix}").read_bytes()
        b = (tmp_path / "b" / f"e2_scoreboard_e2-v1{suffix}").read_bytes()
        assert a == b and a
    assert (tmp_path / "a" / "SCOREBOARD.md").read_bytes() == (tmp_path / "b" / "SCOREBOARD.md").read_bytes()


# --- append-only ---------------------------------------------------------------------------


def test_runs_only_append_and_never_rescore(tmp_path):
    _run(tmp_path, S.utc(2026, 10, 2, 15))
    path = tmp_path / "board" / "e2_scoreboard_e2-v1.jsonl"
    before = list(raw_lines(path))
    _run(tmp_path, AFTER)
    middle = list(raw_lines(path))
    _run(tmp_path, S.utc(2026, 10, 3, 23))
    after = list(raw_lines(path))
    assert middle[: len(before)] == before and after[: len(middle)] == middle
    kinds = [json.loads(line)["kind"] for line in after[len(middle):]]
    assert kinds == ["snapshot"]  # nothing new to ingest, resolve or score: never rescored
    ledger = Ledger(tmp_path / "board", "e2-v1")
    assert ledger.verify(manifest_sha256=S.MANIFEST_INFO["manifest_sha256"])["ok"]
    ids = [r["prediction"]["prediction_id"] for r in ledger.read_all() if r["kind"] == "prediction"]
    assert len(ids) == len(set(ids)) == 10


def test_edited_or_truncated_ledger_is_refused(tmp_path):
    _run(tmp_path, AFTER)
    path = tmp_path / "board" / "e2_scoreboard_e2-v1.jsonl"
    original = path.read_bytes()
    path.write_bytes(original.replace(b'"side":1', b'"side":-1', 1))
    with pytest.raises(ChainError, match="prev_sha256|canonical"):
        _run(tmp_path, S.utc(2026, 10, 3, 23))
    lines = original.split(b"\n")
    path.write_bytes(b"\n".join(lines[:-2]) + b"\n")  # drop the last record
    with pytest.raises(ChainError, match="truncated"):
        _run(tmp_path, S.utc(2026, 10, 3, 23))


def test_back_dated_run_is_refused(tmp_path):
    _run(tmp_path, AFTER)
    with pytest.raises(ChainError, match="back-dated"):
        _run(tmp_path, S.utc(2026, 10, 2, 15))


def test_ledger_started_by_other_code_is_refused(tmp_path):
    _run(tmp_path, AFTER)
    rules = S.rules()
    adapter = S.stream_adapter(tmp_path / "stream.jsonl", rules, S.prices())
    with pytest.raises(ChainError, match="different E2 manifest"):
        board.run(tmp_path / "board", [adapter], S.utc(2026, 10, 3, 23), rules=rules, cost_model=S.cost_model(),
                  manifest_info={"manifest_sha256": "e" * 64}, code_sha=S.CODE_SHA)


def test_changed_source_record_raises_an_alert_and_the_ledger_keeps_the_first_version(tmp_path):
    _run(tmp_path, S.utc(2026, 10, 2, 15))
    records = S.stream_records()
    records[1] = {**records[1], "call": {"kind": "direction", "side": -1}}  # the stream "rewrote" d1
    S.write_chain(tmp_path / "stream.jsonl", records)
    snap = _run(tmp_path, AFTER)["snapshot"]
    ledger = Ledger(tmp_path / "board", "e2-v1").read_all()
    alerts = [r for r in ledger if r["kind"] == "integrity_alert"]
    content = [a for a in alerts if a.get("prediction_id")]
    prefix = [a for a in alerts if a.get("stream")]
    assert len(content) == 1 and content[0]["prediction_id"] == f"{S.STREAM}:d1"
    assert len(prefix) == 1 and "no longer holds" in prefix[0]["detail"]
    assert snap["counts"]["integrity_alerts"] == 2
    kept = [r for r in ledger if r["kind"] == "prediction" and r["prediction"]["prediction_id"] == f"{S.STREAM}:d1"]
    assert len(kept) == 1 and kept[0]["prediction"]["call"]["side"] == 1
    _run(tmp_path, S.utc(2026, 10, 3, 23))  # the same divergence is not alerted twice
    assert len([r for r in Ledger(tmp_path / "board", "e2-v1").read_all() if r["kind"] == "integrity_alert"]) == 2


def test_broken_stream_chain_is_reported_not_ingested(tmp_path):
    log = tmp_path / "stream.jsonl"
    S.write_chain(log, S.stream_records())
    log.write_bytes(log.read_bytes().replace(b'"p":0.8', b'"p":0.9'))
    snap = _run(tmp_path, AFTER, log=log)["snapshot"]
    assert snap["streams"][S.STREAM]["ok"] is False
    assert "prev_sha256" in snap["streams"][S.STREAM]["error"]
    assert snap["counts"]["predictions"] == 0


# --- versioning: a new rule version is a new ledger, never an overwrite -------------------


def test_new_rule_version_writes_a_new_ledger_and_leaves_the_old_one_untouched(tmp_path):
    _run(tmp_path, AFTER)
    v1 = tmp_path / "board" / "e2_scoreboard_e2-v1.jsonl"
    v1_bytes, v1_anchor = v1.read_bytes(), (tmp_path / "board" / "e2_scoreboard_e2-v1.anchors.jsonl").read_bytes()
    rules_v2 = S.rules("e2-v2")
    rules_v2["rules"]["e2.direction.v1"]["note"] = "a changed rule is a new version"
    _run(tmp_path, S.utc(2026, 10, 3, 23), rules=rules_v2)
    assert v1.read_bytes() == v1_bytes
    assert (tmp_path / "board" / "e2_scoreboard_e2-v1.anchors.jsonl").read_bytes() == v1_anchor
    v2 = Ledger(tmp_path / "board", "e2-v2").read_all()
    assert v2[0]["kind"] == "header" and v2[0]["e2_version"] == "e2-v2"
    assert len([r for r in v2 if r["kind"] == "score"]) == 6  # history rescored under v2, in v2's own ledger


def test_pre_registration_outcomes_never_enter_an_official_aggregate(tmp_path):
    rules = S.rules()
    rules["registered_at"] = "2026-10-02T22:00:00+00:00"  # after these outcomes became observable
    snap = _run(tmp_path, AFTER, rules=rules)["snapshot"]
    assert snap["counts"]["official_scores"] == 0
    assert {r["bucket"] for r in snap["aggregates"]} == {"pre_registration"}
