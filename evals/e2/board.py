"""The scoreboard job: ingest -> resolve -> score -> snapshot, appended to the E2 ledger.

One run, at instant ``now`` (the caller's clock; tests pass a fixed one):

1. verify the E2 manifest (the caller does this; :func:`run` takes its result)
   and the ledger chain + anchors; refuse a ledger started by other code;
2. refuse a run instant earlier than the ledger's last record (no back-dated runs);
3. for every adapter: load the stream log read-only (hash chain verified,
   records after ``now`` invisible), ingest new predictions (a prediction id
   is ingested once, ever; a changed record under a known id is an
   ``integrity_alert``, never an overwrite), resolve due outcomes
   point-in-time, and score them under their pre-registered rule;
4. append one ``snapshot`` with every aggregate (per stream, family, sector,
   horizon; windows all / last_20 / last_60; bootstrap and Wilson CIs);
5. regenerate the views (``latest_snapshot.json``, ``SCOREBOARD.md``) -- views,
   not anchors.

Everything written goes through :meth:`evals.e2.chain.Ledger.append_locked`.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from evals.e2 import scoring
from evals.e2.chain import ChainError, Ledger, digest
from evals.e2.records import LookAheadError, RecordError, iso, parse_ts, prediction_sha256, session_close_utc, validate_prediction

PER_PREDICTION_RULES = frozenset({"e2.direction.v1", "e2.probability.v1", "gex.level_hold.v1", "gex.h3_net_pnl.v1"})
METRIC_KIND = {"hit": "binary", "net_return": "sum", "brier": "mean", "log_loss": "mean",
               "rank_ic": "mean", "signed_ic": "mean"}


def ledger_state(records: Iterable[dict]) -> dict:
    state = {"predictions": {}, "prediction_sha": {}, "resolutions": {}, "scores": [], "unit_scored": set(),
             "alerts": set(), "last_run_at": None, "source_seen": {}}
    for r in records:
        kind = r.get("kind")
        if r.get("run_at"):
            state["last_run_at"] = r["run_at"]
        if kind == "prediction":
            p = r["prediction"]
            state["predictions"][p["prediction_id"]] = p
            state["prediction_sha"][p["prediction_id"]] = r["prediction_sha256"]
        elif kind == "resolution":
            state["resolutions"][r["prediction_id"]] = r
        elif kind == "score":
            state["scores"].append(r)
            state["unit_scored"].add(r["unit"])
        elif kind == "integrity_alert":
            state["alerts"].add((r.get("prediction_id") or r.get("stream"), r.get("observed_sha256")))
        elif kind == "snapshot":
            for name, s in (r.get("streams") or {}).items():
                if s.get("ok") and s.get("source_records_seen"):
                    state["source_seen"][name] = (s["source_records_seen"], s["source_head_sha256"])
    return state


def compute_metrics(pred: dict, res: dict, cost_model: dict, rules: dict) -> dict:
    rule, outcome, call = pred["rule_id"], res["outcome"], pred["call"]
    if rule == "e2.direction.v1":
        side = int(call["side"])
        metrics = {"hit": scoring.direction_hit(side, outcome["return"])}
        net = scoring.net_return(side, outcome["entry_price"], outcome["exit_price"],
                                 pred["target"]["instrument_class"], cost_model)
        if net is not None:
            metrics["net_return"] = net
        return metrics
    if rule == "e2.probability.v1":
        eps = float(rules["rules"][rule]["eps"])
        p, y = float(call["p"]), int(outcome["y"])
        return {"brier": scoring.brier(p, y), "log_loss": scoring.log_loss(p, y, eps)}
    if rule == "gex.level_hold.v1":
        return {"hit": 1 if outcome["held"] else 0}
    if rule == "gex.h3_net_pnl.v1":
        net = scoring.net_return(int(outcome["side"]), outcome["entry_price"], outcome["exit_price"],
                                 rules["rules"][rule]["instrument_class"], cost_model)
        return {"net_return": net, "hit": 1 if net > 0 else 0}
    raise RecordError(f"{rule} is not a per-prediction rule")


def _score_record(now: datetime, pred: dict, res: dict, metrics: dict, rules: dict) -> dict:
    registered = parse_ts(rules["registered_at"])
    return {
        "kind": "score", "run_at": iso(now), "unit": pred["prediction_id"], "prediction_ids": [pred["prediction_id"]],
        "rule_id": pred["rule_id"], "stream": pred["stream"], "family": pred["family"], "sector": pred["sector"],
        "horizon": pred["horizon"]["label"], "issued_at": pred["issued_at"], "available_at": res["available_at"],
        "official": parse_ts(res["available_at"]) > registered, "metrics": metrics, "role": role_of(pred),
    }


def role_of(pred: dict) -> str:
    """``control`` for placebo arms (reported, never pooled with candidates), else ``candidate``."""
    return "control" if pred["call"].get("arm") == "placebo" else "candidate"


def _rank_ic_units(stream: str, state: dict, now: datetime, rules: dict) -> list[dict]:
    """Score every closed rank-IC unit (all members resolved, past every member's exit close)."""
    units: dict[str, list[dict]] = {}
    for p in state["predictions"].values():
        if p["stream"] == stream and p["rule_id"] == "e2.rank_ic.v1" and p["unit"] not in state["unit_scored"]:
            units.setdefault(p["unit"], []).append(p)
    out = []
    min_names = int(rules["rules"]["e2.rank_ic.v1"]["min_names"])
    registered = parse_ts(rules["registered_at"])
    for unit, members in sorted(units.items()):
        closes = [session_close_utc(date.fromisoformat(p["horizon"]["exit_date"])) for p in members]
        if now < max(closes) or any(p["prediction_id"] not in state["resolutions"] for p in members):
            continue
        members = sorted(members, key=lambda p: p["prediction_id"])
        resolved = [(p, state["resolutions"][p["prediction_id"]]) for p in members]
        resolved = [(p, r) for p, r in resolved if r["status"] == "resolved"]
        scores = [float(p["call"]["score"]) for p, _ in resolved]
        rets = [float(r["outcome"]["return"]) for _, r in resolved]
        ic = scoring.spearman(scores, rets) if len(resolved) >= min_names else None
        available = max((parse_ts(r["available_at"]) for _, r in resolved), default=now)
        first = members[0]
        out.append({
            "kind": "score", "run_at": iso(now), "unit": unit, "prediction_ids": [p["prediction_id"] for p in members],
            "rule_id": "e2.rank_ic.v1", "stream": stream, "family": first["family"], "sector": first["sector"],
            "horizon": first["horizon"]["label"], "issued_at": min(p["issued_at"] for p in members),
            "available_at": iso(available), "official": available > registered,
            "metrics": {} if ic is None else {"rank_ic": ic},
            "detail": {"names": len(members), "resolved_names": len(resolved), "min_names": min_names},
        })
    return out


def _source_prefix_alert(stream: str, view, state: dict, now: datetime) -> dict | None:
    """The stream log must still hold the prefix E2 saw last run (same count, same head)."""
    seen = state["source_seen"].get(stream)
    if seen is None:
        return None
    count, head = seen
    if len(view.records) >= count and view.line_sha256[count - 1] == head:
        return None
    observed = view.line_sha256[count - 1] if len(view.records) >= count else f"truncated:{len(view.records)}"
    return {"kind": "integrity_alert", "run_at": iso(now), "stream": stream, "ledger_sha256": head,
            "observed_sha256": observed, "detail": f"the {stream} log no longer holds the {count}-record prefix "
            "E2 saw last run (rewritten or truncated upstream); already-ingested predictions are kept as logged"}


def _check_resolution(pred: dict, res: dict, now: datetime) -> None:
    if res.get("status") not in ("resolved", "void"):
        raise RecordError(f"{pred['prediction_id']}: resolution status must be resolved or void")
    available = parse_ts(res["available_at"])
    if available > now:
        raise RecordError(f"{pred['prediction_id']}: resolution available_at {res['available_at']} is after the run")
    if res["status"] == "resolved" and available < parse_ts(pred["outcome_not_before"]):
        raise RecordError(f"{pred['prediction_id']}: outcome observable before outcome_not_before")


def _process_stream(adapter, view, state: dict, new: list, now: datetime, rules: dict, cost_model: dict) -> dict:
    """Ingest, resolve and score one stream; appends to ``new`` and mutates ``state``."""
    alert = _source_prefix_alert(adapter.stream, view, state, now)
    if alert is not None and (adapter.stream, alert["observed_sha256"]) not in state["alerts"]:
        new.append(alert)
        state["alerts"].add((adapter.stream, alert["observed_sha256"]))
    ingested = 0
    for pred in adapter.predictions(view):
        validate_prediction(pred, rules)
        pid, sha = pred["prediction_id"], prediction_sha256(pred)
        known = state["prediction_sha"].get(pid)
        if known is not None:
            if known != sha and (pid, sha) not in state["alerts"]:
                alert = {"kind": "integrity_alert", "run_at": iso(now), "prediction_id": pid,
                         "ledger_sha256": known, "observed_sha256": sha,
                         "detail": "the stream now shows different content under an ingested id; "
                                   "the ledger keeps the first version"}
                new.append(alert)
                state["alerts"].add((pid, sha))
            continue
        timing = "before_outcome" if now < parse_ts(pred["outcome_not_before"]) else "after_outcome"
        new.append({"kind": "prediction", "run_at": iso(now), "prediction": pred,
                    "prediction_sha256": sha, "ingest_timing": timing})
        state["predictions"][pid], state["prediction_sha"][pid] = pred, sha
        ingested += 1
    resolved_now = 0
    for pid in sorted(state["predictions"]):
        pred = state["predictions"][pid]
        if pred["stream"] != adapter.stream or pid in state["resolutions"]:
            continue
        res = adapter.resolve(view, pred, now)
        if res is None:
            continue
        _check_resolution(pred, res, now)
        record = {"kind": "resolution", "run_at": iso(now), "prediction_id": pid, **res}
        new.append(record)
        state["resolutions"][pid] = record
        resolved_now += 1
        if res["status"] == "resolved" and pred["rule_id"] in PER_PREDICTION_RULES:
            score = _score_record(now, pred, res, compute_metrics(pred, res, cost_model, rules), rules)
            new.append(score)
            state["scores"].append(score)
            state["unit_scored"].add(score["unit"])
    for score in adapter.unit_scores(view, state, now) + _rank_ic_units(adapter.stream, state, now, rules):
        registered = parse_ts(rules["registered_at"])
        score = {"prediction_ids": [], "role": "candidate", **score, "kind": "score", "run_at": iso(now),
                 "official": parse_ts(score["available_at"]) > registered}
        new.append(score)
        state["scores"].append(score)
        state["unit_scored"].add(score["unit"])
    return {
        "ok": True, "source_log": view.path.name, "source_records_total": view.total_records,
        "source_records_seen": len(view.records), "source_head_sha256": view.head_sha256,
        "ingested_this_run": ingested, "resolved_this_run": resolved_now,
        "activity": adapter.activity(view),
    }


def run(board_dir: Path, adapters: list, now: datetime, *, rules: dict, cost_model: dict, manifest_info: dict,
        code_sha: str, write_views: bool = True) -> dict:
    """One scoreboard run. Returns ``{"appended": n, "snapshot": <snapshot record>}``."""
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    ledger = Ledger(board_dir, rules["version"])
    manifest_sha = manifest_info["manifest_sha256"]
    with ledger.locked():
        check = ledger.verify(manifest_sha256=manifest_sha)
        if not check["ok"]:
            raise ChainError(f"E2 ledger fails verification: {check['detail']}")
        records = ledger.read_all()
        state = ledger_state(records)
        if state["last_run_at"] and parse_ts(state["last_run_at"]) > now:
            raise ChainError(f"run instant {iso(now)} precedes the ledger's last record ({state['last_run_at']}): "
                             "back-dated runs are refused")
        new: list[dict] = []
        if not records:
            new.append({"kind": "header", "e2_version": rules["version"], "run_at": iso(now), "code_sha": code_sha,
                        "manifest_sha256": manifest_sha, "rules_sha256": digest(rules),
                        "cost_model_sha256": digest(cost_model), "registered_at": rules["registered_at"],
                        "writer": "python -m evals.e2 run (the only writer)"})
        streams: dict[str, dict] = {}
        for adapter in adapters:
            try:
                view = adapter.load(now)
            except (ChainError, OSError, ValueError, KeyError) as exc:
                streams[adapter.stream] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
                continue
            mark = len(new)
            try:
                streams[adapter.stream] = _process_stream(adapter, view, state, new, now, rules, cost_model)
            except (LookAheadError, RecordError, KeyError, TypeError, ValueError) as exc:
                # Everything this stream produced this run is discarded; other streams proceed.
                del new[mark:]
                state = ledger_state(records + new)
                error = f"{type(exc).__name__}: {exc}"
                streams[adapter.stream] = {"ok": False, "error": error}
                if isinstance(exc, LookAheadError):
                    observed = "lookahead:" + digest(error)
                    if (adapter.stream, observed) not in state["alerts"]:
                        new.append({"kind": "integrity_alert", "run_at": iso(now), "stream": adapter.stream,
                                    "ledger_sha256": None, "observed_sha256": observed,
                                    "detail": f"look-ahead refused, nothing from this stream was recorded: {error}"})
                        state["alerts"].add((adapter.stream, observed))
        snapshot = build_snapshot(state, streams, now, rules, cost_model)
        new.append(snapshot)
        ledger.append_locked(new, manifest_sha256=manifest_sha)
    if write_views:
        from evals.e2 import report

        _write_atomic(Path(board_dir) / "latest_snapshot.json",
                      json.dumps(snapshot, indent=2, sort_keys=True, allow_nan=False) + "\n")
        _write_atomic(Path(board_dir) / "SCOREBOARD.md", report.render_markdown(snapshot))
    return {"appended": len(new), "snapshot": snapshot}


def _write_atomic(path: Path, text: str) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def _window(rows: list[dict], window: str) -> list[dict]:
    rows = sorted(rows, key=lambda s: (s["issued_at"], s["unit"]))
    if window == "all":
        return rows
    return rows[-int(window.split("_")[1]):]


def build_snapshot(state: dict, streams: dict, now: datetime, rules: dict, cost_model: dict) -> dict:
    agg = rules["aggregation"]
    interim = {}
    for name, spec in rules["streams"].items():
        activity = (streams.get(name) or {}).get("activity") or {}
        # a stream with a one-time evaluation stays interim unless its activity proves otherwise
        interim[name] = bool(activity.get("interim", bool(spec.get("evaluation_after_valid_sessions"))))
    groups: dict[tuple, list[dict]] = {}
    for score in state["scores"]:
        bucket = "official" if score["official"] else "pre_registration"
        for keys in agg["group_keys"]:
            group = tuple((k, score.get(k)) for k in keys)
            for metric in score["metrics"]:
                groups.setdefault((bucket, score["rule_id"], metric, group), []).append(score)
    rows = []
    for (bucket, rule_id, metric, group), members in sorted(groups.items(), key=lambda kv: json.dumps(kv[0], default=str)):
        stream = dict(group)["stream"]
        for window in agg["windows"]:
            subset = [m for m in _window(members, window) if m["metrics"].get(metric) is not None]
            if window != "all" and len(subset) < int(window.split("_")[1]):
                continue  # a rolling window is reported only once it is full
            values = [s["metrics"][metric] for s in subset]
            summary = scoring.summarize(
                values, kind=METRIC_KIND[metric],
                seed_parts=(rule_id, metric, json.dumps(group), window, bucket), aggregation=agg)
            rows.append({"bucket": bucket, "rule_id": rule_id, "metric": metric, "group": dict(group),
                         "window": window, "interim": interim.get(stream, False),
                         "first_issued_at": subset[0]["issued_at"] if subset else None,
                         "last_issued_at": subset[-1]["issued_at"] if subset else None, **summary})
    counts = {"predictions": len(state["predictions"]), "resolutions": len(state["resolutions"]),
              "pending": len(set(state["predictions"]) - set(state["resolutions"])),
              "void_by_reason": {}, "scores": len(state["scores"]),
              "official_scores": sum(1 for s in state["scores"] if s["official"]),
              "integrity_alerts": len(state["alerts"])}
    for r in state["resolutions"].values():
        if r["status"] == "void":
            counts["void_by_reason"][r["reason"]] = counts["void_by_reason"].get(r["reason"], 0) + 1
    counts["void_by_reason"] = dict(sorted(counts["void_by_reason"].items()))
    return {"kind": "snapshot", "run_at": iso(now), "e2_version": rules["version"], "streams": streams,
            "counts": counts, "aggregates": rows, "aggregates_sha256": digest(rows),
            "rules_sha256": digest(rules), "cost_model_sha256": digest(cost_model),
            "label": "EARLY / LOW-N: forward evidence accrues slowly; interim rows are not a test result"}
