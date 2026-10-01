"""Adapter for the S10 hypothesis-loop forward log (``/data/grid/paper_log/hypothesis_forward_v1/``).

Read-only. The S10 log (``analysis/research_forward_log.py``, rules in
``docs/paper_log/hypothesis-forward-v1-preregistration.md``) holds ``admission``
records (a frozen per-candidate plan), ``prediction`` records (the feature
value GRID held at each decision instant, logged before the label could be
known), ``outcome`` records (the label, read at or after its publication
time) and one ``verdict`` per candidate at the single pre-registered look.

Normalization: every non-excluded S10 prediction becomes one E2 prediction
(``call.kind == "signal"``, rule ``s10.ts_ic.v1``, unit = the candidate). Its
outcome is the S10 outcome record, re-checked point-in-time here (the
outcome must have been read at or after ``label_known_at``; the prediction
must have been logged before it; the feature must have been known by the
decision; the decision must follow the admission).

Disclosure: ``sealed_until_verdict``. The S10 pre-registration allows one
look per candidate, so E2 emits a candidate's score only once its S10
verdict record exists: the signed time-series IC over the same first
``min_n`` valid pairs, recomputed from the log and checked against the
verdict's ``rho``. Before that E2 reports activity counts only.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from evals.e2 import scoring
from evals.e2.adapters import SourceView, build_view
from evals.e2.chain import raw_lines, verify_source_chain
from evals.e2.records import LookAheadError, iso, parse_ts

STREAM = "s10_hypothesis_forward_v1"
LOG_FILENAME = "hypothesis_forward_v1.jsonl"
ANCHOR_FILENAME = "hypothesis_forward_v1.anchors.jsonl"


class S10Adapter:
    stream = STREAM

    def __init__(self, log_dir: Path, stream_rules: dict, rule: dict) -> None:
        self.path = Path(log_dir) / LOG_FILENAME
        self.anchor_path = Path(log_dir) / ANCHOR_FILENAME
        self.rules = stream_rules
        self.rule = rule

    def load(self, now: datetime) -> SourceView:
        pairs = verify_source_chain(self.path, prereg_sha256=self.rules["prereg_sha256"])
        view = build_view(STREAM, self.path, pairs, now)
        admissions, predictions, outcomes, verdicts = {}, {}, {}, {}
        for i, record in enumerate(view.records):
            kind, cid = record.get("kind"), record.get("candidate_id")
            if kind == "admission":
                admissions[cid] = i
            elif kind == "prediction":
                predictions.setdefault((cid, record["k"]), i)
            elif kind == "outcome":
                outcomes.setdefault((cid, record["k"]), i)
            elif kind == "verdict":
                verdicts.setdefault(cid, i)
        view.extra.update(admissions=admissions, predictions=predictions, outcomes=outcomes, verdicts=verdicts,
                          anchors=sum(1 for _ in raw_lines(self.anchor_path)))
        return view

    def _plan(self, view: SourceView, cid: str) -> tuple[dict, dict]:
        admission = view.records[view.extra["admissions"][cid]]
        return admission, admission["plan"]

    def predictions(self, view: SourceView) -> list[dict]:
        out = []
        for (cid, k), i in sorted(view.extra["predictions"].items()):
            record = view.records[i]
            if record.get("excluded") or cid not in view.extra["admissions"]:
                continue
            if parse_ts(record["run_at"]) >= parse_ts(record["label_known_at"]):
                continue  # logged too late to be a prediction (S10 itself excludes these)
            _, plan = self._plan(view, cid)
            out.append({
                "stream": STREAM,
                "family": plan["family"],
                "sector": self.rules.get("sector"),
                "prediction_id": f"{STREAM}:{cid}:k{k}",
                "issued_at": iso(parse_ts(record["run_at"])),
                "log_receipt": view.receipt(i, writer_code_sha=record.get("code_sha"),
                                            witness="stream hash chain + chained anchor file"),
                "target": {"series_id": plan["target"]["series_id"], "label": plan["target"]["label"],
                           "instrument_class": "not_tradable"},
                "horizon": {"label": f"{plan['horizon_sessions']}_sessions", "decision_at": record["decision_at"],
                            "ends_at": record["label_end"], "horizon_sessions": plan["horizon_sessions"], "k": k},
                "outcome_not_before": iso(parse_ts(record["label_known_at"])),
                "call": {"kind": "signal", "feature": record["feature"]["name"],
                         "value": record["feature"]["value"], "feature_known_at": record["feature"]["known_at"],
                         "direction": plan["direction"], "statistic": plan["statistic"]},
                "rule_id": "s10.ts_ic.v1",
                "unit": f"{STREAM}:{cid}",
            })
        return out

    def resolve(self, view: SourceView, pred: dict, now: datetime) -> dict | None:
        cid, k = pred["unit"].split(":", 1)[1], int(pred["horizon"]["k"])
        idx = view.extra["outcomes"].get((cid, k))
        if idx is None:
            return None  # S10 has not read the outcome yet (or is still inside its grace period)
        outcome = view.records[idx]
        known = parse_ts(outcome["label_known_at"])
        read_at = parse_ts(outcome["run_at"])
        if read_at < known:
            raise LookAheadError(f"{pred['prediction_id']}: S10 outcome read at {outcome['run_at']} before its "
                                 f"label was published ({outcome['label_known_at']})")
        if known > now:
            raise LookAheadError(f"{pred['prediction_id']}: outcome not observable at the run instant")
        admission, _ = self._plan(view, cid)
        receipt = {"price_source": f"store.observations latest-vintage panel: {pred['target']['series_id']}",
                   "panel_receipt_sha256": outcome.get("receipt"), "outcome_line_sha256": view.line_sha256[idx],
                   "outcome_line_index": idx, "outcome_run_at": outcome["run_at"],
                   "label_known_at": outcome["label_known_at"]}
        if outcome.get("excluded") or outcome.get("label") is None:
            return {"status": "void", "reason": outcome.get("exclusion_reason") or "target_missing",
                    "available_at": iso(known), "receipt": receipt, "outcome": None}
        problems = []
        feature_known = pred["call"].get("feature_known_at")
        decided = parse_ts(pred["horizon"]["decision_at"])
        if decided <= parse_ts(admission["run_at"]):
            problems.append("decision_not_after_admission")
        if feature_known is None or parse_ts(feature_known) > decided:
            problems.append("feature_not_known_at_decision")
        if not decided <= parse_ts(pred["issued_at"]) < known:
            problems.append("prediction_not_logged_between_decision_and_publication")
        if outcome["decision_at"] != pred["horizon"]["decision_at"]:
            problems.append("outcome_decision_mismatch")
        if pred["call"].get("value") is None:
            problems.append("feature_missing")
        if problems:
            return {"status": "void", "reason": "invalid_pair:" + ",".join(problems), "available_at": iso(known),
                    "receipt": receipt, "outcome": {"label": outcome["label"]}}
        return {"status": "resolved", "reason": None, "available_at": iso(known), "receipt": receipt,
                "outcome": {"label": float(outcome["label"]), "target_start": outcome.get("target_start"),
                            "target_end": outcome.get("target_end")}}

    def unit_scores(self, view: SourceView, ledger_state: dict, now: datetime) -> list[dict]:
        """One score per candidate, only once its S10 verdict exists (sealed until then)."""
        out = []
        for cid, vidx in sorted(view.extra["verdicts"].items()):
            unit = f"{STREAM}:{cid}"
            if unit in ledger_state["unit_scored"] or cid not in view.extra["admissions"]:
                continue
            verdict = view.records[vidx]
            admission, plan = self._plan(view, cid)
            members = sorted(
                (p for p in ledger_state["predictions"].values() if p.get("unit") == unit),
                key=lambda p: int(p["horizon"]["k"]),
            )
            pairs = []
            for p in members:
                res = ledger_state["resolutions"].get(p["prediction_id"])
                if res is not None and res["status"] == "resolved" and len(pairs) < int(plan["min_n"]):
                    pairs.append((int(p["horizon"]["k"]), float(p["call"]["value"]), float(res["outcome"]["label"])))
            x, y = [p[1] for p in pairs], [p[2] for p in pairs]
            stat = scoring.spearman(x, y) if plan["statistic"] == "spearman" else scoring.pearson(x, y)
            metrics = {}
            if stat is not None and len(pairs) >= int(self.rule["min_pairs"]):
                metrics["signed_ic"] = int(plan["direction"]) * stat
            verdict_rho = verdict.get("rho")
            out.append({
                "unit": unit,
                "rule_id": "s10.ts_ic.v1",
                "stream": STREAM,
                "family": plan["family"],
                "sector": self.rules.get("sector"),
                "horizon": f"{plan['horizon_sessions']}_sessions",
                "issued_at": members[0]["issued_at"] if members else iso(parse_ts(admission["run_at"])),
                "available_at": iso(parse_ts(verdict["run_at"])),
                "metrics": metrics,
                "detail": {
                    "n_pairs": len(pairs), "statistic": plan["statistic"], "direction": plan["direction"],
                    "verdict_state": verdict.get("state"), "verdict_n": verdict.get("n"),
                    "verdict_rho": verdict_rho, "verdict_p_one_sided": verdict.get("p_one_sided"),
                    "verdict_line_sha256": view.line_sha256[vidx],
                    "matches_verdict": (verdict.get("n") == len(pairs)
                                        and (verdict_rho is None) == (stat is None)
                                        and (stat is None or abs(stat - float(verdict_rho)) < 1e-9)),
                },
            })
        return out

    def activity(self, view: SourceView) -> dict:
        excluded: dict[str, int] = {}
        for i in view.extra["predictions"].values():
            record = view.records[i]
            if record.get("excluded"):
                reason = record.get("exclusion_reason") or "excluded"
                excluded[reason] = excluded.get(reason, 0) + 1
        states: dict[str, int] = {}
        for i in view.extra["verdicts"].values():
            state = view.records[i].get("state") or "?"
            states[state] = states.get(state, 0) + 1
        return {
            "candidates": len(view.extra["admissions"]),
            "predictions_logged": len(view.extra["predictions"]),
            "prediction_exclusions": excluded,
            "outcomes_logged": len(view.extra["outcomes"]),
            "verdicts": states,
            "source_anchor_lines": view.extra["anchors"],
            "sealed_candidates": len(set(view.extra["admissions"]) - set(view.extra["verdicts"])),
        }
