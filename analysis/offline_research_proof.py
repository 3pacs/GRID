"""Offline, synthetic-only research contract proof. No registry or live wiring.

Reuses GRID's pure correlation primitive, never its DB fetcher or state updater.
Nominal IID Pearson p-values are appropriate only to this synthetic fixture;
this module deliberately refuses real evidence until its contracts are supplied.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
from scipy.stats import pearsonr

from analysis.hypothesis_tester import compute_lagged_correlation


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, allow_nan=False).encode()
    ).hexdigest()


def stamp(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("timestamps must carry a timezone")
    return result


@dataclass(frozen=True)
class Protocol:
    run_id: str
    features: tuple[str, ...]
    split: str
    end: str
    min_n: int = 30
    alpha: float = 0.05
    origin: str = "synthetic_fixture"

    def validate(self):
        if self.origin != "synthetic_fixture":
            raise ValueError("real PIT/source/session contracts are not implemented")
        if (
            not self.run_id
            or not self.features
            or len(set(self.features)) != len(self.features)
        ):
            raise ValueError("unique complete trial universe required")
        if (
            self.min_n < 30
            or not 0 < self.alpha <= 0.05
            or stamp(self.split) >= stamp(self.end)
        ):
            raise ValueError("invalid frozen protocol")


def excluded(feature):
    return any(
        token in feature.lower()
        for token in ("snap:", "llm", "telemetry", "pipeline", "astro")
    )


def validate_rows(rows, protocol, window):
    """Reject future features, crossing labels, overlap, duplicates and silent NaNs."""
    protocol.validate()
    previous_end = None
    horizon = None
    for row in rows:
        decision, end = stamp(row["decision_at"]), stamp(row["label_end"])
        if row.get("origin") != protocol.origin or set(row["features"]) != set(
            protocol.features
        ):
            raise ValueError("origin/universe mismatch")
        if end <= decision or (previous_end is not None and decision < previous_end):
            raise ValueError("overlapping or unordered outcomes")
        if horizon is not None and end - decision != horizon:
            raise ValueError("mixed outcome horizons")
        horizon, previous_end = end - decision, end
        if window == "discovery" and end >= stamp(protocol.split):
            raise ValueError("discovery label crosses holdout boundary")
        if window == "holdout" and (
            decision < stamp(protocol.split) or end >= stamp(protocol.end)
        ):
            raise ValueError("holdout outside frozen window")
        if (
            not end
            <= stamp(row["target_known_at"])
            < stamp(protocol.split if window == "discovery" else protocol.end)
        ):
            raise ValueError("outcome unavailable in evaluation window")
        if not math.isfinite(row["target"]):
            raise ValueError("nonfinite outcome")
        for feature in row["features"].values():
            if stamp(feature["known_at"]) > decision or not math.isfinite(
                feature["value"]
            ):
                raise ValueError("future or nonfinite feature")


def measure(rows, feature, min_n):
    if len(rows) < min_n:
        return {"n": len(rows), "r": None, "p": 1.0, "status": "insufficient_data"}
    x = pd.Series([row["features"][feature]["value"] for row in rows], dtype=float)
    y = pd.Series([row["target"] for row in rows], dtype=float)
    # max_lag=0 avoids a hidden search over lags. Every declared feature is a trial.
    result = compute_lagged_correlation(x, y, max_lag=0)
    if result.get("error"):
        return {"n": len(rows), "r": None, "p": 1.0, "status": result["error"]}
    return {
        "n": len(rows),
        "r": result["optimal_correlation"],
        "p": float(pearsonr(x, y).pvalue),
        "status": "tested",
    }


def corrected_p(p, total):
    """Bonferroni over the ENTIRE declared universe, including refused trials."""
    return min(1.0, p * total)


def discover(protocol, discovery_rows):
    """This API never receives holdout rows. Freeze its result before evaluation."""
    validate_rows(discovery_rows, protocol, "discovery")
    ledger = []
    for feature in protocol.features:
        result = (
            {"n": 0, "r": None, "p": 1.0, "status": "excluded_telemetry"}
            if excluded(feature)
            else measure(discovery_rows, feature, protocol.min_n)
        )
        adjusted = corrected_p(result["p"], len(protocol.features))
        ledger.append(
            {
                "trial_id": digest([protocol.run_id, feature]),
                "feature": feature,
                **result,
                "adjusted_p": adjusted,
                "selected": result["status"] == "tested" and adjusted <= protocol.alpha,
            }
        )
    payload = {
        "protocol": asdict(protocol),
        "discovery_sha256": digest(discovery_rows),
        "horizon_seconds": (
            (
                stamp(discovery_rows[0]["label_end"])
                - stamp(discovery_rows[0]["decision_at"])
            ).total_seconds()
            if discovery_rows
            else None
        ),
        "trial_count": len(ledger),
        "ledger": ledger,
        "method": "fixed lag0 Pearson; Bonferroni full declared universe; synthetic IID only",
        "state": "DISCOVERY_FROZEN",
        "promotion_allowed": False,
    }
    return {"payload": payload, "sha256": digest(payload)}


def evaluate_holdout(frozen, holdout_rows):
    payload = frozen["payload"]
    if digest(payload) != frozen["sha256"]:
        raise ValueError("frozen discovery manifest changed")
    protocol = Protocol(
        **{**payload["protocol"], "features": tuple(payload["protocol"]["features"])}
    )
    validate_rows(holdout_rows, protocol, "holdout")
    if (
        holdout_rows
        and payload["horizon_seconds"]
        != (
            stamp(holdout_rows[0]["label_end"]) - stamp(holdout_rows[0]["decision_at"])
        ).total_seconds()
    ):
        raise ValueError("holdout horizon differs from frozen discovery")
    selected = [trial for trial in payload["ledger"] if trial["selected"]]
    checks, candidates = [], []
    for trial in selected:
        result = measure(holdout_rows, trial["feature"], protocol.min_n)
        adjusted = corrected_p(result["p"], len(selected))
        survives = (
            result["status"] == "tested"
            and adjusted <= protocol.alpha
            and result["r"] * trial["r"] > 0
        )
        checks.append(
            {
                "trial_id": trial["trial_id"],
                **result,
                "adjusted_p": adjusted,
                "retrospective_survivor": survives,
            }
        )
        if survives:
            specification = {
                "feature": trial["feature"],
                "direction": 1 if trial["r"] > 0 else -1,
                "discovery_manifest": frozen["sha256"],
                "horizon_seconds": payload["horizon_seconds"],
                "forward_start_not_before": protocol.end,
                "forward_rule": "not implemented: prospective timestamped predictions required",
                "origin": protocol.origin,
            }
            candidates.append(
                {
                    "specification": specification,
                    "sha256": digest(specification),
                    "state": "FORWARD_EVIDENCE_PENDING",
                    "promotion_allowed": False,
                }
            )
    return {
        "discovery_manifest": frozen["sha256"],
        "holdout_sha256": digest(holdout_rows),
        "holdout_checks": checks,
        "candidates": candidates,
        "state": "SYNTHETIC_PROOF_ONLY",
        "promotion_allowed": False,
        "forward_evidence_count": 0,
    }


def write_once(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)


def run_proof(protocol, discovery_rows, holdout_rows, output):
    """Local receipt directory must be new: reruns cannot overwrite consumed evidence."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    frozen = discover(protocol, discovery_rows)
    write_once(output / "discovery-frozen.json", frozen)
    # Read back the persisted freeze; holdout cannot alter search or trial universe.
    result = evaluate_holdout(
        json.loads((output / "discovery-frozen.json").read_text()), holdout_rows
    )
    write_once(output / "holdout-result.json", result)
    return result
