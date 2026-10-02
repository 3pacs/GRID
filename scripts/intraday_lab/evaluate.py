"""Offline screening, chronological sessions, paired baseline and actual SPYU costs.

Never promotes a signal. Reports are exploratory until admitted to GRID's E2/E3
contracts and a witnessed forward registry. No database or production imports.
"""
from __future__ import annotations

from collections import defaultdict
import hashlib
import json
from pathlib import Path
import random
from statistics import mean

from .features import FEATURES, compute, number

HORIZONS = (60, 300, 900)
THRESHOLDS = {
    "price_momentum": 2., "es_depth": .3, "es_ofi": .5, "es_aggression": .3,
    "es_absorption": .3, "breadth": .3, "breadth_divergence": .3,
    "es_lead": 1., "nq_lead": 2., "constituent_lead": 1., "auction": .2,
    "range_compression": .4, "failed_breaks": 2., "gamma_range": .4,
}
RANGE_FEATURES = {"range_compression", "failed_breaks", "gamma_range"}
SPEC = {"version": "intraday_lab_v1_exploratory", "horizons": HORIZONS,
        "thresholds": THRESHOLDS, "range_half_width_bps": 10,
        "min_holdout_sessions": 60, "bootstrap_seed": 20261002,
        "family_size": len(FEATURES) * len(HORIZONS),
        "extra_roundtrip_cost_bps": [0, 5, 10],
        "strategy": "SPYU long when positive, otherwise cash; no synthetic short returns",
        "splits": "first 60% discovery, next 20% validation, final 20% reserved test; hidden by default"}


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def engine_hash():
    files = ("features.py", "evaluate.py")
    return hashlib.sha256(b"".join((Path(__file__).parent / p).read_bytes().replace(b"\r\n", b"\n")
                                   for p in files) + canonical(SPEC)).hexdigest()


def quote(packet, instrument):
    t = packet["decision_at"]
    matches = []
    for r in packet.get("observations", []):
        v = r.get("values", {})
        event, receipt = r.get("event_at"), r.get("available_at")
        if (r.get("instrument") == instrument and r.get("status") == "available"
                and r.get("clock") == "trusted_receipt" and r.get("source_id")
                and number(event) and number(receipt) and 0 <= t - event <= 10
                and event <= receipt <= t):
            matches.append(r)
    if not matches:
        return None
    latest = max(matches, key=lambda r: (r["event_at"], r["available_at"]))
    v = latest["values"]
    if instrument == "SPY":
        return v if number(v.get("price")) and v["price"] > 0 else None
    if not all(number(v.get(k)) for k in ("bid", "ask", "bid_size", "ask_size")):
        return None
    return v if 0 < v["bid"] <= v["ask"] and min(v["bid_size"], v["ask_size"]) > 0 else None


def outcome(start, future, seconds):
    target = start["decision_at"] + seconds
    path = [p for p in future if start["decision_at"] < p["decision_at"] <= target + 5
            and p["session"] == start["session"] and p.get("provenance") == "prospective"]
    if not path:
        return None
    endings = [p for p in path if target <= p["decision_at"] <= target + 5]
    if not endings:
        return None
    end = endings[0]
    path = [p for p in path if p["decision_at"] <= end["decision_at"]]
    if (abs(end["decision_at"] - target) > 5 or
            any(b["decision_at"] - a["decision_at"] > 10 for a, b in zip([start] + path, path))):
        return None
    first, last = quote(start, "SPY"), quote(end, "SPY")
    prices = [quote(p, "SPY") for p in path]
    if not first or not last or any(p is None for p in prices):
        return None
    moves = [10000 * (p["price"] / first["price"] - 1) for p in prices]
    entry, exit_quote = quote(start, "SPYU"), quote(end, "SPYU")
    # One share, marketable quotes; no fabricated 4x SPY return or short fill.
    pnl = 10000 * (exit_quote["bid"] / entry["ask"] - 1) if entry and exit_quote else None
    if not all(number(x) for x in moves) or (pnl is not None and not number(pnl)):
        return None
    return {"forward_bps": moves[-1], "max_up_bps": max(moves), "max_down_bps": min(moves),
            "stayed_in_range": max(abs(x) for x in moves) <= SPEC["range_half_width_bps"],
            "spyu_long_net_bps": pnl, "exit_at": end["decision_at"]}


def block_lower(values_by_session, family_size=42):
    """Session-block bootstrap; descriptive, not a registered hypothesis test."""
    blocks = [mean(v) for v in values_by_session.values() if v]
    if len(blocks) < 60:
        return None
    rng = random.Random(SPEC["bootstrap_seed"])
    draws = sorted(mean(rng.choices(blocks, k=len(blocks))) for _ in range(10000))
    return draws[int(len(draws) * .05 / family_size)]


def evaluate(packets, include_holdout=False):
    if any(not number(p.get("decision_at")) or not p.get("session") for p in packets):
        raise ValueError("invalid packet identity")
    packets = sorted(packets, key=lambda p: p["decision_at"])
    if len({p["decision_at"] for p in packets}) != len(packets):
        raise ValueError("duplicate decision timestamps")
    sessions = sorted({p["session"] for p in packets})
    a, b = int(len(sessions) * .6), int(len(sessions) * .8)
    splits = {s: "discovery" if i < a else "validation" if i < b else "holdout"
              for i, s in enumerate(sessions)}
    result = {"status": "EXPLORATORY_NO_VALIDATED_EDGE", "spec": SPEC, "engine_sha256": engine_hash(),
              "packet_sha256": hashlib.sha256(canonical(packets)).hexdigest(),
              "session_splits": splits, "holdout_examined": bool(include_holdout),
              "excluded_nonprospective": 0, "results": [],
              "limitations": ["Thresholds are uncalibrated research candidates, not learned probabilities.",
                              "No E2/E3 registration, power gate or external witness: no promotion.",
                              "Quoted spread plus 0/5/10 bp extra-cost sensitivities; no actual fills.",
                              "Short pressure is evaluated directionally; SPYU strategy is long/cash.",
                              "Actual SPYU bid/ask required for economic scoring."]}
    # Features only see each packet's admitted history; outcomes are in a separate pass.
    features = [compute(p) for p in packets]
    for seconds in HORIZONS:
        cases = []
        next_start = {}
        for i, p in enumerate(packets):
            if p.get("provenance") != "prospective":
                if seconds == HORIZONS[0]:
                    result["excluded_nonprospective"] += 1
                continue
            if splits[p["session"]] == "holdout" and not include_holdout:
                continue
            if p["decision_at"] < next_start.get(p["session"], 0):
                continue
            y = outcome(p, packets[i + 1:], seconds)
            if y is None:
                continue
            next_start[p["session"]] = y["exit_at"] + 1
            cases.append((p, features[i]["features"], y))
        for name in FEATURES:
            for split in ("discovery", "validation", "holdout"):
                if split == "holdout" and not include_holdout:
                    continue
                selected = [(p, f, y) for p, f, y in cases if splits[p["session"]] == split
                            and f[name]["status"] == "available" and f["price_momentum"]["status"] == "available"]
                active = [(p, f, y) for p, f, y in selected if abs(f[name]["value"]) >= THRESHOLDS[name]
                          and (name not in RANGE_FEATURES or f[name]["value"] >= THRESHOLDS[name])]
                entry = {"feature": name, "seconds": seconds, "split": split,
                         "eligible": len(selected), "active": len(active),
                         "sessions": len({p["session"] for p, _, _ in active}),
                         "status": "INSUFFICIENT_DATA", "economic_status": "UNAVAILABLE"}
                if active:
                    if name in RANGE_FEATURES:
                        entry["range_hit_rate"] = mean(float(y["stayed_in_range"]) for _, _, y in active)
                        entry["unconditional_range_rate"] = mean(float(y["stayed_in_range"]) for _, _, y in selected)
                        entry["status"] = "DESCRIPTIVE_RANGE_ONLY"
                    else:
                        directed = [(1 if f[name]["value"] > 0 else -1) * y["forward_bps"] for _, f, y in active]
                        entry["mean_signed_spy_bps"] = mean(directed)
                        entry["direction_hit_rate"] = mean(float(x > 0) for x in directed)
                        entry["status"] = "DESCRIPTIVE_DIRECTION_ONLY"
                        paired = defaultdict(list)
                        net = []
                        for p, f, y in active:
                            if y["spyu_long_net_bps"] is None:
                                continue
                            candidate = y["spyu_long_net_bps"] if f[name]["value"] > 0 else 0.
                            baseline = y["spyu_long_net_bps"] if f["price_momentum"]["value"] >= THRESHOLDS["price_momentum"] else 0.
                            paired[p["session"]].append(candidate - baseline)
                            net.append(candidate)
                        if net:
                            sensitivities = {}
                            for extra in SPEC["extra_roundtrip_cost_bps"]:
                                costed = []
                                excess = defaultdict(list)
                                for p, f, y in active:
                                    if y["spyu_long_net_bps"] is None:
                                        continue
                                    trade = y["spyu_long_net_bps"] - extra
                                    candidate = trade if f[name]["value"] > 0 else 0.
                                    baseline = trade if f["price_momentum"]["value"] >= THRESHOLDS["price_momentum"] else 0.
                                    costed.append(candidate)
                                    excess[p["session"]].append(candidate - baseline)
                                sensitivities[str(extra)] = {"mean_net_bps": mean(costed),
                                                            "paired_excess_bps": mean([mean(v) for v in excess.values()]),
                                                            "paired_excess_lower": block_lower(excess)}
                            entry.update(economic_status="DESCRIPTIVE_QUOTED_SPREAD_ONLY",
                                         economic_observations=len(net), mean_spyu_net_bps=mean(net),
                                         paired_excess_bps=mean([mean(v) for v in paired.values()]),
                                         paired_excess_lower=block_lower(paired),
                                         economic_sessions=len(paired), extra_cost_sensitivities=sensitivities)
                    if entry["sessions"] < 60:
                        entry["status"] += "_INSUFFICIENT_SESSIONS"
                result["results"].append(entry)
    return result
