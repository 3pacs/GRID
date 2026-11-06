"""Controlled walk-forward evaluation of implemented Astrogrid mystical inputs.

Offline research only: current-vintage index data, no live ledger/model writes.
"""
from __future__ import annotations

import calendar
import json
from datetime import date
from pathlib import Path

import numpy as np

from analysis.offline_research_proof import bh_adjusted

ASSETS = ("SP500", "NASDAQCOM", "DJIA")
HORIZONS = (1, 5, 20)
CONTROL_SERIES = ("VIXCLS", "DGS10", "T10Y2Y", "BAMLH0A0HYM2")
LAMBDA = 100.0
BOOTSTRAPS = 1999
SEED = 20261106
BLOCK = 20


def ridge_predict(train_x, train_y, test_x):
    """Train-only scale and intercept; fixed regularization, no tuning."""
    mean, scale = train_x.mean(axis=0), train_x.std(axis=0)
    scale[scale < 1e-10] = 1
    x = np.clip((train_x - mean) / scale, -8, 8)
    future = np.clip((test_x - mean) / scale, -8, 8)
    target_mean = train_y.mean()
    weight = np.linalg.solve(x.T @ x + LAMBDA * np.eye(x.shape[1]), x.T @ (train_y - target_mean))
    return future @ weight + target_mean


def build_frame(panel, celestial, asset):
    days = panel["dates"]
    n = len(days)
    prices = np.asarray([panel["series"][asset][d] for d in days])
    if not np.isfinite(prices).all() or (prices <= 0).any():
        raise ValueError("Invalid index observation")
    macro = {}
    for name in CONTROL_SERIES:
        source = panel["series"][name]
        values, current, age = [], float("nan"), 999
        for d in days:
            age += 1
            if d in source:
                current, age = source[d], 0
            values.append(current if age <= 5 else float("nan"))
        macro[name] = np.asarray(values)
    sky_rows = {r["date"]: r for r in celestial["records"]}
    engine_names = list(sky_rows[days[0]]["engine"])
    raw_names = list(sky_rows[days[0]]["sky"])
    engines = np.asarray([[sky_rows[d]["engine"][k] for k in engine_names] for d in days])
    raw = np.asarray([[sky_rows[d]["sky"][k] for k in raw_names] for d in days])
    seer = np.asarray([sky_rows[d]["seer_direction"] for d in days])
    market = np.full((n, 42), np.nan)
    trend, high_vol, momentum = np.zeros(n), np.zeros(n), np.zeros(n)
    broad = {s: np.asarray([panel["series"][s][d] for d in days]) for s in ASSETS}
    for i in range(200, n):
        returns = prices[i-60:i+1][1:] / prices[i-60:i+1][:-1] - 1
        vol20, vol60 = np.std(returns[-20:]), np.std(returns)
        trend[i] = 1 if prices[i] > np.mean(prices[i-199:i+1]) else -1
        high_vol[i] = float(vol20 > 1.25 * vol60)
        momentum[i] = np.sign(prices[i] / prices[i-20] - 1)
        d = date.fromisoformat(days[i])
        annual = 2 * np.pi * (d.timetuple().tm_yday - 1) / 365.25
        features = [prices[i] / prices[i-h] - 1 for h in (1, 5, 20)]
        features += [vol20, vol60, prices[i] / np.mean(prices[i-199:i+1]) - 1]
        features += [broad[s][i] / broad[s][i-h] - 1 for s in ASSETS for h in (5, 20)]
        features += [v[i-2] for v in macro.values()]
        features += [v[i-2] - v[i-7] for v in macro.values()]
        features += [float(d.weekday() == j) for j in range(1, 5)]
        features += [np.sin(annual), np.cos(annual), trend[i], high_vol[i]]
        features += [float(d.month == m) for m in range(2, 13)]
        features += [float(d.day <= 3 or calendar.monthrange(d.year, d.month)[1] - d.day < 3),
                     (d - date(2017, 1, 1)).days / 365.25,
                     float(d.month in (3, 6, 9, 12) and calendar.monthrange(d.year, d.month)[1] - d.day < 5)]
        if len(features) != market.shape[1]:
            raise ValueError(f"Unexpected control feature dimension: {len(features)}")
        market[i] = features
    return {"dates": days, "prices": prices, "market": market, "engines": engines,
            "sky": raw, "engine_names": engine_names, "raw_names": raw_names,
            "trend": trend, "high_vol": high_vol, "momentum": momentum, "seer": seer}


def interaction_columns(values, frame):
    return np.column_stack((values, values * frame["trend"][:, None], values * frame["high_vol"][:, None]))


def walk_forward(frame, horizon):
    days, prices, market = frame["dates"], frame["prices"], frame["market"]
    n = len(days)
    labels = np.full(n, np.nan)
    labels[:n-horizon-1] = prices[horizon+1:] / prices[1:n-horizon] - 1
    valid = np.isfinite(market).all(axis=1) & np.isfinite(labels)
    names = [f"engine:{name}" for name in frame["engine_names"]] + ["all_engines", "all_sky"]
    additions = [frame["engines"][:, [i]] for i in range(12)] + [frame["engines"], frame["sky"]]
    variants = {name: np.column_stack((market, interaction_columns(x, frame))) for name, x in zip(names, additions, strict=True)}
    variants["market_only"] = market
    variants["mystical_only"] = frame["engines"]
    predictions = {name: np.full(n, np.nan) for name in variants}
    train_audit = []
    for year in range(2021, 2027):
        cutoff = f"{year}-01-01"
        train = np.asarray([i for i in range(n-horizon-1) if valid[i] and days[i+1+horizon] < cutoff])
        test = np.asarray([i for i in range(n) if valid[i] and days[i][:4] == str(year)])
        if len(train) < 500 or not len(test):
            continue
        assert max(days[i+1+horizon] for i in train) < cutoff
        train_audit.append({"year": year, "train_labels": len(train),
                            "last_label_end": max(days[i+1+horizon] for i in train), "test_rows": len(test)})
        for name, x in variants.items():
            predictions[name][test] = ridge_predict(x[train], labels[train], x[test])
    return labels, predictions, train_audit


def net_returns(direction, raw_return, cost_bps=5):
    return direction * raw_return - (direction != 0) * (cost_bps / 2 / 10000) * (2 + raw_return)


def bootstrap_means(values, positions, span, n_boot=BOOTSTRAPS):
    """Trading-session blocks; absent/nonselected dates retain zero counts."""
    sums, count = np.zeros((span, values.shape[1])), np.zeros(span)
    sums[positions] = values
    count[positions] = 1
    rng = np.random.default_rng(SEED)
    starts = rng.integers(0, span, (n_boot, int(np.ceil(span / BLOCK))))
    indexes = ((starts[:, :, None] + np.arange(BLOCK)) % span).reshape(n_boot, -1)[:, :span]
    return sums[indexes].sum(axis=1) / count[indexes].sum(axis=1)[:, None]


def case_stats(frame, labels, predictions, horizon, name, *, start="2024-01-01", end="2026-11-01"):
    candidates = [i for i, d in enumerate(frame["dates"]) if start <= d < end
                  and np.isfinite(labels[i]) and np.isfinite(predictions["market_only"][i])
                  and np.isfinite(predictions[name][i]) and frame["dates"][i+1+horizon] < end]
    retained, next_entry = [], -1
    for i in candidates:
        if i+1 >= next_entry:
            retained.append(i)
            next_entry = i+1+horizon
    idx = np.asarray(retained)
    if len(idx) == 0:
        return {"events": 0, "eligible": False, "bootstrap_p": 1.0}
    target = labels[idx]
    direction = np.sign(predictions[name][idx])
    baseline = np.sign(predictions["market_only"][idx])
    current_net, baseline_net = net_returns(direction, target), net_returns(baseline, target)
    delta = current_net - baseline_net
    span = candidates[-1] - candidates[0] + 1
    boot = bootstrap_means(np.column_stack((current_net, baseline_net, delta)), idx - candidates[0], span)
    mean_delta = float(delta.mean())
    p = float((1 + np.count_nonzero(abs(boot[:, 2] - mean_delta) >= abs(mean_delta))) / (len(boot) + 1))
    intervals = np.quantile(boot, [.025, .975], axis=0) * 10000
    regimes = {}
    for trend in (-1, 1):
        for high in (0, 1):
            mask = (frame["trend"][idx] == trend) & (frame["high_vol"][idx] == high)
            regimes[f"{'up' if trend > 0 else 'down'}_trend_{'high' if high else 'normal'}_vol"] = {
                "events": int(mask.sum()), "net_bps": float(current_net[mask].mean() * 10000) if mask.any() else None,
                "advantage_bps": float(delta[mask].mean() * 10000) if mask.any() else None}
    return {"events": len(idx), "first_decision": frame["dates"][idx[0]], "last_decision": frame["dates"][idx[-1]],
            "eligible": len(idx) >= 100 and len(idx) >= 30,
            "mean_gross_bps": float(np.mean(direction * target) * 10000),
            "mean_net_bps": float(current_net.mean() * 10000),
            "net_95ci_bps": intervals[:, 0].tolist(),
            "market_only_net_bps": float(baseline_net.mean() * 10000),
            "paired_advantage_bps": mean_delta * 10000,
            "paired_95ci_bps": intervals[:, 2].tolist(), "bootstrap_p": p,
            "direction_accuracy": float(np.mean(direction == np.sign(target))),
            "forecast_mse": float(np.mean((predictions[name][idx] - target)**2)),
            "market_only_mse": float(np.mean((predictions["market_only"][idx] - target)**2)),
            "baselines_net_bps": {"cash": 0.0,
                                  "always_long": float(net_returns(np.ones(len(idx)), target).mean() * 10000),
                                  "price_momentum": float(net_returns(frame["momentum"][idx], target).mean() * 10000),
                                  "random_expected": float((-.00025 * (2 + target)).mean() * 10000),
                                  "unmixed_seer": float(net_returns(frame["seer"][idx], target).mean() * 10000)},
            "regimes_descriptive": regimes,
            "cost_sensitivity": {str(c): float(net_returns(direction, target, c).mean() * 10000) for c in (0, 5, 10, 20)}}


def evaluate(panel_path: Path, sky_path: Path):
    panel, celestial = json.loads(panel_path.read_text()), json.loads(sky_path.read_text())
    cases, audits, diagnostics = [], {}, {}
    for asset in ASSETS:
        frame = build_frame(panel, celestial, asset)
        diagnostics[asset] = {"sessions": len(frame["dates"]),
                              "finite_market_rows": int(np.isfinite(frame["market"]).all(axis=1).sum()),
                              "engine_variance": dict(zip(frame["engine_names"], frame["engines"].var(axis=0).tolist(), strict=True)),
                              "sky_features": len(frame["raw_names"])}
        for horizon in HORIZONS:
            labels, predictions, audit = walk_forward(frame, horizon)
            audits[f"{asset}:{horizon}"] = audit
            for name in predictions:
                if name in ("market_only", "mystical_only"):
                    continue
                result = case_stats(frame, labels, predictions, horizon, name)
                result.update(asset=asset, horizon_sessions=horizon, augmentation=name)
                result["reference"] = case_stats(frame, labels, predictions, horizon, name, start="2021-01-01", end="2024-01-01")
                cases.append(result)
    if len(cases) != 126:
        raise ValueError("Declared hypothesis family changed")
    adjusted = bh_adjusted([r["bootstrap_p"] if r["eligible"] else 1.0 for r in cases])
    for row, q in zip(cases, adjusted, strict=True):
        row["bh_q"] = float(q)
        row["historical_candidate"] = bool(row["eligible"] and q <= .05 and row["paired_95ci_bps"][0] > 0 and row["net_95ci_bps"][0] > 0)
    return {"version": "astrogrid-celestial-equity-v1", "origin": "current_vintage_walk_forward_historical_research",
            "cases": cases, "training_audit": audits, "input_diagnostics": diagnostics,
            "historical_candidates": sum(r["historical_candidate"] for r in cases),
            "eligible_tests": sum(r["eligible"] for r in cases), "declared_tests": 126,
            "source": panel["source"], "engine_code_sha256": celestial["code_sha256"],
            "input_limitations": celestial["limitations"]}
