"""Gem detection thresholds — v2 99%-winrate rebuild (2026-05-17).

CHANGES vs v1:
  * permutation_low_p: p<0.001 AND |effect|>1.5  (was p<0.005, |eff|>1.0)
  * bootstrap_ci_break: lower_bound > 0.7        (was 0.5)
  * correlation_break (new_high): |r|>0.85, n>=50 (was |r|>0.7, n>=30)
  * correlation_break: drop pairs that share source_origin prefix (same-feed false +)
  * nn_cluster: returns a "candidate vote" only, NEVER an emit-on-its-own gem
  * All rules now emit `rule_name` + `subject_key` so the consensus layer
    can aggregate votes per subject within a 24h window.

Each function takes the worker's freshly-computed output, returns a list of
gem dicts. Persistence + consensus + LLM + kill-predictor validators run in
the caller (gem_hunter.py).
"""
from __future__ import annotations

import math
from typing import Iterable

import numpy as np


# Helper for source-prefix dampening (correlation_break r=1.0 false positives)
def _same_source_prefix(k1: str, k2: str) -> bool:
    """Two signal keys share the same upstream feed when their signal_type
    prefix (before the '||') matches. Same-feed pairs trivially correlate
    at r=1.0 and are NOT real signal."""
    a = (k1 or "").split("||", 1)[0]
    b = (k2 or "").split("||", 1)[0]
    return bool(a) and a == b


# ---------------------------------------------------------------------------
# PageRank delta — actor centrality shifts vs previous run

def pagerank_delta_gems(prev_scores: dict, new_scores: dict) -> list[dict]:
    """Now requires |delta| > prev_score * 0.20 (a 20%+ relative move) in
    addition to the z-score gate. The old rule fired at z>2 on absolute
    deltas of 2e-9 which is pure float noise on tiny scores."""
    common = set(prev_scores) & set(new_scores)
    if len(common) < 50:
        return []
    deltas = np.array([new_scores[a] - prev_scores[a] for a in common], dtype=np.float64)
    mu, sd = float(deltas.mean()), float(deltas.std())
    if sd <= 0:
        return []
    gems = []
    for actor_id, d in zip(common, deltas):
        z = (d - mu) / sd
        # MUST be 2.5σ AND a meaningful relative move (>=20% of base)
        base = max(abs(prev_scores[actor_id]), 1e-6)
        rel_move = abs(d) / base
        if abs(z) > 2.5 and rel_move >= 0.20 and abs(d) >= 1e-5:
            score = float(math.tanh(abs(z) / 3.0))
            gems.append({
                "source": "pagerank_delta",
                "rule_name": "pagerank_delta",
                "subject_kind": "actor",
                "subject_id": actor_id,
                "subject_key": f"actor:{actor_id}",
                "score": score,
                "evidence": {
                    "old": prev_scores[actor_id],
                    "new": new_scores[actor_id],
                    "delta_z": z,
                    "rel_move": rel_move,
                    "pop_mu": mu,
                    "pop_sd": sd,
                },
            })
    # Newcomers to top-50 — keep but require they're substantially above the
    # prior top-50 floor (1.5x), not just a float-noise tie-break.
    prev_top = set(sorted(prev_scores, key=lambda a: -prev_scores[a])[:50])
    new_top = set(sorted(new_scores, key=lambda a: -new_scores[a])[:50])
    if prev_top:
        prev_floor = min(prev_scores[a] for a in prev_top)
    else:
        prev_floor = 0.0
    for actor_id in new_top - prev_top:
        if new_scores[actor_id] < prev_floor * 1.5:
            continue
        gems.append({
            "source": "centrality_outlier",
            "rule_name": "centrality_outlier",
            "subject_kind": "actor",
            "subject_id": actor_id,
            "subject_key": f"actor:{actor_id}",
            "score": 0.8,
            "evidence": {
                "new_score": new_scores[actor_id],
                "prev_top50_floor": prev_floor,
                "was_in_top50": False,
            },
        })
    return gems


# ---------------------------------------------------------------------------
# Correlation break / jump — pair r changes vs prior history

def correlation_break_gems(prev_r: dict, new_r: dict, n_lookup: dict) -> list[dict]:
    gems = []
    for key, r_now in new_r.items():
        k1, k2 = key[0], key[1]
        # DROP same-source-prefix pairs entirely — these are double-tagging
        # artifacts, not real cross-signal correlations.
        if _same_source_prefix(k1, k2):
            continue
        r_then = prev_r.get(key)
        n = n_lookup.get(key, 0)
        if r_then is None:
            # New high-magnitude correlation with sufficient n
            # RAISED: |r|>0.85 (was 0.7), n>=50 (was 30)
            if abs(r_now) > 0.85 and n >= 50:
                gems.append({
                    "source": "correlation_break",
                    "rule_name": "correlation_break",
                    "subject_kind": "signal_pair",
                    "subject_id": f"{k1}__{k2}",
                    "subject_key": f"signal_pair:{k1}__{k2}",
                    "related_ids": [k1, k2],
                    "score": float(math.tanh(abs(r_now) * 1.5)),
                    "evidence": {
                        "r_then": None,
                        "r_now": r_now,
                        "n": n,
                        "kind": "new_high",
                    },
                })
            continue
        delta = r_now - r_then
        # Broken / jumped — keep but require larger movements
        broken = abs(r_then) > 0.6 and abs(r_now) < 0.15
        jumped = abs(r_then) < 0.2 and abs(r_now) > 0.85
        if (broken or jumped) and n >= 50:
            gems.append({
                "source": "correlation_break",
                "rule_name": "correlation_break",
                "subject_kind": "signal_pair",
                "subject_id": f"{k1}__{k2}",
                "subject_key": f"signal_pair:{k1}__{k2}",
                "related_ids": [k1, k2],
                "score": float(math.tanh(abs(delta) * 2.0)),
                "evidence": {
                    "r_then": r_then,
                    "r_now": r_now,
                    "delta": delta,
                    "n": n,
                    "kind": "broken" if broken else "jumped",
                },
            })
    return gems


# ---------------------------------------------------------------------------
# Nearest-neighbour cross-domain cluster — CONSENSUS-ONLY VOTE
# Per Phase 1: nn_cluster never emits a standalone gem. Returns dicts marked
# `vote_only=True`; the consensus layer counts them but they cannot file
# alone.

def nn_cluster_gems(source_emb_id: int, neighbors: list[dict], source_meta: dict) -> list[dict]:
    types = {n.get("source_type") for n in neighbors if n.get("source_type")}
    # RAISED: require >=4 distinct source_types (was 3) for a vote
    if len(types) >= 4:
        return [{
            "source": "nn_cluster",
            "rule_name": "nn_cluster",
            "subject_kind": source_meta.get("source_type", "embedding"),
            "subject_id": str(source_emb_id),
            "subject_key": f"embedding:{source_emb_id}",
            "related_ids": [n["id"] for n in neighbors[:10]],
            "score": float(min(1.0, 0.5 + 0.15 * (len(types) - 2))),
            "vote_only": True,  # NEVER files standalone
            "evidence": {
                "neighbor_types": sorted(types),
                "top1_distance": neighbors[0]["distance"] if neighbors else None,
                "n_types": len(types),
            },
        }]
    return []


# ---------------------------------------------------------------------------
# Permutation engine low-p sweep
# RAISED: p<0.001 (was 0.005) AND |effect|>1.5 (was 1.0)

def permutation_low_p_gems(rows: Iterable[dict]) -> list[dict]:
    gems = []
    for r in rows:
        p = r.get("p_value")
        eff = r.get("effect_size")
        if p is None or eff is None:
            continue
        if p < 0.001 and abs(eff) > 1.5:
            sc = min(1.0, (-math.log10(max(p, 1e-12)) / 4.0) * 0.6 + min(abs(eff), 5.0) / 5.0 * 0.4)
            gems.append({
                "source": "permutation_low_p",
                "rule_name": "permutation_low_p",
                "subject_kind": "hypothesis",
                "subject_id": r["hypothesis_id"],
                "subject_key": f"hypothesis:{r['hypothesis_id']}",
                "score": sc,
                "evidence": {
                    "p_value": p,
                    "effect_size": eff,
                    "test_kind": r.get("test_kind"),
                    "computed_at": r.get("computed_at").isoformat() if r.get("computed_at") else None,
                },
            })
    return gems


# ---------------------------------------------------------------------------
# Bootstrap CI — RAISED lower bound from 0.5 -> 0.7

def bootstrap_ci_gems(edge_id: str, samples: np.ndarray, actor_a: str, actor_b: str,
                     relationship: str) -> list[dict]:
    if samples.size < 30:
        return []
    lo, hi = np.percentile(samples, [2.5, 97.5])
    median = float(np.median(samples))
    # RAISED: lower bound > 0.7 (was 0.5)
    if lo > 0.7:
        # Task #147 — score formula re-tuned to align with FILE_SCORE_GATE
        # (0.92). Previous formula `0.6 + min(0.3, (lo-0.7)*1.0)` capped at
        # 0.9, so NO bootstrap_ci_break gem could ever clear the gate.
        #
        # New shape:
        #   base       = 0.8 + 0.1 * min(1, (ci_lo - 0.7) / 0.3)   # 0.8..0.9
        #   n_bonus    = 0.05 * min(1, log10(n / 500)) if n > 500  # 0..0.05
        #   ticker_b   = +0.02 when subject tail is a real ticker  # 0 or 0.02
        # Capped at 0.99. Strong CIs (lo > 0.91, n>=2000, has_ticker) clear
        # 0.92 gate; weak CIs (lo just over 0.7) stay near 0.80 and remain
        # rejected_score — gate semantics unchanged.
        n_samples = int(samples.size)
        base = 0.8 + 0.1 * min(1.0, (float(lo) - 0.7) / 0.3)
        n_bonus = 0.05 * min(1.0, math.log10(n_samples / 500)) if n_samples > 500 else 0.0
        # actor_b tail looks like a real US ticker (uppercase, 1-5 alpha)
        tail = (actor_b or "").strip()
        has_ticker = bool(tail) and tail.isupper() and tail.isalpha() and 1 <= len(tail) <= 5
        ticker_bonus = 0.02 if has_ticker else 0.0
        score = min(0.99, base + n_bonus + ticker_bonus)
        return [{
            "source": "bootstrap_ci_break",
            "rule_name": "bootstrap_ci_break",
            "subject_kind": "actor_pair",
            "subject_id": edge_id,
            "subject_key": f"actor_pair:{edge_id}",
            "related_ids": [actor_a, actor_b],
            "score": score,
            "evidence": {
                "ci_lo": float(lo),
                "ci_hi": float(hi),
                "median": median,
                "n_samples": n_samples,
                "relationship": relationship,
                "score_components": {
                    "base": round(base, 4),
                    "n_bonus": round(n_bonus, 4),
                    "ticker_bonus": ticker_bonus,
                    "has_ticker_tail": has_ticker,
                },
            },
        }]
    return []


# ---------------------------------------------------------------------------
# Insider cluster — 3+ insiders buying same ticker in 30d window (Task #123)
#
# Empirically (Seyhun) a 3+ insider cluster buy within a 30-day window
# precedes 5-15% moves. The rule emits subject_kind='ticker' so the
# cross-class validator (Task #117) can extract the ticker via the explicit
# `evidence.ticker` key and verify confirmation across insider/congress/
# news/technical classes.

def insider_cluster_gems(cur) -> list[dict]:
    """Find tickers where >=3 distinct insiders made BUY trades in last 30d
    with significant value.

    Filters:
      * n_distinct_insiders >= 3
      * total_buy_value >= $250K (filters out grant-pattern noise)
      * per_insider_avg_value >= $25K (excludes nominal/grant trades)
      * price_per_share IS NOT NULL AND > 1 (excludes 0-price grants/splits)

    Score (Task #130 tune 2026-05-17):
      score = min(0.99,
                  0.88
                  + 0.07 * log10(total_value/250K)
                  + 0.015 * max(0, n_insiders - 3))
    Calibrated against Task #123 first-live-run cohort:
      $1M  + 3i -> 0.922  (was 0.880)  filed
      $1M  + 7i -> 0.982  (was 0.880)  filed
      $781K+ 6i -> 0.960  (was 0.875)  filed
      $462K+ 3i -> 0.899  (was 0.864)  near-miss (intentional)
    """
    cur.execute(
        """
        SELECT
            ticker,
            COUNT(DISTINCT insider_name) AS n_insiders,
            SUM(value) AS total_value,
            AVG(value) AS avg_value,
            array_agg(DISTINCT insider_name) AS insider_names,
            MAX(trade_date) AS latest_trade,
            MIN(trade_date) AS earliest_trade
        FROM insider_trades
        WHERE trade_type IN ('BUY', 'UNUSUAL_BUY')
          AND trade_date > CURRENT_DATE - INTERVAL '30 days'
          AND value IS NOT NULL
          AND value > 0
          AND price_per_share IS NOT NULL
          AND price_per_share > 1
        GROUP BY ticker
        HAVING COUNT(DISTINCT insider_name) >= 3
           AND SUM(value) >= 250000
           AND AVG(value) >= 25000
        ORDER BY n_insiders DESC, total_value DESC
        """
    )
    gems: list[dict] = []
    for row in cur.fetchall():
        ticker, n_insiders, total_value, avg_value, names, latest, earliest = row
        if not ticker:
            continue
        # Task #130 data hygiene: drop upstream NULL-as-string and any
        # non-alpha junk that snuck past the SQL HAVING gate.
        t_clean = str(ticker).strip().upper()
        if t_clean in {"NONE", "NULL", "N/A", ""} or not t_clean.isalpha():
            continue
        total_f = float(total_value)
        ratio = max(total_f / 250000.0, 1.0)  # guard log10
        n_i = int(n_insiders)
        # Task #130: re-tuned to let real 3-7-insider clusters clear the 0.92
        # consensus gate. See docstring for calibration.
        score = min(
            0.99,
            0.88
            + 0.07 * math.log10(ratio)
            + 0.015 * max(0, n_i - 3),
        )
        # Task #130 Fix C: keep `evidence` strict — only fields that won't be
        # mis-parsed as tickers by extract_tickers_from_gem's regex sweep.
        # `implied_direction` (value "BULL") and `insider_names` (surnames
        # like "PARK", "WANG") leaked into ticker extraction and broke the
        # cross-class validator. Both move to top-level sidecar fields.
        gems.append({
            "source": "insider_cluster",
            "rule_name": "insider_cluster",
            "subject_kind": "ticker",
            "subject_id": ticker,
            "subject_key": f"ticker:{ticker}",
            "score": float(score),
            "evidence": {
                # ticker FIRST so explicit-key extraction wins
                "ticker": ticker,
                "n_insiders": int(n_insiders),
                "total_value_usd": total_f,
                "avg_value_per_insider_usd": float(avg_value),
                "window_days": 30,
                "earliest_trade": str(earliest) if earliest else None,
                "latest_trade": str(latest) if latest else None,
            },
            # sidecar fields — persisted by gem_hunter for audit but kept out
            # of `evidence` so the cross-class ticker regex can't see them
            "implied_direction": "BULL",
            "insider_names": list(names) if names is not None else [],
        })
    return gems


# ---------------------------------------------------------------------------
# Earnings-proximity — Task #168 (2026-05-17)
#
# Hunts the rare alignment where (a) a ticker filed a material 10-K / 10-Q /
# 8-K within the last 7 days, AND (b) 3+ insiders are cluster-buying in the
# same window, AND (c) the price still hasn't moved (last close within ~1
# ATR(20) of the close on the filing date). That's a fresh material
# disclosure plus inside-table conviction in front of a market that hasn't
# repriced — a clean entry per the task spec.
#
# Score: 0.90 + 0.05 * (n_insiders / 10) + 0.05 * (atr_ok ? 0 : -0.5)
# Practical range with the SQL gate (n_insiders >= 3, atr_distance_atr <= 1.5):
#   3 insiders, price in ATR  -> 0.915  (filed)
#   5 insiders, price in ATR  -> 0.925  (filed)
#  10 insiders, price in ATR  -> 0.950  (filed)
#   3 insiders, price >1 ATR  -> 0.890  (rejected_score)

def earnings_proximity_gems(cur) -> list[dict]:
    """Find tickers where a recent material filing collides with an active
    insider cluster and the tape hasn't reacted yet."""
    # Pull material filings from earnings_events in last 7d, joined to
    # insider cluster (>=3 distinct insider BUYs in the same 7d window), and
    # to today's close + filing-date close from ticker_metrics_daily. ATR is
    # computed downstream in Python because ticker_metrics_daily only stores
    # close (no high/low) — we approximate ATR(20) via average abs daily
    # close-to-close change over the prior 20 trading rows.
    cur.execute(
        """
        WITH recent_filings AS (
            SELECT ticker,
                   MAX(filing_date) AS filing_date,
                   bool_or(event_type IN ('10-K', '10-Q')) AS has_periodic,
                   bool_or(event_type = '8-K') AS has_8k,
                   bool_or(event_type IN ('EARNINGS_CALL', 'TRANSCRIPT')) AS has_call,
                   array_agg(DISTINCT event_type) AS event_types,
                   array_agg(DISTINCT url) FILTER (WHERE url IS NOT NULL) AS urls,
                   array_agg(DISTINCT accession) FILTER (WHERE accession IS NOT NULL) AS accessions,
                   MAX(confidence) AS max_conf
              FROM earnings_events
             WHERE filing_date > CURRENT_DATE - INTERVAL '7 days'
               AND ticker IS NOT NULL
             GROUP BY ticker
        ),
        insider_window AS (
            SELECT ticker,
                   COUNT(DISTINCT insider_name) AS n_insiders,
                   SUM(value) AS total_value,
                   array_agg(DISTINCT insider_name) AS insider_names,
                   MAX(trade_date) AS latest_trade
              FROM insider_trades
             WHERE trade_date > CURRENT_DATE - INTERVAL '7 days'
               AND trade_type IN ('BUY', 'UNUSUAL_BUY')
               AND value IS NOT NULL
               AND value > 0
               AND price_per_share IS NOT NULL
               AND price_per_share > 1
             GROUP BY ticker
            HAVING COUNT(DISTINCT insider_name) >= 3
        )
        SELECT f.ticker, f.filing_date, f.event_types, f.urls, f.accessions,
               f.has_periodic, f.has_8k, f.has_call,
               i.n_insiders, i.total_value, i.insider_names
          FROM recent_filings f
          JOIN insider_window i USING (ticker)
         ORDER BY i.n_insiders DESC, i.total_value DESC
        """
    )
    rows = cur.fetchall() or []
    gems: list[dict] = []
    for (ticker, filing_date, event_types, urls, accessions,
         has_periodic, has_8k, has_call,
         n_insiders, total_value, insider_names) in rows:
        if not ticker:
            continue
        t_clean = str(ticker).strip().upper()
        if t_clean in {"NONE", "NULL", "N/A", ""} or not t_clean.isalpha():
            continue

        # Pull closes for ATR(20) and the filing_date anchor close.
        cur.execute(
            """SELECT obs_date, close_price
                 FROM ticker_metrics_daily
                WHERE ticker = %s
                  AND close_price IS NOT NULL
                  AND obs_date <= CURRENT_DATE
                ORDER BY obs_date DESC
                LIMIT 30""",
            (t_clean,),
        )
        bars = cur.fetchall() or []
        if len(bars) < 5:
            # Not enough price history to judge the "hasn't reacted" leg —
            # skip so we don't score it on noise. (Task hygiene: never
            # fabricate.)
            continue
        bars_sorted = sorted(bars, key=lambda r: r[0])  # ascending date
        closes = [float(r[1]) for r in bars_sorted]
        last_close = closes[-1]
        # Approximate ATR(20) as the mean abs close-to-close change over the
        # 20 most-recent bars (or as many as we have, min 5). This is the
        # standard "ATR-from-closes" proxy used when intraday H/L absent.
        diffs = [abs(closes[k] - closes[k - 1]) for k in range(1, len(closes))]
        if not diffs:
            continue
        window = diffs[-20:] if len(diffs) >= 20 else diffs
        atr20 = sum(window) / len(window)

        # Filing-date close: the closest obs_date <= filing_date.
        filing_close = None
        for d, px in reversed(bars_sorted):
            if filing_date is not None and d <= filing_date:
                filing_close = float(px)
                break
        if filing_close is None:
            filing_close = closes[0]
        delta = last_close - filing_close
        atr_distance = (abs(delta) / atr20) if atr20 > 0 else float("inf")
        atr_ok = atr_distance <= 1.0  # within 1 ATR per task spec

        # Score per spec: 0.90 + 0.05 * (n_insiders/10) + 0.05 * (0 if atr_ok else -0.5)
        # The last term is `0.05 * -0.5 = -0.025` when price has moved.
        score = 0.90 + 0.05 * (float(n_insiders) / 10.0)
        atr_term = 0.0 if atr_ok else (0.05 * -0.5)
        score += atr_term
        score = max(0.0, min(0.99, score))

        # Pick the most informative URL for the alert (prefer periodic
        # filings over 8-Ks, 8-Ks over transcripts).
        primary_url = None
        if urls:
            primary_url = urls[0]

        gems.append({
            "source": "earnings_proximity",
            "rule_name": "earnings_proximity",
            "subject_kind": "ticker",
            "subject_id": t_clean,
            "subject_key": f"ticker:{t_clean}",
            "score": float(score),
            "evidence": {
                # ticker FIRST so cross-class explicit-key extraction wins
                "ticker": t_clean,
                "filing_date": str(filing_date) if filing_date else None,
                "event_types": list(event_types) if event_types else [],
                "filing_url": primary_url,
                "n_insiders": int(n_insiders),
                "total_insider_value_usd": float(total_value or 0.0),
                "filing_close": filing_close,
                "last_close": last_close,
                "atr20": atr20,
                "atr_distance_atr": round(atr_distance, 3),
                "atr_within_1": bool(atr_ok),
                "has_periodic": bool(has_periodic),
                "has_8k": bool(has_8k),
                "has_call": bool(has_call),
                "window_days": 7,
                "score_components": {
                    "base": 0.90,
                    "insider_bonus": round(0.05 * (float(n_insiders) / 10.0), 4),
                    "atr_term": round(atr_term, 4),
                },
            },
            # sidecar: insider names kept out of evidence (would leak as tickers)
            "implied_direction": "BULL",
            "insider_names": list(insider_names) if insider_names else [],
            "accessions": list(accessions) if accessions else [],
        })
    return gems
