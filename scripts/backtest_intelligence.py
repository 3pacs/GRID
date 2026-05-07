"""GRID Intelligence Backtest Engine.

Validates intelligence boost multipliers against real market data.

CLI:
  PYTHONPATH=. python3 scripts/backtest_intelligence.py edge-table
  PYTHONPATH=. python3 scripts/backtest_intelligence.py replay --tickers NVDA,META,GOOGL
  PYTHONPATH=. python3 scripts/backtest_intelligence.py calibrate
  PYTHONPATH=. python3 scripts/backtest_intelligence.py report
  PYTHONPATH=. python3 scripts/backtest_intelligence.py full
"""
from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
from loguru import logger as log
from scipy import stats
from sqlalchemy import text
from sqlalchemy.engine import Engine

from db import get_engine

OUTPUT_DIR = Path("outputs/backtest")

# Lookback windows per source type (days)
LOOKBACK_WINDOWS = {
    "congressional": 30,
    "insider": 14,
    "options_flow": 7,
    "darkpool": 5,
    "lobbying": 45,
    "hyperliquid": 3,
    "crypto_etf": 7,
    "fear_greed": 7,
    "whale_alert": 3,
    "coingecko": 7,
    "binance_rt": 1,
    "defi_llama": 7,
    "cryptoquant": 7,
    "onchain_rpc": 3,
    "quiverquant:insider": 14,
    "quiverquant:house": 30,
    "quiverquant:senate": 30,
    "quiverquant:lobbying": 45,
    "quiverquant:offexchange": 5,
    "quiverquant:gov_contracts": 30,
    "gov_contract": 30,
    "export_control": 14,
}
DEFAULT_LOOKBACK = 7


@dataclass
class EdgeRow:
    source_type: str
    ticker: str
    signal_direction: str
    n_events: int
    n_absent: int
    hit_rate_present: float
    hit_rate_absent: float
    avg_return_present: float
    avg_return_absent: float
    information_coefficient: float
    p_value: float
    verdict: str


def run_edge_table(engine: Engine) -> list[EdgeRow]:
    """Phase 1: Signal co-occurrence analysis.

    For every scored prediction, check which signals were active beforehand.
    Compute hit rates, information coefficients, and statistical significance.
    """
    log.info("Phase 1: Building edge table...")

    # Load all scored predictions
    with engine.connect() as conn:
        preds = conn.execute(text("""
            SELECT id, ticker, direction, confidence, actual_move_pct, created_at
            FROM oracle_predictions
            WHERE scored_at IS NOT NULL AND actual_move_pct IS NOT NULL
            ORDER BY created_at
        """)).fetchall()

    log.info("Loaded {} scored predictions", len(preds))

    # For each prediction, determine correctness
    pred_data = []
    for p in preds:
        pid, ticker, direction, conf, actual_move, created_at = p
        is_correct = (
            (direction in ("CALL", "bullish", "up") and actual_move > 0)
            or (direction in ("PUT", "bearish", "down") and actual_move < 0)
        )
        pred_data.append({
            "id": pid,
            "ticker": ticker,
            "direction": direction,
            "confidence": float(conf),
            "actual_move": float(actual_move),
            "created_at": created_at,
            "correct": is_correct,
        })

    # Get all signal sources
    with engine.connect() as conn:
        signals = conn.execute(text("""
            SELECT source_type, source_id, ticker, signal_date, signal_type
            FROM signal_sources
            ORDER BY signal_date
        """)).fetchall()

    log.info("Loaded {} signal sources", len(signals))

    # Build signal lookup: (source_type, ticker) -> list of (signal_date, signal_type)
    signal_lookup: dict[tuple[str, str], list[tuple[date, str]]] = {}
    for s in signals:
        key = (s.source_type, s.ticker)
        sig_date = s.signal_date
        sig_type = s.signal_type
        signal_lookup.setdefault(key, []).append((sig_date, sig_type))

    # For each source_type x ticker x direction, compute edge metrics
    edge_rows: list[EdgeRow] = []
    source_ticker_combos = set()
    for key in signal_lookup:
        source_type, ticker = key
        # Get unique signal directions for this source+ticker
        directions = set(st for _, st in signal_lookup[key])
        for sig_dir in directions:
            source_ticker_combos.add((source_type, ticker, sig_dir))

    for source_type, ticker, sig_dir in source_ticker_combos:
        lookback = LOOKBACK_WINDOWS.get(source_type, DEFAULT_LOOKBACK)
        key = (source_type, ticker)
        source_signals = signal_lookup.get(key, [])

        # Filter to relevant signal direction
        sig_dates = sorted(set(
            sd for sd, st in source_signals if st == sig_dir
        ))

        if not sig_dates:
            continue

        # For each prediction on this ticker, check if signal was present in lookback
        ticker_preds = [p for p in pred_data if p["ticker"] == ticker]
        if len(ticker_preds) < 5:
            continue

        present_correct = []
        absent_correct = []
        present_returns = []
        absent_returns = []

        for pred in ticker_preds:
            pred_date = pred["created_at"].date() if hasattr(pred["created_at"], 'date') else pred["created_at"]
            lookback_start = pred_date - timedelta(days=lookback)

            # Was signal present in lookback window?
            signal_present = any(
                lookback_start <= sd <= pred_date for sd in sig_dates
            )

            if signal_present:
                present_correct.append(1.0 if pred["correct"] else 0.0)
                present_returns.append(pred["actual_move"])
            else:
                absent_correct.append(1.0 if pred["correct"] else 0.0)
                absent_returns.append(pred["actual_move"])

        n_present = len(present_correct)
        n_absent = len(absent_correct)

        if n_present < 3:
            continue

        hit_present = np.mean(present_correct) if present_correct else 0
        hit_absent = np.mean(absent_correct) if absent_correct else 0
        avg_ret_present = np.mean(present_returns) if present_returns else 0
        avg_ret_absent = np.mean(absent_returns) if absent_returns else 0

        # Information coefficient: correlation between signal presence and correctness
        all_presence = [1.0] * n_present + [0.0] * n_absent
        all_correct = present_correct + absent_correct
        if len(set(all_presence)) > 1 and len(set(all_correct)) > 1:
            ic, p_val = stats.pearsonr(all_presence, all_correct)
        else:
            ic, p_val = 0.0, 1.0

        # Verdict
        if n_present < 10:
            verdict = "INSUFFICIENT"
        elif p_val < 0.05 and abs(ic) > 0.1:
            verdict = "EDGE"
        elif p_val < 0.10:
            verdict = "WEAK_EDGE"
        else:
            verdict = "NOISE"

        edge_rows.append(EdgeRow(
            source_type=source_type,
            ticker=ticker,
            signal_direction=sig_dir,
            n_events=n_present,
            n_absent=n_absent,
            hit_rate_present=round(hit_present, 4),
            hit_rate_absent=round(hit_absent, 4),
            avg_return_present=round(avg_ret_present, 4),
            avg_return_absent=round(avg_ret_absent, 4),
            information_coefficient=round(ic, 4),
            p_value=round(p_val, 6),
            verdict=verdict,
        ))

    # Sort by verdict priority then IC
    verdict_order = {"EDGE": 0, "WEAK_EDGE": 1, "INSUFFICIENT": 2, "NOISE": 3}
    edge_rows.sort(key=lambda r: (verdict_order.get(r.verdict, 9), -abs(r.information_coefficient)))

    # Write outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # CSV
    csv_path = OUTPUT_DIR / "edge_table.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[field for field in EdgeRow.__dataclass_fields__])
        writer.writeheader()
        for row in edge_rows:
            writer.writerow(asdict(row))

    # Markdown
    md_path = OUTPUT_DIR / "edge_table.md"
    with open(md_path, "w") as f:
        f.write("# Intelligence Edge Table\n\n")
        f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"Predictions analyzed: {len(pred_data)}\n")
        f.write(f"Signal sources: {len(signals)}\n\n")
        f.write("| Source | Ticker | Dir | N | Hit% Present | Hit% Absent | Avg Ret P | Avg Ret A | IC | p-value | Verdict |\n")
        f.write("|--------|--------|-----|---|-------------|-------------|-----------|-----------|-----|---------|----------|\n")
        for r in edge_rows:
            f.write(
                f"| {r.source_type} | {r.ticker} | {r.signal_direction} | {r.n_events} | "
                f"{r.hit_rate_present:.1%} | {r.hit_rate_absent:.1%} | "
                f"{r.avg_return_present:+.2f}% | {r.avg_return_absent:+.2f}% | "
                f"{r.information_coefficient:+.3f} | {r.p_value:.4f} | **{r.verdict}** |\n"
            )

    # Print summary
    edges = [r for r in edge_rows if r.verdict == "EDGE"]
    weak = [r for r in edge_rows if r.verdict == "WEAK_EDGE"]
    noise = [r for r in edge_rows if r.verdict == "NOISE"]
    log.info("Edge table: {} EDGE, {} WEAK_EDGE, {} NOISE, {} INSUFFICIENT",
             len(edges), len(weak), len(noise), len(edge_rows) - len(edges) - len(weak) - len(noise))

    if edges:
        log.info("TOP EDGES:")
        for e in edges[:5]:
            log.info("  {} on {} ({}): hit={:.1%} vs {:.1%}, IC={:+.3f}, p={:.4f}",
                     e.source_type, e.ticker, e.signal_direction,
                     e.hit_rate_present, e.hit_rate_absent, e.information_coefficient, e.p_value)

    return edge_rows


# -- Phase 2: Module Replay --------------------------------------------------

def run_replay(engine: Engine, tickers: list[str]) -> list[dict]:
    """Phase 2: Replay intelligence modules against scored predictions."""
    log.info("Phase 2: Replaying intelligence modules for tickers: {}", tickers)

    with engine.connect() as conn:
        preds = conn.execute(text("""
            SELECT id, ticker, direction, confidence, actual_move_pct, created_at
            FROM oracle_predictions
            WHERE scored_at IS NOT NULL AND actual_move_pct IS NOT NULL
              AND ticker = ANY(:tickers)
            ORDER BY created_at
        """), {"tickers": tickers}).fetchall()

    log.info("Replaying {} predictions", len(preds))
    results = []

    for i, p in enumerate(preds):
        pid, ticker, direction, raw_conf, actual_move, created_at = p
        is_correct = (
            (direction in ("CALL", "bullish", "up") and actual_move > 0)
            or (direction in ("PUT", "bearish", "down") and actual_move < 0)
        )

        row = {
            "prediction_id": pid,
            "ticker": ticker,
            "direction": direction,
            "raw_confidence": float(raw_conf),
            "actual_move_pct": float(actual_move),
            "correct": is_correct,
            "created_at": created_at.isoformat() if hasattr(created_at, 'isoformat') else str(created_at),
        }

        # Call each intelligence module
        for module_name, module_fn in [
            ("lever_pullers", _replay_lever_pullers),
            ("trust_scorer", _replay_trust_scorer),
            ("forensics", _replay_forensics),
        ]:
            try:
                boost = module_fn(engine, ticker, direction, created_at)
                row[f"{module_name}_boost"] = round(boost, 4)
            except Exception as exc:
                row[f"{module_name}_boost"] = 1.0
                log.debug("Replay {} failed for {}: {}", module_name, ticker, exc)

        # Compute adjusted confidence
        total_boost = 1.0
        for key in row:
            if key.endswith("_boost"):
                total_boost *= row[key]
        row["total_boost"] = round(total_boost, 4)
        row["adjusted_confidence"] = round(
            max(min(float(raw_conf) * total_boost, 0.99), 0.01), 4
        )

        results.append(row)

        if (i + 1) % 100 == 0:
            log.info("Replayed {}/{} predictions", i + 1, len(preds))

    # Write results
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUTPUT_DIR / "replay_results.csv"
    if results:
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)

    # Compute accuracy comparison
    if results:
        correct_count = sum(1 for r in results if r["correct"])
        total = len(results)
        raw_accuracy = correct_count / total

        # Brier score: mean squared error of probability vs outcome
        raw_brier = np.mean([(r["raw_confidence"] - (1.0 if r["correct"] else 0.0)) ** 2 for r in results])
        adj_brier = np.mean([(r["adjusted_confidence"] - (1.0 if r["correct"] else 0.0)) ** 2 for r in results])

        log.info("Accuracy: {:.1%} ({}/{})", raw_accuracy, correct_count, total)
        log.info("Brier score -- raw: {:.4f}, adjusted: {:.4f}, improvement: {:.4f}",
                 raw_brier, adj_brier, raw_brier - adj_brier)

        # Per-module lift
        for module in ["lever_pullers", "trust_scorer", "forensics"]:
            key = f"{module}_boost"
            boosted = [r for r in results if r.get(key, 1.0) != 1.0]
            if boosted:
                boost_correct = sum(1 for r in boosted if r["correct"]) / len(boosted)
                log.info("  {}: {}/{} predictions boosted, hit rate {:.1%}",
                         module, len(boosted), total, boost_correct)

    return results


def _replay_lever_pullers(engine: Engine, ticker: str, direction: str, created_at: datetime) -> float:
    """Replay lever puller check for a historical prediction."""
    from intelligence.lever_pullers import get_lever_context_for_ticker
    ctx = get_lever_context_for_ticker(engine, ticker)
    active = ctx.get("active_pullers", [])
    if not active:
        return 1.0

    expected = "bullish" if direction in ("CALL", "bullish", "up") else "bearish"
    aligned = sum(1 for p in active if p.get("direction", "").lower() == expected)
    opposed = sum(
        1 for p in active
        if p.get("direction", "") and p.get("direction", "").lower() not in (expected, "neutral")
    )

    if aligned > opposed:
        return 1.15
    elif opposed > aligned and opposed >= 2:
        return 0.75
    return 1.0


def _replay_trust_scorer(engine: Engine, ticker: str, direction: str, created_at: datetime) -> float:
    """Replay trust scorer check for a historical prediction."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trust_score FROM signal_sources
            WHERE ticker = :ticker AND signal_date <= :before
            ORDER BY signal_date DESC LIMIT 10
        """), {"ticker": ticker, "before": created_at}).fetchall()

    if not rows:
        return 1.0

    avg_trust = np.mean([float(r.trust_score) for r in rows if r.trust_score is not None])
    if avg_trust > 0.6:
        return 1.1
    elif avg_trust < 0.3:
        return 0.85
    return 1.0


def _replay_forensics(engine: Engine, ticker: str, direction: str, created_at: datetime) -> float:
    """Replay forensics check for a historical prediction."""
    from intelligence.forensics import find_significant_moves
    moves = find_significant_moves(engine, ticker, days=30, threshold=0.02)
    if not moves:
        return 1.0

    expected = "bullish" if direction in ("CALL", "bullish", "up") else "bearish"
    aligned = sum(
        1 for m in moves
        if (expected == "bullish" and m.get("change_pct", 0) > 0)
        or (expected == "bearish" and m.get("change_pct", 0) < 0)
    )
    opposed = len(moves) - aligned
    total = len(moves)

    if total > 0 and aligned > opposed * 2:
        return 1.0 + 0.1 * (aligned / total)
    elif total > 0 and opposed > aligned * 2:
        return 1.0 - 0.15 * (opposed / total)
    return 1.0


# -- Phase 3: Outputs --------------------------------------------------------

def run_calibrate(engine: Engine) -> dict:
    """Phase 3B: Generate calibration JSON from replay results."""
    csv_path = OUTPUT_DIR / "replay_results.csv"
    if not csv_path.exists():
        log.error("No replay results found. Run 'replay' first.")
        return {}

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        results = list(reader)

    calibration = {
        "calibration_version": date.today().isoformat(),
        "generated_from": f"{len(results)} predictions",
        "modules": {},
        "per_ticker": {},
    }

    for module in ["lever_pullers", "trust_scorer", "forensics"]:
        key = f"{module}_boost"
        boosted = [r for r in results if float(r.get(key, 1.0)) > 1.0]
        penalized = [r for r in results if float(r.get(key, 1.0)) < 1.0]

        boost_hits = sum(1 for r in boosted if r["correct"] == "True")
        penalty_hits = sum(1 for r in penalized if r["correct"] == "False")

        boost_rate = boost_hits / len(boosted) if boosted else 0
        penalty_rate = penalty_hits / len(penalized) if penalized else 0

        # Compute optimal multiplier from data
        if boosted:
            avg_boost = np.mean([float(r[key]) for r in boosted])
            optimal_boost = 1.0 + (avg_boost - 1.0) * min(boost_rate * 2, 2.0)
        else:
            optimal_boost = 1.0

        if penalized:
            avg_penalty = np.mean([float(r[key]) for r in penalized])
            optimal_penalty = 1.0 - (1.0 - avg_penalty) * min(penalty_rate * 2, 2.0)
        else:
            optimal_penalty = 1.0

        calibration["modules"][module] = {
            "boost": round(max(1.01, min(optimal_boost, 1.5)), 3),
            "penalty": round(max(0.5, min(optimal_penalty, 0.99)), 3),
            "n_boost_events": len(boosted),
            "n_penalty_events": len(penalized),
            "boost_hit_rate": round(boost_rate, 3),
            "penalty_correct_rate": round(penalty_rate, 3),
            "confidence": round(min(len(boosted) + len(penalized), 100) / 100, 2),
        }

    # Per-ticker breakdown
    tickers = set(r["ticker"] for r in results)
    for ticker in tickers:
        ticker_results = [r for r in results if r["ticker"] == ticker]
        calibration["per_ticker"][ticker] = {}
        for module in ["lever_pullers", "trust_scorer", "forensics"]:
            key = f"{module}_boost"
            boosted = [r for r in ticker_results if float(r.get(key, 1.0)) != 1.0]
            if boosted:
                hit_rate = sum(1 for r in boosted if r["correct"] == "True") / len(boosted)
                calibration["per_ticker"][ticker][module] = {
                    "n": len(boosted),
                    "hit_rate": round(hit_rate, 3),
                }

    # Write
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cal_path = OUTPUT_DIR / "calibration.json"
    with open(cal_path, "w") as f:
        json.dump(calibration, f, indent=2)

    log.info("Calibration written to {}", cal_path)
    return calibration


def run_report(engine: Engine) -> None:
    """Phase 3C: Generate per-ticker forensic narratives."""
    edge_path = OUTPUT_DIR / "edge_table.md"
    replay_path = OUTPUT_DIR / "replay_results.csv"

    if not edge_path.exists():
        log.error("No edge table found. Run 'edge-table' first.")
        return

    edge_content = edge_path.read_text()
    replay_content = ""
    if replay_path.exists():
        with open(replay_path) as f:
            reader = csv.DictReader(f)
            replay_data = list(reader)
        # Summarize replay by ticker
        tickers = set(r["ticker"] for r in replay_data)
        for ticker in tickers:
            t_data = [r for r in replay_data if r["ticker"] == ticker]
            correct = sum(1 for r in t_data if r["correct"] == "True")
            replay_content += f"\n### {ticker}: {correct}/{len(t_data)} correct ({correct/len(t_data):.0%})\n"
            for module in ["lever_pullers", "trust_scorer", "forensics"]:
                key = f"{module}_boost"
                boosted = [r for r in t_data if float(r.get(key, 1.0)) != 1.0]
                if boosted:
                    b_correct = sum(1 for r in boosted if r["correct"] == "True")
                    replay_content += f"- {module}: {len(boosted)} boosted, {b_correct}/{len(boosted)} correct\n"

    # Generate narrative per ticker using local LLM
    try:
        from ollama.client import get_client
        client = get_client()
        if not client or not client.is_available:
            raise RuntimeError("Ollama not available")
    except Exception:
        log.warning("LLM not available, writing data-only report")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        report_path = OUTPUT_DIR / "forensic_report.md"
        with open(report_path, "w") as f:
            f.write("# Intelligence Backtest Forensic Report\n\n")
            f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n\n")
            f.write("## Edge Table\n\n")
            f.write(edge_content)
            f.write("\n\n## Replay Results\n\n")
            f.write(replay_content or "(no replay data)")
        log.info("Data-only report written to {}", report_path)
        return

    # With LLM available, generate per-ticker forensic narrative
    tickers_to_report = set()
    if replay_path.exists():
        with open(replay_path) as f:
            reader = csv.DictReader(f)
            for r in reader:
                tickers_to_report.add(r["ticker"])

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for ticker in tickers_to_report:
        prompt = (
            f"You are a quantitative trading analyst writing a forensic intelligence report for {ticker}.\n\n"
            f"Below is the edge table showing which information sources have predictive power, "
            f"and the replay results showing how intelligence modules performed.\n\n"
            f"## Edge Table (filtered to {ticker})\n\n"
        )
        # Filter edge table lines for this ticker
        for line in edge_content.split("\n"):
            if ticker in line or "Source" in line or "---" in line or "# " in line:
                prompt += line + "\n"

        prompt += f"\n## Replay Results for {ticker}\n{replay_content}\n\n"
        prompt += (
            "Write a forensic report covering:\n"
            "1. Which information sources actually predicted moves for this ticker (cite specific examples with dates)\n"
            "2. Which were noise or actively harmful\n"
            "3. What the optimal multiplier strategy is for this ticker\n"
            "4. Name specific actors, dates, and outcomes -- not generic observations\n"
            "Under 500 words. Be specific. Take a stand."
        )

        narrative = client.chat(
            [{"role": "user", "content": prompt}],
            temperature=0.3,
            num_predict=800,
        )

        if narrative:
            report_path = OUTPUT_DIR / f"forensic_{ticker}.md"
            with open(report_path, "w") as f:
                f.write(f"# Forensic Intelligence Report: {ticker}\n\n")
                f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n\n")
                f.write(narrative)
            log.info("Forensic report written for {}", ticker)


# -- CLI ---------------------------------------------------------------------

def main():
    engine = get_engine()

    if len(sys.argv) < 2:
        print("Usage: backtest_intelligence.py <command> [options]")
        print("Commands: edge-table, replay, calibrate, report, full")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "edge-table":
        run_edge_table(engine)

    elif cmd == "replay":
        tickers = ["NVDA", "META", "GOOGL", "AAPL", "MSFT"]
        if "--tickers" in sys.argv:
            idx = sys.argv.index("--tickers")
            if idx + 1 < len(sys.argv):
                tickers = sys.argv[idx + 1].split(",")
        run_replay(engine, tickers)

    elif cmd == "calibrate":
        run_calibrate(engine)

    elif cmd == "report":
        run_report(engine)

    elif cmd == "full":
        log.info("=== FULL BACKTEST ===")
        run_edge_table(engine)
        run_replay(engine, ["NVDA", "META", "GOOGL", "AAPL", "MSFT"])
        run_calibrate(engine)
        run_report(engine)
        log.info("=== FULL BACKTEST COMPLETE ===")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
