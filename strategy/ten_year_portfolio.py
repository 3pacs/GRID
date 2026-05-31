"""Ten-year chart-quality portfolio query.

Ranks long-horizon equity candidates against Nasdaq proxy QQQ, then turns the
ranked list into weekly review allocations for several investor profiles.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import math
import random
from typing import Any, Iterable


BENCHMARK_TICKER = "QQQ"
BENCHMARK_SERIES_IDS = ("YF:QQQ:adj_close", "YF:QQQ:close", "YF:^IXIC:close")

DEFAULT_CHART_UNIVERSE = (
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "AVGO", "TSLA",
    "COST", "NFLX", "AMD", "LLY", "CAT", "GE", "BRK-B", "JPM",
    "V", "MA", "HD",
)

FRONTIER_THEME_CANDIDATES: dict[str, tuple[str, ...]] = {
    "NVDA": ("AI compute", "supercomputers"),
    "AMD": ("AI compute", "supercomputers"),
    "AVGO": ("AI networking", "custom silicon"),
    "TSM": ("AI foundry", "advanced chips"),
    "ASML": ("semicap equipment", "advanced chips"),
    "MU": ("memory", "AI compute"),
    "ANET": ("AI networking",),
    "HPE": ("supercomputers", "HPC systems"),
    "DELL": ("AI servers", "data center"),
    "VRT": ("data center power", "cooling"),
    "ETN": ("data center power",),
    "CEG": ("nuclear power", "AI energy"),
    "CCJ": ("uranium", "nuclear fuel"),
    "UEC": ("uranium",),
    "NXE": ("uranium",),
    "FCX": ("copper", "metals"),
    "SCCO": ("copper", "metals"),
    "BHP": ("metals", "mining"),
    "RIO": ("metals", "mining"),
    "NUE": ("steel", "metals"),
}

FRONTIER_THEMATIC_UNIVERSE = tuple(FRONTIER_THEME_CANDIDATES)

# Keep raw_series page-load reads bounded. Names here have useful Yahoo adjusted
# history and avoid the slowest per-series scans observed on the live table.
FRONTIER_RAW_HISTORY_TICKERS = (
    "ANET", "ASML", "BHP", "CCJ", "CEG", "DELL", "ETN", "FCX", "HPE", "MU", "NUE",
)

ETF_AND_MACRO_PROXIES = {
    "SPY", "DIA", "QQQ", "IWM", "XLK", "XLF", "XLE", "XLV", "XLI",
    "XLY", "XLP", "XLU", "XLRE", "XLB", "XLC", "SMH", "KRE", "ICLN",
    "LIT", "XBI", "ITA", "TLT", "IEF", "SHY", "LQD", "HYG", "JNK",
    "EMB", "MUB", "GLD", "SLV", "USO", "DBA", "PDBC", "UUP", "FXE",
    "FXY", "EEM", "FXI", "EWJ", "EWZ", "EFA", "TIP", "COPX", "SJNK",
    "BKLN", "ANGL", "IBIT", "ETHA", "BND", "BITO", "HYG", "LQD",
}


@dataclass(frozen=True)
class InvestorProfile:
    id: str
    label: str
    description: str
    top_n: int
    max_position: float
    cash_pct: float
    min_years: float
    hold_buffer: int
    weights: dict[str, float]


PROFILES: dict[str, InvestorProfile] = {
    "dad_chartist": InvestorProfile(
        id="dad_chartist",
        label="Dad Chartist",
        description="Up-and-to-the-right first: 10-year chart slope, QQQ-relative strength, and tolerable drawdowns.",
        top_n=12,
        max_position=0.12,
        cash_pct=0.02,
        min_years=8.0,
        hold_buffer=8,
        weights={
            "return": 0.24,
            "trend": 0.30,
            "relative": 0.24,
            "drawdown": 0.12,
            "volatility": 0.04,
            "consistency": 0.04,
            "coverage": 0.02,
        },
    ),
    "conservative_compounder": InvestorProfile(
        id="conservative_compounder",
        label="Conservative Compounder",
        description="Owns durable compounders, penalizes major drawdowns, and trades less.",
        top_n=18,
        max_position=0.08,
        cash_pct=0.08,
        min_years=8.0,
        hold_buffer=12,
        weights={
            "return": 0.16,
            "trend": 0.24,
            "relative": 0.12,
            "drawdown": 0.24,
            "volatility": 0.12,
            "consistency": 0.08,
            "coverage": 0.04,
        },
    ),
    "nasdaq_plus": InvestorProfile(
        id="nasdaq_plus",
        label="Nasdaq Plus",
        description="Tries to beat QQQ by staying with the strongest long-term relative charts.",
        top_n=10,
        max_position=0.16,
        cash_pct=0.00,
        min_years=7.0,
        hold_buffer=6,
        weights={
            "return": 0.28,
            "trend": 0.18,
            "relative": 0.34,
            "drawdown": 0.06,
            "volatility": 0.03,
            "consistency": 0.07,
            "coverage": 0.04,
        },
    ),
    "sleep_well_growth": InvestorProfile(
        id="sleep_well_growth",
        label="Sleep Well Growth",
        description="Prefers smoother charts with Nasdaq participation and smaller position sizes.",
        top_n=20,
        max_position=0.07,
        cash_pct=0.12,
        min_years=8.0,
        hold_buffer=15,
        weights={
            "return": 0.14,
            "trend": 0.26,
            "relative": 0.10,
            "drawdown": 0.22,
            "volatility": 0.16,
            "consistency": 0.08,
            "coverage": 0.04,
        },
    ),
}


def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def parse_yf_series_id(series_id: str) -> tuple[str, str] | None:
    parts = series_id.split(":")
    if len(parts) != 3 or parts[0] != "YF":
        return None
    ticker, field = parts[1].strip().upper(), parts[2].strip().lower()
    if not ticker or field not in {"close", "adj_close"}:
        return None
    return ticker, field


def is_common_stock_candidate(ticker: str) -> bool:
    if not ticker or ticker in ETF_AND_MACRO_PROXIES:
        return False
    if ticker.startswith("^") or "=" in ticker or ticker.endswith("-USD"):
        return False
    if "." in ticker or "/" in ticker:
        return False
    return ticker.replace("-", "").isalnum()


def candidate_themes(ticker: str) -> list[str]:
    return list(FRONTIER_THEME_CANDIDATES.get(ticker, ()))


def _weekly_points(points: list[tuple[date, float]]) -> list[tuple[date, float]]:
    if not points:
        return []
    weekly: list[tuple[date, float]] = []
    last_key: tuple[int, int] | None = None
    for obs_date, value in points:
        key = obs_date.isocalendar()[:2]
        if key != last_key:
            weekly.append((obs_date, value))
            last_key = key
        else:
            weekly[-1] = (obs_date, value)
    return weekly


def _linear_r2(log_values: list[float]) -> float:
    n = len(log_values)
    if n < 3:
        return 0.0
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(log_values) / n
    sxx = sum((x - x_mean) ** 2 for x in xs)
    if sxx <= 0:
        return 0.0
    slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, log_values)) / sxx
    intercept = y_mean - slope * x_mean
    ss_tot = sum((y - y_mean) ** 2 for y in log_values)
    ss_res = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, log_values))
    if ss_tot <= 0:
        return 0.0
    return clamp(1.0 - (ss_res / ss_tot))


def _max_drawdown(values: list[float]) -> float:
    peak = values[0]
    worst = 0.0
    for value in values:
        peak = max(peak, value)
        if peak > 0:
            worst = max(worst, 1.0 - (value / peak))
    return worst


def _annualized_volatility(values: list[float], periods_per_year: int = 52) -> float:
    returns = [
        (values[i] / values[i - 1]) - 1.0
        for i in range(1, len(values))
        if values[i - 1] > 0 and values[i] > 0
    ]
    if len(returns) < 3:
        return 0.0
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    return math.sqrt(variance) * math.sqrt(periods_per_year)


def compute_chart_metrics(
    ticker: str,
    points: list[tuple[date, float]],
    benchmark: dict[str, float] | None = None,
) -> dict[str, Any] | None:
    clean = sorted((d, float(v)) for d, v in points if v and v > 0)
    if len(clean) < 52:
        return None
    weekly = _weekly_points(clean)
    if len(weekly) < 52:
        return None

    values = [v for _, v in weekly]
    first_date = weekly[0][0]
    last_date = weekly[-1][0]
    years = max((last_date - first_date).days / 365.25, 0.01)
    total_return = (values[-1] / values[0]) - 1.0
    cagr = (values[-1] / values[0]) ** (1.0 / years) - 1.0
    positive_weeks = sum(
        1 for i in range(1, len(values)) if values[i] > values[i - 1]
    ) / max(len(values) - 1, 1)
    drawdown = _max_drawdown(values)
    volatility = _annualized_volatility(values)
    r2 = _linear_r2([math.log(v) for v in values])
    benchmark_cagr = float((benchmark or {}).get("cagr", 0.0))
    benchmark_total = float((benchmark or {}).get("total_return", 0.0))
    relative_cagr = cagr - benchmark_cagr
    relative_total = total_return - benchmark_total
    coverage_score = clamp(years / 10.0)

    components = {
        "return": clamp((cagr - 0.02) / 0.18),
        "trend": r2,
        "relative": clamp((relative_cagr + 0.05) / 0.18),
        "drawdown": clamp((0.65 - drawdown) / 0.55),
        "volatility": clamp((0.60 - volatility) / 0.45),
        "consistency": clamp((positive_weeks - 0.45) / 0.18),
        "coverage": coverage_score,
    }

    return {
        "ticker": ticker,
        "first_date": first_date.isoformat(),
        "last_date": last_date.isoformat(),
        "latest_price": round(values[-1], 2),
        "years": round(years, 2),
        "observations": len(weekly),
        "total_return": total_return,
        "cagr": cagr,
        "benchmark_cagr": benchmark_cagr,
        "relative_cagr": relative_cagr,
        "relative_total_return": relative_total,
        "max_drawdown": drawdown,
        "annual_volatility": volatility,
        "trend_r2": r2,
        "positive_week_ratio": positive_weeks,
        "components": components,
        "sparkline": make_normalized_series(weekly, max_points=80),
    }


def make_normalized_series(
    points: list[tuple[date, float]],
    *,
    max_points: int = 80,
) -> list[dict[str, Any]]:
    clean = [(d, float(v)) for d, v in points if v and v > 0]
    if not clean:
        return []
    if len(clean) > max_points:
        step = max(1, math.ceil(len(clean) / max_points))
        sampled = clean[::step]
        if sampled[-1][0] != clean[-1][0]:
            sampled.append(clean[-1])
    else:
        sampled = clean
    base = sampled[0][1]
    if base <= 0:
        return []
    return [
        {"date": d.isoformat(), "value": round((v / base) * 100.0, 2)}
        for d, v in sampled
    ]


def score_candidate(metrics: dict[str, Any], profile: InvestorProfile) -> float:
    components = metrics.get("components", {})
    score = 0.0
    weight_total = 0.0
    for name, weight in profile.weights.items():
        score += float(components.get(name, 0.0)) * weight
        weight_total += weight
    if weight_total <= 0:
        return 0.0
    coverage_penalty = 0.55 + 0.45 * float(components.get("coverage", 0.0))
    return round(100.0 * (score / weight_total) * coverage_penalty, 2)


def build_profile_portfolio(
    ranked_metrics: list[dict[str, Any]],
    profile: InvestorProfile,
    *,
    capital: float,
) -> dict[str, Any]:
    eligible = [
        item for item in ranked_metrics
        if float(item.get("years", 0.0)) >= profile.min_years
    ]
    picks = eligible[:profile.top_n]
    raw_values = [
        max(float(item["score"]), 1.0)
        * (1.0 / max(float(item.get("annual_volatility", 0.20)), 0.10))
        if profile.id in {"conservative_compounder", "sleep_well_growth"}
        else max(float(item["score"]), 1.0)
        for item in picks
    ]
    effective_max_position = max(
        profile.max_position,
        (1.0 / len(picks)) if picks else profile.max_position,
    )
    weights = _capped_weights(raw_values, effective_max_position)
    investable = capital * (1.0 - profile.cash_pct)

    allocations: list[dict[str, Any]] = []
    invested = 0.0
    for item, weight in zip(picks, weights):
        dollars = investable * weight
        price = float(item.get("latest_price", 0.0))
        shares = math.floor(dollars / price) if price > 0 else 0
        allocated = shares * price
        invested += allocated
        allocations.append({
            **item,
            "target_weight": round(weight, 4),
            "target_dollars": round(dollars, 2),
            "whole_shares": shares,
            "estimated_position_value": round(allocated, 2),
            "action": "BUY",
            "hold_until_rank_below": profile.top_n + profile.hold_buffer,
        })

    monte_carlo = build_monte_carlo_projection(
        allocations,
        capital=capital,
        years=10,
        seed=profile.id,
    )

    return {
        "id": profile.id,
        "label": profile.label,
        "description": profile.description,
        "capital": round(capital, 2),
        "cash_target": round(capital * profile.cash_pct, 2),
        "estimated_invested": round(invested, 2),
        "estimated_residual_cash": round(capital - invested, 2),
        "top_n": profile.top_n,
        "max_position": round(effective_max_position, 4),
        "configured_max_position": profile.max_position,
        "hold_buffer": profile.hold_buffer,
        "weekly_policy": {
            "review": "weekly",
            "rebalance_threshold": "Only trade if a holding falls below its hold rank, breaks its trend, or position weight drifts materially.",
            "entry_rule": f"New buys must rank inside top {profile.top_n}.",
            "exit_rule": f"Existing names can be held until rank {profile.top_n + profile.hold_buffer} unless the 10-year chart breaks.",
        },
        "monte_carlo": monte_carlo,
        "allocations": allocations,
    }


def build_monte_carlo_projection(
    allocations: list[dict[str, Any]],
    *,
    capital: float,
    years: int = 10,
    simulations: int = 2000,
    seed: str = "dad_chartist",
) -> dict[str, Any]:
    if not allocations or capital <= 0:
        return {
            "years": years,
            "simulations": 0,
            "p10": capital,
            "p50": capital,
            "p90": capital,
            "probability_above_start": 0.0,
            "expected_annual_return": 0.0,
            "annual_volatility": 0.0,
        }

    def account_weight(item: dict[str, Any]) -> float:
        target_dollars = float(item.get("target_dollars", 0.0))
        if target_dollars > 0:
            return clamp(target_dollars / capital)
        return clamp(float(item.get("target_weight", 0.0)))

    weighted_return = sum(
        account_weight(item) * float(item.get("cagr", 0.0))
        for item in allocations
    )
    weighted_vol = math.sqrt(sum(
        (
            account_weight(item)
            * max(float(item.get("annual_volatility", 0.20)), 0.08)
        ) ** 2
        for item in allocations
    ))
    weighted_vol = clamp(weighted_vol, 0.08, 0.55)
    rng = random.Random(f"{seed}:{round(capital)}:{years}:{len(allocations)}")
    outcomes: list[float] = []
    for _ in range(simulations):
        value = capital
        for _year in range(years):
            annual_return = rng.gauss(weighted_return, weighted_vol)
            annual_return = max(-0.75, min(annual_return, 1.25))
            value *= 1.0 + annual_return
        outcomes.append(value)
    outcomes.sort()

    def percentile(pct: float) -> float:
        idx = min(max(int(round((pct / 100.0) * (len(outcomes) - 1))), 0), len(outcomes) - 1)
        return round(outcomes[idx], 2)

    return {
        "years": years,
        "simulations": simulations,
        "p10": percentile(10),
        "p50": percentile(50),
        "p90": percentile(90),
        "probability_above_start": round(sum(1 for value in outcomes if value > capital) / len(outcomes), 4),
        "expected_annual_return": round(weighted_return, 4),
        "annual_volatility": round(weighted_vol, 4),
        "method": "seeded_normal_proxy_from_historical_cagr_and_volatility",
    }


def _capped_weights(raw_values: list[float], cap: float) -> list[float]:
    if not raw_values:
        return []
    total = sum(max(v, 0.0) for v in raw_values)
    if total <= 0:
        return [1.0 / len(raw_values)] * len(raw_values)

    weights = [max(v, 0.0) / total for v in raw_values]
    capped = [False] * len(weights)
    for _ in range(len(weights) + 1):
        over = [i for i, w in enumerate(weights) if not capped[i] and w > cap]
        if not over:
            break
        for i in over:
            weights[i] = cap
            capped[i] = True
        remaining = 1.0 - sum(weights[i] for i, done in enumerate(capped) if done)
        free = [i for i, done in enumerate(capped) if not done]
        free_total = sum(max(raw_values[i], 0.0) for i in free)
        if not free or remaining <= 0 or free_total <= 0:
            break
        for i in free:
            weights[i] = remaining * (max(raw_values[i], 0.0) / free_total)
    final_total = sum(weights)
    if final_total <= 0:
        return [1.0 / len(weights)] * len(weights)
    return [w / final_total for w in weights]


def build_ranked_universe(
    price_history: dict[str, list[tuple[date, float]]],
    *,
    years: int = 10,
    candidate_tickers: Iterable[str] | None = None,
    include_non_stocks_when_sparse: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    benchmark_points = (
        price_history.get(BENCHMARK_TICKER)
        or price_history.get("^IXIC")
        or []
    )
    benchmark_metrics = compute_chart_metrics(BENCHMARK_TICKER, benchmark_points, None)
    if benchmark_metrics is None:
        benchmark_metrics = {
            "ticker": BENCHMARK_TICKER,
            "cagr": 0.0,
            "total_return": 0.0,
            "sparkline": make_normalized_series(_weekly_points(benchmark_points), max_points=80),
        }

    source_tickers = (
        sorted(set(candidate_tickers))
        if candidate_tickers is not None
        else sorted(price_history)
    )
    available_tickers = [t for t in source_tickers if t in price_history]
    strict_tickers = [t for t in available_tickers if is_common_stock_candidate(t)]
    broad_tickers = [t for t in available_tickers if t not in {BENCHMARK_TICKER, "^IXIC"}]
    candidate_tickers = strict_tickers
    universe_note = "stocks_only"
    if include_non_stocks_when_sparse and len(candidate_tickers) < 8:
        candidate_tickers = broad_tickers
        universe_note = "broad_fallback_sparse_stock_history"

    rows: list[dict[str, Any]] = []
    for ticker in sorted(set(candidate_tickers)):
        metrics = compute_chart_metrics(ticker, price_history[ticker], benchmark_metrics)
        if metrics is None:
            continue
        profile_scores = {
            profile_id: score_candidate(metrics, profile)
            for profile_id, profile in PROFILES.items()
        }
        rows.append({
            **metrics,
            "profile_scores": profile_scores,
            "score": profile_scores["dad_chartist"],
            "themes": candidate_themes(ticker),
        })

    rows.sort(key=lambda item: item["score"], reverse=True)
    universe = {
        "mode": universe_note,
        "requested_years": years,
        "series_available": len(available_tickers),
        "requested_candidates": len(source_tickers),
        "stock_candidates": len(strict_tickers),
        "ranked_candidates": len(rows),
    }
    return rows, benchmark_metrics, universe


def build_candidate_board(
    price_history: dict[str, list[tuple[date, float]]],
    *,
    board_id: str,
    label: str,
    description: str,
    tickers: Iterable[str],
    years: int = 10,
    score_profile: str = "nasdaq_plus",
    limit: int = 20,
) -> dict[str, Any]:
    ranked, _, universe = build_ranked_universe(
        price_history,
        years=years,
        candidate_tickers=tickers,
        include_non_stocks_when_sparse=False,
    )
    profile_id = score_profile if score_profile in PROFILES else "nasdaq_plus"
    rows = [
        {**item, "score": item["profile_scores"][profile_id]}
        for item in ranked
    ]
    rows.sort(key=lambda item: item["score"], reverse=True)
    return {
        "id": board_id,
        "label": label,
        "description": description,
        "score_profile": profile_id,
        "universe": universe,
        "ranked": rows[:limit],
        "themes": sorted(
            {theme for ticker in tickers for theme in candidate_themes(ticker)}
        ),
    }


def build_weekly_recommendation(
    price_history: dict[str, list[tuple[date, float]]],
    *,
    capital: float = 1_000_000.0,
    years: int = 10,
    profile_id: str | None = None,
) -> dict[str, Any]:
    ranked, benchmark, universe = build_ranked_universe(
        price_history,
        years=years,
        candidate_tickers=DEFAULT_CHART_UNIVERSE,
    )
    profiles: Iterable[InvestorProfile]
    if profile_id:
        profiles = [PROFILES.get(profile_id, PROFILES["dad_chartist"])]
    else:
        profiles = PROFILES.values()

    profile_payloads = []
    for profile in profiles:
        profile_ranked = [
            {**item, "score": item["profile_scores"][profile.id]}
            for item in ranked
        ]
        profile_ranked.sort(key=lambda item: item["score"], reverse=True)
        profile_payloads.append(
            build_profile_portfolio(profile_ranked, profile, capital=capital)
        )

    as_of = None
    all_dates = [
        points[-1][0]
        for points in price_history.values()
        if points
    ]
    if all_dates:
        as_of = max(all_dates).isoformat()

    return {
        "status": "ok",
        "as_of": as_of,
        "capital": round(capital, 2),
        "benchmark": {
            "ticker": benchmark.get("ticker", BENCHMARK_TICKER),
            "cagr": benchmark.get("cagr", 0.0),
            "total_return": benchmark.get("total_return", 0.0),
            "sparkline": benchmark.get("sparkline", []),
        },
        "universe": universe,
        "ranked": ranked[:50],
        "profiles": profile_payloads,
        "candidate_boards": [
            build_candidate_board(
                price_history,
                board_id="frontier_infrastructure",
                label="Frontier Infrastructure",
                description=(
                    "AI, compute, uranium, metals, power, and supercomputer "
                    "candidates filtered through GRID chart history."
                ),
                tickers=FRONTIER_THEMATIC_UNIVERSE,
                years=years,
                score_profile="nasdaq_plus",
                limit=25,
            )
        ],
    }
