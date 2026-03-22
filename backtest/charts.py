"""
GRID Backtest Chart Generator.

Produces pitch-grade visualizations from backtest results:
  - Equity curve (GRID vs SPY vs 60/40) with regime bands
  - Regime timeline bar
  - Rolling Sharpe ratio
  - Regime allocation pie charts
  - Correlation heatmap (from orthogonality audit)

Outputs PNG and SVG files suitable for pitch decks.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

_OUTPUT_DIR = Path(__file__).parent.parent / "outputs" / "backtest"
_CHART_DIR = _OUTPUT_DIR / "charts"

REGIME_COLORS = {
    "GROWTH": "#22C55E",
    "NEUTRAL": "#F59E0B",
    "FRAGILE": "#F97316",
    "CRISIS": "#EF4444",
}


def generate_all_charts(result: dict[str, Any] | None = None) -> dict[str, str]:
    """Generate all pitch charts from backtest results.

    Parameters:
        result: Backtest result dict. If None, loads from disk.

    Returns:
        dict: Map of chart name → file path.
    """
    if result is None:
        results_path = _OUTPUT_DIR / "backtest_results.json"
        if not results_path.exists():
            return {"error": "No backtest results found. Run backtest first."}
        with results_path.open() as f:
            result = json.load(f)

    _CHART_DIR.mkdir(parents=True, exist_ok=True)

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from matplotlib.patches import Patch
    except ImportError:
        return {"error": "matplotlib not installed. pip install matplotlib"}

    charts = {}

    # 1. Equity Curve with Regime Bands
    charts["equity_curve"] = _chart_equity_curve(result, plt, mdates, Patch)

    # 2. Regime Timeline
    charts["regime_timeline"] = _chart_regime_timeline(result, plt, mdates, Patch)

    # 3. Rolling Sharpe
    charts["rolling_sharpe"] = _chart_rolling_sharpe(result, plt, mdates)

    # 4. Allocation Pies
    charts["allocation_pies"] = _chart_allocation_pies(plt)

    # 5. Performance Comparison Table
    charts["comparison_table"] = _chart_comparison_table(result, plt)

    return charts


def _chart_equity_curve(result, plt, mdates, Patch) -> str:
    """Equity curve: GRID vs SPY vs 60/40 with regime bands."""
    ec = result.get("equity_curve", {})
    dates = [np.datetime64(d) for d in ec.get("dates", [])]
    if not dates:
        return ""

    fig, ax = plt.subplots(figsize=(14, 6), facecolor="#080C10")
    ax.set_facecolor("#080C10")

    # Regime background bands
    rt = result.get("regime_timeline", {})
    regimes = rt.get("regimes", [])
    if regimes and len(regimes) == len(dates):
        prev_regime = regimes[0]
        band_start = 0
        for i in range(1, len(regimes)):
            if regimes[i] != prev_regime or i == len(regimes) - 1:
                color = REGIME_COLORS.get(prev_regime, "#333")
                ax.axvspan(dates[band_start], dates[i], alpha=0.08, color=color, linewidth=0)
                band_start = i
                prev_regime = regimes[i]

    # Equity curves
    if ec.get("grid"):
        ax.plot(dates, ec["grid"], color="#1A6EBF", linewidth=2, label="GRID", zorder=3)
    if ec.get("spy"):
        ax.plot(dates, ec["spy"], color="#8B8B8B", linewidth=1.2, label="SPY", alpha=0.7, zorder=2)
    if ec.get("sixty_forty"):
        ax.plot(dates, ec["sixty_forty"], color="#B8922A", linewidth=1.2, label="60/40", alpha=0.7, zorder=2)

    ax.set_title("GRID Portfolio vs Benchmarks", color="#E8F0F8", fontsize=16, fontweight="bold", pad=16)
    ax.set_ylabel("Portfolio Value ($)", color="#8AA0B8", fontsize=12)
    ax.legend(loc="upper left", fontsize=11, facecolor="#0D1520", edgecolor="#1A2840", labelcolor="#C8D8E8")
    ax.tick_params(colors="#5A7080")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#1A2840")
    ax.spines["bottom"].set_color("#1A2840")
    ax.grid(axis="y", alpha=0.1, color="#5A7080")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    # Regime legend
    regime_patches = [Patch(color=c, alpha=0.3, label=r) for r, c in REGIME_COLORS.items()]
    ax2 = ax.twinx()
    ax2.set_yticks([])
    ax2.legend(handles=regime_patches, loc="lower right", fontsize=9,
               facecolor="#0D1520", edgecolor="#1A2840", labelcolor="#C8D8E8", title="Regime",
               title_fontsize=9)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    path = str(_CHART_DIR / "equity_curve.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="#080C10")
    plt.close(fig)
    return path


def _chart_regime_timeline(result, plt, mdates, Patch) -> str:
    """Regime timeline with S&P overlay."""
    rt = result.get("regime_timeline", {})
    dates = [np.datetime64(d) for d in rt.get("dates", [])]
    regimes = rt.get("regimes", [])
    if not dates or not regimes:
        return ""

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 4), facecolor="#080C10",
                                    gridspec_kw={"height_ratios": [3, 1]}, sharex=True)

    for a in [ax1, ax2]:
        a.set_facecolor("#080C10")

    # Top: Equity curve
    ec = result.get("equity_curve", {})
    if ec.get("spy"):
        ax1.plot(dates[:len(ec["spy"])], ec["spy"], color="#8B8B8B", linewidth=1)
    ax1.set_title("Regime Timeline", color="#E8F0F8", fontsize=14, fontweight="bold", pad=10)
    ax1.tick_params(colors="#5A7080")
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.spines["left"].set_color("#1A2840")
    ax1.spines["bottom"].set_visible(False)
    ax1.set_ylabel("SPY", color="#8AA0B8", fontsize=10)

    # Bottom: Regime bars
    regime_nums = [["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"].index(r) if r in ["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"] else 1 for r in regimes]
    colors = [REGIME_COLORS.get(r, "#333") for r in regimes]
    ax2.bar(dates, [1] * len(dates), width=1.5, color=colors, linewidth=0)
    ax2.set_yticks([])
    ax2.tick_params(colors="#5A7080")
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    ax2.spines["left"].set_visible(False)
    ax2.spines["bottom"].set_color("#1A2840")

    # Legend
    patches = [Patch(color=c, label=r) for r, c in REGIME_COLORS.items()]
    ax2.legend(handles=patches, loc="lower center", ncol=4, fontsize=9,
               facecolor="#0D1520", edgecolor="#1A2840", labelcolor="#C8D8E8",
               bbox_to_anchor=(0.5, -0.6))

    path = str(_CHART_DIR / "regime_timeline.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="#080C10")
    plt.close(fig)
    return path


def _chart_rolling_sharpe(result, plt, mdates) -> str:
    """Rolling 1-year Sharpe ratio."""
    rs = result.get("rolling_sharpe", {})
    dates = [np.datetime64(d) for d in rs.get("dates", [])]
    values = rs.get("values", [])
    if not dates or not values:
        return ""

    fig, ax = plt.subplots(figsize=(14, 3), facecolor="#080C10")
    ax.set_facecolor("#080C10")

    ax.plot(dates, values, color="#1A6EBF", linewidth=1.5)
    ax.axhline(y=0, color="#8B1F1F", linewidth=0.8, alpha=0.5)
    ax.axhline(y=1, color="#22C55E", linewidth=0.8, alpha=0.3, linestyle="--")
    ax.axhline(y=2, color="#22C55E", linewidth=0.8, alpha=0.3, linestyle="--")
    ax.fill_between(dates, values, 0, alpha=0.1, color="#1A6EBF")

    ax.set_title("Rolling 1-Year Sharpe Ratio", color="#E8F0F8", fontsize=14, fontweight="bold", pad=10)
    ax.set_ylabel("Sharpe", color="#8AA0B8", fontsize=10)
    ax.tick_params(colors="#5A7080")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#1A2840")
    ax.spines["bottom"].set_color("#1A2840")
    ax.grid(axis="y", alpha=0.1, color="#5A7080")

    path = str(_CHART_DIR / "rolling_sharpe.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="#080C10")
    plt.close(fig)
    return path


def _chart_allocation_pies(plt) -> str:
    """Four allocation pie charts, one per regime."""
    from backtest.engine import POSTURE_ALLOCATIONS, REGIME_TO_POSTURE

    fig, axes = plt.subplots(1, 4, figsize=(16, 4), facecolor="#080C10")

    pie_colors = [
        "#1A6EBF", "#22C55E", "#F59E0B", "#EF4444", "#8B5CF6",
        "#EC4899", "#14B8A6", "#F97316", "#6366F1", "#A855F7",
        "#84CC16", "#06B6D4",
    ]

    for i, (regime, posture) in enumerate(REGIME_TO_POSTURE.items()):
        ax = axes[i]
        ax.set_facecolor("#080C10")

        alloc = POSTURE_ALLOCATIONS[posture]
        labels = list(alloc.keys())
        sizes = list(alloc.values())

        # Filter out tiny allocations for readability
        filtered_labels = [l for l, s in zip(labels, sizes) if s >= 0.05]
        filtered_sizes = [s for s in sizes if s >= 0.05]
        other = sum(s for s in sizes if s < 0.05)
        if other > 0:
            filtered_labels.append("Other")
            filtered_sizes.append(other)

        wedges, texts, autotexts = ax.pie(
            filtered_sizes,
            labels=filtered_labels,
            autopct="%1.0f%%",
            colors=pie_colors[:len(filtered_sizes)],
            textprops={"fontsize": 7, "color": "#C8D8E8"},
            pctdistance=0.75,
            startangle=90,
        )
        for t in autotexts:
            t.set_fontsize(6)
            t.set_color("#E8F0F8")

        ax.set_title(f"{regime}", color=REGIME_COLORS[regime], fontsize=12, fontweight="bold", pad=8)

    path = str(_CHART_DIR / "allocation_pies.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="#080C10")
    plt.close(fig)
    return path


def _chart_comparison_table(result, plt) -> str:
    """Performance comparison table as an image."""
    gm = result.get("grid_metrics", {})
    bm = result.get("benchmark_metrics", {})
    spy = bm.get("SPY", {})
    sf = bm.get("60/40", {})

    fig, ax = plt.subplots(figsize=(10, 4), facecolor="#080C10")
    ax.set_facecolor("#080C10")
    ax.axis("off")

    headers = ["Metric", "GRID", "SPY", "60/40"]
    rows = [
        ["Cumulative Return", f"{gm.get('cumulative_return', 0):.1%}",
         f"{spy.get('cumulative_return', 0):.1%}", f"{sf.get('cumulative_return', 0):.1%}"],
        ["Annualized Return", f"{gm.get('annualized_return', 0):.1%}",
         f"{spy.get('annualized_return', 0):.1%}", f"{sf.get('annualized_return', 0):.1%}"],
        ["Sharpe Ratio", f"{gm.get('sharpe_ratio', 0):.2f}",
         f"{spy.get('sharpe_ratio', 0):.2f}", f"{sf.get('sharpe_ratio', 0):.2f}"],
        ["Sortino Ratio", f"{gm.get('sortino_ratio', 0):.2f}", "—", "—"],
        ["Max Drawdown", f"{gm.get('max_drawdown', 0):.1%}",
         f"{spy.get('max_drawdown', 0):.1%}", f"{sf.get('max_drawdown', 0):.1%}"],
        ["Calmar Ratio", f"{gm.get('calmar_ratio', 0):.2f}", "—", "—"],
        ["Win Rate", f"{gm.get('daily_win_rate', 0):.1%}", "—", "—"],
    ]

    table = ax.table(
        cellText=rows,
        colLabels=headers,
        cellLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 1.6)

    # Style
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor("#1A2840")
        if row == 0:
            cell.set_facecolor("#1A2840")
            cell.set_text_props(color="#E8F0F8", fontweight="bold")
        else:
            cell.set_facecolor("#0D1520")
            cell.set_text_props(color="#C8D8E8")
            if col == 1:  # GRID column
                cell.set_text_props(color="#1A6EBF", fontweight="bold")

    ax.set_title("Performance Comparison", color="#E8F0F8", fontsize=14,
                 fontweight="bold", pad=20, y=0.95)

    path = str(_CHART_DIR / "comparison_table.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="#080C10")
    plt.close(fig)
    return path


if __name__ == "__main__":
    charts = generate_all_charts()
    for name, path in charts.items():
        if path:
            print(f"  {name}: {path}")
        else:
            print(f"  {name}: (skipped)")
