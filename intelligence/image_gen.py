"""GRID — AI Image Generation via Gemini Imagen.

Generates financial infographics, flow diagrams, and market summaries
from GRID's capital flow data using Google's Imagen 4.0 model.

Public API:
    generate_flow_infographic(engine, style="dark") -> ImageResult
    generate_sector_heatmap(engine, style="dark") -> ImageResult
    generate_junction_dashboard(engine) -> ImageResult
    generate_market_briefing_image(engine) -> ImageResult
    generate_custom(prompt, style="dark") -> ImageResult

Requires GEMINI_API_KEY in environment.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log


# ── Configuration ─────────────────────────────────────────────────

_API_KEY = os.getenv("GEMINI_API_KEY", "")
_OUTPUT_DIR = Path("/data/grid_v4/grid_repo/outputs/generated_images")

# Model tiers: fast for iteration, standard for production, ultra for hero images
MODELS = {
    "fast": "imagen-4.0-fast-generate-001",
    "standard": "imagen-4.0-generate-001",
    "ultra": "imagen-4.0-ultra-generate-001",
    "gemini_flash": "gemini-3.1-flash-image-preview",
}

DEFAULT_MODEL = "fast"

# Style presets
STYLES = {
    "dark": (
        "Dark background (#0a0a1a), neon accent colors (cyan #00d4ff, "
        "magenta #ff00aa, green #00ff88), clean modern design, "
        "professional financial data visualization, high contrast, "
        "subtle grid lines, minimalist typography"
    ),
    "light": (
        "White background, professional blues and grays, clean lines, "
        "Bloomberg terminal inspired, institutional quality, "
        "serif typography for labels, subtle shadows"
    ),
    "cnbc": (
        "Broadcast TV style, bold colors, large readable text, "
        "gradient backgrounds, high contrast for camera capture, "
        "news ticker aesthetic, breaking news energy"
    ),
    "minimal": (
        "Ultra-clean white background, single accent color (#2563eb), "
        "thin lines, lots of whitespace, Edward Tufte inspired, "
        "maximum data-ink ratio"
    ),
}


@dataclass(frozen=True)
class ImageResult:
    """Immutable result from image generation."""
    image_bytes: bytes
    file_path: str
    prompt: str
    model: str
    style: str
    generated_at: str
    duration_ms: int
    width: int = 1024
    height: int = 1024

    def to_dict(self) -> dict[str, Any]:
        return {
            "file_path": self.file_path,
            "prompt_preview": self.prompt[:200],
            "model": self.model,
            "style": self.style,
            "generated_at": self.generated_at,
            "duration_ms": self.duration_ms,
            "size_bytes": len(self.image_bytes),
        }


# ── Core Generation ──────────────────────────────────────────────

def _get_client():
    """Lazy-load Gemini client."""
    from google import genai
    key = _API_KEY or os.getenv("GEMINI_API_KEY", "")
    if not key:
        raise ValueError("GEMINI_API_KEY not set")
    return genai.Client(api_key=key)


def _ensure_output_dir() -> Path:
    """Create output directory if needed."""
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return _OUTPUT_DIR


def _generate_image(
    prompt: str,
    model_tier: str = DEFAULT_MODEL,
    style: str = "dark",
    filename_prefix: str = "grid",
) -> ImageResult:
    """Core image generation function.

    Parameters:
        prompt: The image generation prompt (will be augmented with style).
        model_tier: One of 'fast', 'standard', 'ultra', 'gemini_flash'.
        style: One of 'dark', 'light', 'cnbc', 'minimal'.
        filename_prefix: Prefix for the saved file.

    Returns:
        ImageResult with the generated image bytes and metadata.
    """
    from google.genai import types

    client = _get_client()
    model_name = MODELS.get(model_tier, MODELS[DEFAULT_MODEL])
    style_text = STYLES.get(style, STYLES["dark"])

    full_prompt = f"{prompt}\n\nVisual style: {style_text}"

    log.info("Generating image: model={m}, style={s}, prompt={p}",
             m=model_name, s=style, p=prompt[:80])

    t0 = time.monotonic()

    response = client.models.generate_images(
        model=model_name,
        prompt=full_prompt,
        config=types.GenerateImagesConfig(
            number_of_images=1,
            output_mime_type="image/png",
        ),
    )

    duration_ms = int((time.monotonic() - t0) * 1000)

    if not response.generated_images:
        raise RuntimeError("Imagen returned no images")

    img_bytes = response.generated_images[0].image.image_bytes

    # Save to disk
    output_dir = _ensure_output_dir()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.png"
    file_path = str(output_dir / filename)

    with open(file_path, "wb") as f:
        f.write(img_bytes)

    log.info("Image saved: {p} ({kb}KB, {ms}ms)",
             p=file_path, kb=len(img_bytes) // 1024, ms=duration_ms)

    return ImageResult(
        image_bytes=img_bytes,
        file_path=file_path,
        prompt=full_prompt,
        model=model_name,
        style=style,
        generated_at=datetime.now(timezone.utc).isoformat(),
        duration_ms=duration_ms,
    )


# ── Data-Driven Prompt Builders ──────────────────────────────────

def _build_flow_data_summary(engine) -> str:
    """Build a text summary of current flow state for the image prompt."""
    from analysis.money_flow_engine import build_flow_map

    flow_map = build_flow_map(engine)

    lines = ["Global Capital Flow State:"]

    # Global metrics
    if flow_map.global_liquidity_total:
        lines.append(
            f"- Global liquidity: ${flow_map.global_liquidity_total / 1e12:.1f}T"
        )
    if flow_map.global_liquidity_change_1m:
        chg = flow_map.global_liquidity_change_1m
        direction = "expanding" if chg > 0 else "contracting"
        lines.append(f"- Liquidity {direction}: ${abs(chg) / 1e9:.0f}B/month")

    # Layer summaries
    for layer in flow_map.layers:
        node_count = len(layer.nodes)
        regime = layer.regime or "neutral"
        if layer.total_value_usd:
            lines.append(
                f"- {layer.label}: ${layer.total_value_usd / 1e12:.1f}T "
                f"({regime}, {node_count} nodes)"
            )
        else:
            lines.append(f"- {layer.label}: {regime} ({node_count} nodes)")

    # Top edges
    top_edges = sorted(flow_map.edges, key=lambda e: e.value_usd, reverse=True)[:5]
    if top_edges:
        lines.append("\nTop capital flows:")
        for edge in top_edges:
            lines.append(
                f"  {edge.source_layer} → {edge.target_layer}: "
                f"${edge.value_usd / 1e9:.1f}B ({edge.channel})"
            )

    return "\n".join(lines)


def _build_sector_summary(engine) -> str:
    """Build sector heatmap data for image prompt."""
    from analysis.flow_aggregator import aggregate_by_sector

    sectors = aggregate_by_sector(engine, days=30)
    if not sectors:
        return "No sector flow data available."

    lines = ["Sector Capital Flows (30-day):"]
    sorted_sectors = sorted(
        sectors.items(),
        key=lambda x: abs(x[1].get("net_flow", 0)),
        reverse=True,
    )

    for name, data in sorted_sectors[:12]:
        net = data.get("net_flow", 0)
        direction = data.get("direction", "neutral")
        accel = data.get("acceleration", "stable")
        if abs(net) > 0:
            lines.append(
                f"- {name}: ${net / 1e6:.0f}M {direction} ({accel})"
            )

    return "\n".join(lines)


def _build_thesis_summary(engine) -> str:
    """Build thesis state summary for image prompt."""
    from analysis.flow_thesis import update_current_states, FLOW_KNOWLEDGE

    update_current_states(engine)

    lines = ["Market Thesis States:"]
    for name, thesis in FLOW_KNOWLEDGE.items():
        state = thesis.get("current_state")
        if state and isinstance(state, dict):
            direction = state.get("direction", "neutral")
            detail = state.get("detail", "")
            conf = thesis.get("confidence", "low")
            lines.append(f"- {name}: {direction} ({conf}) — {detail[:60]}")

    return "\n".join(lines[:15])  # cap at 15 for prompt length


# ── Public Image Generators ──────────────────────────────────────

def generate_flow_infographic(
    engine,
    style: str = "dark",
    model_tier: str = DEFAULT_MODEL,
) -> ImageResult:
    """Generate a capital flow infographic from live data.

    Shows the 8-layer junction point model with flow arrows,
    sized by USD volume, colored by direction.
    """
    data_summary = _build_flow_data_summary(engine)

    prompt = (
        "Create a professional financial infographic showing global capital flows "
        "through 8 layers of the financial system. Layout as a horizontal flow diagram "
        "from left to right:\n\n"
        "Layers: Monetary → Credit → Institutional → Market → Corporate → "
        "Sovereign → Retail → Crypto\n\n"
        "Show flow arrows between layers with width proportional to dollar volume. "
        "Green arrows for inflows, red for outflows. Each layer is a column with "
        "2-4 nodes inside. Include dollar amounts on the largest flows.\n\n"
        f"{data_summary}\n\n"
        "Title: 'GRID Capital Flow Intelligence' with today's date. "
        "Include a small legend explaining arrow colors and sizes."
    )

    return _generate_image(prompt, model_tier, style, "flow_infographic")


def generate_sector_heatmap(
    engine,
    style: str = "dark",
    model_tier: str = DEFAULT_MODEL,
) -> ImageResult:
    """Generate a sector heatmap showing capital flow direction and magnitude."""
    sector_summary = _build_sector_summary(engine)

    prompt = (
        "Create a professional sector heatmap infographic for capital flows. "
        "Show 20 sectors arranged in a grid, each cell colored by flow direction: "
        "deep green = strong inflows, light green = mild inflows, gray = neutral, "
        "light red = mild outflows, deep red = strong outflows.\n\n"
        "Each cell shows: sector name, net flow in millions USD, and an arrow "
        "indicating acceleration (up arrow) or deceleration (down arrow).\n\n"
        f"{sector_summary}\n\n"
        "Title: 'GRID Sector Flow Heatmap'. "
        "Include conviction scores as small dots (green = high, yellow = medium, red = low)."
    )

    return _generate_image(prompt, model_tier, style, "sector_heatmap")


def generate_junction_dashboard(
    engine,
    style: str = "dark",
    model_tier: str = DEFAULT_MODEL,
) -> ImageResult:
    """Generate a junction point dashboard image."""
    data_summary = _build_flow_data_summary(engine)

    prompt = (
        "Create a financial dashboard infographic showing 23 economic junction points "
        "organized by category. Layout as a grid of cards, 4 columns:\n\n"
        "Categories: Monetary (5 cards), Credit (4 cards), Market (4 cards), "
        "Corporate (2 cards), Sovereign (4 cards), Retail (2 cards), Crypto (2 cards)\n\n"
        "Each card shows: metric name, current value in large text, "
        "1-month change with up/down arrow, a tiny sparkline, and a colored dot "
        "(green = confirmed data, yellow = estimated, red = stale).\n\n"
        f"{data_summary}\n\n"
        "Title: 'GRID Junction Point Monitor'. "
        "Clean, dense layout maximizing information per pixel."
    )

    return _generate_image(prompt, model_tier, style, "junction_dashboard")


def generate_market_briefing_image(
    engine,
    style: str = "cnbc",
    model_tier: str = DEFAULT_MODEL,
) -> ImageResult:
    """Generate a broadcast-style market briefing image."""
    flow_summary = _build_flow_data_summary(engine)
    thesis_summary = _build_thesis_summary(engine)

    prompt = (
        "Create a TV broadcast-style market briefing graphic, like a CNBC segment opener. "
        "Split into 3 panels:\n\n"
        "LEFT PANEL: 'Capital Flows' — show the top 5 flow arrows with dollar amounts, "
        "from source to destination sectors.\n\n"
        "CENTER PANEL: 'Market Thesis' — bullet list of 5 key thesis signals "
        "(bullish/bearish with icons), like a teleprompter.\n\n"
        "RIGHT PANEL: 'Key Numbers' — 4 large metric boxes showing global liquidity, "
        "VIX level, HY spread, and trade balance.\n\n"
        f"{flow_summary}\n\n{thesis_summary}\n\n"
        "Title bar: 'GRID INTELLIGENCE BRIEF' with date. "
        "Breaking-news energy, bold typography, gradient background."
    )

    return _generate_image(prompt, model_tier, style, "market_briefing")


def generate_custom(
    prompt: str,
    style: str = "dark",
    model_tier: str = DEFAULT_MODEL,
) -> ImageResult:
    """Generate a custom image from any prompt."""
    return _generate_image(prompt, model_tier, style, "custom")


# ── Batch Generation ─────────────────────────────────────────────

def generate_daily_briefing_pack(engine, style: str = "dark") -> list[ImageResult]:
    """Generate a full set of daily briefing images.

    Returns 4 images: flow infographic, sector heatmap,
    junction dashboard, and market briefing.
    """
    results = []
    generators = [
        ("flow_infographic", generate_flow_infographic),
        ("sector_heatmap", generate_sector_heatmap),
        ("junction_dashboard", generate_junction_dashboard),
        ("market_briefing", generate_market_briefing_image),
    ]

    for name, gen_func in generators:
        try:
            result = gen_func(engine, style=style)
            results.append(result)
            log.info("Generated {n}: {p}", n=name, p=result.file_path)
        except Exception as exc:
            log.error("Failed to generate {n}: {e}", n=name, e=str(exc))

    log.info("Daily briefing pack: {ok}/{total} images generated",
             ok=len(results), total=len(generators))
    return results
