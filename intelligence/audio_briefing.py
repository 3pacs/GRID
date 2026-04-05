"""GRID -- Daily Intelligence Audio Briefing Pipeline.

Generates a NotebookLM-style audio briefing from GRID's live data:
  1. Pulls flow engine state (8 layers, regime, stress)
  2. Pulls CDS dashboard (credit regime, spreads)
  3. Pulls top thesis scores and unified market direction
  4. Builds a narrative briefing script via Gemini
  5. Converts script to audio via OpenAI TTS (tts-1-hd, voice=nova)
  6. Optionally generates a title card + combines into MP4 via ffmpeg

Public API:
    generate_briefing_script(engine) -> BriefingResult
    generate_briefing_audio(engine) -> BriefingResult
    generate_briefing_video(engine) -> BriefingResult
    get_latest_briefing() -> BriefingResult | None

Requires GEMINI_API_KEY and OPENAI_API_KEY in environment.
"""

from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log


# -- Configuration -----------------------------------------------------------

_GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
_OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
_OUTPUT_DIR = Path("/data/grid_v4/grid_repo/output/briefings")

# TTS settings
TTS_MODEL = "tts-1-hd"
TTS_VOICE = "shimmer"
TTS_FORMAT = "mp3"

# Gemini model for script generation
GEMINI_SCRIPT_MODEL = "gemini-2.5-flash"

# Title card image model (Imagen)
TITLE_CARD_MODEL = "imagen-4.0-fast-generate-001"

# Briefing style constants
MAX_SCRIPT_TOKENS = 4096
BRIEFING_DURATION_TARGET = "45-60 seconds when read aloud"


@dataclass(frozen=True)
class BriefingResult:
    """Immutable result from briefing generation."""

    script_text: str
    audio_path: str | None = None
    video_path: str | None = None
    title_card_path: str | None = None
    briefing_date: str = ""
    generated_at: str = ""
    duration_ms: int = 0
    flow_summary: dict = field(default_factory=dict)
    credit_summary: dict = field(default_factory=dict)
    thesis_summary: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "script_text": self.script_text,
            "audio_path": self.audio_path,
            "video_path": self.video_path,
            "title_card_path": self.title_card_path,
            "briefing_date": self.briefing_date,
            "generated_at": self.generated_at,
            "duration_ms": self.duration_ms,
            "flow_summary": self.flow_summary,
            "credit_summary": self.credit_summary,
            "thesis_summary": self.thesis_summary,
        }


# -- Data Collection ---------------------------------------------------------

def _collect_flow_state(engine) -> dict[str, Any]:
    """Pull 8-layer flow engine state for briefing context."""
    try:
        from analysis.money_flow_engine import build_flow_map

        flow_map = build_flow_map(engine)

        layers = []
        for layer in flow_map.layers:
            layer_info = {
                "name": layer.label,
                "regime": layer.regime,
                "stress": layer.stress_score,
                "total_usd": layer.total_value_usd,
                "net_flow_1m": layer.net_flow_1m,
                "node_count": len(layer.nodes),
                "confidence": layer.confidence,
            }
            layers.append(layer_info)

        top_edges = sorted(
            flow_map.edges, key=lambda e: e.value_usd, reverse=True
        )[:5]
        edges = [
            {
                "from": e.source_layer,
                "to": e.target_layer,
                "value_usd": e.value_usd,
                "channel": e.channel,
                "direction": e.direction,
            }
            for e in top_edges
        ]

        return {
            "layers": layers,
            "top_edges": edges,
            "global_liquidity": flow_map.global_liquidity_total,
            "liquidity_change_1m": flow_map.global_liquidity_change_1m,
            "narrative": flow_map.narrative or "",
        }
    except Exception as exc:
        log.error("Failed to collect flow state: {e}", e=str(exc))
        return {"error": str(exc), "layers": [], "top_edges": []}


def _collect_credit_state(engine) -> dict[str, Any]:
    """Pull CDS dashboard for credit regime and spreads."""
    try:
        from intelligence.cds_tracker import build_cds_dashboard, cds_to_dict

        dashboard = build_cds_dashboard(engine)
        return cds_to_dict(dashboard)
    except Exception as exc:
        log.error("Failed to collect CDS state: {e}", e=str(exc))
        return {"error": str(exc), "regime": "unknown"}


def _collect_thesis_state(engine) -> dict[str, Any]:
    """Pull unified thesis scores and top convictions."""
    try:
        from analysis.flow_thesis import generate_unified_thesis

        unified = generate_unified_thesis(engine)

        # Extract top theses by conviction (handles both list and dict formats)
        theses = unified.get("theses", unified.get("models", []))
        top_theses = []
        if isinstance(theses, list):
            for thesis in theses:
                if isinstance(thesis, dict):
                    top_theses.append({
                        "name": thesis.get("name", thesis.get("key", "unknown")),
                        "direction": thesis.get("direction", "neutral"),
                        "detail": thesis.get("reasoning", thesis.get("detail", "")),
                        "confidence": thesis.get("confidence", "low"),
                    })
        elif isinstance(theses, dict):
            for name, thesis in theses.items():
                state = thesis.get("current_state", {})
                if state and isinstance(state, dict):
                    top_theses.append({
                        "name": name,
                        "direction": state.get("direction", "neutral"),
                        "detail": state.get("detail", ""),
                        "confidence": thesis.get("confidence", "low"),
                    })

        # Sort by confidence weight
        confidence_order = {"high": 3, "moderate": 2, "low": 1}
        top_theses.sort(
            key=lambda t: confidence_order.get(t["confidence"], 0),
            reverse=True,
        )

        return {
            "overall_direction": unified.get("overall_direction", "neutral"),
            "conviction": unified.get("conviction", "low"),
            "key_drivers": unified.get("key_drivers", []),
            "risk_factors": unified.get("risk_factors", []),
            "narrative": unified.get("narrative", ""),
            "top_theses": top_theses[:8],
            "agreements": unified.get("agreements", []),
            "contradictions": unified.get("contradictions", []),
        }
    except Exception as exc:
        log.error("Failed to collect thesis state: {e}", e=str(exc))
        return {"error": str(exc), "overall_direction": "unknown"}


def _collect_all_data(engine) -> dict[str, Any]:
    """Collect all briefing data from GRID's engines."""
    return {
        "flow": _collect_flow_state(engine),
        "credit": _collect_credit_state(engine),
        "thesis": _collect_thesis_state(engine),
        "date": date.today().isoformat(),
    }


# -- Script Generation via Gemini -------------------------------------------

def _build_briefing_prompt(data: dict[str, Any]) -> str:
    """Build the Gemini prompt to generate a briefing script."""
    briefing_date = data.get("date", date.today().isoformat())

    flow = data.get("flow", {})
    credit = data.get("credit", {})
    thesis = data.get("thesis", {})

    # Format layer summaries
    layer_lines = []
    for layer in flow.get("layers", []):
        name = layer.get("name", "Unknown")
        regime = layer.get("regime", "neutral")
        stress = layer.get("stress")
        total = layer.get("total_usd")
        net = layer.get("net_flow_1m")
        stress_str = f", stress={stress:.2f}" if stress is not None else ""
        total_str = f", ${total / 1e12:.1f}T" if total else ""
        net_str = f", net flow ${net / 1e9:.0f}B/mo" if net else ""
        layer_lines.append(
            f"  - {name}: {regime}{stress_str}{total_str}{net_str}"
        )
    layers_block = "\n".join(layer_lines) if layer_lines else "  (data unavailable)"

    # Format top edges
    edge_lines = []
    for edge in flow.get("top_edges", []):
        edge_lines.append(
            f"  - {edge['from']} -> {edge['to']}: "
            f"${edge['value_usd'] / 1e9:.1f}B via {edge['channel']}"
        )
    edges_block = "\n".join(edge_lines) if edge_lines else "  (no edges)"

    # Format credit
    regime_str = credit.get("regime", "unknown")
    spreads_lines = []
    for s in credit.get("spreads", []):
        val = s.get("value")
        z = s.get("z_score_2y")
        label = s.get("label", s.get("key", ""))
        if val is not None:
            z_str = f", z={z:.1f}" if z is not None else ""
            spreads_lines.append(f"  - {label}: {val:.2f}%{z_str}")
    spreads_block = "\n".join(spreads_lines) if spreads_lines else "  (unavailable)"

    # Format theses
    thesis_lines = []
    for t in thesis.get("top_theses", []):
        thesis_lines.append(
            f"  - {t['name']}: {t['direction']} "
            f"(confidence: {t['confidence']}) -- {t['detail'][:80]}"
        )
    thesis_block = "\n".join(thesis_lines) if thesis_lines else "  (unavailable)"

    overall = thesis.get("overall_direction", "neutral")
    conviction = thesis.get("conviction", "low")
    key_drivers = thesis.get("key_drivers", [])
    risk_factors = thesis.get("risk_factors", [])
    flow_narrative = flow.get("narrative", "")
    thesis_narrative = thesis.get("narrative", "")

    global_liq = flow.get("global_liquidity")
    liq_str = f"${global_liq / 1e12:.1f}T" if global_liq else "N/A"
    liq_change = flow.get("liquidity_change_1m")
    liq_chg_str = (
        f"{'expanding' if liq_change > 0 else 'contracting'} "
        f"${abs(liq_change) / 1e9:.0f}B/month"
        if liq_change
        else "stable"
    )

    prompt = f"""You are the voice of GRID Intelligence, an advanced capital flow analysis system.
Write a {BRIEFING_DURATION_TARGET} audio briefing script for {briefing_date}.

TONE: Smooth, confident, a little sultry — like the smartest person at the party who also
happens to love markets. Warm energy, not rushed. Lean into pauses and emphasis.
Use specific numbers but make them sound effortless, not clinical.
Lead with what's interesting. When there's risk, make it sound intriguing, not alarming.
A hint of playfulness — she knows something you don't, and she's about to tell you.

STRUCTURE:
1. Opening: "Good morning. GRID Intelligence briefing for [date]."
2. Credit regime status (1-2 sentences) -- what's credit telling us?
3. Flow engine headline (2-3 sentences) -- 8-layer summary, key movements
4. Top thesis signals (2-3 sentences) -- where conviction is highest
5. Key risk or contrarian signal (1 sentence) -- what to watch
6. Close: brief 1-sentence outlook

DATA:
=== 8-Layer Flow Engine ===
Global Liquidity: {liq_str} ({liq_chg_str})
{layers_block}

Top Capital Flows:
{edges_block}

Flow Narrative: {flow_narrative}

=== Credit / CDS Dashboard ===
Credit Regime: {regime_str}
{spreads_block}

=== Market Thesis ===
Overall Direction: {overall} (conviction: {conviction})
Key Drivers: {', '.join(d if isinstance(d, str) else d.get('name', d.get('key', str(d))) for d in key_drivers[:5]) if key_drivers else 'N/A'}
Risk Factors: {', '.join(r if isinstance(r, str) else r.get('name', r.get('key', str(r))) for r in risk_factors[:5]) if risk_factors else 'N/A'}

Top Theses:
{thesis_block}

Thesis Narrative: {thesis_narrative}

RULES:
- Do NOT use markdown, bullet points, or formatting -- pure spoken script
- Use natural speech: "three point one six percent" not "3.16%"
- Round numbers sensibly: "$4.2 trillion" not "$4,218,543,000,000"
- If data is unavailable, skip that section gracefully
- End with a clear, actionable takeaway
- Total length: 150-250 words (approximately {BRIEFING_DURATION_TARGET})
"""
    return prompt


def _get_gemini_client():
    """Lazy-load Gemini client."""
    from google import genai

    key = _GEMINI_API_KEY or os.getenv("GEMINI_API_KEY", "")
    if not key:
        raise ValueError("GEMINI_API_KEY not set")
    return genai.Client(api_key=key)


def _generate_script_text(data: dict[str, Any]) -> str:
    """Generate briefing script. Tries Gemini first (paid), falls back to OpenAI."""
    prompt = _build_briefing_prompt(data)

    # Try Gemini first (paid credits available)
    try:
        client = _get_gemini_client()
        log.info("Generating briefing script via Gemini ({m})", m=GEMINI_SCRIPT_MODEL)
        response = client.models.generate_content(
            model=GEMINI_SCRIPT_MODEL,
            contents=prompt,
        )
        if not response.text:
            raise RuntimeError("Gemini returned empty response")
        script = response.text.strip()
        log.info("Briefing script generated via Gemini: {w} words", w=len(script.split()))
        return script
    except Exception as exc:
        log.warning("Gemini script gen failed, trying OpenAI: {e}", e=str(exc))

    # Fallback to OpenAI
    try:
        client = _get_openai_client()
        log.info("Generating briefing script via OpenAI (gpt-4o)")
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a financial news anchor for GRID Intelligence, a quantitative trading platform. Generate concise, data-driven audio briefings in a professional broadcast style. No markdown, no headers — pure spoken word."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=2000,
            temperature=0.7,
        )
        script = response.choices[0].message.content.strip()
        log.info("Briefing script generated via OpenAI: {w} words", w=len(script.split()))
        return script
    except Exception as exc2:
        raise RuntimeError(f"Both Gemini and OpenAI failed: Gemini={exc}, OpenAI={exc2}")


# -- Audio Generation via OpenAI TTS ----------------------------------------

def _get_openai_client():
    """Lazy-load OpenAI client."""
    from openai import OpenAI

    key = _OPENAI_API_KEY or os.getenv("OPENAI_API_KEY", "")
    if not key:
        raise ValueError("OPENAI_API_KEY not set")
    return OpenAI(api_key=key)


def _ensure_output_dir() -> Path:
    """Create output directory if needed."""
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return _OUTPUT_DIR


def _generate_audio_file(script_text: str, briefing_date: str) -> str:
    """Convert script text to MP3 via OpenAI TTS.

    Returns the file path of the saved audio.
    """
    client = _get_openai_client()
    output_dir = _ensure_output_dir()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"briefing_{briefing_date}_{timestamp}.{TTS_FORMAT}"
    file_path = output_dir / filename

    log.info(
        "Generating audio: model={m}, voice={v}, format={f}",
        m=TTS_MODEL, v=TTS_VOICE, f=TTS_FORMAT,
    )

    response = client.audio.speech.create(
        model=TTS_MODEL,
        voice=TTS_VOICE,
        input=script_text,
        response_format=TTS_FORMAT,
    )

    response.stream_to_file(str(file_path))

    log.info("Audio saved: {p} ({kb}KB)", p=file_path, kb=file_path.stat().st_size // 1024)
    return str(file_path)


# -- Title Card Generation via Gemini Imagen ---------------------------------

def _generate_title_card(briefing_date: str) -> str:
    """Generate a broadcast-style title card image via Gemini Imagen.

    Returns the file path of the saved PNG.
    """
    from google.genai import types

    client = _get_gemini_client()
    output_dir = _ensure_output_dir()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"title_card_{briefing_date}_{timestamp}.png"
    file_path = output_dir / filename

    prompt = (
        f"Create a professional broadcast-style title card for 'GRID INTELLIGENCE BRIEFING' "
        f"dated {briefing_date}. Dark background (#0a0a1a) with cyan (#00d4ff) and "
        f"white text. Large bold title 'GRID INTELLIGENCE' at top. Subtitle 'Daily Briefing' "
        f"below. Date displayed prominently. Subtle financial data visualization in background "
        f"(flowing lines, grid patterns). Professional, Bloomberg-terminal aesthetic. "
        f"16:9 aspect ratio. Clean, modern, high contrast."
    )

    log.info("Generating title card via Imagen")

    response = client.models.generate_images(
        model=TITLE_CARD_MODEL,
        prompt=prompt,
        config=types.GenerateImagesConfig(
            number_of_images=1,
            output_mime_type="image/png",
        ),
    )

    if not response.generated_images:
        raise RuntimeError("Imagen returned no images for title card")

    img_bytes = response.generated_images[0].image.image_bytes

    with open(file_path, "wb") as f:
        f.write(img_bytes)

    log.info("Title card saved: {p} ({kb}KB)", p=file_path, kb=len(img_bytes) // 1024)
    return str(file_path)


# -- Video Generation via ffmpeg ---------------------------------------------

def _combine_to_video(
    audio_path: str,
    title_card_path: str,
    briefing_date: str,
) -> str:
    """Combine title card image + audio into MP4 via ffmpeg.

    Creates a video where the title card is displayed for the full
    duration of the audio track.

    Returns the file path of the saved MP4.
    """
    output_dir = _ensure_output_dir()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"briefing_{briefing_date}_{timestamp}.mp4"
    video_path = output_dir / filename

    cmd = [
        "ffmpeg", "-y",
        "-loop", "1",
        "-i", title_card_path,
        "-i", audio_path,
        "-c:v", "libx264",
        "-tune", "stillimage",
        "-c:a", "aac",
        "-b:a", "192k",
        "-pix_fmt", "yuv420p",
        "-shortest",
        "-movflags", "+faststart",
        str(video_path),
    ]

    log.info("Combining audio + title card into video: {p}", p=video_path)

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=120,
    )

    if result.returncode != 0:
        log.error("ffmpeg failed: {e}", e=result.stderr[:500])
        raise RuntimeError(f"ffmpeg failed: {result.stderr[:300]}")

    log.info(
        "Video saved: {p} ({kb}KB)",
        p=video_path,
        kb=video_path.stat().st_size // 1024,
    )
    return str(video_path)


# -- Public API --------------------------------------------------------------

def generate_briefing_script(engine) -> BriefingResult:
    """Generate the briefing script text only (no audio).

    Collects live data from the flow engine, CDS tracker, and thesis
    system, then uses Gemini to write the script.
    """
    t0 = time.monotonic()
    briefing_date = date.today().isoformat()

    data = _collect_all_data(engine)
    script = _generate_script_text(data)

    duration_ms = int((time.monotonic() - t0) * 1000)

    return BriefingResult(
        script_text=script,
        briefing_date=briefing_date,
        generated_at=datetime.now(timezone.utc).isoformat(),
        duration_ms=duration_ms,
        flow_summary=data.get("flow", {}),
        credit_summary=data.get("credit", {}),
        thesis_summary=data.get("thesis", {}),
    )


def _save_metadata(result: BriefingResult) -> None:
    """Save a JSON sidecar alongside the audio so every recording is preserved."""
    if not result.audio_path:
        return
    import json

    meta_path = Path(result.audio_path).with_suffix(".json")
    try:
        with open(meta_path, "w") as f:
            json.dump(result.to_dict(), f, indent=2, default=str)
        log.info("Metadata saved: {p}", p=meta_path)
    except Exception as exc:
        log.warning("Failed to save metadata: {e}", e=str(exc))


def generate_briefing_audio(engine) -> BriefingResult:
    """Generate the full briefing with script + audio MP3.

    Steps:
        1. Collect data from all GRID engines
        2. Generate script via Gemini
        3. Convert to audio via OpenAI TTS
        4. Save JSON metadata sidecar for archival
    """
    t0 = time.monotonic()
    briefing_date = date.today().isoformat()

    data = _collect_all_data(engine)
    script = _generate_script_text(data)
    audio_path = _generate_audio_file(script, briefing_date)

    duration_ms = int((time.monotonic() - t0) * 1000)

    log.info(
        "Briefing audio complete: {ms}ms, audio={a}",
        ms=duration_ms, a=audio_path,
    )

    result = BriefingResult(
        script_text=script,
        audio_path=audio_path,
        briefing_date=briefing_date,
        generated_at=datetime.now(timezone.utc).isoformat(),
        duration_ms=duration_ms,
        flow_summary=data.get("flow", {}),
        credit_summary=data.get("credit", {}),
        thesis_summary=data.get("thesis", {}),
    )

    _save_metadata(result)
    return result


def generate_briefing_video(engine) -> BriefingResult:
    """Generate full briefing with script + audio + video MP4.

    Steps:
        1. Collect data from all GRID engines
        2. Generate script via Gemini
        3. Convert to audio via OpenAI TTS
        4. Generate title card via Gemini Imagen
        5. Combine into MP4 via ffmpeg
    """
    t0 = time.monotonic()
    briefing_date = date.today().isoformat()

    data = _collect_all_data(engine)
    script = _generate_script_text(data)
    audio_path = _generate_audio_file(script, briefing_date)
    title_card_path = _generate_title_card(briefing_date)
    video_path = _combine_to_video(audio_path, title_card_path, briefing_date)

    duration_ms = int((time.monotonic() - t0) * 1000)

    log.info(
        "Briefing video complete: {ms}ms, video={v}",
        ms=duration_ms, v=video_path,
    )

    return BriefingResult(
        script_text=script,
        audio_path=audio_path,
        video_path=video_path,
        title_card_path=title_card_path,
        briefing_date=briefing_date,
        generated_at=datetime.now(timezone.utc).isoformat(),
        duration_ms=duration_ms,
        flow_summary=data.get("flow", {}),
        credit_summary=data.get("credit", {}),
        thesis_summary=data.get("thesis", {}),
    )


def _load_metadata(mp3_path: Path) -> BriefingResult:
    """Load metadata from JSON sidecar, falling back to file info."""
    import json

    meta_path = mp3_path.with_suffix(".json")
    parts = mp3_path.stem.split("_")
    briefing_date = parts[1] if len(parts) > 1 else ""

    if meta_path.exists():
        try:
            with open(meta_path) as f:
                data = json.load(f)
            return BriefingResult(
                script_text=data.get("script_text", ""),
                audio_path=str(mp3_path),
                briefing_date=data.get("briefing_date", briefing_date),
                generated_at=data.get("generated_at", ""),
                duration_ms=data.get("duration_ms", 0),
                flow_summary=data.get("flow_summary", {}),
                credit_summary=data.get("credit_summary", {}),
                thesis_summary=data.get("thesis_summary", {}),
            )
        except Exception as exc:
            log.warning("Failed to parse briefing metadata from {p}: {e}", p=mp3_path, e=exc)

    return BriefingResult(
        script_text="(no metadata saved for this recording)",
        audio_path=str(mp3_path),
        briefing_date=briefing_date,
        generated_at=datetime.fromtimestamp(
            mp3_path.stat().st_mtime, tz=timezone.utc
        ).isoformat(),
    )


def get_latest_briefing() -> BriefingResult | None:
    """Find the most recent briefing audio file and return a result with metadata."""
    if not _OUTPUT_DIR.exists():
        return None

    mp3_files = sorted(
        _OUTPUT_DIR.glob("briefing_*.mp3"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not mp3_files:
        return None

    return _load_metadata(mp3_files[0])


def list_all_briefings() -> list[dict[str, Any]]:
    """List all saved briefing recordings, newest first.

    Returns a list of dicts with filename, date, size, and whether
    metadata (script text) is available.
    """
    if not _OUTPUT_DIR.exists():
        return []

    mp3_files = sorted(
        _OUTPUT_DIR.glob("briefing_*.mp3"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    results = []
    for mp3 in mp3_files:
        parts = mp3.stem.split("_")
        briefing_date = parts[1] if len(parts) > 1 else ""
        meta_path = mp3.with_suffix(".json")
        has_script = meta_path.exists()

        results.append({
            "filename": mp3.name,
            "briefing_date": briefing_date,
            "size_bytes": mp3.stat().st_size,
            "generated_at": datetime.fromtimestamp(
                mp3.stat().st_mtime, tz=timezone.utc
            ).isoformat(),
            "has_script": has_script,
        })

    return results


def get_briefing_by_filename(filename: str) -> BriefingResult | None:
    """Load a specific briefing by its MP3 filename."""
    if not filename.endswith(".mp3"):
        filename = f"{filename}.mp3"

    file_path = _OUTPUT_DIR / filename
    if not file_path.exists():
        return None

    return _load_metadata(file_path)
