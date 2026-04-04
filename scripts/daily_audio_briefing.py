#!/usr/bin/env python3
"""Daily audio briefing + thesis deep dive cron job.

Generates an audio briefing and triggers a deep dive analysis.
Run via cron: 0 6 * * 1-5 (weekdays 6 AM UTC)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from db import get_engine
from loguru import logger as log


def main():
    engine = get_engine()

    # 1. Generate audio briefing
    log.info("=== Daily Audio Briefing ===")
    try:
        from intelligence.audio_briefing import generate_briefing_audio
        result = generate_briefing_audio(engine)
        log.info(
            "Audio briefing complete: {p} ({ms}ms)",
            p=result.audio_path, ms=result.duration_ms,
        )
    except Exception as exc:
        log.error("Audio briefing failed: {e}", e=str(exc))

    # 2. Run thesis cycle (includes auto deep dive)
    log.info("=== Thesis Cycle + Deep Dive ===")
    try:
        from intelligence.thesis_tracker import run_thesis_cycle
        report = run_thesis_cycle(engine)
        log.info("Thesis cycle: {r}", r=report)
    except Exception as exc:
        log.error("Thesis cycle failed: {e}", e=str(exc))


if __name__ == "__main__":
    main()
