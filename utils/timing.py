"""Lightweight perf timing helper for dashboard/intelligence hot paths.

Usage:
    from utils.timing import timed_section

    with timed_section("trust_scorer.update_trust_scores", log):
        update_trust_scores(engine)

Logs duration in milliseconds at INFO level. Designed to be a no-op cost
addition (a single time.perf_counter() pair + one log line per block) so
it can stay in place across cold and warm requests.

Each line is prefixed with "timed_section " so it can be grep'd cleanly
out of journalctl.
"""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterator


@contextmanager
def timed_section(name: str, logger: Any, *, extra: dict | None = None) -> Iterator[None]:
    """Time a code block and emit one INFO log line on exit.

    Args:
        name: stable identifier for the block (e.g. "dashboard.trust").
        logger: loguru/standard logger with .info / .exception methods.
        extra: optional dict of additional fields appended to the log line.

    The helper never swallows exceptions — it logs the elapsed time even
    when the wrapped block raises, then re-raises.
    """
    trace_id = uuid.uuid4().hex[:8]
    start = time.perf_counter()
    failed = False
    try:
        yield
    except Exception:
        failed = True
        raise
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        suffix = ""
        if extra:
            try:
                suffix = " " + " ".join(f"{k}={v}" for k, v in extra.items())
            except Exception:
                suffix = ""
        status = "FAIL" if failed else "ok"
        try:
            # loguru style: positional message, no %-formatting surprises
            logger.info(
                "timed_section name={n} ms={ms:.1f} status={s} trace={t}{x}",
                n=name, ms=elapsed_ms, s=status, t=trace_id, x=suffix,
            )
        except TypeError:
            # stdlib logging fallback
            try:
                logger.info(
                    "timed_section name=%s ms=%.1f status=%s trace=%s%s",
                    name, elapsed_ms, status, trace_id, suffix,
                )
            except Exception:
                pass
        except Exception:
            pass
