"""In-process contracts metrics.

Thread-safe counters + a simple sum/count histogram rendered as Prometheus
text format. Intentionally tiny — we do not depend on prometheus_client so
that tests and dev installations stay lightweight.
"""
from __future__ import annotations

import threading
from collections import defaultdict
from typing import Any


_lock = threading.Lock()
_emitted: dict[str, int] = defaultdict(int)
_dispatched: dict[tuple[str, str], int] = defaultdict(int)
_failed: dict[tuple[str, str, str], int] = defaultdict(int)
_duration_sum: dict[tuple[str, str], float] = defaultdict(float)
_duration_count: dict[tuple[str, str], int] = defaultdict(int)


def emitted(contract_type: str) -> None:
    with _lock:
        _emitted[contract_type] += 1


def dispatched(contract_type: str, consumer: str) -> None:
    with _lock:
        _dispatched[(contract_type, consumer)] += 1


def failed(contract_type: str, consumer: str, error_type: str) -> None:
    with _lock:
        _failed[(contract_type, consumer, error_type)] += 1


def record_duration(contract_type: str, consumer: str, seconds: float) -> None:
    key = (contract_type, consumer)
    with _lock:
        _duration_sum[key] += seconds
        _duration_count[key] += 1


def snapshot() -> dict[str, Any]:
    """Return a copy of the current metric state (for tests / API)."""
    with _lock:
        return {
            "emitted": dict(_emitted),
            "dispatched": dict(_dispatched),
            "failed": dict(_failed),
            "duration_sum": dict(_duration_sum),
            "duration_count": dict(_duration_count),
        }


def reset() -> None:
    """Clear all metrics. Test-only."""
    with _lock:
        _emitted.clear()
        _dispatched.clear()
        _failed.clear()
        _duration_sum.clear()
        _duration_count.clear()


def render_prometheus() -> str:
    """Render metrics as Prometheus text format."""
    with _lock:
        em = dict(_emitted)
        dp = dict(_dispatched)
        fl = dict(_failed)
        dsum = dict(_duration_sum)
        dcount = dict(_duration_count)

    lines: list[str] = []

    lines.append("# HELP contracts_emitted_total Number of contracts emitted.")
    lines.append("# TYPE contracts_emitted_total counter")
    for ct, n in sorted(em.items()):
        lines.append(f'contracts_emitted_total{{contract="{ct}"}} {n}')

    lines.append("# HELP contracts_dispatched_total Number of handler dispatches.")
    lines.append("# TYPE contracts_dispatched_total counter")
    for (ct, consumer), n in sorted(dp.items()):
        lines.append(
            f'contracts_dispatched_total{{contract="{ct}",consumer="{consumer}"}} {n}'
        )

    lines.append("# HELP contracts_failed_total Number of handler failures.")
    lines.append("# TYPE contracts_failed_total counter")
    for (ct, consumer, err), n in sorted(fl.items()):
        lines.append(
            f'contracts_failed_total{{contract="{ct}",consumer="{consumer}",error="{err}"}} {n}'
        )

    lines.append("# HELP contracts_handler_duration_seconds Handler latency.")
    lines.append("# TYPE contracts_handler_duration_seconds summary")
    for (ct, consumer), s in sorted(dsum.items()):
        c = dcount.get((ct, consumer), 0)
        lines.append(
            f'contracts_handler_duration_seconds_sum{{contract="{ct}",consumer="{consumer}"}} {s}'
        )
        lines.append(
            f'contracts_handler_duration_seconds_count{{contract="{ct}",consumer="{consumer}"}} {c}'
        )

    return "\n".join(lines) + "\n"
