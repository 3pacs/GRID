"""Daily spend ledger for the Hermes analyst bridge.

A tiny, file-backed accumulator keyed by UTC date. It exists so a daily
spend cap can be enforced *before* Hermes is wired into the 6:30 batch
scheduler — the provider records estimated USD per call and refuses to call
once the day's cap is hit, falling back to the local analyst instead.

Persistence is best-effort: if the ledger path is unwritable we keep an
in-process tally so a single long-lived daemon still caps correctly, and a
broken disk never breaks inference (graceful degradation).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger as log


def _utc_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class SpendLedger:
    """Accumulates estimated USD spend per UTC day.

    Parameters:
        path: Optional JSON file backing the ledger. When ``None`` the ledger
            is in-process only (still correct for a single daemon).
        retain_days: Drop day entries older than this on save to bound growth.
    """

    def __init__(self, path: str | os.PathLike[str] | None = None, retain_days: int = 90) -> None:
        self.path = Path(path).expanduser() if path else None
        self.retain_days = retain_days
        self._mem: dict[str, float] = {}
        self._load()

    def _load(self) -> None:
        if not self.path or not self.path.exists():
            return
        try:
            self._mem = {str(k): float(v) for k, v in json.loads(self.path.read_text()).items()}
        except Exception as exc:  # corrupt/unreadable ledger must never crash a call
            log.warning("Hermes spend ledger unreadable ({p}): {e}", p=self.path, e=str(exc))
            self._mem = {}

    def _save(self) -> None:
        if not self.path:
            return
        try:
            if self.retain_days and len(self._mem) > self.retain_days:
                for stale in sorted(self._mem)[: -self.retain_days]:
                    self._mem.pop(stale, None)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps(self._mem, indent=2, sort_keys=True))
            tmp.replace(self.path)  # atomic on POSIX
        except Exception as exc:  # disk problems degrade to in-process tally
            log.warning("Hermes spend ledger unwritable ({p}): {e}", p=self.path, e=str(exc))

    def spend_today(self) -> float:
        """USD already spent in the current UTC day."""
        return round(self._mem.get(_utc_today(), 0.0), 6)

    def record(self, usd: float) -> float:
        """Add ``usd`` to today's tally and persist. Returns the new total."""
        if usd and usd > 0:
            day = _utc_today()
            self._mem[day] = round(self._mem.get(day, 0.0) + float(usd), 6)
            self._save()
        return self.spend_today()

    def would_exceed(self, cap_usd: float) -> bool:
        """True when today's spend is already at/over ``cap_usd`` (cap>0)."""
        return bool(cap_usd and cap_usd > 0 and self.spend_today() >= cap_usd)
