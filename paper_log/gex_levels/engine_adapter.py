"""Thin adapter over ``physics.dealer_gamma.DealerGammaEngine``.

Why this exists: a second lane (``fix/dealer-gamma-sign-spot-20260924``,
merged into this branch 2026-09-24 at head 1bc63cf9 / PR #644) fixed the
engine's sign convention and spot source while this package was being
built. Rather than call ``DealerGammaEngine`` directly from
``preopen.py``, every caller in this package goes through
:class:`DealerGammaAdapter`, and every field the engine's returned dict
might rename/add lives behind :func:`DealerGammaAdapter._translate` — so
adapting to the merged engine (below) only required editing this one
function and its tests, not every call site.

Pre-registration inputs this maps directly (as amended by Amendment 1,
2951e4cc):
  "Levels, from `physics.dealer_gamma.DealerGammaEngine` ...: gamma flip,
  the engine's spot and its source, aggregate GEX, normalized GEX, and
  regime ... Sign convention: dealers modeled long calls and short puts;
  GEX > 0 means dealers long gamma."
  "Walls (Amendment 1). The engine's own put wall and call wall are
  recorded as `engine_put_wall` / `engine_call_wall` but are not tested."
  "`engine_unavailable`: the engine returns its unavailable result (no
  measured spot) or no regime. A missing flip or wall is recorded, not
  excluded."

Note what changed in Amendment 1 versus the original pre-registration:
`put_wall`/`call_wall` are no longer part of what this adapter calls
"available" — they're recorded as `engine_put_wall`/`engine_call_wall`
(untested; the *tested* walls, per Amendment 1, come from
``tested_walls.py``, computed from the full per-strike chain, not from
this adapter). A missing `gamma_flip` also no longer makes a result
unavailable — only a missing `spot` or `regime` does.

The merged engine's ``compute_gex_profile`` returns one of three shapes,
all handled below:
  1. Success: a flat dict with spot/gamma_flip/put_wall/call_wall/
     gex_aggregate/gex_normalized/regime (+ gamma_wall/dealer_delta/
     vanna_exposure/charm_exposure/profile/per_strike, not used here).
  2. No options chain at all (``chain.empty``): the legacy
     ``{"error": "No options data for ...", "ticker": ...}`` — unchanged
     from before the fix, still just those two keys.
  3. No measured spot (checked ``options_daily_signals.spot_price`` then
     ``resolved_series``): the richer ``store.availability.unavailable()``
     payload — ``available: False``, ``status: "unavailable"``,
     ``reason`` (a specific, human-readable explanation — e.g. "no
     measured spot price for SPY on 2026-09-24 (checked
     options_daily_signals.spot_price and resolved_series)"), ``source``,
     every measured field explicitly ``None`` — *plus* a legacy
     ``error: "No spot price for {ticker}"`` key kept for older callers.
     ``_translate`` prefers the specific ``reason`` over the terser
     legacy ``error`` string when both are present.

Note on PIT correctness (task spec's "only data created before the
pre-open run", and the coordinator's note on merging this fix): the
engine's spot source, ``options_daily_signals.spot_price``, is upserted on
every re-pull for a given ``(ticker, signal_date)`` with no ``updated_at``
column — see ``DealerGammaEngine._get_spot``'s docstring. That makes it
contemporaneous (safe) for a *live* pre-open read of *today's* row, which
is the only thing this package ever does (``preopen.py`` always resolves
``snap_date``/``session_date`` from the injected clock, never accepts a
historical date to backfill) — but it would NOT be safe to reuse this
adapter for a historical/backfill read of a past date, since a later
same-day re-pull can silently overwrite what an earlier pre-open run would
have seen. If this package ever grows a backfill mode, that gap needs
closing first (an append-only spot history, or an ``updated_at`` column).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Protocol

from sqlalchemy.engine import Engine

from physics.dealer_gamma import DealerGammaEngine


@dataclass(frozen=True)
class LevelsResult:
    """Canonical, engine-version-independent shape for a levels lookup.

    ``available`` gates only on ``spot``/``regime`` (Amendment 1) — a
    ``None`` ``gamma_flip``, ``engine_put_wall``, or ``engine_call_wall``
    can still appear on an ``available=True`` result; callers record that
    as a missing level, not an exclusion.
    """

    available: bool
    unavailable_reason: str | None
    spot: float | None
    spot_source: str | None
    gamma_flip: float | None
    engine_put_wall: float | None
    engine_call_wall: float | None
    gex_aggregate: float | None
    gex_normalized: float | None
    regime: str | None
    raw: dict[str, Any] = field(default_factory=dict)


class LevelsEngine(Protocol):
    """What :class:`DealerGammaAdapter` needs from an engine — satisfied by
    ``physics.dealer_gamma.DealerGammaEngine`` and by any test double."""

    def compute_gex_profile(self, ticker: str, snap_date: date | None = None) -> dict[str, Any]: ...


# Default label for a *successful* result: the engine's success-path dict
# still has no per-call `spot_source`/`source` key (only its unavailable
# payload does), so this documents, from DealerGammaEngine._get_spot's own
# docstring, what actually supplied `spot` on the success path. If a
# future engine version adds an explicit `spot_source`/`source` key to the
# success dict, `_translate` prefers that value automatically instead —
# see the fallback chain below.
_DEFAULT_SPOT_SOURCE = (
    "options_daily_signals.spot_price (or resolved_series as a secondary "
    "source) via physics.dealer_gamma.DealerGammaEngine"
)

# Amendment 1: only these two gate availability now.
_REQUIRED_KEYS = ("spot", "regime")


class DealerGammaAdapter:
    """Adapts ``DealerGammaEngine`` (or a mock) to :class:`LevelsResult`."""

    def __init__(self, engine: LevelsEngine | None = None, *, db_engine: Engine | None = None,
                 risk_free_rate: float = 0.05) -> None:
        if engine is not None and db_engine is not None:
            raise ValueError("pass either engine= (a LevelsEngine/mock) or db_engine=, not both")
        if engine is not None:
            self._engine: LevelsEngine = engine
        elif db_engine is not None:
            self._engine = DealerGammaEngine(db_engine, risk_free_rate=risk_free_rate)
        else:
            raise ValueError("must pass engine= or db_engine=")

    def get_levels(self, ticker: str, snap_date: date) -> LevelsResult:
        raw = self._engine.compute_gex_profile(ticker, snap_date)
        return self._translate(raw)

    @staticmethod
    def _translate(raw: dict[str, Any] | None) -> LevelsResult:
        if not raw:
            return _unavailable("engine returned no result", raw={})

        if raw.get("available") is False:
            # store.availability.unavailable() payload — the engine's "no
            # measured spot" case. Prefer its specific `reason` over the
            # legacy `error` key the engine also sets for older callers.
            return _unavailable(
                str(raw.get("reason") or raw.get("error") or "engine result unavailable"),
                raw=raw,
            )

        if "error" in raw:
            # Legacy shape only: `{"error": ..., "ticker": ...}` with no
            # other keys at all (e.g. an empty options chain — the engine
            # never got far enough to look up spot).
            return _unavailable(str(raw["error"]), raw=raw)

        values = {k: raw.get(k) for k in _REQUIRED_KEYS}
        missing = [k for k, v in values.items() if v is None]

        if missing:
            # Amendment 1: "engine_unavailable ... means no measured spot
            # or no regime" — narrower than the original pre-registration;
            # a missing flip/wall no longer lands here.
            return _unavailable(f"engine result missing: {', '.join(sorted(missing))}", raw=raw)

        spot_source = raw.get("spot_source") or raw.get("source") or _DEFAULT_SPOT_SOURCE

        return LevelsResult(
            available=True,
            unavailable_reason=None,
            spot=_as_float(raw.get("spot")),
            spot_source=str(spot_source),
            gamma_flip=_as_float(raw.get("gamma_flip")),
            engine_put_wall=_as_float(raw.get("put_wall")),
            engine_call_wall=_as_float(raw.get("call_wall")),
            gex_aggregate=_as_float(raw.get("gex_aggregate")),
            gex_normalized=_as_float(raw.get("gex_normalized")),
            regime=raw.get("regime"),
            raw=raw,
        )


def _unavailable(reason: str, *, raw: dict[str, Any]) -> LevelsResult:
    return LevelsResult(
        available=False,
        unavailable_reason=reason,
        spot=None, spot_source=None, gamma_flip=None,
        engine_put_wall=None, engine_call_wall=None,
        gex_aggregate=None, gex_normalized=None,
        regime=None, raw=raw,
    )


def _as_float(value: Any) -> float | None:
    return None if value is None else float(value)
