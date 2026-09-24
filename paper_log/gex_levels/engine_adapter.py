"""Thin adapter over ``physics.dealer_gamma.DealerGammaEngine``.

Why this exists: a second lane (``fix/dealer-gamma-sign-spot-20260924``) is
fixing the engine's sign convention and spot source while this package is
being built. Rather than call ``DealerGammaEngine`` directly from
``preopen.py``, every caller in this package goes through
:class:`DealerGammaAdapter`, and every field the engine's returned dict
might rename/add lives behind :func:`DealerGammaAdapter._translate`. When
the fix lands and is merged into this branch, only ``_translate`` (and its
tests) should need to change — everything else in ``paper_log.gex_levels``
depends on the stable :class:`LevelsResult` shape, and can also be pointed
at a mock/fake engine in tests without touching a database.

Pre-registration inputs this maps directly:
  "Levels, from `physics.dealer_gamma.DealerGammaEngine` at the pinned code
  commit ...: gamma flip, put wall, call wall, the engine's spot and its
  source, aggregate GEX, normalized GEX, and regime ... Sign convention:
  dealers modeled long calls and short puts; GEX > 0 means dealers long
  gamma."
  "engine_unavailable: the engine returns no spot, flip or walls."
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Protocol

from sqlalchemy.engine import Engine

from physics.dealer_gamma import DealerGammaEngine


@dataclass(frozen=True)
class LevelsResult:
    """Canonical, engine-version-independent shape for a levels lookup."""

    available: bool
    unavailable_reason: str | None
    spot: float | None
    spot_source: str | None
    gamma_flip: float | None
    put_wall: float | None
    call_wall: float | None
    gex_aggregate: float | None
    gex_normalized: float | None
    regime: str | None
    raw: dict[str, Any] = field(default_factory=dict)


class LevelsEngine(Protocol):
    """What :class:`DealerGammaAdapter` needs from an engine — satisfied by
    ``physics.dealer_gamma.DealerGammaEngine`` and by any test double."""

    def compute_gex_profile(self, ticker: str, snap_date: date | None = None) -> dict[str, Any]: ...


# Default label used when the engine's own result carries no explicit
# source for `spot` (true of the engine's interface as of 51e67988). If a
# future engine version adds a `spot_source` / `source` key to its result,
# `_translate` prefers that value automatically — see the fallback chain
# below.
_DEFAULT_SPOT_SOURCE = "physics.dealer_gamma.DealerGammaEngine"

_REQUIRED_KEYS = ("spot", "gamma_flip", "put_wall", "call_wall")


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
            return LevelsResult(
                available=False,
                unavailable_reason="engine returned no result",
                spot=None, spot_source=None, gamma_flip=None, put_wall=None,
                call_wall=None, gex_aggregate=None, gex_normalized=None,
                regime=None, raw={},
            )

        if "error" in raw:
            return LevelsResult(
                available=False,
                unavailable_reason=str(raw["error"]),
                spot=None, spot_source=None, gamma_flip=None, put_wall=None,
                call_wall=None, gex_aggregate=None, gex_normalized=None,
                regime=None, raw=raw,
            )

        values = {k: raw.get(k) for k in _REQUIRED_KEYS}
        missing = [k for k, v in values.items() if v is None]
        spot_source = raw.get("spot_source") or raw.get("source") or _DEFAULT_SPOT_SOURCE

        if missing:
            # "the engine returns no spot, flip or walls" — engine_unavailable.
            return LevelsResult(
                available=False,
                unavailable_reason=f"engine result missing: {', '.join(sorted(missing))}",
                spot=_as_float(values["spot"]),
                spot_source=spot_source if values["spot"] is not None else None,
                gamma_flip=_as_float(values["gamma_flip"]),
                put_wall=_as_float(values["put_wall"]),
                call_wall=_as_float(values["call_wall"]),
                gex_aggregate=_as_float(raw.get("gex_aggregate")),
                gex_normalized=_as_float(raw.get("gex_normalized")),
                regime=raw.get("regime"),
                raw=raw,
            )

        return LevelsResult(
            available=True,
            unavailable_reason=None,
            spot=_as_float(values["spot"]),
            spot_source=str(spot_source),
            gamma_flip=_as_float(values["gamma_flip"]),
            put_wall=_as_float(values["put_wall"]),
            call_wall=_as_float(values["call_wall"]),
            gex_aggregate=_as_float(raw.get("gex_aggregate")),
            gex_normalized=_as_float(raw.get("gex_normalized")),
            regime=raw.get("regime"),
            raw=raw,
        )


def _as_float(value: Any) -> float | None:
    return None if value is None else float(value)
