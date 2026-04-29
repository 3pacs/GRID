"""Options trade outcome handler (SYNTH-40).

Consumes ``OptionsTradeOutcome`` contracts emitted by
``trading/contagion_to_ticket.py`` when a ticket closes (hit, miss,
expiry). The handler maps the outcome's PnL sign into a Sharpe-style
delta and nudges the ``contagion`` oracle model head's weight via the
existing ``ModelRegistry.decay_model_by_source`` Bayesian path.

The handler is a thin adapter — all weight math lives on ``ModelRegistry``.
It must be non-fatal: any exception is logged and swallowed so a single
bad outcome never breaks the contract bus.
"""
from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from loguru import logger as log

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from contracts.schemas import OptionsTradeOutcome


#: Strategy-name prefixes this handler cares about. Anything that doesn't
#: start with one of these is silently ignored (the oracle model head
#: we update is the ``contagion`` head, so non-contagion strategies stay
#: out of this wiring until their own handlers land).
_CONTAGION_STRATEGY_PREFIXES: tuple[str, ...] = (
    "contagion",
    "CONTAGION",
)

#: Multiplicative factor applied on a HIT (winning trade). >1 boosts the
#: weight; the registry clamps the result so repeated boosts converge.
_HIT_FACTOR: float = 1.10

#: Multiplicative factor applied on a MISS. <1 decays the weight.
_MISS_FACTOR: float = 0.92


def _is_contagion_strategy(name: str | None) -> bool:
    if not name:
        return False
    return any(name.startswith(p) for p in _CONTAGION_STRATEGY_PREFIXES)


def on_options_trade_outcome(
    evt: "OptionsTradeOutcome", *, engine: "Engine"
) -> None:
    """Update the contagion model weight from a closed ticket's PnL.

    - HIT (``pnl > 0``) → boost the contagion head by ``_HIT_FACTOR``.
    - MISS (``pnl <= 0``) → decay by ``_MISS_FACTOR``.

    Non-contagion strategies are a no-op.
    """
    strategy = getattr(evt, "strategy", None)
    if not _is_contagion_strategy(strategy):
        log.debug(
            "trade_outcomes.on_options_trade_outcome: skip strategy={s}",
            s=strategy,
        )
        return

    pnl = getattr(evt, "pnl", None)
    try:
        pnl_f = float(pnl) if pnl is not None else 0.0
    except (TypeError, ValueError):
        log.warning(
            "trade_outcomes.on_options_trade_outcome: bad pnl={p}",
            p=pnl,
        )
        return

    factor = _HIT_FACTOR if pnl_f > 0 else _MISS_FACTOR

    try:
        from oracle.engine import ModelRegistry

        registry = ModelRegistry(engine)
        # ``decay_model_by_source`` does a LIKE on signal_families — for
        # the contagion head the tag is ``macro`` or ``supply``, but the
        # registry pattern keys on the haystack string so passing the
        # model name as the needle keeps the update tightly scoped.
        rows = registry.decay_model_by_source(
            source="contagion",
            factor=factor,
        )
    except Exception as exc:
        log.warning(
            "trade_outcomes.on_options_trade_outcome(strategy={s} pnl={p}): "
            "{e}",
            s=strategy, p=pnl_f, e=str(exc),
        )
        return

    log.info(
        "trade_outcomes.on_options_trade_outcome: strategy={s} pnl={p:+.4f} "
        "factor={f:.3f} rows_updated={n}",
        s=strategy, p=pnl_f, f=factor, n=rows,
    )
