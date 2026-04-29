"""
Exit manager tick loop.

Fires once per scheduler tick (Hermes / systemd timer / cron — any
caller works). One tick:

  1. lists every open Solana paper_trade
  2. fetches the current Jupiter price for each
  3. calls :func:`trading.solana.exit_decision.decide_exit`
  4. persists the resulting :class:`ExitAction` via :class:`ExitStateStore`
  5. closes the underlying paper_trade if the action is terminal or the
     last rung of the ladder fires
  6. feeds the learner the realised blended pnl on every final close

Design rules:

  * **No decision logic here.** Everything policy-related lives in
    :mod:`trading.solana.exit_decision` so ticks are deterministic
    given a fixed price feed.
  * **Per-position error isolation.** One ticker going dark must not
    prevent the rest of the book from being managed — errors are
    caught and logged per trade.
  * **Learner is optional.** In paper replays / backtests the caller
    can pass ``learner=None`` to run with a fixed policy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from loguru import logger as log
from sqlalchemy.engine import Engine

from trading.paper_engine import PaperTradingEngine
from trading.solana.executor import SOLANA_STRATEGY_ID
from trading.solana.exit_decision import (
    ACTION_ARM_TRAILING,
    ACTION_HOLD,
    ACTION_TP_RUNG,
    ExitAction,
    ExitState,
    compute_pnl_pct,
    decide_exit,
)
from trading.solana.exit_learner import ExitLearner
from trading.solana.exit_policy import ExitPolicy, policy_by_id
from trading.solana.exit_state import (
    SOURCE_UNKNOWN,
    ExitStateStore,
    PositionStateRow,
)
from trading.solana.jupiter_client import JupiterClient


# ----------------------------------------------------------------------
# DTOs
# ----------------------------------------------------------------------
@dataclass
class TickSummary:
    positions_checked: int = 0
    holds: int = 0
    arm_trailings: int = 0
    tp_rungs: int = 0
    closes: int = 0
    errors: int = 0
    details: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "positions_checked": self.positions_checked,
            "holds": self.holds,
            "arm_trailings": self.arm_trailings,
            "tp_rungs": self.tp_rungs,
            "closes": self.closes,
            "errors": self.errors,
            "details": self.details,
        }


# ----------------------------------------------------------------------
# Manager
# ----------------------------------------------------------------------
ClockFn = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ExitManager:
    """Drives partial exits and stops for open Solana paper_trades."""

    def __init__(
        self,
        engine: Engine,
        jupiter: JupiterClient | None = None,
        store: ExitStateStore | None = None,
        learner: ExitLearner | None = None,
        paper: PaperTradingEngine | None = None,
        strategy_id: str = SOLANA_STRATEGY_ID,
        clock: ClockFn = _utc_now,
        default_source_type: str = SOURCE_UNKNOWN,
    ) -> None:
        self.engine = engine
        self.jupiter = jupiter or JupiterClient()
        self.store = store or ExitStateStore(engine)
        self.learner = learner  # None = no self-learning, fixed policy via kwarg
        self.paper = paper or PaperTradingEngine(engine)
        self.strategy_id = strategy_id
        self.clock = clock
        self.default_source_type = default_source_type

        if self.learner is not None:
            self.learner.ensure_variants((default_source_type,))

    # ------------------------------------------------------------------
    # Public — call this on every tick
    # ------------------------------------------------------------------
    def tick(self) -> TickSummary:
        """Run one pass over every open position. Returns a summary."""
        summary = TickSummary()
        positions = self.store.list_open_positions(self.strategy_id)
        log.info("ExitManager.tick — {n} open positions", n=len(positions))

        for pos in positions:
            summary.positions_checked += 1
            try:
                detail = self._handle_position(pos)
                summary.details.append(detail)
                kind = detail.get("action_kind")
                # A TP rung that drains the position counts as a close.
                finalised = bool(detail.get("final_close"))
                if finalised:
                    summary.closes += 1
                elif kind == ACTION_HOLD:
                    summary.holds += 1
                elif kind == ACTION_ARM_TRAILING:
                    summary.arm_trailings += 1
                elif kind == ACTION_TP_RUNG:
                    summary.tp_rungs += 1
                elif kind is not None:
                    summary.closes += 1
            except Exception as exc:  # noqa: BLE001 — per-position isolation
                summary.errors += 1
                log.warning(
                    "ExitManager: error handling trade {t}: {e}",
                    t=pos.trade_id, e=str(exc),
                )
                summary.details.append(
                    {"trade_id": pos.trade_id, "error": str(exc)}
                )

        log.info(
            "ExitManager.tick done — checked={c} holds={h} arm={a} "
            "tp={tp} close={cl} err={e}",
            c=summary.positions_checked,
            h=summary.holds,
            a=summary.arm_trailings,
            tp=summary.tp_rungs,
            cl=summary.closes,
            e=summary.errors,
        )
        return summary

    # ------------------------------------------------------------------
    # Position handling
    # ------------------------------------------------------------------
    def _handle_position(self, pos: PositionStateRow) -> dict[str, Any]:
        policy = policy_by_id(pos.policy_variant)
        if policy is None:
            raise RuntimeError(
                f"trade {pos.trade_id} references unknown variant "
                f"{pos.policy_variant!r}"
            )

        price = self._fetch_price(pos.ticker)
        if price is None or price <= 0:
            return {
                "trade_id": pos.trade_id,
                "action_kind": None,
                "reason": "no price",
            }

        now = self.clock()
        current_pnl = compute_pnl_pct(pos.direction, pos.entry_price, price)
        peak = max(pos.peak_pnl_pct, current_pnl)
        trough = min(pos.trough_pnl_pct, current_pnl)

        state = ExitState(
            trade_id=pos.trade_id,
            direction=pos.direction,
            entry_price=pos.entry_price,
            current_price=price,
            entry_time=pos.entry_time,
            now=now,
            peak_pnl_pct=peak,
            remaining_fraction=pos.remaining_fraction,
            tp_rungs_hit=pos.tp_rungs_hit,
            trailing_armed=pos.trailing_armed,
        )
        action = decide_exit(state, policy)

        # Always refresh peak/trough + last_tick so the next tick sees
        # the up-to-date state, even on HOLD.
        self.store.update_position(
            pos.trade_id,
            peak_pnl_pct=peak,
            trough_pnl_pct=trough,
            last_tick_at=now,
        )

        if action.kind == ACTION_HOLD:
            return {
                "trade_id": pos.trade_id,
                "action_kind": ACTION_HOLD,
                "pnl_pct": current_pnl,
                "peak": peak,
                "reason": action.reason,
            }

        if action.kind == ACTION_ARM_TRAILING:
            self.store.update_position(pos.trade_id, trailing_armed=True)
            return {
                "trade_id": pos.trade_id,
                "action_kind": ACTION_ARM_TRAILING,
                "pnl_pct": current_pnl,
                "peak": peak,
                "reason": action.reason,
            }

        # All remaining actions close some fraction of the position.
        return self._apply_closing_action(
            pos=pos,
            policy=policy,
            action=action,
            price=price,
            pnl_pct=current_pnl,
            peak=peak,
        )

    # ------------------------------------------------------------------
    # Closing actions
    # ------------------------------------------------------------------
    def _apply_closing_action(
        self,
        pos: PositionStateRow,
        policy: ExitPolicy,
        action: ExitAction,
        price: float,
        pnl_pct: float,
        peak: float,
    ) -> dict[str, Any]:
        # Guard against double-closing — the decision engine clamps
        # close_fraction to remaining, but belt+suspenders.
        close_fraction = min(action.close_fraction, pos.remaining_fraction)
        if close_fraction <= 0:
            return {
                "trade_id": pos.trade_id,
                "action_kind": ACTION_HOLD,
                "reason": "nothing left to close",
            }

        new_remaining = pos.remaining_fraction - close_fraction
        new_filled_value = pos.filled_exit_value + close_fraction * price
        new_filled_fraction = pos.filled_exit_fraction + close_fraction
        new_rungs_hit = pos.tp_rungs_hit + (
            1 if action.kind == ACTION_TP_RUNG else 0
        )
        new_trailing_armed = pos.trailing_armed or action.arm_trailing

        # Record the immutable event BEFORE mutating state — audit wins.
        self.store.record_event(
            trade_id=pos.trade_id,
            event_type=action.kind,
            rung_index=action.rung_index,
            fraction=close_fraction,
            price=price,
            pnl_pct=pnl_pct,
            peak_pnl_pct=peak,
            policy_variant=pos.policy_variant,
            source_type=pos.source_type,
            reason=action.reason,
        )

        self.store.update_position(
            pos.trade_id,
            remaining_fraction=new_remaining,
            tp_rungs_hit=new_rungs_hit,
            trailing_armed=new_trailing_armed,
            filled_exit_value=new_filled_value,
            filled_exit_fraction=new_filled_fraction,
        )

        detail: dict[str, Any] = {
            "trade_id": pos.trade_id,
            "action_kind": action.kind,
            "close_fraction": close_fraction,
            "remaining_fraction": new_remaining,
            "price": price,
            "pnl_pct": pnl_pct,
            "peak": peak,
            "rung_index": action.rung_index,
            "reason": action.reason,
        }

        # If nothing is left OR the action is a terminal stop, finalise
        # the paper_trade and feed the learner.
        if new_remaining <= 1e-9 or action.is_terminal:
            blended_exit_price = self._blended_exit_price(
                entry_price=pos.entry_price,
                new_filled_value=new_filled_value,
                new_filled_fraction=new_filled_fraction,
                new_remaining=new_remaining,
                spot_price=price,
            )
            realised_pnl = compute_pnl_pct(
                pos.direction, pos.entry_price, blended_exit_price
            )
            close_result = self.paper.close_trade(
                trade_id=pos.trade_id,
                exit_price=blended_exit_price,
                notes=f"exit_manager: {action.reason}",
            )
            detail["final_close"] = True
            detail["blended_exit_price"] = blended_exit_price
            detail["realised_pnl_pct"] = realised_pnl
            detail["paper_close"] = close_result

            if self.learner is not None:
                try:
                    reward = self.learner.record_outcome(
                        variant_id=pos.policy_variant,
                        source_type=pos.source_type,
                        pnl_pct=realised_pnl,
                    )
                    detail["learner_reward"] = reward
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "Learner update failed for trade {t}: {e}",
                        t=pos.trade_id, e=str(exc),
                    )
                    detail["learner_error"] = str(exc)

        return detail

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _fetch_price(self, mint: str) -> float | None:
        try:
            data = self.jupiter.get_token_price(mint)
        except Exception as exc:  # noqa: BLE001 — per-position isolation
            log.warning("Exit manager price fetch failed for {m}: {e}", m=mint, e=str(exc))
            return None
        snapshot = data.get(mint) if isinstance(data, dict) else None
        if not isinstance(snapshot, dict):
            return None
        raw = snapshot.get("usdPrice")
        try:
            return float(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None

    def _blended_exit_price(
        self,
        entry_price: float,
        new_filled_value: float,
        new_filled_fraction: float,
        new_remaining: float,
        spot_price: float,
    ) -> float:
        """Compute the effective exit price to record on paper_trades.

        Uses ``Σ(fraction * price)`` across every exit event so the
        paper_trade's ``pnl_pct`` reflects the laddered exit, not just
        the last slice. Any leftover (``new_remaining > 0``) gets marked
        at the current spot — that case happens when a terminal stop
        fires after at least one TP rung.
        """
        if new_filled_fraction <= 0:
            return spot_price
        total_value = new_filled_value + new_remaining * spot_price
        total_fraction = new_filled_fraction + new_remaining
        if total_fraction <= 0:
            return spot_price
        return total_value / total_fraction

    # ------------------------------------------------------------------
    # External — register a new position with the learner
    # ------------------------------------------------------------------
    def register_position(
        self,
        trade_id: int,
        source_type: str | None = None,
        policy: ExitPolicy | None = None,
    ) -> ExitPolicy:
        """Create an exit-state row for a brand new paper_trade.

        Called by :class:`PaperSolanaExecutor` immediately after it
        opens a trade. The learner picks the variant unless one is
        explicitly passed in (useful for forced-policy tests or A/B
        overrides).
        """
        src = source_type or self.default_source_type
        if policy is None:
            if self.learner is None:
                raise ValueError(
                    "register_position requires either a learner or an "
                    "explicit policy"
                )
            policy = self.learner.select_policy(source_type=src)
        self.store.ensure_position(
            trade_id=trade_id,
            policy_variant=policy.variant_id,
            source_type=src,
        )
        return policy
