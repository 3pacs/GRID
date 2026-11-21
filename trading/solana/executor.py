"""
Paper-first executor for Solana pipeline decisions.

Routes a :class:`PipelineDecision` into either:

  * **paper mode** (default) — logged to ``paper_trades`` via
    :class:`trading.paper_engine.PaperTradingEngine` using the live Jupiter
    price as the entry fill. Zero capital at risk.
  * **live mode** — requests an Ultra swap order from Jupiter, signs it with
    the configured :class:`trading.solana.wallet.SolanaWallet`, and submits.
    Requires ``SOLANA_LIVE_TRADING=true`` and a valid ``SOLANA_PRIVATE_KEY``.

The executor deliberately does *not* maintain its own DB schema — it reuses
``paper_strategies`` / ``paper_trades`` so that the existing signal executor,
circuit breaker, and decision journal plumbing all work unchanged.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from trading.paper_engine import PaperTradingEngine
from trading.solana.jupiter_client import JupiterClient, USDC_MINT
from trading.solana.limits import DailyLimits, LimitConfig, LimitDecision
from trading.solana.pipeline import PipelineDecision
from trading.solana.safety import (
    SafetyConfig,
    SolanaSafetyChecker,
    TokenSafetyReport,
)

if False:  # TYPE_CHECKING-style guard without the import
    from trading.solana.exit_manager import ExitManager

SOLANA_STRATEGY_ID = "solana_autohedge"

# Probe wallet used when taker=None and we still want to simulate a sell.
# Any valid base58 address works — the simulation never broadcasts.
_PROBE_TAKER = "11111111111111111111111111111111"
_USDC_DECIMALS = 6
# Largest output-floor tolerance the live path accepts (#1143).
_MAX_PRESIGN_TOLERANCE = 0.20


@dataclass(frozen=True)
class ExecutionResult:
    """Outcome of :meth:`PaperSolanaExecutor.execute`."""

    action: str
    mode: str  # "paper" | "live" | "skipped"
    trade_id: int | None
    symbol: str
    mint: str
    entry_price: float | None
    size_fraction: float
    reason: str
    raw: dict[str, Any] | None = None
    safety_report: TokenSafetyReport | None = None
    limit_decision: LimitDecision | None = None


class PaperSolanaExecutor:
    """Turns :class:`PipelineDecision` objects into paper or live trades.

    By default runs in paper mode. Flip ``live`` to True only after you have:

      1. Funded a Solana wallet
      2. Set ``SOLANA_PRIVATE_KEY`` in the environment
      3. Verified the pipeline on paper for at least a week
    """

    def __init__(
        self,
        engine: Engine,
        jupiter: JupiterClient | None = None,
        live: bool = False,
        wallet: Any | None = None,
        strategy_id: str = SOLANA_STRATEGY_ID,
        quote_mint: str = USDC_MINT,
        safety: SolanaSafetyChecker | None = None,
        safety_config: SafetyConfig | None = None,
        limits: DailyLimits | None = None,
        limit_config: LimitConfig | None = None,
        capital_per_trade_usd: float | None = None,
        exit_manager: "ExitManager | None" = None,
        presign_allowed_programs: frozenset[str] | None = None,
    ) -> None:
        self.engine = engine
        self.paper = PaperTradingEngine(engine)
        self.jupiter = jupiter or JupiterClient()
        self.live = live
        self.wallet = wallet
        self.strategy_id = strategy_id
        self.quote_mint = quote_mint

        # Safety rails — safety check + daily caps. Both are injectable for
        # tests; in production they're constructed with sane defaults.
        self.safety = safety or SolanaSafetyChecker(
            jupiter=self.jupiter, config=safety_config
        )
        self.limits = limits or DailyLimits(
            engine=engine, strategy_id=strategy_id, config=limit_config
        )
        self.capital_per_trade_usd = (
            capital_per_trade_usd
            if capital_per_trade_usd is not None
            else self.limits.config.capital_per_trade_usd
        )
        self.exit_manager = exit_manager
        # Top-level programs a live order may invoke (#1143). None = the
        # reviewed default in trading/solana/presign_check.py.
        self.presign_allowed_programs = presign_allowed_programs

        self._ensure_strategy_row()

        if self.live and self.wallet is None:
            log.warning(
                "PaperSolanaExecutor(live=True) without a wallet — "
                "live swaps will raise until a SolanaWallet is attached",
            )

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    def _ensure_strategy_row(self) -> None:
        """Insert a placeholder paper_strategies row for Solana if missing.

        The existing ``signal_executor`` only walks strategies it finds in
        ``paper_strategies``, so the executor is responsible for making
        sure there's something for the Solana desk to hang trades off.
        """
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO paper_strategies "
                    "(id, leader, follower, description, status) "
                    "VALUES (:id, :leader, :follower, :desc, 'ACTIVE') "
                    "ON CONFLICT (id) DO NOTHING"
                ),
                {
                    "id": self.strategy_id,
                    "leader": "solana_pipeline",
                    "follower": "solana_tokens",
                    "desc": "Solana 4-agent pipeline (AutoHedge-derived)",
                },
            )

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------
    def execute(self, decision: PipelineDecision) -> ExecutionResult:
        """Execute a pipeline decision.

        Every trade runs a two-stage gate before it gets to paper or live:

          1. :class:`SolanaSafetyChecker` — rug / freeze / concentration /
             price-impact filters on the target mint
          2. :class:`DailyLimits` — per-day USD + trade-count + per-mint caps

        Non-actionable decisions, safety blockers, and limit breaches all
        return ``mode="skipped"`` and are NOT logged to ``paper_trades``.
        """
        if not decision.actionable:
            reason = "risk veto" if decision.risk_veto else "action=HOLD"
            log.info(
                "Solana decision skipped — {sym} {action}: {r}",
                sym=decision.symbol,
                action=decision.action,
                r=reason,
            )
            return self._skip(decision, reason, entry_price=None)

        entry_price = self._fetch_entry_price(decision.mint)
        if entry_price is None or entry_price <= 0:
            log.warning(
                "Solana decision skipped for {sym} — no price data",
                sym=decision.symbol,
            )
            return self._skip(decision, "no price data", entry_price=None)

        # ----- Safety rails -------------------------------------------
        # For BUY we simulate a same-size sell (that's the honeypot test);
        # for SELL we already know we can't exit if we can't sell, so the
        # same simulation still applies.
        trade_usd = self._trade_notional_usd(decision)
        trade_size_atoms = self._probe_atoms(
            entry_price=entry_price,
            decimals=9,  # sensible default; updated after safety runs
            trade_usd=trade_usd,
        )
        safety_report = self.safety.check_token(
            mint=decision.mint,
            trade_size_atoms=trade_size_atoms,
            quote_mint=self.quote_mint,
            taker=self._taker_for_probe(),
        )

        # Recompute atoms using the actual decimals the safety check found,
        # if the first pass used a placeholder. This is only meaningful
        # when the caller later wants to send a real live order — we keep
        # the value on the report for observability.
        if safety_report.mint_info is not None:
            trade_size_atoms = self._probe_atoms(
                entry_price=entry_price,
                decimals=safety_report.mint_info.decimals,
                trade_usd=trade_usd,
            )

        if not safety_report.passed:
            reason = f"safety blocked: {safety_report.summary()}"
            log.warning(
                "Solana decision blocked by safety — {sym}: {r}",
                sym=decision.symbol,
                r=reason,
            )
            return self._skip(
                decision,
                reason,
                entry_price=entry_price,
                safety_report=safety_report,
            )

        # ----- Daily caps ---------------------------------------------
        limit_decision = self.limits.check(trade_usd=trade_usd, mint=decision.mint)
        if not limit_decision.passed:
            reason = f"limit blocked: {limit_decision.summary}"
            log.warning(
                "Solana decision blocked by daily limits — {sym}: {r}",
                sym=decision.symbol,
                r=reason,
            )
            return self._skip(
                decision,
                reason,
                entry_price=entry_price,
                safety_report=safety_report,
                limit_decision=limit_decision,
            )

        # ----- Execute -------------------------------------------------
        if self.live:
            return self._execute_live(
                decision,
                entry_price,
                safety_report=safety_report,
                limit_decision=limit_decision,
                trade_size_atoms=trade_size_atoms,
            )

        return self._execute_paper(
            decision,
            entry_price,
            safety_report=safety_report,
            limit_decision=limit_decision,
        )

    # ------------------------------------------------------------------
    # Helpers for the gate
    # ------------------------------------------------------------------
    def _trade_notional_usd(self, decision: PipelineDecision) -> float:
        """Translate the pipeline's portfolio-fraction into USD notional.

        The pipeline emits ``size_fraction`` in [0, 1]; we multiply by the
        configured per-trade capital cap. This keeps the executor honest —
        the LLM cannot unilaterally size up past ``capital_per_trade_usd``.
        """
        fraction = max(0.0, min(1.0, float(decision.size_fraction)))
        return fraction * self.capital_per_trade_usd

    def _probe_atoms(
        self,
        entry_price: float,
        decimals: int,
        trade_usd: float,
    ) -> int:
        """Convert a USD notional into raw token atoms for the sim."""
        if entry_price <= 0 or trade_usd <= 0:
            return 0
        tokens = trade_usd / entry_price
        return max(1, int(tokens * (10**decimals)))

    def _taker_for_probe(self) -> str:
        """Taker address used for the honeypot simulation.

        If a real wallet is attached, use its address (matches production
        routing). Otherwise fall back to a canonical probe address — the
        simulation never broadcasts, so the value is purely cosmetic.
        """
        if self.wallet is not None:
            try:
                return self.wallet.address
            except Exception:  # noqa: BLE001 — e.g. missing solders
                pass
        return _PROBE_TAKER

    def _skip(
        self,
        decision: PipelineDecision,
        reason: str,
        entry_price: float | None,
        safety_report: TokenSafetyReport | None = None,
        limit_decision: LimitDecision | None = None,
    ) -> ExecutionResult:
        return ExecutionResult(
            action=decision.action,
            mode="skipped",
            trade_id=None,
            symbol=decision.symbol,
            mint=decision.mint,
            entry_price=entry_price,
            size_fraction=decision.size_fraction,
            reason=reason,
            safety_report=safety_report,
            limit_decision=limit_decision,
        )

    # ------------------------------------------------------------------
    # Paper path
    # ------------------------------------------------------------------
    def _execute_paper(
        self,
        decision: PipelineDecision,
        entry_price: float,
        safety_report: TokenSafetyReport | None = None,
        limit_decision: LimitDecision | None = None,
    ) -> ExecutionResult:
        direction = "LONG" if decision.action == "BUY" else "SHORT"
        trade_id = self.paper.open_trade(
            strategy_id=self.strategy_id,
            ticker=decision.symbol,
            direction=direction,
            entry_price=entry_price,
            position_size=decision.size_fraction,
            signal_strength=decision.risk_score,
            hypothesis_id=None,
            threshold_used=decision.stop_loss_pct,
        )
        mode = "paper" if trade_id > 0 else "skipped"
        reason = "paper trade opened" if trade_id > 0 else "paper engine refused trade"

        # Hand the freshly opened trade off to the exit manager so its
        # next tick sees the position and starts managing exits.
        if trade_id > 0 and self.exit_manager is not None:
            try:
                self.exit_manager.register_position(trade_id=trade_id)
            except Exception as exc:  # noqa: BLE001 — degrade gracefully
                log.warning(
                    "Exit manager register_position failed for trade {t}: {e}",
                    t=trade_id, e=str(exc),
                )

        return ExecutionResult(
            action=decision.action,
            mode=mode,
            trade_id=trade_id if trade_id > 0 else None,
            symbol=decision.symbol,
            mint=decision.mint,
            entry_price=entry_price,
            size_fraction=decision.size_fraction,
            reason=reason,
            safety_report=safety_report,
            limit_decision=limit_decision,
        )

    # ------------------------------------------------------------------
    # Live path
    # ------------------------------------------------------------------
    def _execute_live(
        self,
        decision: PipelineDecision,
        entry_price: float,
        safety_report: TokenSafetyReport | None = None,
        limit_decision: LimitDecision | None = None,
        trade_size_atoms: int = 0,
    ) -> ExecutionResult:
        if self.wallet is None:
            raise RuntimeError(
                "live=True but no SolanaWallet attached to executor"
            )

        # For BUY we swap quote → token, for SELL we swap token → quote.
        # ``trade_size_atoms`` comes from ``execute()``: the USD notional
        # converted into *target-token* atoms with the decimals the safety
        # check found. It is the input amount for a SELL and the expected
        # output for a BUY. A BUY spends the notional in quote atoms; before
        # #1143 it passed the token-atom count as the USDC input amount.
        if trade_size_atoms <= 0:
            raise ValueError(
                "live execution requires a positive trade_size_atoms; "
                "the safety gate should have computed this"
            )
        if self.quote_mint != USDC_MINT:
            raise ValueError(
                "live execution supports a USDC quote only; quote-atom "
                "sizing for other quote mints is not implemented"
            )
        trade_usd = self._trade_notional_usd(decision)
        quote_atoms = int(trade_usd * 10**_USDC_DECIMALS)
        if quote_atoms <= 0:
            raise ValueError("live execution requires a positive USD notional")

        # Output floor from our own sizing and price, never from the
        # Jupiter /order response: the safety check's price-impact limit plus
        # a 2-point buffer, clamped to 20 %. The price itself is Jupiter's
        # price API (no independent oracle covers long-tail tokens), so
        # the floor is a sanity bound, not an oracle. The hard bound is the
        # input ceiling, which is ours.
        tolerance = (float(self.safety.config.max_price_impact_pct) + 2.0) / 100.0
        if not 0.0 < tolerance <= _MAX_PRESIGN_TOLERANCE:
            # A mis-set price-impact limit must not collapse the floor.
            raise ValueError(
                f"pre-sign output tolerance {tolerance:.3f} outside "
                f"(0, {_MAX_PRESIGN_TOLERANCE}]; check SafetyConfig.max_price_impact_pct"
            )
        if decision.action == "BUY":
            input_mint, output_mint = self.quote_mint, decision.mint
            input_atoms, expected_out = quote_atoms, trade_size_atoms
        else:
            input_mint, output_mint = decision.mint, self.quote_mint
            input_atoms, expected_out = trade_size_atoms, quote_atoms
        min_output_atoms = max(1, int(expected_out * (1.0 - tolerance)))

        order = self.jupiter.get_order(
            input_mint=input_mint,
            output_mint=output_mint,
            amount=input_atoms,
            taker=self.wallet.address,
        )
        verify = functools.partial(
            self._verify_before_signing,
            expected_input_mint=input_mint,
            expected_output_mint=output_mint,
            max_input_atoms=input_atoms,
            min_output_atoms=min_output_atoms,
        )
        raw = self.jupiter.execute_swap(order, self.wallet, verify=verify)

        log.info(
            "Solana live swap submitted — {sym} {action} req={r}",
            sym=decision.symbol,
            action=decision.action,
            r=order.request_id,
        )
        return ExecutionResult(
            action=decision.action,
            mode="live",
            trade_id=None,
            symbol=decision.symbol,
            mint=decision.mint,
            entry_price=entry_price,
            size_fraction=decision.size_fraction,
            reason=f"jupiter request_id={order.request_id}",
            raw=raw,
            safety_report=safety_report,
            limit_decision=limit_decision,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _verify_before_signing(
        self,
        order: Any,
        wallet_address: str,
        *,
        expected_input_mint: str,
        expected_output_mint: str,
        max_input_atoms: int,
        min_output_atoms: int,
    ) -> Any:
        """Simulate the Jupiter order on our own RPC before it is signed (#1143)."""
        from trading.solana.presign_check import (
            DEFAULT_ALLOWED_PROGRAMS,
            verify_order_before_signing,
        )

        return verify_order_before_signing(
            order,
            wallet_address,
            self.safety.rpc,
            expected_input_mint=expected_input_mint,
            expected_output_mint=expected_output_mint,
            max_input_atoms=max_input_atoms,
            min_output_atoms=min_output_atoms,
            allowed_programs=self.presign_allowed_programs or DEFAULT_ALLOWED_PROGRAMS,
        )

    def _fetch_entry_price(self, mint: str) -> float | None:
        try:
            snapshot = self.jupiter.get_token_price(mint).get(mint, {})
        except Exception as exc:  # noqa: BLE001 — graceful degradation
            log.warning("Jupiter price lookup failed for {m}: {e}", m=mint, e=str(exc))
            return None

        raw = snapshot.get("usdPrice")
        if raw is None:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None
