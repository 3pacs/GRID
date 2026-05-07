"""
Fast entry path — deterministic LLM-bypass for launch events.

Wires the launch monitor, cross-referencer, and
:class:`PaperSolanaExecutor` into a single synchronous pipeline:

    on_launch(LaunchEvent)
        → CrossReferencer.evaluate(launch)
        → gate on composite_score ≥ min_score
        → synthesize a PipelineDecision
        → executor.execute(decision)   # still runs safety + limits

The hot path does **no LLM calls**. The only source of latency is:
  * the cross-referencer's DB reads (deployer + smart money lookups)
  * the executor's existing safety / limits gate
  * a single Jupiter Ultra ``get_order`` round-trip in live mode

Everything else is in-process dataclass math.

The class exposes a single method ``handle`` so it can be registered
directly with :meth:`trading.solana.launch_monitor.LaunchMonitor.on_launch`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from loguru import logger as log

from trading.solana.cross_ref import (
    CrossReferencer,
    CrossRefReport,
    LaunchEvent,
)
from trading.solana.executor import ExecutionResult, PaperSolanaExecutor
from trading.solana.pipeline import PipelineDecision


@dataclass(frozen=True)
class FastEntryConfig:
    """Tunable thresholds for the fast entry path.

    Attributes:
        min_composite_score: gate — skip anything below this.
        base_size_fraction: pipeline ``size_fraction`` for a score of 1.0;
            the actual size is scaled linearly by the composite score so
            low-conviction hits get smaller positions.
        stop_loss_pct: hard stop pushed into the synthesized decision.
            The exit manager will also enforce its own learned stop.
        take_profit_pct: advisory target written to the decision for
            observability — the exit manager actually runs the ladder.
        require_deployer: if True, skip any launch without a tracked
            deployer score above zero. Useful once the registry is warm.
    """

    min_composite_score: float = 0.40
    base_size_fraction: float = 0.6
    stop_loss_pct: float = 0.30
    take_profit_pct: float = 1.00
    require_deployer: bool = False


DEFAULT_FAST_ENTRY_CONFIG = FastEntryConfig()


@dataclass(frozen=True)
class FastEntryResult:
    """Outcome of a single fast-entry pass."""

    mint: str
    skipped: bool
    reason: str
    report: CrossRefReport | None
    decision: PipelineDecision | None
    executor_result: ExecutionResult | None


class FastEntryPath:
    """Deterministic entry path — the LLM never runs."""

    def __init__(
        self,
        executor: PaperSolanaExecutor,
        cross_referencer: CrossReferencer,
        config: FastEntryConfig = DEFAULT_FAST_ENTRY_CONFIG,
    ) -> None:
        self.executor = executor
        self.cross_ref = cross_referencer
        self.config = config

    # ------------------------------------------------------------------
    # Launch handler — register with LaunchMonitor.on_launch
    # ------------------------------------------------------------------
    def handle(self, launch: LaunchEvent) -> FastEntryResult:
        if not launch.mint:
            return _skip(launch, "empty mint")

        try:
            report = self.cross_ref.evaluate(launch)
        except Exception as exc:  # noqa: BLE001 — hot-path guard
            log.warning(
                "CrossReferencer failed for {m}: {e}",
                m=launch.mint, e=str(exc),
            )
            return _skip(launch, f"cross_ref error: {exc}")

        if self.config.require_deployer and report.deployer_score <= 0:
            return _skip(launch, "no tracked deployer score", report=report)

        if report.composite_score < self.config.min_composite_score:
            return _skip(
                launch,
                (
                    f"composite {report.composite_score:.3f} < "
                    f"min {self.config.min_composite_score:.3f}"
                ),
                report=report,
            )

        decision = self._synthesize_decision(launch, report)

        try:
            executor_result = self.executor.execute(decision)
        except Exception as exc:  # noqa: BLE001 — hot-path guard
            log.warning(
                "Executor raised for {m}: {e}",
                m=launch.mint, e=str(exc),
            )
            return FastEntryResult(
                mint=launch.mint,
                skipped=True,
                reason=f"executor error: {exc}",
                report=report,
                decision=decision,
                executor_result=None,
            )

        log.info(
            "FastEntry {m}: score={s:.3f} mode={mode} reason={r}",
            m=launch.mint[:12] + "...",
            s=report.composite_score,
            mode=executor_result.mode,
            r=executor_result.reason,
        )
        return FastEntryResult(
            mint=launch.mint,
            skipped=False,
            reason=f"dispatched: {executor_result.mode}",
            report=report,
            decision=decision,
            executor_result=executor_result,
        )

    # ------------------------------------------------------------------
    # Decision synthesis
    # ------------------------------------------------------------------
    def _synthesize_decision(
        self,
        launch: LaunchEvent,
        report: CrossRefReport,
    ) -> PipelineDecision:
        size = self.config.base_size_fraction * max(
            0.0, min(1.0, report.composite_score)
        )
        # Build a thesis string from the cross-ref reasons so the audit
        # trail explains why we entered.
        thesis = "; ".join(report.reasons) or "fast_entry"
        symbol = launch.symbol or (launch.mint[:8] + "…")

        return PipelineDecision(
            generated_at=_now_iso(),
            task=f"fast_entry:{launch.mint}",
            symbol=symbol,
            mint=launch.mint,
            thesis=thesis,
            action="BUY",
            size_fraction=size,
            stop_loss_pct=self.config.stop_loss_pct,
            take_profit_pct=self.config.take_profit_pct,
            risk_score=report.composite_score,
            risk_veto=False,
            quant={
                "path": "fast_entry",
                "deployer_score": report.deployer_score,
                "smart_money_hits": report.smart_money_hits,
                "smart_money_trust": report.smart_money_trust,
                "narrative_weight": report.narrative_weight,
                "convergence_score": report.convergence_score,
            },
            execution={
                "source": launch.source,
                "pool": launch.pool_address,
                "initial_liquidity_usd": launch.initial_liquidity_usd,
            },
        )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _skip(
    launch: LaunchEvent,
    reason: str,
    report: CrossRefReport | None = None,
) -> FastEntryResult:
    return FastEntryResult(
        mint=launch.mint,
        skipped=True,
        reason=reason,
        report=report,
        decision=None,
        executor_result=None,
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
