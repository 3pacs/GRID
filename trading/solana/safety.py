"""
Safety rails for Solana trading.

Every order — paper or live — should be gated through a
:class:`SolanaSafetyChecker` before being placed. The checker produces a
:class:`TokenSafetyReport` which is cheap to log into the decision journal
and trivially replayable from tests.

Checks implemented:

  * ``mint_authority``   — rejects tokens where the mint authority has not
    been renounced (the dev can still print more supply)
  * ``freeze_authority`` — rejects tokens where the freeze authority still
    exists (the dev can freeze your tokens mid-trade)
  * ``holder_concentration`` — rejects tokens where the top-N token
    accounts hold more than a configurable percentage of supply
  * ``price_impact`` — simulates a sell via Jupiter Ultra's ``get_order``
    and compares ``outAmount`` against the spot USD price to estimate
    slippage. Acts as a lightweight honeypot / thin-liquidity filter.

Each check is returned as an individual :class:`SafetyCheck` so callers
can distinguish *blockers* (must fix) from *warnings* (informational).
This matches the severity taxonomy in ``.claude/rules/common/code-review.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from loguru import logger as log

from trading.solana.jupiter_client import JupiterClient, JupiterError, USDC_MINT
from trading.solana.solana_rpc import (
    MintInfo,
    SolanaRPC,
    SolanaRPCError,
    TokenHolder,
)

# ----------------------------------------------------------------------
# Severity levels — mirror .claude/rules/common/code-review.md
# ----------------------------------------------------------------------
SEVERITY_BLOCK = "block"
SEVERITY_WARN = "warn"
SEVERITY_INFO = "info"

# Well-known placeholder owners used by burn addresses / LP locks.
# If a "large holder" slot is held by one of these, it's not a real holder.
BURN_ADDRESSES: frozenset[str] = frozenset(
    {
        "11111111111111111111111111111111",  # System program / burn
        "1nc1nerator11111111111111111111111111111111",
    }
)


# ----------------------------------------------------------------------
# Data classes
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class SafetyConfig:
    """Thresholds for safety checks.

    All fields have sane memecoin defaults; override per-deployment via
    ``config.Settings`` and thread through :class:`SolanaSafetyChecker`.

    ``blocked_mints`` is the operator's **hard blocklist** — any mint in
    this set fails the gate with a single ``operator_conflict_of_interest``
    blocker regardless of every other signal. The intent is to fence GRID
    off from tokens the operator has a beneficial interest in (bags they
    market, CTO coins they're active in, etc.), so the trading bot can
    never accidentally act on signal the operator themselves generated.
    """

    require_mint_renounced: bool = True
    require_freeze_renounced: bool = True
    max_top10_holder_pct: float = 25.0  # top-10 cannot hold > 25% of supply
    max_price_impact_pct: float = 5.0   # rejected if simulated sell slips > 5%
    min_supply: int = 1                 # zero-supply mint is obviously dead
    blocked_mints: frozenset[str] = frozenset()


def parse_mint_blocklist(value: str | None) -> frozenset[str]:
    """Turn a comma-separated env-var string into a frozenset of mints.

    Whitespace and empty entries are stripped. Case is preserved because
    Solana addresses are case-sensitive base58. Returns an empty set on
    ``None`` or empty string.
    """
    if not value:
        return frozenset()
    return frozenset(
        part.strip() for part in value.split(",") if part.strip()
    )


@dataclass(frozen=True)
class SafetyCheck:
    """Result of a single check."""

    name: str
    passed: bool
    severity: str
    detail: str
    metric: float | None = None


@dataclass(frozen=True)
class TokenSafetyReport:
    """Aggregate result returned by :meth:`SolanaSafetyChecker.check_token`.

    ``passed`` is True iff every *blocker* check passed. Warnings are
    surfaced in ``checks`` but do not flip ``passed``.
    """

    mint: str
    checks: tuple[SafetyCheck, ...]
    passed: bool
    mint_info: MintInfo | None = None
    top_holders: tuple[TokenHolder, ...] = field(default_factory=tuple)

    @property
    def blockers(self) -> tuple[SafetyCheck, ...]:
        return tuple(
            c for c in self.checks if not c.passed and c.severity == SEVERITY_BLOCK
        )

    @property
    def warnings(self) -> tuple[SafetyCheck, ...]:
        return tuple(
            c for c in self.checks if not c.passed and c.severity == SEVERITY_WARN
        )

    def summary(self) -> str:
        if self.passed:
            return f"OK ({len(self.checks)} checks, {len(self.warnings)} warnings)"
        blocker_names = ", ".join(c.name for c in self.blockers)
        return f"BLOCKED on: {blocker_names}"


# ----------------------------------------------------------------------
# Protocols for loose coupling in tests
# ----------------------------------------------------------------------
class _RPCProtocol(Protocol):
    def get_mint_info(self, mint: str) -> MintInfo: ...
    def get_token_largest_accounts(self, mint: str) -> list[TokenHolder]: ...


class _JupiterProtocol(Protocol):
    def get_token_price(
        self, ids: str | list[str]
    ) -> dict[str, dict[str, Any]]: ...
    def get_order(
        self,
        input_mint: str,
        output_mint: str,
        amount: int,
        taker: str,
    ) -> Any: ...


# ----------------------------------------------------------------------
# Checker
# ----------------------------------------------------------------------
class SolanaSafetyChecker:
    """Gate a mint + intended trade size through every configured check.

    The checker is stateless — create one per trade, or cache at module
    scope if you want to reuse the underlying RPC/Jupiter clients.
    """

    def __init__(
        self,
        rpc: _RPCProtocol | None = None,
        jupiter: _JupiterProtocol | None = None,
        config: SafetyConfig | None = None,
    ) -> None:
        self.rpc = rpc or SolanaRPC()
        self.jupiter = jupiter or JupiterClient()
        self.config = config or SafetyConfig()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def check_token(
        self,
        mint: str,
        trade_size_atoms: int = 0,
        quote_mint: str = USDC_MINT,
        taker: str | None = None,
    ) -> TokenSafetyReport:
        """Run every check against ``mint`` and return a report.

        Args:
            mint: the token's mint address.
            trade_size_atoms: raw integer amount (in token atoms) we'd sell
                for the price-impact simulation. Set to 0 to skip that check.
            quote_mint: the mint to sell into for the impact check. Defaults
                to USDC.
            taker: wallet address to pass to Jupiter Ultra for the impact
                simulation. Can be any base58 address — the simulation
                never broadcasts, so an unfunded probe wallet is fine.
                If omitted, price-impact check is skipped.

        Returns:
            A :class:`TokenSafetyReport`. Always returns — failures show up
            as ``passed=False``; the method does not raise.
        """
        if not mint:
            raise ValueError("mint is required")

        checks: list[SafetyCheck] = []
        mint_info: MintInfo | None = None
        holders: tuple[TokenHolder, ...] = ()

        # ----- 0. Operator conflict-of-interest blocklist -------------
        # Short-circuit before any RPC or Jupiter call so a blocked mint
        # never touches the network. Any entry in the blocklist is a
        # hard blocker — no config knob can downgrade it.
        if mint in self.config.blocked_mints:
            block = SafetyCheck(
                name="operator_conflict_of_interest",
                passed=False,
                severity=SEVERITY_BLOCK,
                detail=(
                    f"{mint} is on the operator's hard blocklist — "
                    "GRID will never trade tokens the operator has "
                    "a beneficial interest in"
                ),
            )
            report = TokenSafetyReport(
                mint=mint,
                checks=(block,),
                passed=False,
            )
            log.warning(
                "Safety BLOCK (blocklist): {m}",
                m=mint[:12] + "...",
            )
            return report

        # ----- 1. On-chain mint state ---------------------------------
        try:
            mint_info = self.rpc.get_mint_info(mint)
            checks.extend(self._evaluate_mint_info(mint_info))
        except (SolanaRPCError, ValueError) as exc:
            checks.append(
                SafetyCheck(
                    name="mint_info",
                    passed=False,
                    severity=SEVERITY_BLOCK,
                    detail=f"could not read mint account: {exc}",
                )
            )

        # ----- 2. Holder concentration --------------------------------
        if mint_info is not None and mint_info.supply > 0:
            try:
                raw_holders = self.rpc.get_token_largest_accounts(mint)
                holders = tuple(raw_holders)
                checks.append(
                    self._evaluate_holder_concentration(mint_info, holders)
                )
            except (SolanaRPCError, ValueError) as exc:
                checks.append(
                    SafetyCheck(
                        name="holder_concentration",
                        passed=False,
                        severity=SEVERITY_WARN,
                        detail=f"holder query failed: {exc}",
                    )
                )

        # ----- 3. Price-impact simulation -----------------------------
        if trade_size_atoms > 0 and taker:
            checks.append(
                self._evaluate_price_impact(
                    mint=mint,
                    trade_size_atoms=trade_size_atoms,
                    quote_mint=quote_mint,
                    taker=taker,
                    decimals=mint_info.decimals if mint_info else 9,
                )
            )

        passed = all(c.passed or c.severity != SEVERITY_BLOCK for c in checks)
        report = TokenSafetyReport(
            mint=mint,
            checks=tuple(checks),
            passed=passed,
            mint_info=mint_info,
            top_holders=holders,
        )
        log.info(
            "Safety report for {m}: {s}",
            m=mint[:12] + "...",
            s=report.summary(),
        )
        return report

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------
    def _evaluate_mint_info(self, info: MintInfo) -> list[SafetyCheck]:
        results: list[SafetyCheck] = []

        if not info.is_initialized:
            results.append(
                SafetyCheck(
                    name="mint_initialized",
                    passed=False,
                    severity=SEVERITY_BLOCK,
                    detail="mint account not initialized",
                )
            )
        else:
            results.append(
                SafetyCheck(
                    name="mint_initialized",
                    passed=True,
                    severity=SEVERITY_INFO,
                    detail="mint initialized",
                )
            )

        if info.supply < self.config.min_supply:
            results.append(
                SafetyCheck(
                    name="min_supply",
                    passed=False,
                    severity=SEVERITY_BLOCK,
                    detail=f"supply {info.supply} < {self.config.min_supply}",
                    metric=float(info.supply),
                )
            )

        # Authority checks always report the raw on-chain state via
        # ``passed``. Severity is the only thing that depends on config —
        # so disabling a requirement downgrades the check from blocker
        # to warning but does not hide it. That way the report always
        # tells operators the truth.
        results.append(
            SafetyCheck(
                name="mint_authority",
                passed=info.mint_authority_renounced,
                severity=(
                    SEVERITY_BLOCK
                    if self.config.require_mint_renounced
                    else SEVERITY_WARN
                ),
                detail=(
                    "mint authority renounced"
                    if info.mint_authority_renounced
                    else "mint authority still active — dev can print supply"
                ),
            )
        )
        results.append(
            SafetyCheck(
                name="freeze_authority",
                passed=info.freeze_authority_renounced,
                severity=(
                    SEVERITY_BLOCK
                    if self.config.require_freeze_renounced
                    else SEVERITY_WARN
                ),
                detail=(
                    "freeze authority renounced"
                    if info.freeze_authority_renounced
                    else "freeze authority still active — dev can freeze your tokens"
                ),
            )
        )
        return results

    def _evaluate_holder_concentration(
        self,
        mint_info: MintInfo,
        holders: tuple[TokenHolder, ...],
    ) -> SafetyCheck:
        # Filter burn addresses and an empty list defensively.
        real_holders = [
            h for h in holders if h.address not in BURN_ADDRESSES
        ]
        if not real_holders:
            return SafetyCheck(
                name="holder_concentration",
                passed=False,
                severity=SEVERITY_WARN,
                detail="no non-burn holders returned",
            )

        top10 = sorted(real_holders, key=lambda h: h.amount, reverse=True)[:10]
        top10_sum = sum(h.amount for h in top10)
        supply = mint_info.supply
        pct = (top10_sum / supply * 100.0) if supply > 0 else 100.0

        passed = pct <= self.config.max_top10_holder_pct
        return SafetyCheck(
            name="holder_concentration",
            passed=passed,
            severity=SEVERITY_BLOCK,
            detail=(
                f"top-10 hold {pct:.1f}% of supply "
                f"(limit {self.config.max_top10_holder_pct:.0f}%)"
            ),
            metric=pct,
        )

    def _evaluate_price_impact(
        self,
        mint: str,
        trade_size_atoms: int,
        quote_mint: str,
        taker: str,
        decimals: int,
    ) -> SafetyCheck:
        """Estimate price impact by comparing a sell-quote to the spot price.

        Implementation note: we intentionally use ``get_order`` (Ultra)
        here because it's the endpoint we'll actually use for live swaps,
        so the slippage we see during the check is the slippage we'll see
        at execution time. The alternative — Jupiter's quote API — may
        return a slightly better route.
        """
        try:
            spot = self.jupiter.get_token_price([mint, quote_mint])
        except JupiterError as exc:
            return SafetyCheck(
                name="price_impact",
                passed=False,
                severity=SEVERITY_WARN,
                detail=f"spot price lookup failed: {exc}",
            )

        token_usd = _safe_price(spot.get(mint))
        quote_usd = _safe_price(spot.get(quote_mint)) or 1.0
        if token_usd is None:
            return SafetyCheck(
                name="price_impact",
                passed=False,
                severity=SEVERITY_WARN,
                detail=f"no spot price for {mint}",
            )

        try:
            order = self.jupiter.get_order(
                input_mint=mint,
                output_mint=quote_mint,
                amount=trade_size_atoms,
                taker=taker,
            )
        except (JupiterError, ValueError) as exc:
            # A Jupiter Ultra order failing for a specific size is strong
            # evidence of a honeypot or thin liquidity — block it.
            return SafetyCheck(
                name="price_impact",
                passed=False,
                severity=SEVERITY_BLOCK,
                detail=f"Jupiter refused to route sell order: {exc}",
            )

        # Expected USD value at spot.
        tokens_out = trade_size_atoms / (10**decimals)
        expected_usd = tokens_out * token_usd

        # Realised USD value from the quote.
        # We can't know the quote mint decimals without another RPC call,
        # so approximate using the USDC case (6 decimals) when relevant.
        quote_decimals = 6 if quote_mint == USDC_MINT else 9
        realised_usd = (order.out_amount / (10**quote_decimals)) * quote_usd

        if expected_usd <= 0:
            return SafetyCheck(
                name="price_impact",
                passed=False,
                severity=SEVERITY_WARN,
                detail="expected USD value is zero or negative",
            )

        impact_pct = max(0.0, (1.0 - realised_usd / expected_usd) * 100.0)
        passed = impact_pct <= self.config.max_price_impact_pct
        return SafetyCheck(
            name="price_impact",
            passed=passed,
            severity=SEVERITY_BLOCK,
            detail=(
                f"simulated sell slippage {impact_pct:.2f}% "
                f"(limit {self.config.max_price_impact_pct:.2f}%)"
            ),
            metric=impact_pct,
        )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _safe_price(snapshot: Any) -> float | None:
    if not isinstance(snapshot, dict):
        return None
    raw = snapshot.get("usdPrice")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None
