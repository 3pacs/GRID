"""
Solana trading package.

Foundation for autonomous Solana trading in GRID, inspired by the AutoHedge
project (https://github.com/The-Swarm-Corporation/AutoHedge, MIT license).

AutoHedge's core reusable pieces — the Jupiter Ultra / Price V3 API client
and the 4-stage Director → Quant → Risk → Execution agent pipeline — have
been ported here and reworked to:

  * depend only on GRID's existing LLM router (no Swarms / OpenAI hard dep)
  * route execution through GRID's paper trading engine first
  * honour GRID's PIT correctness and immutable journal rules

Public API:
    JupiterClient       — Jupiter Price V3 + Ultra swap HTTP client
    SolanaWallet        — Base58 private-key wallet helper (graceful degradation)
    SolanaPipeline      — Director/Quant/Risk/Execution agent pipeline
    PipelineDecision    — dataclass returned by the pipeline
    PaperSolanaExecutor — routes pipeline decisions into paper_trades

Environment variables (add to .env):
    JUPITER_API_KEY          — optional; unlocks higher Jupiter rate limits
    SOLANA_PRIVATE_KEY       — base58 private key; required for live execution
    SOLANA_RPC_URL           — optional RPC endpoint (defaults to mainnet)
    SOLANA_LIVE_TRADING      — "true" to enable live execution (default false)
"""

from __future__ import annotations

from trading.solana.exit_decision import (
    ACTION_ARM_TRAILING,
    ACTION_HOLD,
    ACTION_MAX_HOLD,
    ACTION_STOP_LOSS,
    ACTION_TP_RUNG,
    ACTION_TRAILING_STOP,
    ExitAction,
    ExitState,
    compute_pnl_pct,
    decide_exit,
)
from trading.solana.exit_learner import ExitLearner, VariantPosterior
from trading.solana.exit_manager import ExitManager, TickSummary
from trading.solana.exit_policy import (
    AGGRESSIVE,
    BALANCED,
    CONSERVATIVE,
    SCALPER,
    SEED_VARIANTS,
    ExitPolicy,
    ExitRung,
    policy_by_id,
)
from trading.solana.exit_state import (
    ExitStateStore,
    PositionStateRow,
    SOURCE_UNKNOWN,
    VariantStatsRow,
)
from trading.solana.jupiter_client import (
    JupiterClient,
    JupiterError,
    SOL_MINT,
    USDC_MINT,
)
from trading.solana.limits import DailyLimits, LimitConfig, LimitDecision
from trading.solana.pipeline import (
    PipelineDecision,
    SolanaPipeline,
)
from trading.solana.safety import (
    SafetyCheck,
    SafetyConfig,
    SolanaSafetyChecker,
    TokenSafetyReport,
)
from trading.solana.solana_rpc import (
    MintInfo,
    SolanaRPC,
    SolanaRPCError,
    TokenHolder,
)
from trading.solana.wallet import SolanaWallet, WalletUnavailableError

__all__ = [
    "ACTION_ARM_TRAILING",
    "ACTION_HOLD",
    "ACTION_MAX_HOLD",
    "ACTION_STOP_LOSS",
    "ACTION_TP_RUNG",
    "ACTION_TRAILING_STOP",
    "AGGRESSIVE",
    "BALANCED",
    "CONSERVATIVE",
    "DailyLimits",
    "ExitAction",
    "ExitLearner",
    "ExitManager",
    "ExitPolicy",
    "ExitRung",
    "ExitState",
    "ExitStateStore",
    "JupiterClient",
    "JupiterError",
    "LimitConfig",
    "LimitDecision",
    "MintInfo",
    "PipelineDecision",
    "PositionStateRow",
    "SafetyCheck",
    "SafetyConfig",
    "SCALPER",
    "SEED_VARIANTS",
    "SOL_MINT",
    "SOURCE_UNKNOWN",
    "SolanaPipeline",
    "SolanaRPC",
    "SolanaRPCError",
    "SolanaSafetyChecker",
    "SolanaWallet",
    "TickSummary",
    "TokenHolder",
    "TokenSafetyReport",
    "USDC_MINT",
    "VariantPosterior",
    "VariantStatsRow",
    "WalletUnavailableError",
    "compute_pnl_pct",
    "decide_exit",
    "policy_by_id",
]
