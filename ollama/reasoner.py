"""
GRID Ollama-powered reasoning layer.

Mirrors the Hyperspace reasoner but uses local Ollama inference with
GRID's knowledge documents for richer context. Provides hypothesis
generation, economic mechanism explanation, and backtest critique.
"""

from __future__ import annotations

import re
from typing import Any

from loguru import logger as log

from ollama.client import get_client
from outputs.llm_logger import log_insight


SYSTEM_PROMPT: str = (
    "You are a financial economist and quantitative researcher with deep "
    "knowledge of macroeconomic cycles, market microstructure, and "
    "empirical asset pricing. You reason carefully about causal mechanisms. "
    "You distinguish between statistical patterns and economic causation. "
    "You are skeptical of overfitting and always ask whether a pattern "
    "has a plausible economic mechanism. Respond concisely and precisely."
)


class OllamaReasoner:
    """LLM-assisted reasoning for GRID using Ollama with knowledge context.

    Provides the same interface as GRIDReasoner (hyperspace.reasoner) but
    uses Ollama and injects relevant GRID knowledge documents for
    deeper, more contextual analysis.

    Attributes:
        client: OllamaClient instance.
    """

    def __init__(self, ollama_client: Any = None) -> None:
        if ollama_client is None:
            ollama_client = get_client()
        self.client = ollama_client
        log.info("OllamaReasoner initialised — available={a}", a=self.client.is_available)

    def explain_relationship(
        self,
        feature_a: str,
        feature_b: str,
        observed_pattern: str,
    ) -> str | None:
        """Explain the economic mechanism behind an observed pattern.

        Parameters:
            feature_a: Name of the first feature.
            feature_b: Name of the second feature.
            observed_pattern: Abstract description of the statistical pattern.

        Returns:
            str: Explanation, or None if unavailable.
        """
        if not self.client.is_available:
            return None

        prompt = (
            f"In market data, we observe that {feature_a} and {feature_b} show "
            f"the following pattern: {observed_pattern}. What economic mechanisms "
            f"could explain this relationship, and why might it have changed over "
            f"time? Be specific about the transmission channels."
        )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ]

        result = self.client.chat(
            messages,
            temperature=0.3,
            num_predict=1000,
            system_knowledge=["03_feature_families", "07_economic_mechanisms"],
        )
        log_insight(
            category="explanation",
            title=f"Mechanism: {feature_a} x {feature_b}",
            content=result,
            metadata={"feature_a": feature_a, "feature_b": feature_b,
                       "observed_pattern": observed_pattern},
            provider="ollama",
        )
        return result

    def generate_hypothesis_candidates(
        self,
        pattern_description: str,
        n_candidates: int = 3,
    ) -> list[str] | None:
        """Generate falsifiable hypothesis statements from a pattern.

        Parameters:
            pattern_description: Abstract description of the discovered pattern.
            n_candidates: Number of hypotheses to generate.

        Returns:
            list[str]: Hypothesis strings, or None if unavailable.
        """
        if not self.client.is_available:
            return None

        prompt = (
            f"Given the following statistical pattern in market data:\n"
            f"{pattern_description}\n\n"
            f"Generate {n_candidates} distinct, falsifiable hypotheses that could "
            f"explain or exploit this pattern. Each hypothesis must:\n"
            f"1. Specify which variables are involved\n"
            f"2. Specify the direction of the expected effect\n"
            f"3. Specify over what horizon\n"
            f"4. Be stated in a way that could be directly tested with historical data\n\n"
            f"Format each hypothesis on a separate line starting with H1:, H2:, H3:"
        )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ]

        response = self.client.chat(
            messages,
            temperature=0.5,
            num_predict=1500,
            system_knowledge=["04_regime_detection", "05_derived_signals"],
        )

        if response is None:
            return None

        # Parse H1:, H2:, H3: lines
        hypotheses: list[str] = []
        pattern = re.compile(r"H\d+:\s*(.*)")
        for line in response.split("\n"):
            match = pattern.match(line.strip())
            if match:
                hypothesis = match.group(1).strip()
                if hypothesis:
                    hypotheses.append(hypothesis)

        # Fallback: numbered lines
        if not hypotheses:
            numbered = re.compile(r"^\d+[\.\)]\s*(.*)")
            for line in response.split("\n"):
                match = numbered.match(line.strip())
                if match:
                    hypothesis = match.group(1).strip()
                    if hypothesis:
                        hypotheses.append(hypothesis)

        if hypotheses:
            log_insight(
                category="hypothesis",
                title=f"Hypotheses from pattern ({len(hypotheses)} candidates)",
                content="\n".join(f"- {h}" for h in hypotheses),
                metadata={"pattern": pattern_description, "raw_response": response},
                provider="ollama",
            )
        return hypotheses if hypotheses else None

    def critique_backtest_result(
        self,
        hypothesis: str,
        metric_name: str,
        metric_value: float,
        baseline_value: float,
        n_periods: int,
    ) -> str | None:
        """Identify potential failure modes in a backtest result.

        Parameters:
            hypothesis: The hypothesis statement.
            metric_name: Evaluation metric name.
            metric_value: Achieved metric value.
            baseline_value: Baseline metric value.
            n_periods: Number of test periods.

        Returns:
            str: Critique, or None if unavailable.
        """
        if not self.client.is_available:
            return None

        prompt = (
            f"A backtest of the following hypothesis:\n"
            f"'{hypothesis}'\n"
            f"produced {metric_name} = {metric_value:.3f} vs baseline {baseline_value:.3f} "
            f"over {n_periods} test periods.\n\n"
            f"What are the most likely reasons this result could be spurious? "
            f"Consider: data snooping, regime-specific overfitting, survivorship bias, "
            f"lookahead bias, and whether the economic mechanism is plausible."
        )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ]

        result = self.client.chat(
            messages,
            temperature=0.3,
            num_predict=1000,
            system_knowledge=["09_pit_correctness", "04_regime_detection"],
        )
        log_insight(
            category="critique",
            title=f"Backtest critique: {hypothesis[:60]}",
            content=result,
            metadata={"hypothesis": hypothesis, "metric_name": metric_name,
                       "metric_value": metric_value, "baseline_value": baseline_value,
                       "n_periods": n_periods},
            provider="ollama",
        )
        return result

    def analyze_regime_transition(
        self,
        from_regime: str,
        to_regime: str,
        feature_changes: dict[str, dict[str, float]],
    ) -> str | None:
        """Analyze a detected regime transition with economic context.

        Parameters:
            from_regime: Previous regime label.
            to_regime: New regime label.
            feature_changes: Dict of feature_name -> {before, after, change}.

        Returns:
            str: Analysis, or None if unavailable.
        """
        if not self.client.is_available:
            return None

        changes_text = "\n".join(
            f"- {name}: {vals.get('before', '?')} → {vals.get('after', '?')} "
            f"(change: {vals.get('change', '?')})"
            for name, vals in feature_changes.items()
        )

        prompt = (
            f"GRID has detected a regime transition from '{from_regime}' to "
            f"'{to_regime}'. The key feature changes driving this transition:\n\n"
            f"{changes_text}\n\n"
            f"1. What economic mechanisms explain this transition?\n"
            f"2. What historical episodes had similar configurations?\n"
            f"3. What are the likely implications for the next 1-3 months?\n"
            f"4. What would cause a reversal back to the previous regime?\n"
            f"5. What is the biggest risk the market is not pricing?"
        )

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ]

        result = self.client.chat(
            messages,
            temperature=0.3,
            num_predict=1500,
            system_knowledge=[
                "04_regime_detection",
                "07_economic_mechanisms",
                "08_historical_regimes",
            ],
        )
        log_insight(
            category="regime_analysis",
            title=f"Regime transition: {from_regime} -> {to_regime}",
            content=result,
            metadata={"from_regime": from_regime, "to_regime": to_regime,
                       "feature_changes": feature_changes},
            provider="ollama",
        )
        return result


if __name__ == "__main__":
    reasoner = OllamaReasoner()

    explanation = reasoner.explain_relationship(
        "yield_curve_2s10s",
        "hy_spread_proxy",
        "correlation drops from 0.65 to 0.21 after 2015",
    )
    if explanation:
        print("Explanation:")
        print(explanation)
    else:
        print("Ollama unavailable — no reasoning available")
