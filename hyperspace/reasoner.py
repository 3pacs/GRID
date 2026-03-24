# PRIVACY BOUNDARY: This module uses Hyperspace for local inference
# and embeddings only. No GRID signal logic, feature values, discovered
# cluster structures, or hypothesis details are sent to the network.
"""
GRID LLM-assisted reasoning layer.

Uses the Hyperspace node's local inference for hypothesis generation,
economic mechanism explanation, and backtest critique.  Only abstract
descriptions and public economic concepts are sent — never raw market
data, feature values, discovered cluster structures, or hypothesis
details that would reveal GRID's signal logic.
"""

from __future__ import annotations

import re

from loguru import logger as log

from hyperspace.client import HyperspaceClient
from outputs.llm_logger import log_insight

# System prompt establishing the LLM's role
SYSTEM_PROMPT: str = (
    "You are a financial economist and quantitative researcher with deep "
    "knowledge of macroeconomic cycles, market microstructure, and "
    "empirical asset pricing. You reason carefully about causal mechanisms. "
    "You distinguish between statistical patterns and economic causation. "
    "You are skeptical of overfitting and always ask whether a pattern "
    "has a plausible economic mechanism. Respond concisely and precisely."
)


class GRIDReasoner:
    """LLM-assisted reasoning for GRID hypothesis lifecycle.

    Provides economic mechanism explanation, hypothesis candidate
    generation, and backtest critique through the local Hyperspace
    inference endpoint.

    All methods return ``None`` if Hyperspace is unavailable.
    GRID never depends on this module for required operations.

    Attributes:
        client: HyperspaceClient instance.
    """

    def __init__(self, hyperspace_client: HyperspaceClient) -> None:
        """Initialise the reasoner.

        Parameters:
            hyperspace_client: A connected HyperspaceClient.
        """
        self.client = hyperspace_client
        log.info("GRIDReasoner initialised — available={a}", a=self.client.is_available)

    def explain_relationship(
        self,
        feature_a: str,
        feature_b: str,
        observed_pattern: str,
    ) -> str | None:
        """Ask the LLM to explain the economic mechanism behind an observed pattern.

        Only abstract pattern descriptions are sent — never raw data values.

        Parameters:
            feature_a: Name of the first feature (public concept).
            feature_b: Name of the second feature (public concept).
            observed_pattern: Abstract description of the statistical pattern
                (e.g. "correlation drops from 0.65 to 0.21 after 2015").

        Returns:
            str: LLM explanation, or ``None`` if unavailable.
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

        log.debug(
            "Requesting mechanism explanation: {a} × {b}",
            a=feature_a,
            b=feature_b,
        )
        result = self.client.chat(messages, max_tokens=800, temperature=0.3)
        log_insight(
            category="explanation",
            title=f"Mechanism: {feature_a} x {feature_b}",
            content=result,
            metadata={"feature_a": feature_a, "feature_b": feature_b,
                       "observed_pattern": observed_pattern},
            provider="hyperspace",
        )
        return result

    def generate_hypothesis_candidates(
        self,
        pattern_description: str,
        n_candidates: int = 3,
    ) -> list[str] | None:
        """Generate falsifiable hypothesis statements from a pattern description.

        Parameters:
            pattern_description: Abstract description of the discovered pattern.
                Must not contain raw data, feature values, or cluster assignments.
            n_candidates: Number of hypotheses to generate (default 3).

        Returns:
            list[str]: Parsed hypothesis strings, or ``None`` if unavailable.
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

        log.debug("Requesting {n} hypothesis candidates", n=n_candidates)
        response = self.client.chat(messages, max_tokens=1200, temperature=0.5)
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

        # Fallback: if no H1/H2/H3 format, split on numbered lines
        if not hypotheses:
            numbered = re.compile(r"^\d+[\.\)]\s*(.*)")
            for line in response.split("\n"):
                match = numbered.match(line.strip())
                if match:
                    hypothesis = match.group(1).strip()
                    if hypothesis:
                        hypotheses.append(hypothesis)

        log.info("Generated {n} hypothesis candidates", n=len(hypotheses))
        if hypotheses:
            log_insight(
                category="hypothesis",
                title=f"Hypotheses from pattern ({len(hypotheses)} candidates)",
                content="\n".join(f"- {h}" for h in hypotheses),
                metadata={"pattern": pattern_description, "raw_response": response},
                provider="hyperspace",
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
        """Ask the LLM to identify potential failure modes in a backtest result.

        Parameters:
            hypothesis: The hypothesis statement (abstract, no signal detail).
            metric_name: Name of the evaluation metric.
            metric_value: The achieved metric value.
            baseline_value: The baseline metric value.
            n_periods: Number of test periods.

        Returns:
            str: LLM critique, or ``None`` if unavailable.
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

        log.debug("Requesting backtest critique for: {h}", h=hypothesis[:60])
        result = self.client.chat(messages, max_tokens=800, temperature=0.3)
        log_insight(
            category="critique",
            title=f"Backtest critique: {hypothesis[:60]}",
            content=result,
            metadata={"hypothesis": hypothesis, "metric_name": metric_name,
                       "metric_value": metric_value, "baseline_value": baseline_value,
                       "n_periods": n_periods},
            provider="hyperspace",
        )
        return result


if __name__ == "__main__":
    from hyperspace.client import get_client

    client = get_client()
    reasoner = GRIDReasoner(client)

    explanation = reasoner.explain_relationship(
        "yield_curve_2s10s",
        "hy_spread_proxy",
        "correlation drops from 0.65 to 0.21 after 2015",
    )
    if explanation:
        print("Explanation:")
        print(explanation)
    else:
        print("Hyperspace unavailable — no reasoning available")
