"""
GRID promotion gate enforcement module.

Defines and checks the gate requirements for each model state transition.
Gates ensure that only properly validated models advance through the
lifecycle.
"""

from __future__ import annotations

from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


class GateChecker:
    """Checks promotion gate requirements for model state transitions.

    Each gate is a specific requirement that must be met before a model
    can transition to the next state.

    Attributes:
        engine: SQLAlchemy engine for database queries.
    """

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the gate checker.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.engine = db_engine
        log.info("GateChecker initialised")

    def check_candidate_to_shadow(self, model_id: int) -> dict[str, Any]:
        """Check gates for CANDIDATE -> SHADOW transition.

        Requirements:
        - validation_run_id must be set on the model
        - The associated hypothesis must be in PASSED state

        Parameters:
            model_id: Model registry ID.

        Returns:
            dict: Gate check result with keys 'passed' (bool) and 'details' (list[str]).
        """
        details: list[str] = []
        passed = True

        with self.engine.connect() as conn:
            model = conn.execute(
                text("SELECT validation_run_id, hypothesis_id FROM model_registry WHERE id = :id"),
                {"id": model_id},
            ).fetchone()

            if model is None:
                return {"passed": False, "details": [f"Model {model_id} not found"]}

            # Gate 1: validation_run_id must be set
            if model[0] is None:
                details.append("validation_run_id is not set")
                passed = False
            else:
                details.append("validation_run_id is set")

            # Gate 2: hypothesis must be PASSED
            hyp = conn.execute(
                text("SELECT state FROM hypothesis_registry WHERE id = :id"),
                {"id": model[1]},
            ).fetchone()

            if hyp is None:
                details.append("Associated hypothesis not found")
                passed = False
            elif hyp[0] != "PASSED":
                details.append(f"Hypothesis state is {hyp[0]}, must be PASSED")
                passed = False
            else:
                details.append("Hypothesis is in PASSED state")

        log.info("Gate check CANDIDATE->SHADOW for model {m}: {r}", m=model_id, r="PASS" if passed else "FAIL")
        return {"passed": passed, "details": details}

    def check_shadow_to_staging(self, model_id: int) -> dict[str, Any]:
        """Check gates for SHADOW -> STAGING transition.

        Requirements:
        - Operator approval (checked at transition time)

        Parameters:
            model_id: Model registry ID.

        Returns:
            dict: Gate check result.
        """
        details = ["Operator approval required (checked at transition time)"]
        log.info("Gate check SHADOW->STAGING for model {m}: requires operator approval", m=model_id)
        return {"passed": True, "details": details}

    def check_staging_to_production(self, model_id: int) -> dict[str, Any]:
        """Check gates for STAGING -> PRODUCTION transition.

        Requirements:
        - At least 20 journal entries for this model
        - Operator sign-off (checked at transition time)
        - No other PRODUCTION model in the same layer

        Parameters:
            model_id: Model registry ID.

        Returns:
            dict: Gate check result.
        """
        details: list[str] = []
        passed = True

        with self.engine.connect() as conn:
            # Get model info
            model = conn.execute(
                text("SELECT layer FROM model_registry WHERE id = :id"),
                {"id": model_id},
            ).fetchone()

            if model is None:
                return {"passed": False, "details": [f"Model {model_id} not found"]}

            layer = model[0]

            # Gate 1: >= 20 journal entries
            journal_count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM decision_journal "
                    "WHERE model_version_id = :mid"
                ),
                {"mid": model_id},
            ).fetchone()[0]

            if journal_count < 20:
                details.append(f"Only {journal_count}/20 journal entries (need >= 20)")
                passed = False
            else:
                details.append(f"{journal_count} journal entries (>= 20)")

            # Gate 2: No other PRODUCTION model in same layer
            existing = conn.execute(
                text(
                    "SELECT id, name FROM model_registry "
                    "WHERE layer = :layer AND state = 'PRODUCTION' AND id != :mid"
                ),
                {"layer": layer, "mid": model_id},
            ).fetchone()

            if existing is not None:
                details.append(
                    f"Existing PRODUCTION model {existing[1]} (id={existing[0]}) "
                    f"in layer {layer} — will be demoted to SHADOW"
                )
            else:
                details.append(f"No existing PRODUCTION model in layer {layer}")

        log.info(
            "Gate check STAGING->PRODUCTION for model {m}: {r}",
            m=model_id,
            r="PASS" if passed else "FAIL",
        )
        return {"passed": passed, "details": details}

    def check_all_gates(self, model_id: int, target_state: str) -> dict[str, Any]:
        """Run all applicable gate checks for a target state transition.

        Parameters:
            model_id: Model registry ID.
            target_state: The desired target state.

        Returns:
            dict: Gate check result with 'passed' and 'details'.

        Raises:
            ValueError: If the target state has no defined gates.
        """
        gate_map = {
            "SHADOW": self.check_candidate_to_shadow,
            "STAGING": self.check_shadow_to_staging,
            "PRODUCTION": self.check_staging_to_production,
        }

        checker = gate_map.get(target_state)
        if checker is None:
            # States like FLAGGED, RETIRED don't have promotion gates
            return {"passed": True, "details": [f"No gates defined for target state {target_state}"]}

        return checker(model_id)


if __name__ == "__main__":
    from db import get_engine

    gc = GateChecker(db_engine=get_engine())
    print("GateChecker ready")
