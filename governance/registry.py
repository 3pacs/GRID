"""
GRID model governance registry.

Manages the model lifecycle state machine with enforced transition rules,
gate checks, and audit trails.  Ensures only one PRODUCTION model per
layer at any time.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from validation.gates import GateChecker

# Valid state transitions: (current_state, target_state)
_VALID_TRANSITIONS: dict[str, set[str]] = {
    "CANDIDATE": {"SHADOW", "RETIRED"},
    "SHADOW": {"STAGING", "RETIRED"},
    "STAGING": {"PRODUCTION", "RETIRED"},
    "PRODUCTION": {"FLAGGED", "RETIRED"},
    "FLAGGED": {"RETIRED", "PRODUCTION"},
}


class ModelRegistry:
    """Model lifecycle state machine with gate enforcement.

    Manages model transitions through CANDIDATE -> SHADOW -> STAGING ->
    PRODUCTION -> FLAGGED -> RETIRED, enforcing gate requirements at each
    promotion step.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        gate_checker: GateChecker for validating promotion requirements.
    """

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the model registry.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.engine = db_engine
        self.gate_checker = GateChecker(db_engine)
        log.info("ModelRegistry initialised")

    def transition(
        self,
        model_id: int,
        new_state: str,
        operator_id: str,
        reason: str | None = None,
    ) -> bool:
        """Transition a model to a new state.

        Validates the transition is allowed, checks all gate requirements,
        and handles demotion of existing PRODUCTION models when promoting.

        Parameters:
            model_id: The model_registry.id to transition.
            new_state: The target state.
            operator_id: Identifier of the operator authorising the transition.
            reason: Optional reason for the transition.

        Returns:
            bool: True on successful transition.

        Raises:
            ValueError: If the transition is not allowed, gates fail,
                        or the model is not found.
        """
        log.info(
            "Transition requested — model={m}, target={t}, operator={o}",
            m=model_id,
            t=new_state,
            o=operator_id,
        )

        # Get current model state
        with self.engine.connect() as conn:
            model = conn.execute(
                text("SELECT state, layer, name, version FROM model_registry WHERE id = :id"),
                {"id": model_id},
            ).fetchone()

        if model is None:
            raise ValueError(f"Model {model_id} not found")

        current_state = model[0]
        layer = model[1]

        # Validate transition is allowed
        allowed = _VALID_TRANSITIONS.get(current_state, set())
        if new_state not in allowed:
            raise ValueError(
                f"Invalid transition: {current_state} -> {new_state}. "
                f"Allowed transitions from {current_state}: {allowed}"
            )

        # Check gates
        gate_result = self.gate_checker.check_all_gates(model_id, new_state)
        if not gate_result["passed"]:
            raise ValueError(
                f"Gate check failed for {current_state} -> {new_state}: "
                f"{gate_result['details']}"
            )

        now = datetime.now(timezone.utc)

        with self.engine.begin() as conn:
            # Handle STAGING -> PRODUCTION: demote current PRODUCTION model
            if new_state == "PRODUCTION":
                existing_prod = conn.execute(
                    text(
                        "SELECT id FROM model_registry "
                        "WHERE layer = :layer AND state = 'PRODUCTION' AND id != :mid"
                    ),
                    {"layer": layer, "mid": model_id},
                ).fetchone()

                if existing_prod is not None:
                    log.info(
                        "Demoting existing PRODUCTION model {eid} to SHADOW",
                        eid=existing_prod[0],
                    )
                    conn.execute(
                        text(
                            "UPDATE model_registry "
                            "SET state = 'SHADOW', updated_at = :now "
                            "WHERE id = :id"
                        ),
                        {"id": existing_prod[0], "now": now},
                    )

            # Apply the transition
            update_fields = {
                "id": model_id,
                "state": new_state,
                "now": now,
            }

            update_sql = (
                "UPDATE model_registry "
                "SET state = :state, updated_at = :now"
            )

            if new_state == "PRODUCTION":
                update_sql += ", promoted_at = :now, promoted_by = :operator"
                update_fields["operator"] = operator_id

            if new_state == "RETIRED":
                update_sql += ", retired_at = :now, retire_reason = :reason"
                update_fields["reason"] = reason or "Operator decision"

            update_sql += " WHERE id = :id"

            conn.execute(text(update_sql), update_fields)

        log.info(
            "Transition complete — model={m}: {old} -> {new} (operator={o})",
            m=model_id,
            old=current_state,
            new=new_state,
            o=operator_id,
        )
        return True

    def get_production_model(self, layer: str) -> dict[str, Any] | None:
        """Return the current PRODUCTION model for a given layer.

        Parameters:
            layer: Model layer ('REGIME', 'TACTICAL', or 'EXECUTION').

        Returns:
            dict: Model record, or None if no PRODUCTION model exists
                  for this layer.
        """
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT * FROM model_registry "
                    "WHERE layer = :layer AND state = 'PRODUCTION'"
                ),
                {"layer": layer},
            ).fetchone()

        if row is None:
            log.info("No PRODUCTION model for layer {l}", l=layer)
            return None

        return dict(row._mapping)

    def flag_model(self, model_id: int, reason: str) -> bool:
        """Flag a model (automatic, from monitoring).

        Sets the model state to FLAGGED.  Does not require operator action.

        Parameters:
            model_id: The model_registry.id to flag.
            reason: Reason for flagging.

        Returns:
            bool: True on success.

        Raises:
            ValueError: If the model is not in PRODUCTION state.
        """
        log.warning("Flagging model {m}: {r}", m=model_id, r=reason)

        with self.engine.connect() as conn:
            model = conn.execute(
                text("SELECT state FROM model_registry WHERE id = :id"),
                {"id": model_id},
            ).fetchone()

        if model is None:
            raise ValueError(f"Model {model_id} not found")

        if model[0] != "PRODUCTION":
            raise ValueError(
                f"Can only flag PRODUCTION models. Model {model_id} is in "
                f"state {model[0]}."
            )

        now = datetime.now(timezone.utc)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE model_registry "
                    "SET state = 'FLAGGED', updated_at = :now "
                    "WHERE id = :id"
                ),
                {"id": model_id, "now": now},
            )

        log.warning("Model {m} flagged — reason: {r}", m=model_id, r=reason)
        return True

    def rollback(self, model_id: int, operator_id: str) -> bool:
        """Rollback a model: retire it and promote its predecessor.

        Parameters:
            model_id: The model to retire.
            operator_id: Operator authorising the rollback.

        Returns:
            bool: True on success.

        Raises:
            ValueError: If the model has no predecessor.
        """
        log.info("Rollback requested — model={m}, operator={o}", m=model_id, o=operator_id)

        with self.engine.connect() as conn:
            model = conn.execute(
                text(
                    "SELECT predecessor_id, layer, state FROM model_registry "
                    "WHERE id = :id"
                ),
                {"id": model_id},
            ).fetchone()

        if model is None:
            raise ValueError(f"Model {model_id} not found")

        predecessor_id = model[0]
        if predecessor_id is None:
            raise ValueError(
                f"Model {model_id} has no predecessor — cannot rollback"
            )

        # Retire the current model
        self.transition(model_id, "RETIRED", operator_id, reason="Rollback")

        # Promote the predecessor to PRODUCTION
        # First set it back to STAGING so we can promote it
        now = datetime.now(timezone.utc)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE model_registry "
                    "SET state = 'STAGING', updated_at = :now "
                    "WHERE id = :id"
                ),
                {"id": predecessor_id, "now": now},
            )

        self.transition(predecessor_id, "PRODUCTION", operator_id, reason="Rollback promotion")

        log.info(
            "Rollback complete — model {m} retired, predecessor {p} promoted",
            m=model_id,
            p=predecessor_id,
        )
        return True


if __name__ == "__main__":
    from db import get_engine

    registry = ModelRegistry(db_engine=get_engine())

    for layer in ("REGIME", "TACTICAL", "EXECUTION"):
        prod = registry.get_production_model(layer)
        if prod:
            print(f"  {layer}: {prod['name']} v{prod['version']}")
        else:
            print(f"  {layer}: (none)")
