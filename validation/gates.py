"""
GRID promotion gate enforcement module.

Defines and checks the gate requirements for each model state transition.
Gates ensure that only properly validated models advance through the
lifecycle.

Behavior notes:
- No model can currently pass STAGING -> PRODUCTION. The gate reads skill
  only from a typed, versioned OOS record (``gate_detail["oos_evidence"]``)
  through a registered reader that proves which forward predictions were
  scored and when (cohort and clock provenance). No reader is registered
  because no writer emits such a record: the shipped writer,
  ``WalkForwardBacktest._store_result``, stores no ``model_version_id`` and
  no record version. Generic counts or rates (``n_scored``, ``hit_rate``,
  ``brier`` ...) are refused wherever they sit.
- SHADOW -> STAGING refuses with ``drift_check_unavailable`` when the drift
  checker fails or returns no evaluated metrics.
- ``force`` and ``override_reason`` on ``GateChecker`` are intentionally not
  wired through ``governance/registry.py`` or ``api/routers/models.py`` until
  a transition-log table exists to audit overrides.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

MIN_OOS_SCORED_PREDICTIONS: int = 30
WILSON_Z_95: float = 1.959963984540054
CRITICAL_DRIFT_SCORE_THRESHOLD: float = 0.8

#: Typed OOS evidence sits at ``gate_detail[OOS_EVIDENCE_KEY]`` and carries
#: ``schema == OOS_EVIDENCE_SCHEMA`` plus an integer ``version``.
OOS_EVIDENCE_KEY: str = "oos_evidence"
OOS_EVIDENCE_SCHEMA: str = "grid.oos_forward_evidence"

#: ``reader(record, model_version_id) -> proven counts`` (``n_scored`` plus
#: ``n_hits``/``hit_rate``, and/or ``brier`` with ``climatology_brier``). It must
#: prove the cohort (which forward predictions of this version were scored)
#: and the clock (each made after the version existed, before its outcome),
#: and raise to refuse.
OOSEvidenceReader = Callable[[dict[str, Any], int], dict[str, Any]]

#: Readers by version. Empty on purpose (Codex HOLD 6011907730): no typed OOS
#: writer exists, so STAGING -> PRODUCTION stays fail-closed until a typed
#: writer and its reader land together.
_OOS_EVIDENCE_READERS: dict[int, OOSEvidenceReader] = {}


def _read_typed_oos_evidence(
    gate_detail: dict[str, Any], model_id: int
) -> tuple[dict[str, Any] | None, str]:
    """Return ``(proven_counts, "")`` from a typed OOS record, else ``(None, refusal)``.

    Never reads generic counts or rates: a bare ``n_scored`` or ``hit_rate``
    says nothing about which predictions were scored or when.
    """
    record = gate_detail.get(OOS_EVIDENCE_KEY)
    if not isinstance(record, dict):
        return None, (
            f"untyped_oos_evidence: no typed gate_detail.{OOS_EVIDENCE_KEY} record; "
            "generic counts or rates without OOS cohort and clock provenance are refused"
        )
    version = record.get("version")
    if record.get("schema") != OOS_EVIDENCE_SCHEMA or type(version) is not int:
        return None, (
            f"unversioned_oos_evidence: gate_detail.{OOS_EVIDENCE_KEY} must carry "
            f"schema={OOS_EVIDENCE_SCHEMA!r} and an integer version"
        )
    reader = _OOS_EVIDENCE_READERS.get(version)
    if reader is None:
        return None, (
            f"unsupported_oos_evidence_version: no registered reader for "
            f"{OOS_EVIDENCE_SCHEMA} v{version} (no typed OOS writer/reader exists yet)"
        )
    try:
        proven = reader(record, model_id)
    except Exception as exc:  # noqa: BLE001 - a reader that fails refuses
        return None, f"oos_evidence_rejected: {type(exc).__name__}: {exc}"
    if not isinstance(proven, dict):
        return None, "oos_evidence_rejected: reader returned no proven counts"
    return proven, ""


def _wilson_lower_95(p: float, n: int) -> float:
    """Lower bound of the two-sided 95% Wilson score interval for proportion ``p`` over ``n`` trials."""
    if n <= 0 or not math.isfinite(p) or p < 0.0 or p > 1.0:
        return 0.0
    z2 = WILSON_Z_95 * WILSON_Z_95
    denom = 1.0 + z2 / n
    centre = (p + z2 / (2.0 * n)) / denom
    half = WILSON_Z_95 * math.sqrt(p * (1.0 - p) / n + z2 / (4.0 * n * n)) / denom
    return max(0.0, centre - half)


def _parse_json_dict(raw: Any) -> dict[str, Any]:
    """Coerce a JSONB dict or JSON string column into a plain dict (fail closed on malformed)."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _extract_int(sources: tuple[dict[str, Any], ...], keys: tuple[str, ...]) -> int | None:
    for src in sources:
        for key in keys:
            if key not in src or src[key] is None or isinstance(src[key], bool):
                continue
            val = src[key]
            try:
                fval = float(val)
            except (TypeError, ValueError):
                continue
            if math.isfinite(fval) and fval >= 0 and fval == int(fval):
                return int(fval)
    return None


def _extract_float(sources: tuple[dict[str, Any], ...], keys: tuple[str, ...]) -> float | None:
    for src in sources:
        for key in keys:
            if key not in src or src[key] is None or isinstance(src[key], bool):
                continue
            try:
                fval = float(src[key])
            except (TypeError, ValueError):
                continue
            if math.isfinite(fval):
                return fval
    return None


def _check_recorded_overfitting(
    gate_detail: dict[str, Any],
    full_metrics: dict[str, Any],
) -> tuple[bool | None, str | None]:
    """Inspect recorded overfitting checks in ``gate_detail`` / ``full_period_metrics``.

    Returns:
        ``(None, None)`` if no overfitting check is recorded,
        ``(True, None)`` if recorded and passed,
        ``(False, reason)`` if recorded and failed (or inconclusive).
    """
    recorded = False
    for container in (gate_detail, full_metrics):
        for key in ("overfit_gate", "overfitting", "overfit_check"):
            if key not in container or container[key] is None:
                continue
            recorded = True
            entry = container[key]
            if isinstance(entry, bool):
                if entry is False:
                    return False, f"{key}=False"
            elif isinstance(entry, str):
                if entry.strip().upper() != "PASS":
                    return False, f"{key}={entry!r}"
            elif isinstance(entry, dict):
                if entry.get("passed") is False or (
                    "passed" in entry and entry.get("passed") is not True
                ):
                    reasons = entry.get("reasons")
                    suffix = f" ({'; '.join(map(str, reasons))})" if reasons else ""
                    return False, f"{key}.passed={entry.get('passed')!r}{suffix}"
                for status_key in ("result", "status", "verdict"):
                    if status_key in entry and entry[status_key] is not None:
                        if str(entry[status_key]).strip().upper() == "FAIL":
                            return False, f"{key}.{status_key}=FAIL"
                for sub_bool in ("pbo_passed", "dsr_passed", "deflated_sharpe_passed"):
                    if entry.get(sub_bool) is False:
                        return False, f"{key}.{sub_bool}=False"
            else:
                return False, f"{key} has unrecognised shape"

        for bool_key in (
            "overfit_passed",
            "overfitting_passed",
            "pbo_passed",
            "dsr_passed",
            "deflated_sharpe_passed",
        ):
            if bool_key in container and container[bool_key] is not None:
                recorded = True
                if container[bool_key] is False:
                    return False, f"{bool_key}=False"

        for verdict_key in ("overfit_verdict", "overfitting_verdict"):
            if verdict_key in container and container[verdict_key] is not None:
                recorded = True
                if str(container[verdict_key]).strip().upper() == "FAIL":
                    return False, f"{verdict_key}=FAIL"

    return (True, None) if recorded else (None, None)


class GateChecker:
    """Checks promotion gate requirements for model state transitions.

    Each gate is a specific requirement that must be met before a model
    can transition to the next state.

    Attributes:
        engine: SQLAlchemy engine for database queries.
        drift_checker: Optional callable ``(model_id) -> drift_report_dict``
            used by the SHADOW -> STAGING drift gate.
    """

    def __init__(
        self,
        db_engine: Engine,
        drift_checker: Callable[[int], dict[str, Any]] | None = None,
    ) -> None:
        """Initialise the gate checker.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            drift_checker: Optional callable returning a drift report dict for
                a model_id. When None, attempts to invoke
                ``FeatureImportanceTracker.get_comprehensive_drift_report``.
        """
        self.engine = db_engine
        self.drift_checker = drift_checker
        log.info("GateChecker initialised")

    def _check_critical_drift(self, model_id: int) -> dict[str, Any]:
        """Run the repository's model drift check, failing closed when unavailable."""
        try:
            if self.drift_checker is not None:
                report = self.drift_checker(model_id)
            else:
                from features.importance import FeatureImportanceTracker
                from store.pit import PITStore

                tracker = FeatureImportanceTracker(
                    db_engine=self.engine,
                    pit_store=PITStore(self.engine),
                )
                report = tracker.get_comprehensive_drift_report(model_id)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "Drift check unavailable for model {m}: {t}: {e}",
                m=model_id,
                t=type(exc).__name__,
                e=str(exc),
            )
            return {"available": False, "reason": f"{type(exc).__name__}: {exc}"}

        if not isinstance(report, dict):
            return {"available": False, "reason": "drift checker returned non-dict report"}

        action = str(report.get("recommended_action", "") or "").upper()
        raw_score = report.get("overall_drift_score")
        score: float | None = None
        if isinstance(raw_score, (int, float)) and not isinstance(raw_score, bool):
            fscore = float(raw_score)
            if math.isfinite(fscore):
                score = fscore

        is_critical = (
            report.get("critical_drift") is True
            or action == "FLAG"
            or (score is not None and score > CRITICAL_DRIFT_SCORE_THRESHOLD)
        )
        if is_critical:
            return {
                "available": True,
                "critical": True,
                "action": action or "FLAG",
                "score": score,
            }

        # Guard against an empty report defaulting overall_drift_score to 0.0 / OK
        # when none of the underlying drift checks actually had data.
        imp = report.get("importance_drift")
        dd = report.get("data_distribution_drift")
        cd = report.get("prediction_confidence_drift")
        has_imp = bool(
            isinstance(imp, dict)
            and any(isinstance(v, dict) and "drifted" in v and not v.get("error") for v in imp.values())
        )
        has_data = bool(
            isinstance(dd, dict)
            and any(isinstance(v, dict) and "drifted" in v for v in dd.values())
        )
        has_conf = bool(isinstance(cd, dict) and cd.get("sufficient_data") is True)
        explicit_checked = report.get("checked") is True

        if not (has_imp or has_data or has_conf or explicit_checked):
            return {"available": False, "reason": "drift report contains no evaluated metrics"}

        return {
            "available": True,
            "critical": False,
            "action": action or "OK",
            "score": score,
        }

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

            # Gate 2: hypothesis must be a validated PASSED (legacy PASSED
            # rows are labelled unvalidated; governance.hypothesis_labels)
            from governance.hypothesis_labels import passed_validation

            hyp = passed_validation(conn, model[1])
            if hyp["state"] is None:
                details.append("Associated hypothesis not found")
                passed = False
            elif not hyp["validated"]:
                details.append(f"Hypothesis is not a validated PASSED: {hyp['reason']}")
                passed = False
            else:
                details.append("Hypothesis is in PASSED state")

        log.info("Gate check CANDIDATE->SHADOW for model {m}: {r}", m=model_id, r="PASS" if passed else "FAIL")
        return {"passed": passed, "details": details}

    def check_shadow_to_staging(
        self,
        model_id: int,
        *,
        force: bool = False,
        override_reason: str | None = None,
    ) -> dict[str, Any]:
        """Check gates for SHADOW -> STAGING transition.

        Requirements:
        - For trained models: model artifact must exist
        - Validation verdict must be PASS or CONDITIONAL
        - At least 14 days of shadow scoring data
        - No critical drift detected (refuses with ``drift_check_unavailable``
          when the drift check is unavailable unless ``force=True`` AND a
          non-empty ``override_reason`` are supplied)

        Parameters:
            model_id: Model registry ID.
            force: If True AND ``override_reason`` is non-empty, allows
                bypassing ``drift_check_unavailable`` (never bypasses detected
                critical drift or other failed gates).
            override_reason: Required non-empty justification when ``force=True``.

        Returns:
            dict: Gate check result.
        """
        details: list[str] = []
        passed = True
        override_record: dict[str, Any] | None = None

        with self.engine.connect() as conn:
            model = conn.execute(
                text("SELECT model_type FROM model_registry WHERE id = :id"),
                {"id": model_id},
            ).fetchone()

            if model is None:
                return {"passed": False, "details": [f"Model {model_id} not found"]}

            model_type = model[0] or "rule_based"

            # Gate 1: For trained models, artifact must exist
            if model_type != "rule_based":
                artifact = conn.execute(
                    text("SELECT id FROM model_artifacts WHERE model_id = :mid LIMIT 1"),
                    {"mid": model_id},
                ).fetchone()
                if artifact is None:
                    details.append("No trained model artifact found")
                    passed = False
                else:
                    details.append("Model artifact exists")

            # Gate 2: Validation verdict
            val = conn.execute(
                text(
                    "SELECT overall_verdict FROM validation_results "
                    "WHERE model_version_id = :mid OR hypothesis_id IN "
                    "(SELECT hypothesis_id FROM model_registry WHERE id = :mid) "
                    "ORDER BY run_timestamp DESC LIMIT 1"
                ),
                {"mid": model_id},
            ).fetchone()
            if val is None:
                details.append("No validation results found")
                # Not a hard failure for rule-based models
                if model_type != "rule_based":
                    passed = False
            elif val[0] not in ("PASS", "CONDITIONAL"):
                details.append(f"Validation verdict is {val[0]}, need PASS or CONDITIONAL")
                passed = False
            else:
                details.append(f"Validation verdict: {val[0]}")

            # Gate 3: Shadow scoring history (14+ days)
            shadow_count = conn.execute(
                text(
                    "SELECT COUNT(DISTINCT as_of_date) FROM shadow_scores "
                    "WHERE shadow_model_id = :mid"
                ),
                {"mid": model_id},
            ).fetchone()
            shadow_days = shadow_count[0] if shadow_count else 0
            if shadow_days < 14:
                details.append(f"Only {shadow_days}/14 days of shadow scoring")
                # Soft gate — warn but don't block for rule-based
                if model_type != "rule_based":
                    passed = False
            else:
                details.append(f"{shadow_days} days of shadow scoring (>= 14)")

        # Gate 4: No critical drift detected (fail closed when unavailable)
        drift = self._check_critical_drift(model_id)
        if drift["available"]:
            if drift["critical"]:
                details.append(
                    f"critical_drift_detected: action={drift['action']}, score={drift['score']}"
                )
                passed = False
            else:
                details.append(
                    f"No critical drift detected (action={drift['action']}, score={drift['score']})"
                )
        else:
            reason_clean = override_reason.strip() if isinstance(override_reason, str) else ""
            if force is True and reason_clean:
                override_record = {
                    "gate": "drift",
                    "code": "drift_check_unavailable",
                    "reason": reason_clean,
                    "drift_error": drift["reason"],
                }
                details.append(
                    f"drift_check_unavailable ({drift['reason']}) — overridden with "
                    f"force=True (override_reason={reason_clean})"
                )
                log.warning(
                    "Gate override SHADOW->STAGING for model {m}: "
                    "drift_check_unavailable bypassed with force=True, override_reason={r}",
                    m=model_id,
                    r=reason_clean,
                )
            else:
                details.append(f"drift_check_unavailable: {drift['reason']}")
                passed = False

        details.append("Operator approval required")
        if override_record is not None:
            log.info(
                "Gate check SHADOW->STAGING for model {m}: {r} (override_reason={o})",
                m=model_id,
                r="PASS" if passed else "FAIL",
                o=override_record["reason"],
            )
            return {"passed": passed, "details": details, "override": override_record}

        log.info("Gate check SHADOW->STAGING for model {m}: {r}", m=model_id, r="PASS" if passed else "FAIL")
        return {"passed": passed, "details": details}

    def check_staging_to_production(self, model_id: int) -> dict[str, Any]:
        """Check gates for STAGING -> PRODUCTION transition.

        Requirements:
        - At least 20 journal entries for this model
        - Out-of-sample evidence record in ``validation_results`` for this
          ``model_version_id`` whose ``gate_detail`` holds a typed, versioned
          OOS record that a registered reader accepts (none is registered
          yet, so this gate refuses; generic counts or rates are refused),
          with the reader's proven counts showing:
          (a) ``n >= 30`` scored forward predictions produced by this model,
          (b) hit rate's Wilson 95% lower bound > 0.5 OR Brier < climatology,
          (c) no failed overfitting check if one is recorded
        - Operator sign-off (checked at transition time)
        - No other PRODUCTION model in the same layer (warning; existing will be demoted)

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

            # Gate 3: typed out-of-sample evidence bound to this model_version_id.
            # No hypothesis_id fallback: rows the shipped WalkForwardBacktest
            # writer stores carry no model_version_id and never match.
            val_row = conn.execute(
                text(
                    "SELECT overall_verdict, full_period_metrics, "
                    "baseline_comparison, gate_detail "
                    "FROM validation_results "
                    "WHERE model_version_id = :mid "
                    "ORDER BY run_timestamp DESC LIMIT 1"
                ),
                {"mid": model_id},
            ).fetchone()

            proven: dict[str, Any] | None = None
            if val_row is None:
                details.append(
                    f"missing_oos_evidence: no validation_results record for model_version_id={model_id}"
                )
                passed = False
            else:
                overall_verdict = val_row[0]
                full_metrics = _parse_json_dict(val_row[1])
                # val_row[2] (baseline_comparison) is not OOS evidence and is not read.
                gate_detail = _parse_json_dict(val_row[3])

                if overall_verdict not in ("PASS", "CONDITIONAL"):
                    details.append(
                        f"validation_verdict_failed: overall_verdict={overall_verdict!r} "
                        "(need PASS or CONDITIONAL)"
                    )
                    passed = False

                # (c) No failed overfitting check if one is recorded
                overfit_ok, overfit_reason = _check_recorded_overfitting(
                    gate_detail, full_metrics
                )
                if overfit_ok is False:
                    details.append(f"overfitting_check_failed: {overfit_reason}")
                    passed = False
                elif overfit_ok is True:
                    details.append("Overfitting check passed")
                else:
                    details.append("No overfitting check recorded")

                # (a) + (b) need a typed record a registered reader accepts
                proven, refusal = _read_typed_oos_evidence(gate_detail, model_id)
                if proven is None:
                    details.append(refusal)
                    passed = False

            if proven is not None:
                # Skill is read only from the counts a typed reader proved.
                # (a) n >= 30 scored forward predictions produced by this model
                n_scored = _extract_int((proven,), ("n_scored",))
                if n_scored is None:
                    details.append(
                        "missing_scored_predictions_count: proven OOS evidence lacks n_scored"
                    )
                    passed = False
                elif n_scored < MIN_OOS_SCORED_PREDICTIONS:
                    details.append(
                        f"insufficient_scored_predictions: {n_scored}/{MIN_OOS_SCORED_PREDICTIONS} "
                        f"scored forward predictions (need >= {MIN_OOS_SCORED_PREDICTIONS})"
                    )
                    passed = False
                else:
                    details.append(
                        f"{n_scored} scored forward predictions (>= {MIN_OOS_SCORED_PREDICTIONS})"
                    )

                # (b) Hit rate Wilson 95% lower bound > 0.5 OR Brier < climatology
                hit_rate = _extract_float((proven,), ("hit_rate",))
                if hit_rate is None and n_scored and n_scored > 0:
                    n_hits = _extract_int((proven,), ("n_hits",))
                    if n_hits is not None and n_hits <= n_scored:
                        hit_rate = n_hits / n_scored

                wilson_lo: float | None = None
                hit_rate_ok = False
                if hit_rate is not None and 0.0 <= hit_rate <= 1.0 and n_scored and n_scored > 0:
                    wilson_lo = _wilson_lower_95(hit_rate, n_scored)
                    hit_rate_ok = wilson_lo > 0.5

                brier = _extract_float((proven,), ("brier",))
                clim_brier = _extract_float((proven,), ("climatology_brier",))
                brier_present = (
                    brier is not None
                    and clim_brier is not None
                    and brier >= 0.0
                    and clim_brier >= 0.0
                )
                brier_ok = bool(brier_present and brier < clim_brier)

                if wilson_lo is None and not brier_present:
                    details.append(
                        "missing_skill_metrics: proven OOS evidence must give hit_rate (or n_hits) "
                        "or both brier and climatology_brier"
                    )
                    passed = False
                elif not (hit_rate_ok or brier_ok):
                    parts = []
                    if wilson_lo is not None:
                        parts.append(f"hit_rate={hit_rate:.4f} (Wilson 95% lo={wilson_lo:.4f} <= 0.5)")
                    if brier_present:
                        parts.append(f"brier={brier:.4f} >= climatology={clim_brier:.4f}")
                    details.append("oos_skill_below_threshold: " + "; ".join(parts))
                    passed = False
                else:
                    if hit_rate_ok:
                        details.append(
                            f"OOS hit_rate={hit_rate:.4f} (Wilson 95% lo={wilson_lo:.4f} > 0.5)"
                        )
                    if brier_ok:
                        details.append(
                            f"OOS brier={brier:.4f} < climatology={clim_brier:.4f}"
                        )

        log.info(
            "Gate check STAGING->PRODUCTION for model {m}: {r}",
            m=model_id,
            r="PASS" if passed else "FAIL",
        )
        return {"passed": passed, "details": details}

    def check_all_gates(
        self,
        model_id: int,
        target_state: str,
        *,
        force: bool = False,
        override_reason: str | None = None,
    ) -> dict[str, Any]:
        """Run all applicable gate checks for a target state transition.

        Parameters:
            model_id: Model registry ID.
            target_state: The desired target state.
            force: Forwarded to ``check_shadow_to_staging`` when ``target_state == "STAGING"``.
            override_reason: Forwarded to ``check_shadow_to_staging`` when ``target_state == "STAGING"``.

        Returns:
            dict: Gate check result with 'passed' and 'details'.
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

        if target_state == "STAGING" and (force or override_reason is not None):
            return self.check_shadow_to_staging(
                model_id, force=force, override_reason=override_reason
            )

        return checker(model_id)


if __name__ == "__main__":
    from db import get_engine

    gc = GateChecker(db_engine=get_engine())
    print("GateChecker ready")
