"""E3B forward admission and promotion adapter."""

from __future__ import annotations
import json
import math
import os
from pathlib import Path
from typing import Any

from analysis import offline_research_proof as orp
from analysis.research_forward_log import first_decision, shift_sessions


class Admission:
    """Judge-owned admission and promotion adapter for E3B."""

    def __init__(self, judge: Any, receipt_dir: str | Path) -> None:
        self.judge = judge
        self.receipt_dir = Path(receipt_dir)
        self.receipt_dir.mkdir(parents=True, exist_ok=True)
        cfg_path = Path(__file__).with_name("config.json")
        cfg = (
            json.loads(cfg_path.read_text(encoding="utf-8"))
            if cfg_path.is_file()
            else {}
        )
        min_decisions = cfg.get("minimum_decisions", 40)
        if not (
            isinstance(min_decisions, int)
            and not isinstance(min_decisions, bool)
            and min_decisions >= 40
        ):
            raise ValueError("minimum_decisions must be an integer >= 40")
        min_sessions = cfg.get("minimum_sessions", 120)
        if not (
            isinstance(min_sessions, int)
            and not isinstance(min_sessions, bool)
            and min_sessions >= 120
        ):
            raise ValueError("minimum_sessions must be an integer >= 120")
        self.min_decisions = min_decisions
        self.min_sessions = min_sessions

    def _spec_sha(self, cand: Any) -> str:
        return cand.proposed["spec_sha256"]

    def _write_receipt(self, filename: str, payload: dict) -> tuple[Path, str]:
        sha = orp.digest(payload)
        path = self.receipt_dir / filename
        raw = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        with os.fdopen(fd, "wb") as f:
            f.write(raw)
            f.flush()
            os.fsync(fd)
        return path, sha

    def admit(
        self, cid: str, *, admitted_at: Any, forward_check: dict
    ) -> dict[str, Any]:
        if not (
            isinstance(forward_check, dict)
            and forward_check
            and isinstance(forward_check.get("check_id"), str)
            and forward_check.get("check_id").strip()
        ):
            raise ValueError("forward_check must be non-empty with string check_id")

        cand = self.judge.state().candidates[cid]
        if "forward" in cand.entered:
            raise ValueError("Candidate already entered forward stage")
        if getattr(cand, "suspended", False) or getattr(cand, "terminal", None):
            raise ValueError("Candidate suspended or terminal")

        holdout = getattr(cand, "results", {}).get("holdout")
        h_res = (
            holdout.get("result")
            if isinstance(holdout, dict)
            else getattr(holdout, "result", None)
        )
        if h_res != "pass":
            raise ValueError("Holdout stage must pass before forward admission")

        adm_stamp = orp.stamp(admitted_at)
        first_dec = first_decision(adm_stamp)
        first_stamp = orp.stamp(first_dec.isoformat())
        if first_dec <= adm_stamp:
            raise ValueError("First decision date must be strictly after admission")

        receipt = {
            "kind": "e3_admitted",
            "candidate_id": cid,
            "spec_sha256": self._spec_sha(cand),
            "admitted_at": str(adm_stamp),
            "first_decision_at": str(first_stamp),
            "forward_check": forward_check,
            "forward_check_sha256": orp.digest(forward_check),
        }
        _, r_sha = self._write_receipt(f"{cid}.admission.json", receipt)
        self.judge.stage_entered(cid, "forward")
        self.judge.stage_result(cid, "forward", "pass", receipt_sha256=r_sha)
        return {
            "state": "FORWARD_ADMITTED",
            "payload": receipt,
            "receipt_sha256": r_sha,
        }

    def _admission(self, cid: str) -> tuple[dict, str]:
        cand = self.judge.state().candidates[cid]
        fwd = getattr(cand, "results", {}).get("forward")
        expected_sha = (
            fwd.get("receipt_sha256")
            if isinstance(fwd, dict)
            else getattr(fwd, "receipt_sha256", None)
        )
        fwd_res = (
            fwd.get("result") if isinstance(fwd, dict) else getattr(fwd, "result", None)
        )
        if fwd_res != "pass" or not expected_sha:
            raise ValueError("Candidate forward stage has not passed")

        path = self.receipt_dir / f"{cid}.admission.json"
        if not path.is_file():
            raise ValueError(f"Missing admission artifact: {path}")
        receipt = json.loads(path.read_text(encoding="utf-8"))
        actual_sha = orp.digest(receipt)
        if actual_sha != expected_sha:
            raise ValueError("Admission digest mismatch with stage result")
        if receipt.get("kind") != "e3_admitted" or receipt.get("candidate_id") != cid:
            raise ValueError("Admission artifact identity mismatch")
        if receipt.get("spec_sha256") != self._spec_sha(cand):
            raise ValueError("Candidate spec sha mismatch")
        if orp.digest(receipt.get("forward_check")) != receipt.get(
            "forward_check_sha256"
        ):
            raise ValueError("Forward check digest tamper detected")

        adm_stamp = orp.stamp(receipt.get("admitted_at"))
        first_dec = first_decision(adm_stamp)
        if (
            str(orp.stamp(first_dec.isoformat()))
            != str(receipt.get("first_decision_at"))
            or first_dec <= adm_stamp
        ):
            raise ValueError("Computed first decision date validation failed")
        return receipt, actual_sha

    def promote(
        self, cid: str, *, decisions: list[dict], evaluation: dict
    ) -> dict[str, Any]:
        cand = self.judge.state().candidates[cid]
        if getattr(cand, "suspended", False) or getattr(cand, "terminal", None):
            raise ValueError("Candidate suspended or terminal")
        if getattr(cand, "promoted", False):
            raise ValueError("Candidate already promoted")

        adm, adm_sha = self._admission(cid)
        first_dec = first_decision(orp.stamp(adm["admitted_at"]))
        window_floor = shift_sessions(first_dec, self.min_sessions)

        self.judge.stage_entered(cid, "promotion")

        if not isinstance(decisions, list) or not isinstance(evaluation, dict):
            err_sha = orp.digest({"error": "invalid_shape"})
            self.judge.stage_result(cid, "promotion", "fail", receipt_sha256=err_sha)
            return {"passed": False, "reason": "invalid_shape"}

        dec_valid = True
        prev_t = None
        for d in decisions:
            if (
                not isinstance(d, dict)
                or "decision_at" not in d
                or "gross_return" not in d
                or "cost" not in d
            ):
                dec_valid = False
                break
            g, c = d["gross_return"], d["cost"]
            if not (
                isinstance(g, (int, float))
                and not isinstance(g, bool)
                and math.isfinite(g)
                and isinstance(c, (int, float))
                and not isinstance(c, bool)
                and math.isfinite(c)
                and c >= 0
            ):
                dec_valid = False
                break
            try:
                t = orp.stamp(d["decision_at"])
            except (ValueError, TypeError):
                dec_valid = False
                break
            if (prev_t is not None and t <= prev_t) or (t < first_dec):
                dec_valid = False
                break
            prev_t = t

        if not dec_valid:
            err_sha = orp.digest({"error": "invalid_decisions"})
            self.judge.stage_result(cid, "promotion", "fail", receipt_sha256=err_sha)
            return {"passed": False, "reason": "invalid_decisions"}

        dec_sha = orp.digest(decisions)
        last_dec = prev_t
        try:
            ev_through = (
                orp.stamp(evaluation.get("evaluated_through"))
                if evaluation.get("evaluated_through")
                else None
            )
        except (ValueError, TypeError):
            ev_through = None
        net_twice_cost = sum(
            float(d["gross_return"]) - 2.0 * float(d["cost"]) for d in decisions
        )

        gates = {
            "decisions_count": len(decisions) >= self.min_decisions,
            "window_elapsed": bool(last_dec and last_dec >= window_floor),
            "net_positive": net_twice_cost > 0.0,
            "check_binding": (
                evaluation.get("check_id") == adm["forward_check"].get("check_id")
                and evaluation.get("forward_check_sha256")
                == adm["forward_check_sha256"]
            ),
            "decisions_binding": evaluation.get("decisions_sha256") == dec_sha,
            "forward_passed": evaluation.get("passed") is True,
            "no_retirement": evaluation.get("retirement_triggered") is False,
            "eval_coverage": bool(
                ev_through
                and last_dec
                and ev_through >= last_dec
                and ev_through >= window_floor
            ),
        }
        passed = all(gates.values())

        receipt = {
            "kind": "e3_promotion",
            "candidate_id": cid,
            "passed": passed,
            "gates": gates,
            "admission_sha256": adm_sha,
            "decisions_sha256": dec_sha,
            "evaluation_sha256": orp.digest(evaluation),
            "net_at_twice_cost": net_twice_cost,
            "decisions_count": len(decisions),
        }
        _, promo_sha = self._write_receipt(f"{cid}.promotion.json", receipt)
        res_str = "pass" if passed else "fail"
        self.judge.stage_result(cid, "promotion", res_str, receipt_sha256=promo_sha)
        if passed:
            self.judge.promoted_research(cid, receipt_sha256=promo_sha)
        return {"passed": passed, "receipt": receipt, "receipt_sha256": promo_sha}
