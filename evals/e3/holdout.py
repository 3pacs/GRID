"""E3B holdout custodian adapter.

In-process Python is not an OS security boundary and judge-only extraction accepted; live use remains controller-gated.
"""

from __future__ import annotations

from typing import Any
from analysis import ledger_steered_exploration as s11_mod
from analysis import offline_research_proof as orp

_JUDGE_TOKEN = object()


def _get_candidate(judge: Any, candidate_id: str) -> Any:
    return judge.state().candidates.get(candidate_id)


class Custodian:
    """Custodian adapter enforcing stage entry, single evaluation, and S11 accounting."""

    def __init__(self, judge: Any, s11: Any, reader: Any) -> None:
        self.judge = judge
        self.s11 = s11
        self.reader = reader
        self._results: dict[str, dict[str, Any]] = {}

    def passed(self, candidate_id: str) -> bool:
        """Proposer-facing read-only query. Does not evaluate or expose stats."""
        if candidate_id in self._results:
            return bool(self._results[candidate_id]["passed"])
        cand = _get_candidate(self.judge, candidate_id)
        if cand and hasattr(cand, "results"):
            return cand.results.get("holdout", {}).get("result") == "pass"
        return False

    def check(
        self,
        token: object,
        candidate_id: str,
        frozen: dict[str, Any],
        allocation: dict[str, Any],
    ) -> dict[str, Any]:
        """Evaluate holdout under judge token authorization."""
        if token is not _JUDGE_TOKEN:
            raise PermissionError("token must be identity _JUDGE_TOKEN")

        cand = _get_candidate(self.judge, candidate_id)
        if cand is None:
            raise ValueError(f"Unknown candidate {candidate_id}")
        if candidate_id in self._results or "holdout" in getattr(cand, "results", {}):
            raise ValueError(f"Candidate {candidate_id} already evaluated")
        if "holdout" in getattr(cand, "entered", []):
            raise ValueError("Holdout stage already entered")

        open_alloc = self.s11.open_allocation()
        if open_alloc is None:
            raise ValueError("No open S11 allocation")
        alloc_rec, alloc_sha = open_alloc
        if alloc_sha != allocation.get("sha256") or alloc_rec != allocation.get(
            "record"
        ):
            raise ValueError(
                "Passed allocation does not match current open S11 allocation"
            )

        # Stage entry recorded before label reading; crash/restart rejects re-read
        self.judge.stage_entered(
            candidate_id,
            "holdout",
            s11=self.s11,
            s11_allocation_sha256=allocation["sha256"],
        )

        try:
            payload = frozen.get("payload", {})
            if orp.digest(payload) != frozen.get("sha256"):
                raise ValueError("frozen discovery manifest changed")

            protocol = orp.protocol_from_payload(payload["protocol"])
            record = allocation["record"]
            windows = record["windows"]
            alpha = record["alpha"]

            if protocol.run_id != record["run_id"]:
                raise ValueError("run_id mismatch")
            if protocol.allocation_sha256 != allocation["sha256"]:
                raise ValueError("allocation_sha256 mismatch")
            if (protocol.start, protocol.split, protocol.end) != (
                windows["start"],
                windows["split"],
                windows["end"],
            ):
                raise ValueError("window mismatch")
            if protocol.selection != "ledger_holm":
                raise ValueError("selection must be ledger_holm")
            if protocol.selection_alpha != alpha:
                raise ValueError("selection_alpha mismatch")
            if protocol.alpha != alpha:
                raise ValueError("Protocol.alpha must equal allocation alpha exactly")

            declared = {(t["family"], t["feature"]) for t in record["trials"]}
            if set(protocol.trials) != declared or len(protocol.trials) != len(
                record["trials"]
            ):
                raise ValueError("manifest trials differ from allocation")

            if self.s11.alpha_spent() > self.s11.q:
                raise ValueError("S11 alpha budget exceeded")

            # Active allocation refreshed / fail closed before reading labels
            cur_open = self.s11.open_allocation()
            if cur_open is None or cur_open[1] != allocation["sha256"]:
                raise ValueError("Active allocation is no longer open")

            holdout_rows = self.reader.read_holdout(protocol)
            holdout_result = orp.evaluate_holdout(frozen, holdout_rows)
            s11_mod.record_run(self.s11, frozen, holdout_result)

            receipt_hash = orp.digest(holdout_result)
            checks = {
                c["trial_id"]: c for c in holdout_result.get("holdout_checks", [])
            }
            trials_by_ident = {t["identity_sha256"]: t for t in record["trials"]}

            cand_prop = (
                cand.get("proposed", {})
                if isinstance(cand, dict)
                else getattr(cand, "proposed", {})
            )
            identities = cand_prop.get("identity_sha256", [])
            expected_sign = cand_prop.get("expected_sign", 0)
            discovery_by_trial = {t["trial_id"]: t for t in payload.get("ledger", [])}

            p_values: list[float] = []
            survived = False
            for ident in identities:
                trial = trials_by_ident.get(ident)
                check = checks.get(trial["trial_id"]) if trial else None
                if check is not None:
                    p_values.append(float(check["p"]))
                    disc = discovery_by_trial.get(trial["trial_id"])
                    disc_r = disc.get("r", 0.0) if disc else 0.0
                    sign_ok = expected_sign == 0 or disc_r * expected_sign > 0
                    if check.get("retrospective_survivor") and sign_ok:
                        survived = True
                else:
                    p_values.append(1.0)

            result_str = "pass" if survived else "fail"
            self.judge.stage_result(
                candidate_id,
                "holdout",
                result_str,
                receipt_sha256=receipt_hash,
                alpha_spent=alpha,
                p_values=p_values,
            )
            out = {
                "passed": survived,
                "receipt": holdout_result,
                "allocation": allocation,
            }
            self._results[candidate_id] = out
            return out
        except Exception as exc:
            open_alloc_post = self.s11.open_allocation()
            if open_alloc_post is not None and open_alloc_post[1] == allocation.get(
                "sha256"
            ):
                closure = s11_mod.abandon(self.s11, str(exc))
                self.judge.abandoned(
                    candidate_id,
                    str(exc),
                    s11=self.s11,
                    s11_abandoned_sha256=closure["sha256"],
                )
            else:
                self.judge.abandoned(candidate_id, str(exc), s11=self.s11)
            raise

    def abandon(self, token: object, candidate_id: str, reason: str) -> dict[str, Any]:
        """Close current open allocation and abandon candidate in judge."""
        if token is not _JUDGE_TOKEN:
            raise PermissionError("token must be identity _JUDGE_TOKEN")
        cand = _get_candidate(self.judge, candidate_id)
        if cand is None:
            raise ValueError(f"Unknown candidate {candidate_id}")
        cand_alloc = (
            cand.get("allocation")
            if isinstance(cand, dict)
            else getattr(cand, "allocation", None)
        )
        open_alloc = self.s11.open_allocation()
        if open_alloc is None or open_alloc[1] != cand_alloc:
            raise ValueError("open S11 allocation does not match candidate allocation")
        closure = s11_mod.abandon(self.s11, reason)
        return self.judge.abandoned(
            candidate_id,
            reason,
            s11=self.s11,
            s11_abandoned_sha256=closure["sha256"],
        )
