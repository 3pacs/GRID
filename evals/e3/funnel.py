"""E3B constrained evaluation funnel adapter."""

from __future__ import annotations

import copy
import dataclasses

from analysis import ledger_steered_exploration as lse
from analysis import offline_research_proof as orp
from evals.e3 import gates
from evals.e3.holdout import _JUDGE_TOKEN


class Funnel:
    """Adapter coordinating screen, gates, and holdout stages."""

    def __init__(self, judge, s11, reader, custodian):
        self.judge = judge
        self.s11 = s11
        self.reader = reader
        self.custodian = custodian

    def screen(self, candidate_id: str, protocol: orp.Protocol) -> dict:
        """Screen stage: discovery-only Holm test at next S11 alpha."""
        if self.s11.open_allocation() is not None:
            raise ValueError("S11 has an open allocation")

        candidate = self.judge.state().candidates[candidate_id]
        proposed = candidate.proposed
        candidate_identities = proposed["identity_sha256"]
        expected_sign = proposed["expected_sign"]

        if not protocol.trials:
            raise ValueError("Protocol must have explicit nonempty trials")

        protocol_identities = {lse.identity(fam, feat) for fam, feat in protocol.trials}
        if not set(candidate_identities).issubset(protocol_identities):
            raise ValueError(
                "Candidate trials not fully covered by protocol declared trials"
            )

        k = len(self.s11.allocations()) + 1
        alpha = lse.run_alpha(k, self.s11.q)
        s11_head_before = self.s11.head

        protocol = dataclasses.replace(
            protocol,
            selection="ledger_holm",
            selection_alpha=alpha,
            alpha=alpha,
            allocation_sha256=s11_head_before,
        )
        protocol.validate()

        window = {"start": protocol.start, "end": protocol.split}
        self.judge.stage_entered(candidate_id, "screen", s11=self.s11, window=window)

        try:
            rows = self.reader.read_discovery(protocol)
            frozen = orp.discover(protocol, rows)
        except Exception as exc:
            self.judge.abandoned(candidate_id, str(exc))
            raise

        ledger = frozen["payload"]["ledger"]
        measured_by_ident = {lse.identity(t["family"], t["feature"]): t for t in ledger}

        all_present = all(i in measured_by_ident for i in candidate_identities)
        if not all_present:
            self.judge.abandoned(candidate_id, "missing declared candidate trial")
            raise ValueError("missing declared candidate trial")

        p_values = [measured_by_ident[i]["p"] for i in candidate_identities]

        passed = any(
            measured_by_ident[i]["selected"]
            and (
                expected_sign == 0
                or (expected_sign > 0 and measured_by_ident[i]["r"] > 0)
                or (expected_sign < 0 and measured_by_ident[i]["r"] < 0)
            )
            for i in candidate_identities
        )

        receipt_sha256 = orp.digest(frozen)
        self.judge.stage_result(
            candidate_id,
            "screen",
            "pass" if passed else "fail",
            receipt_sha256=receipt_sha256,
            p_values=p_values,
        )

        return {
            "passed": passed,
            "frozen": frozen,
            "s11_head_before": s11_head_before,
            "run_index": k,
        }

    def gates(
        self,
        candidate_id: str,
        *,
        commit_sha: str,
        ci_receipt: str | dict,
        baseline_hashes: dict,
        baseline_cards: dict,
        baseline_digests: dict,
        repo_root: str,
        runner=None,
        profile: str = "ci",
    ) -> dict:
        """Gates stage: CI and E0 validation gate checks."""
        self.judge.stage_entered(candidate_id, "gates")
        try:
            candidate = self.judge.state().candidates[candidate_id]
            candidate_kind = candidate.proposed["candidate_kind"]
            result = gates.evaluate_gates(
                commit_sha,
                ci_receipt,
                candidate_kind,
                baseline_hashes=baseline_hashes,
                baseline_cards=baseline_cards,
                baseline_digests=baseline_digests,
                repo_root=repo_root,
                runner=runner,
                profile=profile,
            )
        except Exception as exc:
            self.judge.abandoned(candidate_id, str(exc))
            raise

        passed = bool(result["passed"])
        receipt_sha256 = orp.digest(result)
        self.judge.stage_result(
            candidate_id,
            "gates",
            "pass" if passed else "fail",
            receipt_sha256=receipt_sha256,
        )
        return result

    def holdout(self, candidate_id: str, screened: dict, allocation: dict):
        """Holdout stage: validate allocation and delegate custody check."""
        candidate = self.judge.state().candidates[candidate_id]
        if "holdout" in candidate.entered:
            raise ValueError("Candidate already entered holdout stage")
        try:
            screen_res = candidate.results.get("screen", {})
            gates_res = candidate.results.get("gates", {})
            if screen_res.get("result") != "pass" or gates_res.get("result") != "pass":
                raise ValueError("Screen and gates stages must be passed")
            if screened.get("passed") is not True:
                raise ValueError("Screened passed must be literal True")

            frozen = screened["frozen"]
            if screen_res.get("receipt_sha256") != orp.digest(frozen):
                raise ValueError("Screen receipt digest mismatch")
            if frozen["sha256"] != orp.digest(frozen["payload"]):
                raise ValueError("Screened digest mismatch")

            s11_head_before = self.judge.records()[candidate.entered_seq["screen"]][
                "s11_head_sha256"
            ]
            if screened.get("s11_head_before") != s11_head_before:
                raise ValueError("Screened s11_head_before mismatch")

            rec = allocation["record"]
            if rec["run_index"] != screened["run_index"]:
                raise ValueError(
                    "Allocation run_index does not match screened run_index"
                )
            if rec["prev_sha256"] != s11_head_before:
                raise ValueError(
                    "Allocation prev_sha256 does not match screened s11_head_before"
                )

            open_alloc = self.s11.open_allocation()
            if open_alloc is None or open_alloc[1] != allocation["sha256"]:
                raise ValueError("Allocation is not the current open allocation")

            orig_proto = frozen["payload"]["protocol"]
            if orig_proto["run_id"] != rec["run_id"]:
                raise ValueError("Protocol run_id mismatch")
            if orig_proto.get("allocation_sha256") != s11_head_before:
                raise ValueError("Protocol allocation_sha256 mismatch")
            if orig_proto.get("selection_alpha") != rec["alpha"]:
                raise ValueError("Protocol selection_alpha mismatch")
            if orig_proto.get("alpha") != rec["alpha"]:
                raise ValueError("Protocol alpha mismatch")

            windows = rec["windows"]
            if (
                orig_proto["start"] != windows["start"]
                or orig_proto["split"] != windows["split"]
                or orig_proto["end"] != windows["end"]
            ):
                raise ValueError("Protocol windows mismatch")

            alloc_trials = tuple((t["family"], t["feature"]) for t in rec["trials"])
            orig_trials = tuple(tuple(pair) for pair in orig_proto["trials"])
            if set(orig_trials) != set(alloc_trials) or len(orig_trials) != len(
                alloc_trials
            ):
                raise ValueError("Protocol trials mismatch allocation trials")

            ledger_pairs = tuple(
                (t["family"], t["feature"]) for t in frozen["payload"]["ledger"]
            )
            if set(ledger_pairs) != set(alloc_trials) or len(ledger_pairs) != len(
                alloc_trials
            ):
                raise ValueError("Discovery ledger trials mismatch allocation trials")

            fixed_keys = {
                "run_id",
                "features",
                "families",
                "trials",
                "self_lag",
                "selection",
                "selection_alpha",
                "allocation_sha256",
                "start",
                "split",
                "end",
            }
            settings = {k: v for k, v in orig_proto.items() if k not in fixed_keys}
            allocated_protocol = lse.protocol_for_allocation(allocation, **settings)

            if tuple(orig_proto["features"]) != allocated_protocol.features:
                raise ValueError("Protocol features mismatch")
            if tuple(orig_proto["families"]) != allocated_protocol.families:
                raise ValueError("Protocol families mismatch")
            orig_self_lag = tuple(tuple(p) for p in orig_proto.get("self_lag", ()))
            if orig_self_lag != allocated_protocol.self_lag:
                raise ValueError("Protocol self_lag mismatch")
            if orig_proto["alpha"] != allocated_protocol.alpha:
                raise ValueError("Protocol alpha mismatch")

            payload = copy.deepcopy(frozen["payload"])
            payload["protocol"] = dataclasses.asdict(allocated_protocol)
            rebound = {"payload": payload, "sha256": orp.digest(payload)}
        except Exception as exc:
            open_alloc = self.s11.open_allocation()
            if open_alloc is not None and open_alloc[1] == allocation.get("sha256"):
                lse.abandon(self.s11, str(exc))
            self.judge.abandoned(candidate_id, str(exc))
            raise

        return self.custodian.check(_JUDGE_TOKEN, candidate_id, rebound, allocation)
