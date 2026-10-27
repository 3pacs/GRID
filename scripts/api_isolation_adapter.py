"""Transaction Port implementation over an explicitly supplied kernel boundary.

No default OS/systemd/credential implementation is selected. The kernel contract
requires a lifetime exclusive stable-inode lock, descriptor-relative operations,
durable atomic CAS and receipt verification. Tests inject a volatile kernel and
inert service callable; they prove adapter sequencing, not POSIX guarantees.
"""
from copy import deepcopy
from deploy_api_isolated import require, digest, sha


class Adapter:
    def __init__(self, kernel, service):
        self.k, self.service = kernel, service
        self.lease = None
        self.fence = None
        self.recovery = None
        self.service_expectation = None

    def protected(self, p):
        return self.k.observe_protected(p["binding"])

    def preflight(self, p):
        self.k.verify_bound_receipts_and_topology(p)
        require(self.k.current() == p["old"], "deployment old target mismatch")

    def _claim(self, p, approval=None):
        require(self.lease is None, "adapter already claimed")
        lease = self.k.acquire_stable_lock_nonblocking()
        # If checks fail, retain this lease until caller exits; never steal/retry.
        self.lease = lease
        old, journal = self.k.read_record_and_journal(lease)
        if approval is None:
            require(old is None, "abandoned record requires explicit recovery")
            epoch = self.k.last_epoch(lease) + 1
        else:
            require(old is not None and old["plan"] == p["plan_sha256"], "foreign recovery")
            require(isinstance(journal, dict), "missing durable recovery journal")
            require(digest(old) == approval["lock_record_sha256"] and digest(journal) == approval["journal_sha256"], "recovery changed before claim")
            self.k.verify_recovery_approval(p, approval)
            epoch = old["epoch"] + 1
        # Recover the durable expected operation, never infer emptiness from a
        # missing ledger. Every R6 journal and ownership record carries this pin.
        reference = journal if journal is not None else old
        if reference is not None:
            require("service_expectation" in reference, "missing durable service expectation")
            self.service_expectation = deepcopy(reference["service_expectation"])
            require(isinstance(self.service_expectation, dict), "unknown durable service expectation")
        # Process-lock loss never settles a request already handed to a manager.
        # Check under the stable lock BEFORE changing the ownership record/alias.
        self._service_settled(p)
        record = {"plan": p["plan_sha256"], "epoch": epoch, "nonce": self.k.fresh_nonce(lease),
                  "service_expectation": deepcopy(self.service_expectation)}
        fence = digest(record)
        record["fence"] = fence
        self.k.cas_record_durable(lease, old, record)
        self.fence = fence
        self.recovery = approval
        return fence

    def claim(self, p):
        return self._claim(p)

    def claim_recovery(self, p, approval):
        return self._claim(p, approval)

    def _guard(self, p):
        require(self.lease is not None and p.get("executor_fence") == self.fence, "stale adapter fence")
        self.k.assert_live_fence(self.lease, self.fence, p["plan_sha256"])

    def recovery_preflight(self, p, approval):
        self.k.verify_bound_receipts_and_topology(p)
        self.k.verify_recovery_approval(p, approval)
        if self.fence is not None:
            self._guard(p)
            require(self.recovery == approval, "recovery substitution")

    def stage(self, p):
        self._guard(p)
        # Kernel must verify both baseline and candidate native inventories;
        # protected inode set, every ancestor, mount and owner before writes.
        self.k.copy_bundle_exclusive(self.lease, self.fence, p)
        self._guard(p)
        self.k.verify_staged_descriptors(p)

    def journal(self, p, state):
        self._guard(p)
        self.k.write_journal_durable(self.lease, self.fence,
                                    {"plan": p["plan_sha256"], "state": state, "fence": self.fence,
                                     "service_expectation": deepcopy(self.service_expectation)})

    def promote(self, p, expected, target):
        self._guard(p)
        self._service_settled(p)
        require((expected, target) in ((p["old"], p["candidate"]), (p["candidate"], p["old"])), "foreign promotion")
        self.k.compare_exchange_current_durable(self.lease, self.fence, expected, target)

    def current(self, p):
        return self.k.current()

    def _service_settled(self, p):
        """Read-only reconciliation; absence/timeout/lock loss is not terminal.

Kernel must retain the operation ledger independently of overwritten transaction
states. A trusted broker must durably deduplicate operation_id BEFORE dispatch,
map it to a specific manager identity/request, and prove no further effects for
terminal results. No suitable broker or observer is implemented/adopted here.
"""
        observation = self.k.read_service_operation(self.lease)
        require(isinstance(observation, dict), "missing/unknown service ledger")
        require(observation.get("state") in ("NEVER_ISSUED", "ISSUED"), "missing/unknown service ledger")
        if self.service_expectation is not None:
            require(observation == self.service_expectation, "service ledger differs from expected operation")
        if observation["state"] == "NEVER_ISSUED":
            proof = observation.get("receipt")
            require(isinstance(proof, dict) and proof.get("state") == "NEVER_ISSUED" and
                    proof.get("host") == p["binding"]["host"] and
                    proof.get("service") == "grid-api.service" and
                    proof.get("namespace") == "grid-api-isolated" and
                    sha(proof.get("manager_identity_sha256")) and sha(proof.get("receipt_sha256")),
                    "unbound never-issued history")
            require(self.k.verify_never_issued_service_receipt(proof) is True,
                    "untrusted never-issued history")
            self.service_expectation = deepcopy(observation)
            return None
        operation = observation.get("operation")
        require(isinstance(operation, dict), "missing issued operation")
        require(operation["host"] == p["binding"]["host"] and
                operation["service"] == "grid-api.service" and
                sha(operation["operation_id"]) and sha(operation["manager_identity_sha256"]),
                "unbound durable service operation")
        require(operation["operation_id"] == digest({k:v for k,v in operation.items() if k != "operation_id"}),
                "operation identity mismatch")
        receipt = self.k.observe_terminal_service_operation(self.lease, operation)
        require(isinstance(receipt, dict), "prior API service operation unresolved")
        require(receipt.get("phase") == "terminal" and
                receipt.get("outcome") in ("succeeded", "failed", "cancelled") and
                receipt.get("no_further_effects") is True,
                "prior API service operation unresolved")
        for key in ("operation_id", "manager_identity_sha256", "host", "service", "target", "plan"):
            require(receipt.get(key) == operation[key], "service reconciliation identity mismatch")
        require(sha(receipt.get("receipt_sha256")), "unbound terminal receipt")
        require(self.k.verify_terminal_service_receipt(operation, receipt) is True,
                "untrusted terminal service receipt")
        self.service_expectation = deepcopy(observation)
        return receipt

    def _expected_issued(self, p, target):
        require(isinstance(self.service_expectation, dict) and
                self.service_expectation.get("state") == "ISSUED", "no expected issued service operation")
        operation = self.service_expectation["operation"]
        require(operation["plan"] == p["plan_sha256"] and operation["target"] == target,
                "expected operation plan/target mismatch")
        return operation

    def restart_api(self, p):
        self._guard(p)
        terminal = self._service_settled(p)
        operation = {"plan": p["plan_sha256"], "fence": self.fence,
                     "host": p["binding"]["host"], "service": "grid-api.service",
                     "target": self.current(p), "nonce": self.k.fresh_nonce(self.lease),
                     "manager_identity_sha256": self.k.service_manager_identity(p)}
        require(operation["target"] in (p["old"], p["candidate"]) and
                sha(operation["manager_identity_sha256"]), "service operation binding")
        operation["operation_id"] = digest(operation)
        # Pin to memory AND durable journal before preparing/dispatching. A crash
        # anywhere after this intent cannot turn missing state into never-issued.
        self.service_expectation = {"state": "ISSUED", "operation": deepcopy(operation)}
        self.journal(p, "API_SERVICE_PREPARE_INTENT")
        # Atomic durable append checks previous terminal receipt and preserves all
        # operation history. A crash before/after dispatch leaves this operation
        # uncertain; no blind resend, including an intent with unknown submission.
        self.k.prepare_service_operation_durable(self.lease, self.fence, operation, terminal)
        self.journal(p, "API_SERVICE_REQUEST_INTENT")
        result = self.service(("restart", "grid-api.service"), self.lease, self.fence, operation)
        self._guard(p)
        require(result == "request-accepted", "uncertain API service request")
        self._service_settled(p)  # refuse missing/stale ledger before later mutations

    def verify_api(self, p, target):
        self._guard(p)
        self._expected_issued(p, target)
        self._service_settled(p)  # acknowledgement alone does not mean completed
        self.k.verify_api_observation(p, target)

    def complete(self, p, state):
        self._guard(p)
        self._expected_issued(p, self.current(p))
        self._service_settled(p)
        self.journal(p, state)
        # Kernel persists terminal receipt and monotonic epoch before clearing
        # logical record/releasing flock. The stable lock inode is never removed.
        self.k.finish_durable(self.lease, self.fence, state)
        self.lease = self.fence = None
