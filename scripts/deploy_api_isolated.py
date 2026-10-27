"""SOURCE PROTOTYPE: isolated API transaction contract; no production adapter.

The executable CLI validates plans only. --execute fails closed. A future native
adapter must implement Port, be independently reviewed, and be qualified before
this workflow proposal can be adopted. No pip/npm, application imports, DB calls,
systemd commands, or filesystem deletions occur in this module.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import PurePosixPath
from typing import Protocol


class Refusal(RuntimeError):
    pass


ROOT = "/data/grid_v4/api-isolated"
LEGACY = (
    "/data/grid_v4/grid_release",
    "/data/grid_v4/grid_release.releases",
    "/data/grid_v4/grid_repo",
    "/home/grid/grid_v4/grid_repo",
    "/home/grid/.local",
    "/usr/bin",
    "/usr/lib",
    "/usr/local",
)
EVIDENCE = (
    "adoption", "protected_inventory", "native_python310", "bundle_build",
    "readonly_db_observer", "rollback_compatibility",
)


def require(value, reason):
    if not value:
        raise Refusal(reason)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def sha(value, length=64):
    return isinstance(value, str) and len(value) == length and all(c in "0123456789abcdef" for c in value)


def path(value):
    require(isinstance(value, str) and value.startswith("/"), "absolute path required")
    p = PurePosixPath(value)
    require(str(p) == value and ".." not in p.parts, "noncanonical path")
    return p


def overlaps(a, b):
    a, b = path(a), path(b)
    return a == b or a in b.parents or b in a.parents


def plan(binding):
    """Validate already reviewed evidence bindings; never create proof by flag.

    A Port MUST independently verify the referenced receipt contents, exact bundle
    files, ownership and observations. Hash-shaped strings alone are not evidence.
    """
    b = binding
    require(b["schema"] == "grid.api-isolation.v1", "schema")
    require(b["root"] == ROOT, "unadopted root")
    require(b["python"] == "3.10", "Python cutover is a separate scope")
    require(sha(b["candidate_commit"], 40) and sha(b["candidate_tree"], 40), "source pins")
    require(sha(b["old_commit"], 40), "old source pin")
    require(b["candidate_commit"] != b["old_commit"], "already deployed/retry refused")
    require(b["host"] == "grid-svr", "host binding")
    require(b["service"] == "grid-api.service", "API service only")
    require(b["current"] == ROOT + "/current", "independent alias required")
    require(b["old_release"] == ROOT + "/releases/" + b["old_commit"], "adopted old target required")
    candidate = ROOT + "/releases/" + b["candidate_commit"]
    require(b["bundle_root"] == ROOT + "/inbox/" + b["candidate_commit"], "isolated inbox required")
    require(sha(b["bundle_manifest"]), "bundle manifest pin")
    require(isinstance(b["uid"], int) and b["uid"] > 0, "dedicated owner uid required")
    require(b["uid"] != b["hermes_uid"], "shared writable identity cannot establish isolation")
    require(b["protected_roots"] and all(not overlaps(ROOT, p) for p in b["protected_roots"]), "protected overlap")
    require(all(any(path(p) == path(q) or path(q) in path(p).parents for q in b["protected_roots"]) for p in LEGACY), "legacy coverage incomplete")
    require(b["installed_heads"] and b["installed_heads"] == b["candidate_heads"], "installed migration state unknown/pending")
    require(len(set(b["installed_heads"])) == len(b["installed_heads"]), "duplicate migration heads")
    require(all(sha(b["evidence"].get(k)) for k in EVIDENCE), "missing reviewed receipt")
    require(sha(b["protected_snapshot"]), "protected snapshot pin")
    return {"schema": "grid.api-plan.v1", "binding": b,
            "candidate": candidate, "old": b["old_release"],
            "interpreter": candidate + "/runtime/bin/python3.10",
            "source": candidate + "/source", "plan_sha256": digest(b)}


class Port(Protocol):
    """Native adapter obligations are normative in CONTRACT.md.

    Every read MUST reject symlink/hardlink/mount/owner escapes before mutation.
    No method may touch legacy aliases, packages, units or other service actions.
    claim: atomically acquire one stable-inode executor lock and return a fence.
    claim_recovery: under that SAME exclusive lock, compare expected durable
    lock-record/journal hashes and adopt an explicitly admitted abandoned record;
    never rely on observing an absent old PID or age alone. Hold lock through
    terminal completion. All mutating methods reject stale executor_fence.
    stage: exclusive copy, no package hooks; check all bytes and independent inodes.
    journal: durable intent BEFORE swap; each state atomically replace+fsync.
    promote: single atomic rename of newly created link, old target compare guard.
    verify_api: actual new PID/start/cwd/interpreter/source and route checks.
    protected: kernel/service/files snapshot from reviewed bounded observer.
    complete: durable terminal receipt then release owned lock, never remove trees.
    """
    def preflight(self, p): ...
    def protected(self, p): ...
    def claim(self, p): ...
    def claim_recovery(self, p, approval): ...
    def stage(self, p): ...
    def journal(self, p, state): ...
    def promote(self, p, expected, target): ...
    def restart_api(self, p): ...
    def verify_api(self, p, target): ...
    def complete(self, p, state): ...
    def recovery_preflight(self, p, approval): ...
    def current(self, p): ...


def unchanged(port, p):
    require(port.protected(p) == p["binding"]["protected_snapshot"], "protected drift: stop")


def deploy(binding, port: Port):
    p = plan(binding)
    port.preflight(p)  # all receipt contents and topology, not only JSON schema
    unchanged(port, p)
    p["executor_fence"] = port.claim(p)
    require(sha(p["executor_fence"]), "missing executor fence")
    # Revalidate after claiming. A crash from here leaves lock/journal for owner.
    # No finally cleanup: deletion/automatic rollback can destroy recovery evidence.
    port.preflight(p)
    unchanged(port, p)
    port.journal(p, "CLAIMED")
    port.stage(p)
    unchanged(port, p)
    port.preflight(p)
    port.journal(p, "PROMOTION_INTENT")
    port.promote(p, p["old"], p["candidate"])
    port.journal(p, "PROMOTED")
    unchanged(port, p)
    port.restart_api(p)
    port.journal(p, "API_RESTART_REQUESTED")
    port.verify_api(p, p["candidate"])
    unchanged(port, p)
    port.complete(p, "VERIFIED")
    return p


def recover(binding, port: Port, approval):
    """Explicit rollback of code only. No schema/data undo or automatic retry.

    The native recovery preflight must verify no active controller/process owns
    the lock, exact journal bytes, approval freshness and both retained bundles.
    A boolean schema-compatible flag alone is intentionally insufficient.
    """
    p = plan(binding)
    require(approval["plan_sha256"] == p["plan_sha256"], "recovery plan mismatch")
    require(sha(approval["journal_sha256"]) and sha(approval["owner_receipt"]) and sha(approval["lock_record_sha256"]), "unbound recovery")
    require(approval["compatibility_receipt"] == binding["evidence"]["rollback_compatibility"], "rollback compatibility")
    port.recovery_preflight(p, approval)
    p["executor_fence"] = port.claim_recovery(p, approval)
    require(sha(p["executor_fence"]), "missing recovery executor fence")
    port.recovery_preflight(p, approval)
    unchanged(port, p)
    current = port.current(p)
    require(current in (p["old"], p["candidate"]), "ambiguous alias: no repair")
    port.journal(p, "ROLLBACK_INTENT")
    if current == p["candidate"]:
        port.promote(p, current, p["old"])
    port.journal(p, "ROLLBACK_POINTER_RESTORED")
    # Even old pointer may have a running candidate after a partial activation.
    port.restart_api(p)
    port.verify_api(p, p["old"])
    unchanged(port, p)
    port.complete(p, "CODE_ROLLED_BACK_DATA_UNCHANGED_BY_TOOL")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if args.execute:
        parser.exit(78, "NOT_ADOPTED: native adapter, bundle producer and operator bindings are not implemented/qualified in this SOURCE prototype.\n")
    with open(args.binding, encoding="utf-8") as stream:
        result = plan(json.load(stream))
    require(result["binding"]["candidate_commit"] == args.expected_commit, "receiving head mismatch")
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
