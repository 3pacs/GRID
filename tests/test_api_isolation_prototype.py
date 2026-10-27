"""Pure in-memory simulation: NO POSIX/filesystem/native/service qualification."""
import copy
import importlib.util
from pathlib import Path
import unittest


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).resolve().parents[1] / "scripts" / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


core = load("deploy_api_isolated")
graph = load("migration_graph_offline")


class Simulation:
    """Fake files and fake commands, with an explicitly modeled atomic point."""
    def __init__(self):
        self.held = {"pid": 556685, "start": "SIMULATED", "unit": "held", "dropin": "held", "cwd": "old", "source": "old-bytes", "alias": "old", "python": "old-bytes", "packages": "old-bytes"}
        self.b = {"schema": "grid.api-isolation.v1", "root": core.ROOT,
                  "host": "grid-svr", "service": "grid-api.service", "python": "3.10",
                  "old_commit": "a" * 40, "candidate_commit": "b" * 40, "candidate_tree": "c" * 40,
                  "uid": 2001, "hermes_uid": 1000, "current": core.ROOT + "/current",
                  "old_release": core.ROOT + "/releases/" + "a" * 40,
                  "bundle_root": core.ROOT + "/inbox/" + "b" * 40,
                  "bundle_manifest": core.digest("new-bundle"), "protected_roots": list(core.LEGACY),
                  "installed_heads": ["reviewed-head"], "candidate_heads": ["reviewed-head"],
                  "protected_snapshot": self.protected(None),
                  "evidence": {k: core.digest("SIMULATION:" + k) for k in core.EVIDENCE}}
        self.fs = {"current": self.b["old_release"], "old": "old-code", "bundle": "new-bundle"}
        self.nodes = {"root": ("dir", 2001, 1), "ancestor": ("dir", 0, 1), "bundle": ("file", 2001, 1), "current": ("symlink", 2001, 1)}
        self.normal_nodes = dict(self.nodes)
        self.events, self.commands = [], []
        self.staged, self.failure = False, None
        self.executor_fence, self.epoch = None, 0

    def crash_executor(self):
        # Models OS releasing the held executor lock after process termination.
        # The durable journal/ownership record survives. This is NOT a PID probe.
        self.executor_fence = None

    def locked(self, p):
        core.require(self.executor_fence is not None and p.get("executor_fence") == self.executor_fence, "stale executor fence")

    def fail(self, point):
        if point == self.failure:
            raise RuntimeError("injected:" + point)

    def protected(self, p):
        return core.digest(self.held)

    def preflight(self, p):
        self.fail("preflight")
        core.require(self.nodes == self.normal_nodes, "link/owner/topology collision")
        core.require(self.current(p) == p["old"], "old alias mismatch")
        core.require("candidate" not in self.fs or self.staged, "unowned candidate")
        core.require(core.digest(self.fs["bundle"]) == p["binding"]["bundle_manifest"], "bundle drift")

    def claim(self, p):
        core.require(self.executor_fence is None, "executor still holds lock")
        core.require("lock" not in self.fs, "lock exists: no steal/expiry")
        self.fs["lock"] = p["plan_sha256"]
        self.events.append("CLAIM")
        self.epoch += 1
        self.executor_fence = core.digest({"plan": p["plan_sha256"], "epoch": self.epoch})
        self.fs["lock"] = {"plan": p["plan_sha256"], "epoch": self.epoch, "fence": self.executor_fence}
        self.fail("claim_after")
        return self.executor_fence

    def claim_recovery(self, p, approval):
        # Modeled compare-and-acquire atomic point, not native lock proof.
        core.require(self.executor_fence is None, "executor still holds lock")
        core.require(core.digest(self.fs.get("lock")) == approval["lock_record_sha256"], "lock record changed")
        core.require(core.digest(self.fs.get("journal")) == approval["journal_sha256"], "journal changed before atomic claim")
        self.epoch += 1
        self.executor_fence = core.digest({"plan": p["plan_sha256"], "epoch": self.epoch})
        self.fs["lock"] = {"plan": p["plan_sha256"], "epoch": self.epoch, "fence": self.executor_fence}
        return self.executor_fence

    def stage(self, p):
        self.locked(p)
        self.fail("stage_before")
        core.require("candidate" not in self.fs, "candidate collision")
        self.fs["candidate"] = self.fs["bundle"]
        self.staged = True
        self.events.append("STAGE")
        self.fail("stage_after")

    def journal(self, p, state):
        self.locked(p)
        self.fs["journal"] = {"plan": p["plan_sha256"], "state": state}
        self.events.append(state)
        self.fail("journal_" + state)

    def current(self, p):
        return self.fs["current"]

    def promote(self, p, expected, target):
        self.locked(p)
        core.require(self.current(p) == expected, "compare guard")
        self.fail("promote_before")
        self.fs["current"] = target
        self.events.append("PROMOTE")
        self.fail("promote_after")

    def restart_api(self, p):
        self.locked(p)
        self.fail("restart_before")
        self.commands.append(("systemctl", "restart", "grid-api.service"))
        self.fs["process"] = self.current(p)
        self.fail("restart_after")

    def verify_api(self, p, target):
        self.fail("verify")
        core.require(self.fs["process"] == target == self.current(p), "API target mismatch")

    def complete(self, p, state):
        self.locked(p)
        self.journal(p, state)
        self.fail("complete")
        del self.fs["lock"]
        self.executor_fence = None

    def approval(self):
        return {"plan_sha256": core.plan(self.b)["plan_sha256"], "journal_sha256": core.digest(self.fs["journal"]),
                "lock_record_sha256": core.digest(self.fs.get("lock")),
                "owner_receipt": core.digest("SIMULATION approval"), "compatibility_receipt": self.b["evidence"]["rollback_compatibility"]}

    def recovery_preflight(self, p, a):
        core.require(self.nodes == self.normal_nodes, "recovery topology")
        record = self.fs.get("lock")
        core.require(isinstance(record, dict) and record.get("plan") == p["plan_sha256"], "unowned recovery lock")
        core.require(core.digest(self.fs["journal"]) == a["journal_sha256"], "stale journal")


class IsolationTests(unittest.TestCase):
    def setUp(self):
        self.s = Simulation()
        self.held = self.s.protected(None)

    def tearDown(self):
        self.assertEqual(self.s.protected(None), self.held)
        self.assertTrue(all(c == ("systemctl", "restart", "grid-api.service") for c in self.s.commands))

    def test_success_only_changes_api(self):
        p = core.deploy(self.s.b, self.s)
        self.assertEqual(self.s.current(p), p["candidate"])
        self.assertEqual(self.s.fs["old"], "old-code")
        self.assertEqual(len(self.s.commands), 1)
        self.assertLess(self.s.events.index("PROMOTION_INTENT"), self.s.events.index("PROMOTE"))
        self.assertNotIn("lock", self.s.fs)

    def test_paused_enabled_hermes_remains_paused_enabled(self):
        self.s.held.update(pid=0, start=None, state="paused", enabled=True)
        self.s.b["protected_snapshot"] = self.s.protected(None)
        self.held = self.s.protected(None)
        core.deploy(self.s.b, self.s)
        self.assertEqual(self.s.held["pid"], 0)
        self.assertEqual(self.s.held["state"], "paused")
        self.assertTrue(self.s.held["enabled"])
        self.assertEqual(self.s.commands, [("systemctl", "restart", "grid-api.service")])

    def test_protected_child_does_not_cover_whole_legacy_directory(self):
        self.s.b["protected_roots"] = [p if p != "/usr/bin" else "/usr/bin/only-one-child" for p in self.s.b["protected_roots"]]
        with self.assertRaisesRegex(core.Refusal, "coverage"):
            core.deploy(self.s.b, self.s)
        self.assertEqual(self.s.events, [])

    def test_bad_bindings_stop_before_mutation(self):
        cases = [("root", core.LEGACY[0]), ("uid", 1000), ("python", "3.11"), ("installed_heads", []),
                 ("installed_heads", ["pending"]), ("service", "grid-hermes.service"),
                 ("bundle_root", core.ROOT + "/inbox/../escape"), ("protected_roots", [core.ROOT])]
        for key, value in cases:
            with self.subTest(key=key, value=value):
                b = copy.deepcopy(self.s.b)
                b[key] = value
                with self.assertRaises(core.Refusal):
                    core.deploy(b, self.s)
                self.assertEqual(self.s.events, [])

    def test_missing_native_receipt(self):
        del self.s.b["evidence"]["native_python310"]
        with self.assertRaises(core.Refusal):
            core.deploy(self.s.b, self.s)
        self.assertEqual(self.s.events, [])

    def test_modeled_symlink_hardlink_owner_collisions(self):
        for key, node in [("root", ("symlink", 2001, 1)), ("ancestor", ("symlink", 0, 1)),
                          ("root", ("dir", 1000, 1)), ("bundle", ("symlink", 2001, 1)),
                          ("bundle", ("file", 2001, 2)), ("current", ("dir", 2001, 1))]:
            with self.subTest(key=key, node=node):
                s = Simulation()
                s.nodes[key] = node
                with self.assertRaises(core.Refusal):
                    core.deploy(s.b, s)
                self.assertEqual(s.events, [])
                self.assertEqual(s.protected(None), self.held)

    def test_unowned_candidate(self):
        self.s.fs["candidate"] = "foreign"
        with self.assertRaises(core.Refusal):
            core.deploy(self.s.b, self.s)
        self.assertEqual(self.s.events, [])
        self.assertEqual(self.s.fs["candidate"], "foreign")

    def test_changed_bundle(self):
        self.s.fs["bundle"] = "tampered"
        with self.assertRaises(core.Refusal):
            core.deploy(self.s.b, self.s)
        self.assertEqual(self.s.events, [])

    def test_existing_lock_never_stolen(self):
        self.s.fs["lock"] = "foreign"
        with self.assertRaises(core.Refusal):
            core.deploy(self.s.b, self.s)
        self.assertEqual(self.s.fs["lock"], "foreign")

    def failed_promotion(self):
        self.s.failure = "promote_after"
        with self.assertRaises(RuntimeError):
            core.deploy(self.s.b, self.s)
        self.s.failure = None
        self.s.crash_executor()

    def test_partial_promotion_explicit_rollback(self):
        self.failed_promotion()
        self.assertEqual(self.s.fs["journal"]["state"], "PROMOTION_INTENT")
        self.assertIn("lock", self.s.fs)
        core.recover(self.s.b, self.s, self.s.approval())
        self.assertEqual(self.s.current(None), self.s.b["old_release"])
        self.assertIn("candidate", self.s.fs)
        self.assertIn("DATA_UNCHANGED_BY_TOOL", self.s.fs["journal"]["state"])

    def test_wrong_recovery_approval(self):
        self.failed_promotion()
        for key in ["plan_sha256", "journal_sha256", "compatibility_receipt"]:
            with self.subTest(key=key):
                approval = self.s.approval()
                approval[key] = "f" * 64
                before = list(self.s.events)
                with self.assertRaises(core.Refusal):
                    core.recover(self.s.b, self.s, approval)
                self.assertEqual(self.s.events, before)

    def test_ambiguous_alias_never_repaired(self):
        self.failed_promotion()
        self.s.fs["current"] = "foreign"
        before = list(self.s.events)
        with self.assertRaises(core.Refusal):
            core.recover(self.s.b, self.s, self.s.approval())
        self.assertEqual(self.s.events, before)

    def test_failure_points_retain_evidence_and_held_state(self):
        for stage in ["preflight", "claim_after", "stage_before", "stage_after", "journal_PROMOTION_INTENT", "promote_before", "restart_before", "restart_after", "verify", "complete"]:
            with self.subTest(stage=stage):
                s = Simulation()
                s.failure = stage
                with self.assertRaises(RuntimeError):
                    core.deploy(s.b, s)
                self.assertEqual(s.protected(None), self.held)
                self.assertEqual(s.fs["old"], "old-code")
                if stage != "preflight":
                    self.assertIn("lock", s.fs)
                if s.staged:
                    self.assertIn("candidate", s.fs)

    def test_graph_never_executes_bodies_refuses_dynamic_missing_cycle(self):
        good = b"revision='a'\ndown_revision=None\ndef upgrade():\n    raise AssertionError('NEVER EXECUTE')\n"
        self.assertEqual(graph.graph_sources({"a.py": good})["heads"], ["a"])
        for source in [b"revision=make_revision()\ndown_revision=None\n", b"revision='a'\ndown_revision='missing'\n", b"revision='a'\ndown_revision='a'\n", b"revision='a'\nrevision='b'\ndown_revision=None\n", b"revision='a'\ndown_revision=None\nglobals().update(revision='x')\n"]:
            with self.subTest(source=source), self.assertRaises((ValueError, TypeError)):
                graph.graph_sources({"a.py": source})


if __name__ == "__main__":
    unittest.main(verbosity=2)
