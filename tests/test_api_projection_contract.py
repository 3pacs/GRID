"""Real Git link metadata and adversarial pure projection/fencing controls."""
import copy
import importlib.util
import json
from pathlib import Path
import unittest


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HERE = Path(__file__).resolve().parent
contract = load(HERE.parent / "scripts/api_bundle_contract.py", "projection_contract")
models = load(HERE / "test_api_isolation_prototype.py", "fencing_model")
bundle_cases = load(HERE / "test_api_bundle_contract.py", "bundle_cases")
REAL = json.loads((HERE / "fixtures/api_isolation_git_provenance.json").read_text(encoding="utf-8"))


def real_projection():
    return {"schema": "grid.api-projection.v1", "tree": REAL["tree"],
            "entries": {name: copy.deepcopy(REAL["link_decisions"].get(name, {"action": "copy"})) for name in REAL["provenance"]["entries"]}}


class ProjectionTests(unittest.TestCase):
    def test_actual_1d81_full_provenance_and_runtime_projection_representable(self):
        self.assertEqual(REAL["tree"], "1d81d2c416c207ad1c7f5c5275e24e08a02d92e1")
        provenance = REAL["provenance"]
        self.assertEqual(len(provenance["entries"]), 4734)
        self.assertEqual(len(provenance["link_targets"]), 35)
        result = contract.verify_projection(provenance, real_projection(), REAL["tree"])
        self.assertEqual(len(result["files"]), 4706)
        self.assertEqual(len(result["omitted"]), 28)
        self.assertFalse(result["native_or_behavior_qualified"])
        for name, choice in REAL["link_decisions"].items():
            with self.subTest(path=name):
                self.assertEqual(provenance["entries"][name][0], "120000")
                self.assertEqual(contract.git_blob(provenance["link_targets"][name].encode()), provenance["entries"][name][1])
                if choice["action"] == "materialize":
                    self.assertEqual(result["files"][name], tuple(provenance["entries"][choice["target"]]))
                    self.assertIn(result["files"][name][0], ("100644", "100755"))
                else:
                    self.assertNotIn(name, result["files"])
                    self.assertTrue(provenance["link_targets"][name].startswith("/"))

    def test_canonical_links_cannot_be_dropped_retyped_or_retargeted(self):
        name = "models/Hermes-3-Llama-3.1-8B.Q4_K_M.gguf"
        for change in ("drop", "retype", "retarget", "missing_target"):
            with self.subTest(change=change):
                p = copy.deepcopy(REAL["provenance"])
                if change == "drop":
                    del p["entries"][name]
                elif change == "retype":
                    p["entries"][name][0] = "100644"
                elif change == "retarget":
                    p["link_targets"][name] = "/different/model"
                else:
                    del p["link_targets"][name]
                with self.assertRaises(contract.Refusal):
                    contract.verify_provenance(p, REAL["tree"])

    def test_actual_external_links_never_materialized_or_followed(self):
        for name, decision in REAL["link_decisions"].items():
            if decision["action"] != "omit":
                continue
            with self.subTest(path=name):
                projection = real_projection()
                projection["entries"][name] = {"action": "materialize", "target": REAL["provenance"]["link_targets"][name]}
                with self.assertRaisesRegex(contract.Refusal, "external"):
                    contract.verify_projection(REAL["provenance"], projection, REAL["tree"])

    def test_link_escape_chain_and_nonregular_source_are_refused(self):
        for target in ("../../outside", "/absolute", "C:\\outside", "next", "directory"):
            with self.subTest(target=target):
                entries = {"a/link": ["120000", contract.git_blob(target.encode())],
                           "a/next": ["120000", contract.git_blob(b"target")],
                           "a/directory/file": ["100644", contract.git_blob(b"file")],
                           "a/target": ["100644", contract.git_blob(b"target")]}
                provenance = {"entries": entries, "link_targets": {"a/link": target, "a/next": "target"}}
                tree = contract.tree_hash(entries)
                decisions = {name: {"action": "copy"} for name in entries}
                decisions["a/link"] = {"action": "materialize", "target": "a/" + target}
                decisions["a/next"] = {"action": "omit", "reason": "test", "behavior": "unavailable-until-projection-qualification"}
                with self.assertRaises(contract.Refusal):
                    contract.verify_projection(provenance, {"schema": "grid.api-projection.v1", "tree": tree, "entries": decisions}, tree)
        p = {"entries": {"device": ["160000", "a" * 40]}, "link_targets": {}}
        with self.assertRaisesRegex(contract.Refusal, "unsupported"):
            contract.verify_provenance(p, contract.tree_hash(p["entries"]))

    def test_projection_coverage_and_explicit_omission_behavior_are_required(self):
        for change in ("missing", "copy_link", "behavior", "substitute"):
            with self.subTest(change=change):
                p = real_projection()
                link = "astrogrid/src/lib/contract.js"
                if change == "missing":
                    del p["entries"][link]
                elif change == "copy_link":
                    p["entries"][link] = {"action": "copy"}
                elif change == "behavior":
                    p["entries"]["outputs/analyst_reports"]["behavior"] = "assume-equivalent"
                else:
                    p["entries"][link]["target"] = "astrogrid_shared/endpoints.js"
                with self.assertRaises(contract.Refusal):
                    contract.verify_projection(REAL["provenance"], p, REAL["tree"])

    def test_bundle_materializes_regular_bytes_but_retains_original_link_provenance(self):
        m, observations, expected = bundle_cases.fixture()
        # Same real relative link text as one accepted AstroGrid link; its target
        # content is synthetic. Real full tree/35 blobs are checked independently.
        name, target = "astrogrid/src/lib/contract.js", "astrogrid_shared/contract.js"
        text = REAL["provenance"]["link_targets"][name]
        data = b"synthetic target bytes"
        provenance = m["provenance"]
        provenance["entries"][name] = ["120000", contract.git_blob(text.encode())]
        provenance["entries"][target] = ["100644", contract.git_blob(data)]
        provenance["link_targets"][name] = text
        m["tree"] = expected["tree"] = contract.tree_hash(provenance["entries"])
        m["projection"] = {"schema": "grid.api-projection.v1", "tree": m["tree"], "entries": {"api/main.py": {"action": "copy"}, target: {"action": "copy"}, name: {"action": "materialize", "target": target}}}
        for inode, path in enumerate((name, target), 200):
            key = "source/" + path
            m["files"][key] = {"size": len(data), "sha256": __import__("hashlib").sha256(data).hexdigest(), "mode": "100644"}
            observations[key] = {"kind": "regular", "nlink": 1, "uid": 2001, "device": 9, "inode": inode, "content": data, "mode": "100644"}
        expected.update(provenance_sha256=contract.json_hash(provenance), projection_sha256=contract.json_hash(m["projection"]), inventory_sha256=contract.json_hash(m["files"]))
        self.assertEqual(contract.verify_bundle(m, observations, expected, set())["files"], 7)
        self.assertEqual(provenance["entries"][name][0], "120000")
        for bad_kind in ("symlink", "fifo", "directory"):
            with self.subTest(kind=bad_kind):
                altered = copy.deepcopy(observations)
                altered["source/" + name]["kind"] = bad_kind
                with self.assertRaises(contract.Refusal):
                    contract.verify_bundle(m, altered, expected, set())
        changed = copy.deepcopy(m)
        changed["projection"]["entries"][name] = {"action": "omit", "reason": "changed", "behavior": "unavailable-until-projection-qualification"}
        with self.assertRaisesRegex(contract.Refusal, "unbound runtime projection"):
            contract.verify_bundle(changed, observations, expected, set())


class FencingTests(unittest.TestCase):
    def failed(self):
        s = models.Simulation()
        s.failure = "promote_after"
        with self.assertRaises(RuntimeError):
            models.core.deploy(s.b, s)
        s.failure = None
        return s

    def test_recovery_cannot_use_pid_observation_to_override_active_executor(self):
        s = self.failed()
        before = copy.deepcopy(s.fs)
        with self.assertRaisesRegex(models.core.Refusal, "executor still"):
            models.core.recover(s.b, s, s.approval())
        self.assertEqual(s.fs, before)

    def test_one_recovery_claim_fences_competitor_and_old_mutator(self):
        s = self.failed()
        old = models.core.plan(s.b)
        old["executor_fence"] = s.executor_fence
        s.crash_executor()
        approval = s.approval()
        new = models.core.plan(s.b)
        new["executor_fence"] = s.claim_recovery(new, approval)
        self.assertNotEqual(new["executor_fence"], old["executor_fence"])
        before = copy.deepcopy(s.fs)
        with self.assertRaises(models.core.Refusal):
            s.claim_recovery(models.core.plan(s.b), approval)
        with self.assertRaisesRegex(models.core.Refusal, "stale"):
            s.promote(old, old["candidate"], old["old"])
        self.assertEqual(s.fs, before)

    def test_change_between_observation_and_atomic_claim_refuses(self):
        for changed in ("journal", "lock"):
            with self.subTest(changed=changed):
                s = self.failed()
                s.crash_executor()
                approval = s.approval()
                plan = models.core.plan(s.b)
                s.recovery_preflight(plan, approval)
                s.fs[changed] = "changed-after-observation"
                before = copy.deepcopy(s.fs)
                with self.assertRaises(models.core.Refusal):
                    s.claim_recovery(plan, approval)
                self.assertEqual(s.fs, before)
                self.assertIsNone(s.executor_fence)

    def test_recovery_claim_consumes_old_lock_record_even_before_journal_write(self):
        s = self.failed()
        s.crash_executor()
        approval = s.approval()
        plan = models.core.plan(s.b)
        s.claim_recovery(plan, approval)
        s.crash_executor()
        before = copy.deepcopy(s.fs)
        with self.assertRaisesRegex(models.core.Refusal, "lock record changed"):
            s.claim_recovery(plan, approval)
        self.assertEqual(s.fs, before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
