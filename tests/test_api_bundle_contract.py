"""Adversarial pure-data tests. No temporary fixtures, OS links or services."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


contract = load("api_bundle_contract")
builder = load("deploy_api_bundle")


def fixture():
    contents = {"source/api/main.py": b"safe source\n", "runtime/bin/python3.10": b"SYNTHETIC interpreter",
                "runtime/lib/python3.10/os.py": b"synthetic stdlib", "runtime/lib/python3.10/encodings/__init__.py": b"synthetic encodings",
                "runtime/lib/python3.10/site-packages/uvicorn/__init__.py": b"synthetic uvicorn"}
    files, observations = {}, {}
    for inode, (name, content) in enumerate(contents.items(), 100):
        mode = "100755" if name == "runtime/bin/python3.10" else "100644"
        files[name] = {"sha256": hashlib.sha256(content).hexdigest(), "size": len(content), "mode": mode, "tracked": name.startswith("source/")}
        observations[name] = {"kind": "regular", "nlink": 1, "uid": 2001, "device": 9, "inode": inode, "content": content, "mode": mode}
    manifest = {"schema": "grid.api-bundle.v2", "commit": "b" * 40,
                # Fixed Git object vector for tracked api/main.py = b'safe source\n'.
                "tree": "0bd3a684507afb98fc49ee19fa4f64b19269dfe5", "python": "3.10", "platform": "linux-x86_64",
                "lock_sha256": "c" * 64, "runtime_archive_sha256": "d" * 64, "native_receipt_sha256": "e" * 64, "files": files}
    manifest["provenance"] = {"entries": {"api/main.py": ["100644", contract.git_blob(b"safe source\n")]}, "link_targets": {}}
    manifest["projection"] = {"schema": "grid.api-projection.v1", "tree": manifest["tree"], "entries": {"api/main.py": {"action": "copy"}}}
    expected = {k: manifest[k] for k in ("commit", "tree", "lock_sha256", "runtime_archive_sha256", "native_receipt_sha256")}
    expected.update(bundle_uid=2001, interpreter_sha256=files["runtime/bin/python3.10"]["sha256"],
                    provenance_sha256=contract.json_hash(manifest["provenance"]), projection_sha256=contract.json_hash(manifest["projection"]),
                    inventory_sha256=hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                    required_runtime_files=[p for p in files if p.startswith("runtime/")])
    return manifest, observations, expected


class BundleTests(unittest.TestCase):
    def test_valid_synthetic_inventory_and_fixed_git_tree(self):
        m, o, e = fixture()
        result = contract.verify_bundle(m, o, e, set())
        self.assertEqual(result["tree"], "0bd3a684507afb98fc49ee19fa4f64b19269dfe5")
        self.assertEqual(result["files"], 5)

    def test_partial_extra_or_replaced_payload_refused(self):
        for mutation in ("missing", "extra", "truncated", "replacement", "mode", "owner", "symlink", "hardlink", "shared_inode"):
            with self.subTest(mutation=mutation):
                m, o, e = fixture()
                key = "runtime/lib/python3.10/os.py"
                protected = set()
                if mutation == "missing":
                    del o[key]
                elif mutation == "extra":
                    o["runtime/foreign.py"] = dict(o[key])
                elif mutation == "truncated":
                    o[key]["content"] = b"x"
                elif mutation == "replacement":
                    o[key]["content"] = b"x" * len(o[key]["content"])
                elif mutation == "mode":
                    o[key]["mode"] = "104755"
                elif mutation == "owner":
                    o[key]["uid"] = 1000
                elif mutation == "symlink":
                    o[key]["kind"] = "symlink"
                elif mutation == "hardlink":
                    o[key]["nlink"] = 2
                else:
                    protected.add((o[key]["device"], o[key]["inode"]))
                before = copy.deepcopy((m, o, e))
                with self.assertRaises(contract.Refusal):
                    contract.verify_bundle(m, o, e, protected)
                self.assertEqual((m, o, e), before)

    def test_self_consistent_wrong_source_cannot_match_expected_tree(self):
        m, o, e = fixture()
        o["source/api/main.py"]["content"] = b"evil source\n"
        m["files"]["source/api/main.py"]["sha256"] = hashlib.sha256(b"evil source\n").hexdigest()
        e["inventory_sha256"] = hashlib.sha256(json.dumps(m["files"], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        with self.assertRaisesRegex(contract.Refusal, "source"):
            contract.verify_bundle(m, o, e, set())

    def test_manifest_cannot_omit_qualified_runtime_member(self):
        m, o, e = fixture()
        del m["files"]["runtime/lib/python3.10/os.py"]
        del o["runtime/lib/python3.10/os.py"]
        with self.assertRaisesRegex(contract.Refusal, "inventory"):
            contract.verify_bundle(m, o, e, set())

    def test_pth_and_untracked_python_shadow_are_rejected_even_if_hashed(self):
        for key in ("runtime/lib/python3.10/site-packages/evil.pth", "source/api/extra.py", "source/pwa_dist/__init__.py"):
            with self.subTest(key=key):
                m, o, e = fixture()
                sample = "runtime/lib/python3.10/os.py"
                m["files"][key] = dict(m["files"][sample])
                o[key] = dict(o[sample], inode=999)
                e["inventory_sha256"] = hashlib.sha256(json.dumps(m["files"], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                with self.assertRaises(contract.Refusal):
                    contract.verify_bundle(m, o, e, set())

    def test_wrong_alias_and_absolute_import_leaks_refused(self):
        commit = "b" * 40
        root = "/data/grid_v4/api-isolated/releases/" + commit
        origins = {"api.main": root + "/source/api/main.py", "sys": "built-in"}
        good = contract.launch_plan(root, commit, {}, origins)
        self.assertEqual(good["source_alias_follow_count"], 1)
        self.assertEqual(good["cwd"], root + "/source")
        self.assertEqual(good["interpreter"], root + "/runtime/bin/python3.10")
        screened = contract.launch_plan(root, commit, {"DB_PASSWORD": "synthetic-do-not-report"}, origins)
        self.assertEqual(screened["environment_keys"], ["DB_PASSWORD"])
        self.assertNotIn("synthetic-do-not-report", str(screened))
        for leak in ("/home/grid/.local/lib/python3.10/site-packages/x.py", "/data/grid_v4/grid_release/api/main.py", root + "/source/../escape.py"):
            with self.subTest(leak=leak), self.assertRaises(contract.Refusal):
                contract.launch_plan(root, commit, {}, {"leaked": leak})
        with self.assertRaises(contract.Refusal):
            contract.launch_plan("/data/grid_v4/grid_release", commit, {}, origins)
        for key in ("PYTHONPATH", "PYTHONHOME", "LD_LIBRARY_PATH", "VIRTUAL_ENV", "PATH"):
            with self.subTest(environment=key), self.assertRaises(contract.Refusal):
                contract.launch_plan(root, commit, {key: "legacy"}, origins)

    def test_builder_refuses_unpinned_or_shared_inputs(self):
        p = {"schema": "grid.api-builder.v2", "source_tree": "b" * 40, "python": "3.10", "network": "none", "production_mounts": [], "credential_mounts": [], "wheels": {"example.whl": "a" * 64}}
        fields = ("builder_image_sha256", "runtime_archive_sha256", "interpreter_sha256", "requirements_lock_sha256", "node_archive_sha256", "package_lock_sha256", "projection_sha256")
        p.update({k: "a" * 64 for k in fields})
        recipe = builder.build_plan(p, "b" * 40)
        self.assertFalse(recipe["implemented_executor"])
        self.assertEqual(recipe["target_installs"], [])
        for key, value in [("network", "host"), ("production_mounts", ["/home/grid"]), ("credential_mounts", ["credential"]), ("python", "3.11"), ("wheels", {})] + [(k, "UNKNOWN") for k in fields]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                builder.build_plan(dict(p, **{key: value}), "b" * 40)

    def test_rollback_preserves_data_and_retains_both_releases(self):
        b = {"candidate": "new", "old": "old", "plan_sha256": "a" * 64}
        r = {"expected_current": "new", "old_target": "old", "plan_sha256": "a" * 64}
        r.update({k: "b" * 64 for k in ("old_bundle_manifest", "new_bundle_manifest", "compatibility_receipt", "journal_sha256", "owner_receipt")})
        plan = contract.rollback_plan(b, r)
        self.assertEqual(plan["retain"], ["old", "new"])
        self.assertFalse(plan["undo_database"])
        self.assertEqual(plan["cleanup"], "none")
        for key in ("expected_current", "old_target", "plan_sha256", "compatibility_receipt"):
            with self.subTest(key=key), self.assertRaises(contract.Refusal):
                contract.rollback_plan(b, dict(r, **{key: "UNKNOWN"}))

    def test_all_execution_interfaces_refuse_before_opening_bindings(self):
        cases = [("deploy_api_isolated", ["--binding", "NEVER_OPEN", "--expected-commit", "b" * 40, "--execute"]),
                 ("deploy_api_bundle", ["--expected-commit", "b" * 40, "--execute"]),
                 ("deploy_api_launcher", ["--execute"]),
                 ("deploy_api_rollback", ["--binding", "NEVER_OPEN", "--approval", "NEVER_OPEN", "--execute"])]
        for name, args in cases:
            with self.subTest(name=name):
                module = load(name)
                with patch("sys.argv", [name] + args), patch("builtins.open", side_effect=AssertionError("binding read before guard")), self.assertRaises(SystemExit) as caught:
                    module.main()
                self.assertEqual(caught.exception.code, 78)


if __name__ == "__main__":
    unittest.main(verbosity=2)
