"""Rules R1-R9 of the released-suite registry (evals/RELEASED.json) and the
shape of the evals-freeze-guard workflow. See evals/README.md.

Every rule test builds a BASE and a HEAD tree in temp directories (copies of
the real registry, guard files and evals/e0), mutates HEAD and runs the guard.
"""

from __future__ import annotations

import ast
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from evals import released
from evals.e0 import manifest as e0_manifest

REPO = Path(__file__).resolve().parents[1]
E0_V1_SHA = "75489d5091d82af64312f4523c41bdb52b0951a11e017644a022baa08a172822"
WORKFLOW = REPO / ".github" / "workflows" / "evals-freeze.yml"
REQUIRED_GUARD_FILES = {
    "tests/test_e0_manifest_guard.py",
    "tests/test_e0_benchmark.py",
    "tests/test_e2_manifest_guard.py",
    "tests/test_evals_released.py",
    "evals/released.py",
    "evals/README.md",
    "evals/__init__.py",
    ".github/workflows/evals-freeze.yml",
}
BINARY = {".npz", ".gz", ".parquet"}


# --------------------------------------------------------------------------- helpers

def _copy_tree(dst: Path) -> Path:
    """A minimal repo: the registry, every guarded file and every released suite."""
    entries = released.entries(REPO)
    rels = {"evals/RELEASED.json"}
    rels |= set(released.latest_by_key(entries)[released.GUARDS_KEY]["files"])
    for rel in sorted(rels):
        (dst / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO / rel, dst / rel)
    for entry in entries:
        if entry["kind"] == "suite" and not (dst / entry["path"]).exists():
            shutil.copytree(REPO / entry["path"], dst / entry["path"],
                            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache"))
    return dst


@pytest.fixture()
def trees(tmp_path):
    base = _copy_tree(tmp_path / "base")
    head = _copy_tree(tmp_path / "head")
    return base, head


def _doc(root: Path) -> dict:
    return json.loads((root / "evals" / "RELEASED.json").read_text(encoding="utf-8"))


def _save(root: Path, doc: dict) -> None:
    (root / "evals" / "RELEASED.json").write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")


def _append(root: Path, **entry) -> dict:
    doc = _doc(root)
    entry = {"seq": len(doc["entries"]), **entry}
    doc["entries"].append(entry)
    _save(root, doc)
    return entry


def _suite_entry(suite, version, path, sha, **extra):
    entry = {"kind": "suite", "suite": suite, "version": version, "path": path,
             "manifest_sha256": sha, "released_in": "test", "approved_by": "test",
             "note": "test entry"}
    entry.update(extra)
    return entry


def _lf(path: Path) -> str:
    return released.lf_sha256_bytes(path.read_bytes())


def _make_plain_suite(root: Path, rel: str) -> str:
    """A headerless (E1-style) suite at ``root/rel``; returns its manifest's LF sha256."""
    suite = root / rel
    suite.mkdir(parents=True)
    (suite / "__init__.py").write_text('"""toy suite"""\n', encoding="utf-8")
    (suite / "check.py").write_text("VALUE = 1\n", encoding="utf-8")
    lines = [f"{_lf(suite / name)}  {name}\n" for name in ("__init__.py", "check.py")]
    (suite / "MANIFEST.sha256").write_bytes("".join(lines).encode())
    return _lf(suite / "MANIFEST.sha256")


def _make_e0_sibling(root: Path, rel: str, version: str) -> str:
    """A copy of evals/e0 at ``rel`` re-released as ``version``; returns its manifest sha."""
    dst = root / rel
    shutil.copytree(root / "evals" / "e0", dst)
    config = dst / "config.json"
    config.write_text(config.read_text(encoding="utf-8").replace('"e0-v1"', f'"{version}"'),
                      encoding="utf-8")
    e0_manifest.write(version, root=dst)
    return _lf(dst / "MANIFEST.sha256")


def _fails(base, head, *needles):
    failures = released.check(base, head)
    text = "\n".join(failures)
    assert failures, "guard passed but should have failed"
    for needle in needles:
        assert needle in text, f"{needle!r} not in:\n{text}"
    return failures


def _passes(base, head):
    failures = released.check(base, head)
    assert failures == [], "\n".join(failures)


# --------------------------------------------------------------------------- the real registry

def test_real_registry_passes_against_itself():
    _passes(REPO, REPO)


def test_e0_v1_is_entry_zero_with_its_released_hash():
    first = released.entries(REPO)[0]
    assert first["kind"] == "suite" and first["seq"] == 0
    assert (first["suite"], first["version"], first["path"]) == ("e0", "e0-v1", "evals/e0")
    assert first["manifest_sha256"] == E0_V1_SHA
    assert released.latest_entry("e0", path="evals/e0")["manifest_sha256"] == E0_V1_SHA
    assert e0_manifest.manifest_sha256() == E0_V1_SHA


def test_every_suite_on_main_is_enrolled_at_its_package_version():
    from evals import e1, e2, e3
    from evals.e0 import VERSION as E0_VERSION

    assert released.latest_entry("e0", path="evals/e0")["version"] == E0_VERSION
    # E1's manifest has no version header, so its version is checked here.
    e1_entry = released.latest_entry("e1", path="evals/e1")
    assert e1_entry["version"] == e1.SUITE_VERSION, (
        "evals.e1.SUITE_VERSION and the latest released e1 entry differ: a re-pin of "
        "evals/e1/MANIFEST.sha256 appends a NEW e1 entry with the new version")
    assert e1_entry["manifest_sha256"] == _lf(REPO / "evals" / "e1" / "MANIFEST.sha256")
    assert released.latest_entry("e2", path="evals/e2")["version"] == e2.VERSION
    assert released.latest_entry("e3", path="evals/e3")["version"] == e3.VERSION


def test_guards_entry_pins_the_guard_files():
    guards = released.latest_by_key(released.entries(REPO))[released.GUARDS_KEY]
    assert REQUIRED_GUARD_FILES <= set(guards["files"])
    for rel, digest in guards["files"].items():
        assert _lf(REPO / rel) == digest, f"{rel} differs from {guards['version']}"


def test_released_py_is_stdlib_only():
    tree = ast.parse((REPO / "evals" / "released.py").read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, "released.py must not use relative imports"
            names.add(node.module.split(".")[0])
    assert names <= set(sys.stdlib_module_names) | {"__future__"}, names


# --------------------------------------------------------------------------- acceptance cases 1a-1l

def test_a_appending_a_valid_entry_passes(trees):
    base, head = trees
    _passes(base, head)
    sha = _make_plain_suite(head, "evals/e9")
    _append(head, **_suite_entry("e9", "e9-v1", "evals/e9", sha))
    _passes(base, head)


def test_b_editing_a_released_hash_fails(trees):
    base, head = trees
    doc = _doc(head)
    doc["entries"][0]["manifest_sha256"] = "0" * 64
    _save(head, doc)
    _fails(base, head, "released entry 0 changed")


def test_c_deleting_entry_zero_fails(trees):
    base, head = trees
    doc = _doc(head)
    del doc["entries"][0]
    for i, entry in enumerate(doc["entries"]):
        entry["seq"] = i
    _save(head, doc)
    _fails(base, head, "R1: released entry 0 changed/deleted")


def test_d_reordering_entries_fails(trees):
    base, head = trees
    doc = _doc(head)
    doc["entries"][0], doc["entries"][1] = doc["entries"][1], doc["entries"][0]
    doc["entries"][0]["seq"], doc["entries"][1]["seq"] = 0, 1
    _save(head, doc)
    _fails(base, head, "released entry 0 changed", "released entry 1 changed")


def test_e_changing_an_e0_file_without_a_release_fails(trees):
    base, head = trees
    config = head / "evals" / "e0" / "config.json"
    data = bytearray(config.read_bytes())
    data[data.index(b"2")] = ord("3")
    config.write_bytes(bytes(data))
    _fails(base, head, "R4:", "changed: evals/e0/config.json")
    # Re-pinning the manifest without a released entry is caught by R3.
    e0_manifest.write("e0-v1", root=head / "evals" / "e0")
    failures = _fails(base, head, "e0 manifest is not a released version")
    assert not any(f.startswith("R4:") for f in failures)


def test_f_editing_a_guard_file_needs_a_guards_entry(trees):
    base, head = trees
    guard = head / "tests" / "test_e0_manifest_guard.py"
    guard.write_text(guard.read_text(encoding="utf-8") + "\n# weakened\n", encoding="utf-8")
    _fails(base, head, "R5: guard file tests/test_e0_manifest_guard.py changed")
    files = dict(released.latest_by_key(released.entries(head))[released.GUARDS_KEY]["files"])
    files["tests/test_e0_manifest_guard.py"] = _lf(guard)
    _append(head, kind="guards", version="guards-v99", files=files)
    _passes(base, head)


def test_f_a_guards_entry_cannot_unpin_a_guard_file(trees):
    base, head = trees
    files = dict(released.latest_by_key(released.entries(head))[released.GUARDS_KEY]["files"])
    del files["tests/test_e0_benchmark.py"]
    _append(head, kind="guards", version="guards-v99", files=files)
    _fails(base, head, "guards-v99 unpins guard file tests/test_e0_benchmark.py")


def test_f_the_guard_and_workflow_are_always_pinned(trees):
    base, head = trees
    (base / "evals" / "RELEASED.json").unlink()  # even with no base guards entry
    files = dict(released.latest_by_key(released.entries(head))[released.GUARDS_KEY]["files"])
    del files["evals/released.py"]
    doc = _doc(head)
    latest = max(i for i, e in enumerate(doc["entries"]) if e.get("kind") == "guards")
    doc["entries"][latest]["files"] = files
    _save(head, doc)
    _fails(base, head, "unpins guard file evals/released.py")


def test_f_deleting_a_guard_file_fails(trees):
    base, head = trees
    (head / "evals" / "released.py").unlink()
    _fails(base, head, "R5: guard file evals/released.py is missing")


def test_g_crlf_checkout_still_passes(trees):
    base, head = trees
    for root in (base, head):
        for path in root.rglob("*"):
            if path.is_file() and path.suffix.lower() not in BINARY:
                data = path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")
                path.write_bytes(data)
    _passes(base, head)


def test_h_duplicate_suite_version_fails(trees):
    base, head = trees
    _append(head, **_suite_entry("e0", "e0-v1", "evals/e0", "1" * 64))
    _fails(base, head, "R6: duplicate release e0 e0-v1")


def test_i_unregistered_suite_fails(trees):
    base, head = trees
    _make_plain_suite(head, "evals/e9")
    _fails(base, head, "R7: unreleased suite evals/e9")


def test_j_guard_runs_stdlib_only_in_isolated_mode(trees, tmp_path):
    _, head = trees
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTHON")}
    env["PYTHONPATH"] = ""
    # -I ignores PYTHON* env vars and the script dir; -S drops site-packages,
    # so only the standard library is importable.
    cmd = [sys.executable, "-I", "-S", "base/evals/released.py", "check", "--base-dir", "base",
           "--head-dir", "head"]
    ok = subprocess.run(cmd, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120,
                        check=False)
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert "evals-freeze-guard: OK" in ok.stdout
    doc = _doc(head)
    doc["entries"][0]["note"] = "rewritten"
    _save(head, doc)
    bad = subprocess.run(cmd, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120,
                         check=False)
    assert bad.returncode == 1, bad.stdout + bad.stderr
    assert "released entry 0 changed" in bad.stdout


def test_k_deleting_a_released_suite_fails(trees):
    base, head = trees
    shutil.rmtree(head / "evals" / "e0")
    _fails(base, head, "R9: released suite e0 (evals/e0) is missing")


def test_l_two_live_paths_for_one_suite(trees):
    base, head = trees
    for root in (base, head):
        sha = _make_e0_sibling(root, "evals/e0v2", "e0-v2")
        _append(root, **_suite_entry("e0", "e0-v2", "evals/e0v2", sha))
    _passes(base, head)
    assert released.latest_entry("e0", path="evals/e0", root=head)["manifest_sha256"] == E0_V1_SHA
    assert released.latest_entry("e0", root=head)["version"] == "e0-v2"

    for target in ("evals/e0", "evals/e0v2"):
        scorer = head / target / "scorer.py"
        original = scorer.read_bytes()
        scorer.write_bytes(original + b"\n# edit\n")
        _fails(base, head, f"changed: {target}/scorer.py")
        scorer.write_bytes(original)
    _passes(base, head)


def test_l_releasing_a_sibling_version_in_a_pr_passes(trees):
    base, head = trees
    sha = _make_e0_sibling(head, "evals/e0v2", "e0-v2")
    _append(head, **_suite_entry("e0", "e0-v2", "evals/e0v2", sha))
    _passes(base, head)


# --------------------------------------------------------------------------- other rules

def test_re_releasing_a_path_with_a_new_entry_passes(trees):
    """A sanctioned re-pin (e.g. #767/#768 re-pinning evals/e1) is a NEW entry."""
    base, head = trees
    sha = _make_plain_suite(base, "evals/e9")
    _append(base, **_suite_entry("e9", "e9-v1", "evals/e9", sha))
    _make_plain_suite(head, "evals/e9")
    _append(head, **_suite_entry("e9", "e9-v1", "evals/e9", sha))
    _passes(base, head)
    (head / "evals" / "e9" / "check.py").write_text("VALUE = 2\n", encoding="utf-8")
    _fails(base, head, "changed: evals/e9/check.py")
    suite = head / "evals" / "e9"
    lines = [f"{_lf(suite / n)}  {n}\n" for n in ("__init__.py", "check.py")]
    (suite / "MANIFEST.sha256").write_bytes("".join(lines).encode())
    _fails(base, head, "e9 manifest is not a released version")
    _append(head, **_suite_entry("e9", "e9-v1.1", "evals/e9", _lf(suite / "MANIFEST.sha256")))
    _passes(base, head)


def test_r8_one_new_entry_per_path(trees):
    base, head = trees
    sha = _make_plain_suite(head, "evals/e9")
    _append(head, **_suite_entry("e9", "e9-v1", "evals/e9", sha))
    _append(head, **_suite_entry("e9", "e9-v2", "evals/e9", sha))
    _fails(base, head, "R8: 2 new entries for evals/e9")


def test_r2_seq_and_schema(trees):
    base, head = trees
    doc = _doc(head)
    doc["entries"][1]["seq"] = 5
    _save(head, doc)
    _fails(base, head, "seq must be contiguous from 0")
    doc["entries"][1]["seq"] = 1
    doc["schema"] = 2
    _save(head, doc)
    _fails(base, head, "schema must be 1")


def test_r2_unknown_fields_and_kinds(trees):
    base, head = trees
    _append(head, kind="hotfix", version="x")
    _fails(base, head, "unknown kind 'hotfix'")


def test_r2_duplicate_json_keys_fail(trees):
    base, head = trees
    path = head / "evals" / "RELEASED.json"
    text = path.read_text(encoding="utf-8")
    sha = '"manifest_sha256": "' + E0_V1_SHA + '"'
    path.write_text(text.replace(sha, sha + ', "manifest_sha256": "' + "0" * 64 + '"', 1),
                    encoding="utf-8")
    _fails(base, head, "duplicate key 'manifest_sha256'")


def test_r2_seq_must_be_a_plain_int(trees):
    base, head = trees
    sha = _make_plain_suite(head, "evals/e9")
    entry = _append(head, **_suite_entry("e9", "e9-v1", "evals/e9", sha))
    doc = _doc(head)
    doc["entries"][entry["seq"]]["seq"] = float(entry["seq"])
    _save(head, doc)
    _fails(base, head, "seq must be contiguous from 0")


def test_r2_suite_path_is_one_level_under_evals(trees):
    base, head = trees
    _append(head, **_suite_entry("e9", "e9-v1", "evals/e0/data", "1" * 64))
    _fails(base, head, "bad path 'evals/e0/data'")


def test_committed_bytecode_fails_in_a_fresh_checkout(trees):
    base, head = trees
    pyc = head / "evals" / "e0" / "__pycache__" / "scorer.cpython-310.pyc"
    pyc.parent.mkdir()
    pyc.write_bytes(b"not really bytecode")
    _passes(base, head)  # a developer working copy regenerates bytecode
    failures = released.check(base, head, fresh_checkout=True)
    assert any("committed bytecode not allowed: evals/e0/__pycache__/" in f for f in failures), failures
    shutil.rmtree(pyc.parent)
    (head / "tests" / "__pycache__").mkdir()
    failures = released.check(base, head, fresh_checkout=True)
    assert any("committed bytecode not allowed: tests/__pycache__/" in f for f in failures), failures
    shutil.rmtree(head / "tests" / "__pycache__")
    assert released.check(base, head, fresh_checkout=True) == []


def test_trailing_newline_in_a_version_is_rejected(trees):
    base, head = trees
    sha = _make_plain_suite(head, "evals/e9")
    _append(head, **_suite_entry("e9", "e9-v1\n", "evals/e9", sha))
    _fails(base, head, "bad version")


def test_shadowing_the_guard_module_fails(trees):
    base, head = trees
    (head / "evals" / "released").mkdir()
    (head / "evals" / "released" / "__init__.py").write_text("", encoding="utf-8")
    _fails(base, head, "evals/released would shadow the guard module")


def test_header_version_must_match_the_entry(trees):
    base, head = trees
    sha = _make_e0_sibling(head, "evals/e0v2", "e0-v2")
    _append(head, **_suite_entry("e0", "e0-v3", "evals/e0v2", sha))
    _fails(base, head, "header says e0-v2")


def test_a_path_cannot_change_suite(trees):
    base, head = trees
    _append(head, **_suite_entry("e5", "e5-v1", "evals/e0", E0_V1_SHA))
    _fails(base, head, "already holds suite e0")


def test_nested_manifest_is_reported(trees):
    base, head = trees
    (head / "evals" / "e0" / "data" / "MANIFEST.sha256").write_text("", encoding="utf-8")
    _fails(base, head, "unpinned: evals/e0/data/MANIFEST.sha256")


def test_symlinks_are_refused(trees):
    base, head = trees
    target = head / "evals" / "e0" / "scorer.py"
    target.unlink()
    try:
        os.symlink(base / "evals" / "e0" / "scorer.py", target)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable on this platform")
    _fails(base, head, "symlink not allowed: evals/e0/scorer.py")


def test_bootstrap_base_without_registry(trees):
    """The PR that creates the registry records history (several entries per path)."""
    base, head = trees
    (base / "evals" / "RELEASED.json").unlink()
    _passes(base, head)


def test_e1_style_repin_appends_a_new_entry(trees):
    """What an e1 release does (#768): re-pin evals/e1 and append the next e1 version; editing the released entry instead fails."""
    base, head = trees
    suite = head / "evals" / "e1"
    init = suite / "__init__.py"
    init.write_bytes(init.read_bytes() + b"\n# v1.2\n")
    _fails(base, head, "changed: evals/e1/__init__.py")
    lines = [f"{_lf(suite / rel)}  {rel}\n"
             for rel in released.walk_suite(head, "evals/e1", released.PLAIN_SKIP_DIRS)]
    (suite / "MANIFEST.sha256").write_bytes("".join(lines).encode())
    _fails(base, head, "e1 manifest is not a released version")
    new_sha = _lf(suite / "MANIFEST.sha256")
    doc = _doc(head)
    latest = max(i for i, e in enumerate(doc["entries"]) if e.get("path") == "evals/e1")
    doc["entries"][latest]["manifest_sha256"] = new_sha  # editing the released entry
    _save(head, doc)
    _fails(base, head, f"released entry {latest} changed")
    doc["entries"][latest]["manifest_sha256"] = _doc(base)["entries"][latest]["manifest_sha256"]
    _save(head, doc)
    # The next version after the latest released one; a literal ("e1-v1.2")
    # collides with the real e1-v1.2 entry once #768 lands it (R6).
    nxt = doc["entries"][latest]["version"] + "-next"
    _append(head, **_suite_entry("e1", nxt, "evals/e1", new_sha))
    _passes(base, head)


def test_missing_head_registry_fails(trees):
    base, head = trees
    (head / "evals" / "RELEASED.json").unlink()
    _fails(base, head, "RELEASED.json is missing")


def test_cli_lf_sha256_matches(tmp_path):
    path = tmp_path / "x.txt"
    path.write_bytes(b"a\r\nb\n")
    out = subprocess.run([sys.executable, "-I", str(REPO / "evals" / "released.py"), "lf-sha256",
                          str(path)], capture_output=True, text=True, timeout=60, check=False)
    assert out.returncode == 0
    assert out.stdout.split()[0] == released.lf_sha256_bytes(b"a\nb\n")


# --------------------------------------------------------------------------- workflow

def test_workflow_shape():
    text = WORKFLOW.read_text(encoding="utf-8")
    doc = yaml.safe_load(text)
    triggers = doc.get("on", doc.get(True))  # YAML 1.1 reads a bare `on` as True
    assert set(triggers) == {"pull_request_target", "push"}
    assert triggers["pull_request_target"]["branches"] == ["main"]
    assert triggers["push"]["branches"] == ["main"]
    assert doc["permissions"] == {"contents": "read"}

    assert list(doc["jobs"]) == ["evals-freeze-guard"]
    job = doc["jobs"]["evals-freeze-guard"]
    assert job["name"] == "evals-freeze-guard"
    assert "permissions" not in job or job["permissions"] == {"contents": "read"}

    assert len(job["steps"]) == 3
    checkouts = [s for s in job["steps"] if str(s.get("uses", "")).startswith("actions/checkout@")]
    assert len(checkouts) == 2 and job["steps"][:2] == checkouts
    for step in checkouts:
        assert step["with"]["persist-credentials"] is False
    base, head = checkouts
    assert base["with"]["path"] == "base"
    assert base["with"]["ref"] == "${{ github.event.pull_request.base.sha || github.event.before }}"
    assert head["with"]["path"] == "head"
    assert head["with"]["ref"] == "${{ github.event.pull_request.head.sha || github.sha }}"

    runs = [s["run"] for s in job["steps"] if "run" in s]
    assert len(runs) == 1
    tokens = runs[0].split()
    scripts = [t for t in tokens if t.endswith("released.py")]
    assert scripts and all(t.startswith("base/") for t in scripts), scripts
    assert "--head-dir" in tokens and tokens[tokens.index("--head-dir") + 1] == "head"
    assert "--base-dir" in tokens and tokens[tokens.index("--base-dir") + 1] == "base"
    assert "--fresh-checkout" in tokens
    assert "bootstrap: no base guard" in runs[0]

    lowered = text.lower()
    assert "pip" not in lowered
    assert "secrets." not in lowered
    assert "head/" not in runs[0].replace("--head-dir head", "")
