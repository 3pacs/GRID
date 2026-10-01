"""E2 is frozen and proposer-proof.

* Every file under ``evals/e2/`` matches ``MANIFEST.sha256``, and the manifest is
  the one released for its version (append-only map below): scoring code,
  adapters, rules and the cost model cannot change without a new E2 version.
* ``rules.json`` and the package agree on the version; every rule a stream
  adapter emits is registered; the cost model covers every class the rules use.
* Nothing outside ``evals/e2`` can write the ledger: no module outside the
  package (tests excepted) imports the ledger writer or the board job, and the
  package's only writer entry point is ``python -m evals.e2 run``.
* The E2 timer template exists, is not installed by any workflow, and its unit
  passes the release-tree / backup-window invariants
  (``tests/test_systemd_template_invariants.py`` checks it with the others).

Changing E2 is an owner-approved version bump: bump ``evals.e2.VERSION`` and
``rules.json``, run ``python -m evals.e2 manifest --write --version e2-vN`` and ADD
the new manifest's sha256 below (never edit a released entry). The new version
writes its own ledger file; old ledgers are never rescored in place.
"""

from __future__ import annotations

import ast
import json
import shutil
from pathlib import Path

import pytest

from evals.e2 import VERSION, manifest

REPO = Path(__file__).resolve().parent.parent

#: version -> sha256 of evals/e2/MANIFEST.sha256 (LF). Append-only.
RELEASED_MANIFESTS = {
    "e2-v1": "15b70f3f637f2478825c1dc3a8917aeca9b8ed073582e3ef1060008e9273d35f",
}


def test_every_pinned_file_matches_the_manifest():
    assert manifest.verify()["version"] == VERSION


def test_manifest_is_the_released_one_for_its_version():
    version, _ = manifest.parse(manifest.MANIFEST.read_text(encoding="utf-8"))
    assert version == VERSION, "evals.e2.VERSION and the manifest header differ"
    assert version in RELEASED_MANIFESTS, f"{version} is not a released E2 version (RELEASED_MANIFESTS)"
    assert manifest.manifest_sha256() == RELEASED_MANIFESTS[version], (
        f"MANIFEST.sha256 changed without a version bump: {version} was released with a different manifest"
    )


def test_rules_and_cost_model_are_consistent():
    rules = json.loads((manifest.PACKAGE / "rules.json").read_text(encoding="utf-8"))
    costs = json.loads((manifest.PACKAGE / "cost_model.json").read_text(encoding="utf-8"))
    assert rules["version"] == VERSION
    emitted = set()
    for path in (manifest.PACKAGE / "adapters").glob("*.py"):
        emitted |= {node.value for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
                    if isinstance(node, ast.Constant) and isinstance(node.value, str)
                    and node.value.count(".") == 2 and node.value.endswith(".v1")}
    assert emitted and emitted <= set(rules["rules"]), sorted(emitted - set(rules["rules"]))
    for stream in rules["streams"].values():
        if "instrument_class" in stream:
            assert stream["instrument_class"] in costs["classes"]
    for rule in rules["rules"].values():
        if "instrument_class" in rule:
            assert costs["classes"][rule["instrument_class"]] is not None


def _copy(tmp_path: Path) -> Path:
    root = tmp_path / "e2"
    shutil.copytree(manifest.PACKAGE, root, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    return root


def test_guard_detects_a_changed_rule_or_cost(tmp_path):
    root = _copy(tmp_path)
    manifest.verify(root)
    costs = root / "cost_model.json"
    costs.write_text(costs.read_text(encoding="utf-8").replace('"slippage_bps": 2.0', '"slippage_bps": 0.0'),
                     encoding="utf-8")
    with pytest.raises(manifest.ManifestError, match="changed: cost_model.json"):
        manifest.verify(root)


def test_guard_detects_added_and_removed_files(tmp_path):
    root = _copy(tmp_path)
    (root / "adapters" / "sneaky.py").write_text("x = 1\n", encoding="utf-8")
    with pytest.raises(manifest.ManifestError, match="unpinned: adapters/sneaky.py"):
        manifest.verify(root)
    (root / "adapters" / "sneaky.py").unlink()
    (root / "scoring.py").unlink()
    with pytest.raises(manifest.ManifestError, match="missing: scoring.py"):
        manifest.verify(root)


def test_line_endings_do_not_break_verification(tmp_path):
    root = _copy(tmp_path)
    path = root / "scoring.py"
    path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
    manifest.verify(root)


WRITER_MODULES = ("evals.e2.board", "evals.e2.chain", "evals.e2.witness")


def _imports(path: Path) -> set[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        return set()
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            out.add(node.module)
            out |= {f"{node.module}.{a.name}" for a in node.names}
    return out


def test_no_module_outside_e2_can_write_the_ledger():
    offenders = []
    for path in REPO.rglob("*.py"):
        rel = path.relative_to(REPO).as_posix()
        if rel.startswith(("evals/e2/", "tests/", ".git/", "node_modules/")) or "/node_modules/" in rel:
            continue
        hits = {m for m in _imports(path) if m.startswith(WRITER_MODULES)}
        if hits:
            offenders.append(f"{rel}: {sorted(hits)}")
    assert not offenders, "only `python -m evals.e2 run` may write the E2 ledger:\n" + "\n".join(offenders)


def test_the_api_reads_through_the_read_only_report_module_only():
    imports = _imports(REPO / "api" / "routers" / "evals_e2.py")
    e2 = {m for m in imports if m.startswith("evals.e2")}
    assert e2 <= {"evals.e2", "evals.e2.VERSION", "evals.e2.report", "evals.e2.report.load_board",
                  "evals.e2.report.render_markdown"}, e2


def test_the_board_job_is_the_only_caller_of_append_locked_inside_e2():
    callers = []
    for path in manifest.PACKAGE.rglob("*.py"):
        if "append_locked(" in path.read_text(encoding="utf-8") and path.name not in ("chain.py", "board.py"):
            callers.append(path.name)
    assert callers == []


def test_timer_template_ships_uninstalled():
    systemd = REPO / "deploy" / "systemd"
    assert (systemd / "grid-e2-scoreboard.service.template").is_file()
    assert (systemd / "grid-e2-scoreboard.timer.template").is_file()
    for workflow in (REPO / ".github" / "workflows").glob("*.yml"):
        assert "grid-e2-scoreboard" not in workflow.read_text(encoding="utf-8"), workflow.name
