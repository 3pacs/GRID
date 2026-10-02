"""Guard: the E1 suite is hash-pinned, its known violations are consistent, and CI runs it.

* Every file in ``evals/e1`` matches ``MANIFEST.sha256`` (no edit, addition or
  removal without a deliberate re-pin: ``python -m evals.e1.manifest --write``).
* Every ID in ``known_violations.KNOWN`` is used by a gate and every gate's
  ID is registered, so a violation can neither be silently dropped nor
  silently added.
* ``.github/workflows/test.yml`` still has the "E1 integrity gates" step:
  runs this directory with ``E1_REQUIRE_PG=1`` and fails on any skip.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from evals.e1 import manifest
from evals.e1.known_violations import KNOWN

SUITE = Path(__file__).resolve().parent
REPO = SUITE.parents[1]


def test_manifest_pins_every_suite_file():
    pinned = manifest.parse((SUITE / manifest.MANIFEST_NAME).read_text(encoding="utf-8"))
    actual = {rel: manifest.lf_sha256(SUITE / rel) for rel in manifest.suite_files()}
    changed = sorted(k for k in pinned.keys() & actual.keys() if pinned[k] != actual[k])
    assert not changed, f"eval files changed without re-pinning MANIFEST.sha256: {changed}"
    assert sorted(actual.keys() - pinned.keys()) == [], "unpinned eval files"
    assert sorted(pinned.keys() - actual.keys()) == [], "pinned eval files are missing"


def test_known_violations_are_exactly_the_ones_the_gates_use():
    used = set()
    for path in SUITE.glob("test_*.py"):
        used |= set(re.findall(r"""["'](E1-V\d+[a-z]?)["']""", path.read_text(encoding="utf-8")))
    assert used == set(KNOWN), {"unregistered": sorted(used - set(KNOWN)), "unused": sorted(set(KNOWN) - used)}
    for vid, v in KNOWN.items():
        assert v.gate and v.title and v.location and len(v.detail) > 80, vid


def test_ci_runs_the_suite_and_fails_on_a_skip():
    workflow = yaml.safe_load((REPO / ".github" / "workflows" / "test.yml").read_text(encoding="utf-8"))
    steps = [s for job in workflow["jobs"].values() for s in job.get("steps", [])]
    step = next((s for s in steps if s.get("name") == "E1 integrity gates"), None)
    assert step is not None, "the E1 integrity gates CI step is missing"
    run = step["run"]
    assert "pytest evals/e1" in run and "evals.e1.manifest --check" in run
    assert "set -o pipefail" in run and 'grep -qE "^SKIPPED \\[" /tmp/grid-e1-gates.txt' in run
    assert step["env"].get("E1_REQUIRE_PG") == "1" and step["env"].get("GRID_TEST_DB_URL")
