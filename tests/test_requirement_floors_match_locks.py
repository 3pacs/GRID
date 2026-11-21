"""Every pinned lock must satisfy requirements.txt.

The deploy build installs ``requirements.txt`` under
``deploy/python314/constraints-3.14.txt``. A floor raised in requirements.txt
(Dependabot does this) without moving the lock makes pip's resolution
impossible, and the deploy build fails for every later merge too
(2026-11-21: boto3 >=1.44.3 against a boto3==1.43.123 lock, #1264). The
exact-minimum floor file must likewise sit inside each requirement's range.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS = ROOT / "requirements.txt"
# Each lock with the Python versions it is installed under (deploy/CI), so
# requirement markers such as python_version are evaluated per version.
LOCKS = [
    (ROOT / "deploy" / "python314" / "constraints-3.14.txt", ("3.14",)),
    (ROOT / "ci" / "py_floor" / "constraints-exact-minimum.txt", ("3.10", "3.11")),
]


def _requirements(path: Path, python_version: str) -> dict[str, Requirement]:
    """Requirements whose markers apply on ``python_version``."""
    out: dict[str, Requirement] = {}
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            continue
        try:
            req = Requirement(line)
        except InvalidRequirement:
            continue
        if req.marker is not None and not req.marker.evaluate({"python_version": python_version}):
            continue
        out[canonicalize_name(req.name)] = req
    return out


def _pins(path: Path) -> dict[str, Version]:
    out: dict[str, Version] = {}
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if "==" not in line or line.startswith("-"):
            continue
        name, _, ver = line.partition("==")
        name = name.split("[", 1)[0].strip()
        ver = ver.split(";", 1)[0].strip()
        try:
            out[canonicalize_name(name)] = Version(ver)
        except Exception:  # noqa: BLE001 -- a non-version pin is not this test's concern
            continue
    return out


@pytest.mark.parametrize(
    ("lock", "python_version"),
    [(lock, py) for lock, versions in LOCKS for py in versions],
    ids=lambda v: v.name if isinstance(v, Path) else v,
)
def test_every_lock_pin_satisfies_requirements(lock: Path, python_version: str) -> None:
    if not lock.exists():
        pytest.skip(f"{lock} not present")
    reqs = _requirements(REQUIREMENTS, python_version)
    pins = _pins(lock)
    bad = [
        f"{name}=={pins[name]} not in requirements.txt '{reqs[name].specifier}'"
        for name in sorted(set(reqs) & set(pins))
        if reqs[name].specifier and not reqs[name].specifier.contains(pins[name], prereleases=True)
    ]
    assert not bad, "lock out of range of requirements.txt:\n" + "\n".join(bad)


def test_parser_reads_the_real_files() -> None:
    reqs = _requirements(REQUIREMENTS, "3.14")
    pins = _pins(LOCKS[0][0])
    assert "boto3" in reqs and "boto3" in pins
    assert len(set(reqs) & set(pins)) >= 10


def test_markers_pick_the_line_for_each_python() -> None:
    # requirements.txt splits edgartools by python_version: 3.10 keeps 5.x.
    assert str(_requirements(REQUIREMENTS, "3.10")["edgartools"].specifier) == ">=4.6.3"
    assert str(_requirements(REQUIREMENTS, "3.14")["edgartools"].specifier) == ">=6.3.0"
