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
LOCKS = [
    ROOT / "deploy" / "python314" / "constraints-3.14.txt",
    ROOT / "ci" / "py_floor" / "constraints-exact-minimum.txt",
]


def _requirements(path: Path) -> dict[str, Requirement]:
    out: dict[str, Requirement] = {}
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            continue
        try:
            req = Requirement(line)
        except InvalidRequirement:
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


@pytest.mark.parametrize("lock", LOCKS, ids=lambda p: p.name)
def test_every_lock_pin_satisfies_requirements(lock: Path) -> None:
    if not lock.exists():
        pytest.skip(f"{lock} not present")
    reqs = _requirements(REQUIREMENTS)
    pins = _pins(lock)
    bad = [
        f"{name}=={pins[name]} not in requirements.txt '{reqs[name].specifier}'"
        for name in sorted(set(reqs) & set(pins))
        if reqs[name].specifier and not reqs[name].specifier.contains(pins[name], prereleases=True)
    ]
    assert not bad, "lock out of range of requirements.txt:\n" + "\n".join(bad)


def test_parser_reads_the_real_files() -> None:
    reqs = _requirements(REQUIREMENTS)
    pins = _pins(LOCKS[0])
    assert "boto3" in reqs and "boto3" in pins
    assert len(set(reqs) & set(pins)) >= 10
