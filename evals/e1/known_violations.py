"""Real violations the E1 gates found on main, tracked as strict, named xfails.

A gate is never weakened to pass. When it finds a genuine defect on main, the
failing case is marked ``@known_violation("<ID>")``: a *strict* xfail, so

* the suite stays green while the defect is open and recorded here, and
* the day the code is fixed, the xfail turns into an XPASS **failure** until
  the entry is deleted from this registry (and ``MANIFEST.sha256`` re-pinned).

``test_manifest_guard.py`` checks every ID here is used by at least one gate
and every use names an ID here. Each entry names the code location, what is
wrong and why it matters. Report: ``GRID-E1-GATES-V1-20260930.md``.

Closed (entry removed, gate now enforced): E1-V1, E1-V2, E1-V5 (suite
``e1-v1.1``); E1-V3 (registry entries with no callable pull method), E1-V4
(raw_series writers without pull_status / with non-schema columns / rewriting
stored rows) and E1-V6 (coingecko writing resolved_series directly) -- suite
``e1-v1.2``. The registry is empty until a gate finds a new defect.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest


@dataclass(frozen=True)
class Violation:
    gate: str
    title: str
    location: str
    detail: str


KNOWN: dict[str, Violation] = {}


def known_violation(vid: str):
    """Strict xfail for a registered violation (unknown IDs fail at import).

    Only an ``AssertionError`` (the gate's own check) counts as the known
    failure; any other exception is a real error, not the tracked violation.
    """
    v = KNOWN[vid]
    return pytest.mark.xfail(strict=True, raises=AssertionError, reason=f"{vid} [{v.gate}] {v.title} -- {v.location}")
