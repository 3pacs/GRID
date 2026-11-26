"""Frozen evaluation code for registrations sealed before a change to the evaluation modules.

A registration pins ``code_sha256``: the hash of its family, numpy's major.minor, the policy
payload and the AST of every evaluation module (``families.EVALUATION_MODULES``). Any change
to those modules, the policy included, changes the hash of every family. An open registration
would then be abandoned rather than judged on code it was not sealed with.

A tree here is a verbatim copy of the edge-stream package as deployed when a registration was
sealed. A registration whose hash the live code no longer reproduces is judged by the tree that
does reproduce it, recomputed from the tree's own files on every call (:func:`evaluator`). Its
code check, readiness, look and report then run on exactly the code it was sealed with. No map
from hash to tree is trusted: a tree that has been edited no longer reproduces any hash, and the
registration is abandoned as before.

A tree is added only by a reviewed PR, as a byte copy of a deployed release tree, with its file
digests pinned in :data:`TREES` and checked by ``tests/test_edge_stream_sealed.py``. A tree is
never edited. It is removed only once no open registration in the live ledger reproduces under it.
"""

from __future__ import annotations

import importlib
from types import ModuleType

#: name -> provenance. ``git_tree`` is ``git rev-parse <release>:alpha_research/edge_stream``
#: of the deployed release the copy was taken from; ``files`` are the copied files' sha256.
TREES: dict[str, dict] = {
    "tree_a1d9bd4c": {
        "git_tree": "a1d9bd4c94fd2ecf5febc6078b845460963e7aaa",
        "deployed_as": "grid_release from ca04a2d5 (2026-11-18, the last commit touching the package) to c6cdc663",
        #: the open registrations sealed under it on 2026-11-25 (both forward, both under policy 3)
        "seals": {
            "IM1-v1:us_equity_index:forward:2026-11-23":
                "a9f28c3a6d641358147cd6a1f3c8c8555a63dff39f7d9558f2fbb1b646648e79",
            "EC1-v1:sp500_events:forward:2026-11-26:confirms-603ba3e2332b":
                "185fb33dd668cdeb016db335a65a24ae7e9ce5faa9a067fc85d4c4ad28c230aa",
        },
        "files": {
            "__init__.py": "b118f090a79a441fd068b7981a4aac7837dbe2339cdf84f1c4efa3b064cccb2b",
            "data.py": "15eb869808806287001d41628f7e8a38fcde73afbfb7d7f76ea6a6e75d8774ad",
            "families.py": "76d2a0373cd6da6bcd5281ce87cddb2a5c575fa01094782f94bc7feac8d8f008",
            "ledger.py": "95831ea0e8860209d8f24dd447a1fa2063a2e18e536b1ec1b8d77f03d0f7ebd1",
            "policy.py": "559688b86aa4140c00f746661d5bdae19ffa5dd9499c64fbd932b2db3dbfa057",
            "runner.py": "ca54a048e6d9be05cdefd07a4559008fc6970b7070723b4e887293b060ceeceb",
            "sanity.py": "dc458a91a6b4b17ee7a0cccb09da575b4d8dcac7634fd59f9fc614a25c542833",
            "stats.py": "1ac160d721e23a8bf07043a2e8480d5c299ae3a64478a999c1d57092e25b23e0",
            "universe.py": "8b1f5e048fd6d63694f7986d52abcaf09c8caa3b0ac3112f575ac3f56a3e04e1",
        },
    },
}


def _modules(name: str) -> tuple[ModuleType, ModuleType]:
    base = f"{__name__}.{name}"
    return importlib.import_module(f"{base}.families"), importlib.import_module(f"{base}.runner")


def evaluator(reg: dict) -> ModuleType | None:
    """The sealed ``runner`` module whose own files reproduce ``reg["code_sha256"]``, or None."""
    for name in TREES:
        families, runner = _modules(name)
        family = families.FAMILIES.get(reg.get("family"))
        if family is not None and families.code_sha256(family) == reg.get("code_sha256"):
            return runner
    return None
