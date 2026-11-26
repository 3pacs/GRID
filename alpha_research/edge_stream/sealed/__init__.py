"""Frozen evaluation code for registrations sealed before a change to the evaluation modules.

A registration pins ``code_sha256``: the hash of its family, numpy's major.minor, the policy
payload and the AST of every evaluation module (``families.EVALUATION_MODULES``). Any change
to those modules, the policy included, changes the hash of every family. An open registration
would then be abandoned rather than judged on code it was not sealed with.

A tree here is a verbatim copy of the edge-stream package as deployed when a registration was
sealed. A registration whose hash the live code no longer reproduces is judged by the tree that
does reproduce it (:func:`evaluator`). Its code check, readiness, look and report then run on
exactly the code it was sealed with. On every call, in this order, failing closed at each step:

1. every file of the tree is present, readable and byte-equal to its pinned sha256 in
   :data:`TREES`, and the tree holds no other ``.py`` file. This covers the modules the
   registration hash leaves out (``ledger``, ``sanity``, ``universe``, ``__init__``), which the
   sealed runner still imports;
2. only then is the tree imported; an import error excludes the tree;
3. every imported module of the tree must come from the tree's own directory;
4. the tree's own ``families.code_sha256``, recomputed from its files, must equal the
   registration's ``code_sha256``. No map from hash to tree is trusted.

A tree that fails any step judges nothing, and the registration is abandoned as before.

**Trust boundary.** The bytes are checked from disk just before import. A tree already imported
in this process is re-checked on disk at every call, but the code in memory is what was imported.
So the check assumes the deployed release root is immutable while a cycle runs: it is written
only by the deploy, owned by ``grid``, and never edited in place. Stale bytecode is covered by the
same assumption (a ``__pycache__`` entry is trusted as Python trusts it).

**What the tree does not freeze.** The sealed look reads bars through the *live* sources and
``extras`` the cycle passes in. The live runner records reads, failed attempts and abandonments
under the *live* fetch-retry rules (:func:`runner._attempt_failed`). ``tests/test_edge_stream_sealed.py``
holds both to the tree. It checks that the live ``data`` module is AST-equal to the tree's, that the
retry thresholds are equal, and that a cap is not counted against a sealed registration, while
a source error is counted and abandons it on the same days as the tree's own rules would.
Changing any of that while a sealed registration is open fails that test. It then needs a frozen
adapter, or the registration's loss accepted in the PR.

A tree is added only by a reviewed PR, as a byte copy of a deployed release tree, with its file
digests pinned in :data:`TREES` and checked by ``tests/test_edge_stream_sealed.py``. A tree is
never edited. It is removed only once no open registration in the live ledger reproduces under it.
"""

from __future__ import annotations

import hashlib
import importlib
import sys
from pathlib import Path
from types import ModuleType

from loguru import logger as log  # same logger config.log re-exports; no settings import

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


def tree_dir(name: str) -> Path:
    return Path(__file__).resolve().parent / name


def verified(name: str, root: Path | None = None) -> bool:
    """Step 1: the tree's files are exactly the pinned ones, byte for byte. Missing, unreadable,
    changed or extra ``.py`` files: False."""
    root = tree_dir(name) if root is None else root
    pinned = TREES[name]["files"]
    try:
        if sorted(p.name for p in root.iterdir() if p.suffix == ".py") != sorted(pinned):
            return False
        return all(hashlib.sha256((root / f).read_bytes()).hexdigest() == sha for f, sha in pinned.items())
    except OSError:
        return False


def _modules(name: str) -> tuple[ModuleType, ModuleType]:
    base = f"{__name__}.{name}"
    return importlib.import_module(f"{base}.families"), importlib.import_module(f"{base}.runner")


def _imported_from_tree(name: str) -> bool:
    """Step 3: every module of the tree loaded in this process was loaded from the tree's files."""
    root, base = tree_dir(name), f"{__name__}.{name}"
    for f in TREES[name]["files"]:
        mod = sys.modules.get(base if f == "__init__.py" else f"{base}.{f[:-3]}")
        if mod is not None and Path(getattr(mod, "__file__", "") or "").resolve() != root / f:
            return False
    return True


def evaluator(reg: dict) -> ModuleType | None:
    """The sealed ``runner`` module of the verified tree whose own files reproduce
    ``reg["code_sha256"]``, or None (see the module docstring for the four steps)."""
    for name in TREES:
        if not verified(name):
            log.warning("edge_stream sealed tree {n}: files differ from the pinned bytes; not used", n=name)
            continue
        try:
            families, runner = _modules(name)
        except Exception as exc:  # a tree that cannot be imported judges nothing
            log.warning("edge_stream sealed tree {n} failed to import: {e}", n=name, e=repr(exc))
            continue
        if not _imported_from_tree(name):
            log.warning("edge_stream sealed tree {n}: a module was loaded from elsewhere; not used", n=name)
            continue
        family = families.FAMILIES.get(reg.get("family"))
        if family is not None and families.code_sha256(family) == reg.get("code_sha256"):
            return runner
    return None
