"""Contract routing table.

ROUTES maps each contract type to the list of handler paths that should be
invoked when the contract fires. Handler paths are dotted Python imports in
the ``contracts.handlers.*`` namespace.

**Phase 1:** ROUTES is empty. Phase 2 will add the 13 contract bindings
defined in the spec.
"""
from __future__ import annotations

import importlib
from typing import Callable

from contracts.schemas import BaseContract


ROUTES: dict[type[BaseContract], list[str]] = {
    # Phase 2 additions will go here, e.g.:
    # PostmortemCompleted: [
    #     "contracts.handlers.trust.on_postmortem_completed",
    #     ...
    # ],
}


def resolve_handler(dotted_path: str) -> Callable:
    """Import a handler from a dotted path.

    Raises ModuleNotFoundError or AttributeError if the path is invalid.
    """
    module_path, _, attr = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(f"handler path must be dotted: {dotted_path!r}")
    module = importlib.import_module(module_path)
    return getattr(module, attr)
