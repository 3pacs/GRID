#!/usr/bin/env python3
"""Normalize only pinned XNYS manifest modes in a private build candidate.

The pins are the literal PINS and PRESCREEN_PINS in analysis/factory_session_calendar.py: every
hash-pinned XNYS part a deployed reader opens (the factory calendar, and the market-state record's
session ages, which also read the prescreen parts).
No GRID imports or source execution. All files are checked before any mode change.
Absent factory feature is a no-op for older candidates. Never call on a live tree.
"""
from __future__ import annotations

import ast
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys


def prepare(candidate: Path) -> dict:
    root = candidate.absolute()
    if root.resolve(strict=True) != root:
        raise ValueError("candidate alias/symlink refused")
    source = root / "analysis/factory_session_calendar.py"
    if not source.exists() and not source.is_symlink():
        return {"status": "not_applicable", "files": []}
    if source.resolve(strict=True) != source or not source.is_file():
        raise ValueError("factory source must be a regular non-symlink file")
    tree = ast.parse(source.read_text())
    # PINS (factory-xnys-*) is required. PRESCREEN_PINS (prescreen-xnys-*, which the deployed market-state record
    # counts session ages on) is optional, so a candidate from before it existed prepares PINS alone.
    pins = []
    names = set()
    for collection, pattern, required in (("PINS", r"factory-xnys-[0-9-]+\.json", True),
                                          ("PRESCREEN_PINS", r"prescreen-xnys-[0-9-]+\.json", False)):
        assignments = [n.value for n in tree.body if isinstance(n, ast.Assign)
                       and any(isinstance(t, ast.Name) and t.id == collection for t in n.targets)]
        if not assignments and not required:
            continue
        if len(assignments) != 1:
            raise ValueError(f"exactly one literal {collection} assignment required")
        found = ast.literal_eval(assignments[0])
        if not isinstance(found, tuple) or not 1 <= len(found) <= 16:
            raise ValueError(f"invalid {collection} collection")
        for pair in found:
            if not isinstance(pair, tuple) or len(pair) != 2:
                raise ValueError("invalid pin pair")
            name, digest = pair
            if (not isinstance(name, str) or not re.fullmatch(pattern, name)
                    or name in names or not isinstance(digest, str)
                    or not re.fullmatch(r"[a-f0-9]{64}", digest)):
                raise ValueError("invalid pin name/hash")
            names.add(name)
            pins.append(pair)
    pending = []
    with ExitStack() as stack:
        for name, expected in pins:
            path = root / "docs/reference/calendar-manifests" / name
            if path.resolve(strict=True) != path:
                raise ValueError("manifest symlink or linked parent refused")
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            f = stack.enter_context(os.fdopen(fd, "rb"))
            info = os.fstat(f.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError("manifest must be regular and not hardlinked")
            if info.st_size > 2_000_000 or hashlib.sha256(f.read()).hexdigest() != expected:
                raise ValueError(f"manifest hash/size mismatch: {name}")
            pending.append((name, f, stat.S_IMODE(info.st_mode)))
        rows = []
        for name, f, before in pending:
            after = before & ~0o022
            if before != after:
                os.fchmod(f.fileno(), after)
            if stat.S_IMODE(os.fstat(f.fileno()).st_mode) != after:
                raise ValueError(f"mode normalization failed: {name}")
            rows.append({"file": name, "before": oct(before), "after": oct(after)})
    return {"status": "prepared", "files": rows}


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: prepare_factory_calendars.py CANDIDATE_DIR", file=sys.stderr)
        return 2
    try:
        result = prepare(Path(sys.argv[1]))
    except (OSError, ValueError, SyntaxError) as exc:
        print(f"factory calendar preparation refused: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
