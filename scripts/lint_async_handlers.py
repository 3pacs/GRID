#!/usr/bin/env python3
"""lint_async_handlers.py — flag API handlers that say async def but
only do synchronous work.

Closes part of consolidated audit HIGH #13 (async/sync mixing). The
audit observation: many FastAPI handlers under api/routers are declared
`async def` but their bodies only call synchronous SQLAlchemy operations
(`engine.connect()`, `conn.execute(text(...))`, etc.). The async-def
wrapper provides no concurrency benefit and actively hurts: the sync
DB call blocks the event loop instead of running in FastAPI's
threadpool.

This script scans api/routers/*.py and prints handlers that fit the
pattern `async def` + sync DB + no `await` of an async-meaningful
expression. The output is the punch-list for the mass refactor.

Usage:
    python3 scripts/lint_async_handlers.py
    python3 scripts/lint_async_handlers.py --paths api/routers/journal.py
    python3 scripts/lint_async_handlers.py --strict   # exit nonzero on any hits

The strict mode is suitable for CI gating once the audit is fully
closed; until then, running without --strict is informational.
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path


SYNC_DB_NAMES = {
    "engine.connect",
    "engine.begin",
    "conn.execute",
    "session.execute",
    "session.query",
}

# Names that, if awaited inside the handler, mean the function genuinely
# is async and shouldn't be downgraded. Anything not in this list AND
# not a sync DB attribute might still be async (asyncio.sleep, httpx
# AsyncClient, etc.) so we skip declaring the handler "downgrade-safe"
# whenever we see ANY await — false negatives are fine here, false
# positives would mean an unsafe rewrite.
KNOWN_SAFE_AWAITS: set[str] = set()  # intentionally empty


def _is_router_decorator(decorator: ast.expr) -> bool:
    """True if the decorator looks like @router.{get,post,...}."""
    if isinstance(decorator, ast.Call):
        decorator = decorator.func
    if isinstance(decorator, ast.Attribute):
        if decorator.attr in {"get", "post", "put", "delete", "patch", "options", "head"}:
            value = decorator.value
            if isinstance(value, ast.Name) and value.id in {"router", "app"}:
                return True
            if isinstance(value, ast.Attribute) and value.attr in {"router", "app"}:
                return True
    return False


def _attr_chain(node: ast.AST) -> str:
    """Render an attribute chain as a dotted string (best effort)."""
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


class HandlerVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.handlers: list[tuple[ast.AsyncFunctionDef, bool, bool]] = []

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        if not any(_is_router_decorator(d) for d in node.decorator_list):
            self.generic_visit(node)
            return

        has_sync_db = False
        has_any_await = False
        for sub in ast.walk(node):
            if isinstance(sub, ast.Await):
                has_any_await = True
            if isinstance(sub, ast.Attribute):
                chain = _attr_chain(sub)
                tail = ".".join(chain.split(".")[-2:]) if "." in chain else chain
                if tail in SYNC_DB_NAMES:
                    has_sync_db = True

        self.handlers.append((node, has_sync_db, has_any_await))


def lint_file(path: Path) -> list[tuple[int, str, bool, bool]]:
    """Return list of (lineno, name, has_sync_db, has_any_await)."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return []
    v = HandlerVisitor()
    v.visit(tree)
    return [(h.lineno, h.name, sync, aw) for h, sync, aw in v.handlers]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Lint async-def API handlers for sync-only bodies."
    )
    parser.add_argument(
        "--paths",
        nargs="+",
        default=None,
        help="Specific files to scan (default: api/routers/*.py).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit 1 if any downgrade-eligible handlers found.",
    )
    args = parser.parse_args(argv)

    if args.paths:
        paths = [Path(p) for p in args.paths]
    else:
        # Walk up to find repo root + api/routers
        cur = Path(__file__).resolve().parent
        while cur != cur.parent and not (cur / "api" / "routers").is_dir():
            cur = cur.parent
        if not (cur / "api" / "routers").is_dir():
            print("[lint_async_handlers] api/routers not found", file=sys.stderr)
            return 2
        paths = sorted((cur / "api" / "routers").glob("*.py"))

    eligible = 0
    total_async = 0
    for p in paths:
        hits = lint_file(p)
        if not hits:
            continue
        file_eligible = [(ln, n) for ln, n, sync, aw in hits if sync and not aw]
        total_async += len(hits)
        if file_eligible:
            print(f"\n{p.relative_to(p.parent.parent.parent) if 'api' in p.parts else p}:")
            for ln, name in file_eligible:
                print(f"  L{ln:>4} {name} — async-def with sync DB and no awaits")
            eligible += len(file_eligible)

    print()
    print("─" * 64)
    print(
        f"summary: {eligible} downgrade-eligible handlers across "
        f"{total_async} total async-def handlers in {len(paths)} files"
    )
    return 1 if args.strict and eligible else 0


if __name__ == "__main__":
    sys.exit(main())
