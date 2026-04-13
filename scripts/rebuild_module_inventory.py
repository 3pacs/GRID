#!/usr/bin/env python3
"""rebuild_module_inventory.py — Regenerator for docs/MODULE_INVENTORY.md.

SKELETON ONLY. This script is a placeholder invoked by
`scripts/lint_module_inventory.py --rebuild` and by the git pre-push hook
instructions in the failure message.

# TODO(full-rebuilder): implement the full regenerator. It must:
#   1. Walk the canonical SCAN_DIRS from lint_module_inventory.py (reuse the
#      same list — do not fork it).
#   2. For every .py file, collect: LOC, module-level docstring first line,
#      public function signatures (via ast.parse, not regex), detected DB
#      table reads/writes, and the set of files that import it.
#   3. Group the results by top-level directory and emit a Markdown file with
#      matching frontmatter:
#          # GRID Module Inventory
#
#          Generated: YYYY-MM-DD
#          Total modules: N
#          Total LOC: M
#      followed by a "Directory summary" table and per-directory #### entries
#      that match MODULE_HEADING_RE in the linter.
#   4. Write atomically (tmp file + os.replace) so a half-finished rebuild
#      never leaves the inventory in a broken state.
#   5. Print a summary diff of what changed vs the previous inventory.
#
# This is a separate follow-up task — queued via the parent agent's return
# notes for task #87. Until then, running this script prints a warning so the
# caller knows to regenerate by hand.

Usage:
    python3 scripts/rebuild_module_inventory.py         # prints warning (stub)
    python3 scripts/rebuild_module_inventory.py --help
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


WARNING = """\
[rebuild_module_inventory] STUB — full regenerator not yet implemented.

This script is a placeholder. To refresh docs/MODULE_INVENTORY.md today:
  1. Hand-edit the 'Generated: YYYY-MM-DD' line at the top.
  2. Update 'Total modules: N' and 'Total LOC: M' to match the filesystem.
  3. Add/remove any #### module entries that drifted.

The full rebuilder is queued as a follow-up task (see the TODO at the top of
this file). Once implemented, this stub will be replaced with a working
AST-based walker.
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Regenerate docs/MODULE_INVENTORY.md (STUB)."
    )
    parser.add_argument(
        "--root",
        default=None,
        help="Repository root (unused by the stub; accepted for forward compat).",
    )
    parser.parse_args(argv)

    print(WARNING, file=sys.stderr)
    # Exit non-zero so any caller that expected a real regeneration notices.
    return 2


if __name__ == "__main__":
    sys.exit(main())
