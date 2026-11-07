"""Label legacy ``hypothesis_registry`` PASSED/TESTING rows (append-only).

Dry run (default, READ ONLY transaction)::

    python3 scripts/label_legacy_hypotheses.py

Write the labels (idempotent; a re-run inserts nothing)::

    python3 scripts/label_legacy_hypotheses.py --apply --decided-by "anik 2026-11-07"

Needs migration ``hyp_registry_labels_20261107``. Existing registry rows
are never updated or deleted; see ``governance.hypothesis_labels``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from governance.hypothesis_labels import label_legacy_registry


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Label legacy hypothesis_registry verdicts")
    parser.add_argument("--apply", action="store_true", help="write the labels (default: dry run)")
    parser.add_argument("--decided-by", default="", help="who authorised the relabel (required with --apply)")
    args = parser.parse_args(argv)
    if args.apply and not args.decided_by.strip():
        parser.error("--apply needs --decided-by")

    from db import get_engine

    result = label_legacy_registry(get_engine(), dry_run=not args.apply,
                                   decided_by=args.decided_by or None)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
