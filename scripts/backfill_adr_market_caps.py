"""RETIRED -- this script no longer touches the database under any invocation.

The one-off ADR ratio correction this script existed to apply already ran
successfully on 2026-04-12, for TSM and BHP. Since then, the
sec_xbrl_shares puller (ingestion/altdata/sec_xbrl_shares.py) has applied
the same _ADR_RATIOS table to every row it writes or refreshes, so
ticker_metrics_daily's ADR entries are already correct going forward.
There is nothing left for this script to do.

INCIDENT (2026-09-28): this script had no argparse, so any invocation --
including `--help` -- ran the real
`UPDATE ticker_metrics_daily SET market_cap_usd = market_cap_usd / :ratio, ...`
unconditionally, for every ratio in _ADR_RATIOS. It was run twice in prod
as a result, dividing already-corrected values a second time. That
incident is being restored separately (this script does not touch it).

WHY RETIRE INSTEAD OF ADDING A GUARD: a run-ledger + explicit-override
idempotency guard was built and reviewed first, but rejected: grid-svr
deploys into a fresh per-commit release directory on every deploy, so any
guard file living under scripts/ (e.g. scripts/.state/...) would be empty
again on the very next deploy and could never see that the 2026-04-12 run
already happened -- the next --execute would have divided a third time.
A migration-backed per-ticker marker was rejected earlier for the same
reason a one-shot script shouldn't own a schema change. Since the
correction is already complete and the puller now keeps new rows correct
on its own, retiring the script outright is the correct fix, not another
guard that has to survive redeploys.

This script now parses its arguments (so --help keeps working) and then
refuses to do anything, including with --execute, printing this
explanation and exiting non-zero without importing or contacting the
database in any way.

If ADR market caps ever look wrong again: investigate
ingestion/altdata/sec_xbrl_shares.py and its _ADR_RATIOS table directly.
Do not re-enable this script -- write a new one, scoped to whatever the
new problem actually is.
"""

from __future__ import annotations

import argparse
import sys
from typing import Optional

_RETIREMENT_MESSAGE = """\
backfill_adr_market_caps.py is RETIRED. It will not touch the database,
even with --execute.

Why: the one-off ADR ratio correction it existed to apply already ran
successfully on 2026-04-12, for TSM and BHP. The sec_xbrl_shares puller
(ingestion/altdata/sec_xbrl_shares.py) has applied the same _ADR_RATIOS
table to every row it has written since, so ticker_metrics_daily's ADR
entries are already correct -- there is nothing left for this script to
correct.

This script was also run twice in prod on 2026-09-28: it had no argparse,
so any invocation (including --help) ran the real UPDATE unconditionally.
That incident is being restored separately. A run-ledger idempotency guard
was tried and rejected for this script: grid-svr deploys into a fresh
per-commit release directory each time, so a guard file under scripts/
would never survive a deploy and could not have prevented a third
division -- retiring the script is the actual fix.

If ADR market caps look wrong again, fix
ingestion/altdata/sec_xbrl_shares.py (_ADR_RATIOS) directly. Do not
re-enable this script.\
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help=(
            "Ignored. This script is retired and refuses to write to the "
            "database regardless of this flag -- kept only so old callers "
            "that pass --execute don't hit an argparse error."
        ),
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    # Parsing still happens (so --help works, and unknown flags still
    # error the normal argparse way) -- but no path from here reaches the
    # database, and nothing below this line imports db or any ingestion
    # module.
    build_parser().parse_args(argv)
    print(_RETIREMENT_MESSAGE, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
