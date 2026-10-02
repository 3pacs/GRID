#!/usr/bin/env python3
"""Rewrite grid-svr's ``grid`` user crontab so GRID jobs run from the release tree.

OPS-W1. Until this cutover the GRID cron jobs ran from the old, diverged
checkout ``/home/grid/grid_v4/grid_repo``. This tool reads a crontab (a
``crontab -l`` backup), rewrites exactly the known GRID job lines and writes
the new crontab text. It never calls ``crontab`` itself: the operator
installs the output, and rollback is ``crontab <backup>``.

Rules (each must match exactly one line, or the tool refuses):

* REWRITE: hourly catch-up, AstroGrid hourly catch-up, the three
  ``grid_cron.sh`` jobs, auto-improve and price alerts now call the release
  tree's scripts. Schedules, lock files, log files and markers are unchanged.
* DISABLE (commented out with a dated reason, never deleted):
  - the standalone dashboard cache warm, which has failed every run since
    2026-04-11 (no env file) and duplicates the hourly catch-up's
    ``dashboard_cache_warm`` step;
  - the trial ingestor, which has never completed from cron (``/bin/sh`` is
    dash and has no ``source``); turning a dead DB writer back on is a
    separate owner decision;
  - ``grid-git-catchup.sh``, which stashes and merges inside the old
    checkout (it is a no-op today because its fetch fails); the old checkout
    has to stay frozen during the cooling-off period.

A rule whose target line is already present (and whose source line is gone)
counts as already applied, so a second run is a no-op. After rewriting, no
active line may mention ``grid_repo`` except as the shared env file path
``/home/grid/grid_v4/grid_repo/.env`` (the paper-log jobs read it; moving that
file is a later, separate step).

Usage::

    python3 scripts/crontab_release_cutover.py --in BACKUP --out NEW [--stamp YYYYMMDD]
    python3 scripts/crontab_release_cutover.py --print-target
"""

from __future__ import annotations

import argparse
import datetime as _dt
import re
import sys
from dataclasses import dataclass
from pathlib import Path

OLD_ROOT = "/home/grid/grid_v4/grid_repo"
RELEASE_ROOT = "/data/grid_v4/grid_release"
SHARED_ENV_FILE = f"{OLD_ROOT}/.env"
DISABLED_TAG = "OPS-W1-DISABLED"


@dataclass(frozen=True)
class Rule:
    key: str
    action: str  # "rewrite" or "disable"
    pattern: str  # full-line regex (applied to the line without trailing space)
    target: str = ""  # rewrite template; str.format(**groupdict)
    reason: str = ""  # disable reason


def _lit(text: str) -> str:
    return re.escape(text)


RULES: tuple[Rule, ...] = (
    Rule(
        key="hourly-catchup",
        action="rewrite",
        pattern=_lit(
            "0 * * * * cd /home/grid/grid_v4/grid_repo && /usr/bin/flock -n /tmp/grid-hourly-catchup.lock "
            "/bin/bash /home/grid/grid_v4/grid_repo/scripts/grid_hourly_catchup.sh "
            ">> /data/grid/logs/hourly-catchup.log 2>&1 # GRID-CRON-hourly-catchup"
        ),
        target=(
            "0 * * * * /usr/bin/flock -n /tmp/grid-hourly-catchup.lock "
            f"/bin/bash {RELEASE_ROOT}/scripts/grid_hourly_catchup.sh "
            ">> /data/grid/logs/hourly-catchup.log 2>&1 # GRID-CRON-hourly-catchup"
        ),
    ),
    Rule(
        key="astrogrid-hourly-catchup",
        action="rewrite",
        pattern=_lit(
            "5 * * * * cd /home/grid/grid_v4/grid_repo && /usr/bin/flock -n /tmp/astrogrid-hourly-catchup.lock "
            "/bin/bash /home/grid/grid_v4/grid_repo/scripts/astrogrid_hourly_catchup.sh "
            ">> /data/grid/logs/astrogrid-hourly-catchup.log 2>&1 # GRID-CRON-astrogrid-hourly-catchup"
        ),
        target=(
            "5 * * * * /usr/bin/flock -n /tmp/astrogrid-hourly-catchup.lock "
            f"/bin/bash {RELEASE_ROOT}/scripts/astrogrid_hourly_catchup.sh "
            ">> /data/grid/logs/astrogrid-hourly-catchup.log 2>&1 # GRID-CRON-astrogrid-hourly-catchup"
        ),
    ),
    Rule(
        key="briefing-daily",
        action="rewrite",
        pattern=_lit(
            "0 6 * * 1-5 /home/grid/grid_v4/grid_repo/scripts/grid_cron.sh briefing daily # GRID-CRON-briefing-daily"
        ),
        target=f"0 6 * * 1-5 {RELEASE_ROOT}/scripts/grid_cron.sh briefing daily # GRID-CRON-briefing-daily",
    ),
    Rule(
        key="analyst",
        action="rewrite",
        pattern=_lit(
            "30 6 * * 1-5 /home/grid/grid_v4/grid_repo/scripts/grid_cron.sh analyst # GRID-CRON-analyst"
        ),
        target=f"30 6 * * 1-5 {RELEASE_ROOT}/scripts/grid_cron.sh analyst # GRID-CRON-analyst",
    ),
    Rule(
        key="briefing-weekly",
        action="rewrite",
        pattern=_lit(
            "0 7 * * 1 /home/grid/grid_v4/grid_repo/scripts/grid_cron.sh briefing weekly # GRID-CRON-briefing-weekly"
        ),
        target=f"0 7 * * 1 {RELEASE_ROOT}/scripts/grid_cron.sh briefing weekly # GRID-CRON-briefing-weekly",
    ),
    Rule(
        key="auto-improve",
        action="rewrite",
        pattern=_lit(
            "0 5 * * * cd /home/grid/grid_v4/grid_repo && /usr/bin/python3 -m scripts.auto_improve_from_postmortems "
            ">> /home/grid/logs/auto-improve.log 2>&1 # GRID-CRON-auto-improve"
        ),
        target=(
            f"0 5 * * * {RELEASE_ROOT}/scripts/grid_cron_run.sh -m scripts.auto_improve_from_postmortems "
            ">> /home/grid/logs/auto-improve.log 2>&1 # GRID-CRON-auto-improve"
        ),
    ),
    Rule(
        key="price-alerts",
        action="rewrite",
        # The recipient value is carried over verbatim and never printed.
        pattern=(
            _lit(
                "*/15 * * * * cd /home/grid/grid_v4/grid_repo && /usr/bin/flock -n /tmp/sd-price-alerts.lock "
                "/usr/bin/env SD_IMESSAGE_DAD="
            )
            + r"(?P<dad>\S+)"
            + _lit(
                " PYTHONPATH=/home/grid/grid_v4/grid_repo /usr/bin/python3 -m scripts.check_price_alerts "
                ">> /data/grid/logs/price-alerts.log 2>&1 # GRID-CRON-price-alerts"
            )
        ),
        target=(
            "*/15 * * * * /usr/bin/flock -n /tmp/sd-price-alerts.lock /usr/bin/env SD_IMESSAGE_DAD={dad} "
            f"{RELEASE_ROOT}/scripts/grid_cron_run.sh -m scripts.check_price_alerts "
            ">> /data/grid/logs/price-alerts.log 2>&1 # GRID-CRON-price-alerts"
        ),
    ),
    Rule(
        key="dashboard-cache-warm",
        action="disable",
        pattern=_lit(
            "0 0,6,12,18 * * * cd /home/grid/grid_v4/grid_repo && /usr/bin/python3 scripts/warm_dashboard_cache.py "
            ">> /data/grid/logs/cache-warm.log 2>&1"
        ),
        reason=(
            "failing every run since 2026-04-11 (no env file -> GRID_JWT_SECRET unset); "
            "duplicate of the hourly catch-up's dashboard_cache_warm step"
        ),
    ),
    Rule(
        key="trial-ingestor",
        action="disable",
        pattern=_lit(
            "0 6 * * * cd ~/grid_v4/grid_repo && set -a && source .env && set +a && "
            "python3 -m grid.ingestors.trial_ingestor >> /var/log/grid/trial_ingestor.log 2>&1"
        ),
        reason=(
            "never completed from cron (/bin/sh is dash: 'source: not found'); "
            "re-enabling this DB writer needs a separate owner decision"
        ),
    ),
    Rule(
        key="git-catchup",
        action="disable",
        pattern=_lit(
            "*/30 * * * * /usr/bin/flock -n /tmp/grid-git-catchup.lock /bin/bash /home/grid/bin/grid-git-catchup.sh "
            "# GRID-CRON-git-catchup"
        ),
        reason=(
            "stashes and merges inside the old grid_repo checkout (fetch failing since 2026-07-14); "
            "old checkout stays frozen during cooling-off"
        ),
    ),
)


class CutoverError(RuntimeError):
    """The crontab does not look the way the cutover expects; nothing written."""


def _disabled_line(rule: Rule, original: str, stamp: str) -> str:
    return f"# {DISABLED_TAG} {stamp} {rule.key} ({rule.reason}) # {original}"


def _is_active(line: str) -> bool:
    stripped = line.strip()
    return bool(stripped) and not stripped.startswith("#")


def transform(text: str, stamp: str) -> tuple[str, list[str]]:
    """Return (new crontab text, summary lines). Raises CutoverError on drift."""
    if "\r" in text:
        raise CutoverError(
            "crontab text contains CR bytes; expected crontab -l output (LF only)"
        )
    # split("\n"), not splitlines(): only LF separates crontab lines, and the
    # join below must give back every untouched byte, trailing newline included.
    lines = text.split("\n")
    summary: list[str] = []
    problems: list[str] = []

    for rule in RULES:
        regex = re.compile(rule.pattern)
        hits = [i for i, line in enumerate(lines) if regex.fullmatch(line.rstrip())]
        if len(hits) > 1:
            problems.append(f"{rule.key}: source line appears {len(hits)} times")
            continue
        if not hits:
            if rule.action == "rewrite":
                # Already applied iff the rewritten form of SOME prior source is present.
                # The price-alerts target embeds a captured value, so match it by shape.
                target_re = re.compile(
                    re.escape(rule.target).replace(re.escape("{dad}"), r"\S+")
                )
                done = [line for line in lines if target_re.fullmatch(line.rstrip())]
            else:
                done = [
                    line
                    for line in lines
                    if line.startswith(f"# {DISABLED_TAG} ")
                    and f" {rule.key} (" in line
                ]
            if len(done) == 1:
                summary.append(f"already  {rule.key}")
            else:
                problems.append(
                    f"{rule.key}: source line not found (and not already applied)"
                )
            continue
        i = hits[0]
        original = lines[i].rstrip()
        if rule.action == "rewrite":
            match = regex.fullmatch(original)
            assert match is not None
            lines[i] = rule.target.format(**match.groupdict())
            summary.append(f"rewrite  {rule.key}")
        else:
            lines[i] = _disabled_line(rule, original, stamp)
            summary.append(f"disable  {rule.key}")

    if problems:
        raise CutoverError("; ".join(problems))

    leftovers = []
    for n, line in enumerate(lines, 1):
        if _is_active(line) and "grid_repo" in line.replace(SHARED_ENV_FILE, ""):
            leftovers.append(n)
    if leftovers:
        raise CutoverError(
            f"active lines still reference grid_repo (beyond {SHARED_ENV_FILE}): {leftovers}"
        )

    env_readers = sum(
        1 for line in lines if _is_active(line) and SHARED_ENV_FILE in line
    )
    summary.append(
        f"note     {env_readers} active line(s) still read {SHARED_ENV_FILE} (env file only, by design)"
    )

    return "\n".join(lines), summary


def target_block() -> str:
    """The GRID job lines as they look after the cutover (recipient value elided)."""
    out = []
    for rule in RULES:
        if rule.action == "rewrite":
            out.append(
                rule.target.format(
                    dad="<SD_IMESSAGE_DAD value carried over from the current crontab>"
                )
            )
        else:
            out.append(
                f"# {DISABLED_TAG} <stamp> {rule.key} ({rule.reason}) # <original line>"
            )
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--in", dest="src", type=Path, help="crontab backup to read (crontab -l output)"
    )
    parser.add_argument(
        "--out", dest="dst", type=Path, help="where to write the new crontab text"
    )
    parser.add_argument(
        "--stamp", default=_dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d")
    )
    parser.add_argument(
        "--print-target",
        action="store_true",
        help="print the post-cutover GRID lines and exit",
    )
    args = parser.parse_args(argv)

    if args.print_target:
        sys.stdout.write(target_block())
        return 0
    if not args.src or not args.dst:
        parser.error("--in and --out are required (or use --print-target)")
    if args.dst.exists():
        parser.error(f"refusing to overwrite existing {args.dst}")

    try:
        with open(args.src, encoding="utf-8", newline="") as fh:  # keep bytes as-is
            new_text, summary = transform(fh.read(), args.stamp)
    except CutoverError as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 2
    with open(args.dst, "w", encoding="utf-8", newline="\n") as fh:  # cron wants LF
        fh.write(new_text)
    for line in summary:
        print(line, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
