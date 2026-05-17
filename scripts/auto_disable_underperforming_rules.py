"""Auto-disable gem rules whose win-rate falls below the floor.

Reads ``v_rule_win_rate`` (decisive-verdict rollup over ``gem_outcomes``)
and flips ``gem_rules_config.enabled`` based on calibrated win-rate:

  * win_rate < ``DISABLE_THRESHOLD`` (default 0.40) AND n >= ``MIN_SAMPLES``
    (default 30) → set ``enabled=false``.
  * win_rate >= ``PROMOTE_THRESHOLD`` (default 0.60) AND n >= ``MIN_SAMPLES``
    → ensure ``enabled=true`` (re-enables previously-disabled rules whose
    fortunes turned around).
  * Otherwise → no-op.

This complements the human-labeling auto-disable already in
``gem_hunter.py`` (``rate < 0.95 AND n_labeled >= 10`` from operator
labels). That path runs at insert time on labeled gems; this script
runs on a timer over the realized-outcome view so calibration kicks
in even without human labeling.

Cron pattern (drop-in /etc/systemd/system/grid-gem-rule-autotune.timer):
  daily, 03:00 UTC, after ``evaluate_gem_outcomes`` has run.

Usage::

    python -m scripts.auto_disable_underperforming_rules            # apply
    python -m scripts.auto_disable_underperforming_rules --dry-run  # preview
    python -m scripts.auto_disable_underperforming_rules \
        --min-samples 50 --disable-below 0.35 --promote-at 0.65
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


DEFAULT_MIN_SAMPLES: int = 30
DEFAULT_DISABLE_THRESHOLD: float = 0.40
DEFAULT_PROMOTE_THRESHOLD: float = 0.60


@dataclass(frozen=True)
class RuleDecision:
    """One row in the autotune summary."""

    rule_name: str
    n: int
    win_rate: float
    current_enabled: bool
    new_enabled: bool
    action: str  # "disable", "promote", "keep", "skip_low_n"

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "n": int(self.n),
            "win_rate": round(self.win_rate, 4),
            "current_enabled": bool(self.current_enabled),
            "new_enabled": bool(self.new_enabled),
            "action": self.action,
        }


_WIN_RATE_SQL = text(
    """
    SELECT rule_source AS rule_name,
           SUM(n)::bigint                 AS n_total,
           SUM(n_hit)::bigint             AS n_hit,
           SUM(n_miss)::bigint            AS n_miss,
           SUM(n_wrong)::bigint           AS n_wrong,
           -- Decisive-only win_rate: hits / (hits + miss + wrong).
           -- Excludes INCONCLUSIVE rows so thin tickers don't drag the
           -- average down through their unevaluated tail.
           CASE
               WHEN SUM(n_hit + n_miss + n_wrong) > 0 THEN
                    SUM(n_hit)::float
                    / NULLIF(SUM(n_hit + n_miss + n_wrong), 0)
               ELSE NULL
           END AS decisive_win_rate
      FROM v_rule_win_rate
     GROUP BY rule_source
    """
)


_CONFIG_SQL = text(
    """
    SELECT rule_name, enabled FROM gem_rules_config
    """
)


_UPDATE_SQL = text(
    """
    UPDATE gem_rules_config
       SET enabled = :enabled,
           last_evaluated_at = NOW(),
           notes = COALESCE(notes, '') || :note
     WHERE rule_name = :rule_name
    """
)


def autotune_rules(
    engine: Engine,
    *,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    disable_threshold: float = DEFAULT_DISABLE_THRESHOLD,
    promote_threshold: float = DEFAULT_PROMOTE_THRESHOLD,
    dry_run: bool = False,
) -> list[RuleDecision]:
    """Apply the autotune policy. Returns the per-rule decisions.

    Pure read on ``v_rule_win_rate`` + ``gem_rules_config``; the only
    write is the per-rule ``UPDATE`` for rules that actually change state.
    """
    # 1. Pull current win-rate per rule (decisive-only).
    with engine.connect() as conn:
        win_rows = conn.execute(_WIN_RATE_SQL).fetchall()
        config_rows = conn.execute(_CONFIG_SQL).fetchall()

    current_enabled = {r[0]: bool(r[1]) for r in config_rows}
    decisions: list[RuleDecision] = []

    for row in win_rows:
        rule_name = row[0]
        n_total = int(row[1] or 0)
        n_hit = int(row[2] or 0)
        n_miss = int(row[3] or 0)
        n_wrong = int(row[4] or 0)
        win_rate = row[5]
        n_decisive = n_hit + n_miss + n_wrong

        enabled_now = current_enabled.get(rule_name, True)

        # Skip rules without enough decisive verdicts.
        if n_decisive < min_samples or win_rate is None:
            decisions.append(RuleDecision(
                rule_name=rule_name,
                n=n_decisive,
                win_rate=float(win_rate or 0.0),
                current_enabled=enabled_now,
                new_enabled=enabled_now,
                action="skip_low_n",
            ))
            continue

        win_rate_f = float(win_rate)
        if win_rate_f < disable_threshold:
            new_enabled = False
            action = "disable"
        elif win_rate_f >= promote_threshold:
            new_enabled = True
            action = "promote"
        else:
            new_enabled = enabled_now
            action = "keep"

        decisions.append(RuleDecision(
            rule_name=rule_name,
            n=n_decisive,
            win_rate=win_rate_f,
            current_enabled=enabled_now,
            new_enabled=new_enabled,
            action=action,
        ))

    # 2. Apply state changes.
    if dry_run:
        log.info(
            "auto_disable_underperforming_rules: DRY RUN — {n} decisions, no DB writes",
            n=len(decisions),
        )
        return decisions

    changed = [d for d in decisions if d.new_enabled != d.current_enabled]
    if not changed:
        log.info(
            "auto_disable_underperforming_rules: no state changes ({n} decisions evaluated)",
            n=len(decisions),
        )
        return decisions

    with engine.begin() as conn:
        for d in changed:
            note = (
                f" | autotune {d.action} {d.win_rate:.3f} n={d.n} "
                f"(disable<{disable_threshold} promote>={promote_threshold})"
            )
            conn.execute(_UPDATE_SQL, {
                "enabled": d.new_enabled,
                "note": note,
                "rule_name": d.rule_name,
            })
            log.info(
                "autotune {action} {rule}: enabled {cur} → {new} (win_rate={wr:.3f}, n={n})",
                action=d.action,
                rule=d.rule_name,
                cur=d.current_enabled,
                new=d.new_enabled,
                wr=d.win_rate,
                n=d.n,
            )
    return decisions


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="auto_disable_underperforming_rules")
    p.add_argument(
        "--min-samples", type=int, default=DEFAULT_MIN_SAMPLES,
        help=f"Minimum decisive verdicts before autotuning (default {DEFAULT_MIN_SAMPLES})",
    )
    p.add_argument(
        "--disable-below", type=float, default=DEFAULT_DISABLE_THRESHOLD,
        help=f"win_rate below this disables the rule (default {DEFAULT_DISABLE_THRESHOLD})",
    )
    p.add_argument(
        "--promote-at", type=float, default=DEFAULT_PROMOTE_THRESHOLD,
        help=f"win_rate at or above this re-enables the rule (default {DEFAULT_PROMOTE_THRESHOLD})",
    )
    p.add_argument("--dry-run", action="store_true", help="Don't write to gem_rules_config")
    args = p.parse_args(argv)

    from db import get_engine
    engine = get_engine()
    decisions = autotune_rules(
        engine,
        min_samples=int(args.min_samples),
        disable_threshold=float(args.disable_below),
        promote_threshold=float(args.promote_at),
        dry_run=bool(args.dry_run),
    )

    # Tabular summary
    print(f"{'rule':<24} {'n':>5} {'win_rate':>9} {'now':>5} {'new':>5}  action")
    for d in decisions:
        print(
            f"{d.rule_name:<24} {d.n:>5d} {d.win_rate:>9.3f} "
            f"{'on' if d.current_enabled else 'off':>5s} "
            f"{'on' if d.new_enabled else 'off':>5s}  {d.action}"
        )
    print(
        f"\n{len(decisions)} decisions evaluated; "
        f"{sum(1 for d in decisions if d.new_enabled != d.current_enabled)} state changes"
        + (" (dry-run, no writes)" if args.dry_run else "")
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
