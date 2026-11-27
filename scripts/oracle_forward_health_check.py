#!/usr/bin/env python3
"""Report whether the Oracle forward pass produced a usable forecast (read-only unless --alert).

    python3 scripts/oracle_forward_health_check.py            # current or latest session
    python3 scripts/oracle_forward_health_check.py --ended    # the last session that closed
    python3 scripts/oracle_forward_health_check.py --json
    python3 scripts/oracle_forward_health_check.py --notes    # add possible causes (operator terminal)
    python3 scripts/oracle_forward_health_check.py --ended --alert --write-alert DIR   # the daily sweep

Exit code 1 when the session has no usable forecast (every candidate abstained,
passes ran and stored nothing, or no pass ran), and when a session that has
ended left a cohort ticker without one.

--alert sends the operator e-mail when one is due and none was sent for that
session yet, by the Hermes operator or by an earlier run of this script
(oracle/forward_alert.py: one marker row per session). So running it again on
Saturday, Sunday and a holiday Monday for Friday's session sends nothing new.
--write-alert DIR writes DIR/oracle-forward-<session date>.txt for a failing
session: one file per session, overwritten, not one per day.

Operator-run: it reads every prediction, private-use ones included. --notes
prints when a private-use capture completed; that goes to this terminal only,
never into the e-mail or the alert file.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from oracle.forward_alert import alert_due, send_session_alert  # noqa: E402
from oracle.forward_health import FAILING, STATUS_PARTIAL, alert_text, forward_health  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ended", action="store_true", help="judge the last session that closed")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--notes", action="store_true", help="print possible causes (operator terminal only)")
    ap.add_argument("--alert", action="store_true", help="send the operator e-mail when due and not yet sent")
    ap.add_argument("--write-alert", metavar="DIR", help="write DIR/oracle-forward-<session>.txt when failing")
    args = ap.parse_args(argv)

    from db import get_engine
    engine = get_engine()
    with engine.connect() as conn:
        health = forward_health(conn, now=datetime.now(timezone.utc), include_schwab=True,
                                ended_only=args.ended)
    failing = health["status"] in FAILING or (
        health["status"] == STATUS_PARTIAL and health["session_state"] == "ended")
    alert = None
    if args.alert and alert_due(health)[0]:
        try:
            alert = send_session_alert(engine, health, extra="Sent by the daily freshness sweep.")
        except Exception as exc:  # noqa: BLE001 - the verdict below still reaches the caller
            alert = f"error: {type(exc).__name__}"
    _subject, body = alert_text(health)                      # no capture metadata
    if failing and args.write_alert:
        Path(args.write_alert, f"oracle-forward-{health['session_date']}.txt").write_text(
            body + "\n", encoding="utf-8")
    if args.json:
        shown = dict(health) if args.notes else {k: v for k, v in health.items() if k != "operator_notes"}
        print(json.dumps({**shown, "alert": alert}, indent=1))
    else:
        print(alert_text(health, with_notes=args.notes)[1] if failing else health["message"])
        if alert is not None:
            print(f"alert: {alert}")
    return 1 if failing else 0


if __name__ == "__main__":
    sys.exit(main())
