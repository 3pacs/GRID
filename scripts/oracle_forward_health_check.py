#!/usr/bin/env python3
"""Report whether the Oracle forward pass produced a usable forecast (read-only).

    python3 scripts/oracle_forward_health_check.py            # current or latest session
    python3 scripts/oracle_forward_health_check.py --ended    # the last session that closed
    python3 scripts/oracle_forward_health_check.py --json
    python3 scripts/oracle_forward_health_check.py --ended --alert   # e-mail when it has none

Exit code 1 when the session has no usable forecast (every candidate abstained,
passes ran and stored nothing, or no pass ran). Operator-run: it reads every
prediction, private-use ones included, and prints the operator notes. See
oracle/forward_health.py.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from oracle.forward_health import FAILING, alert_text, forward_health  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ended", action="store_true", help="judge the last session that closed")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--alert", action="store_true", help="send the operator e-mail when failing")
    args = ap.parse_args(argv)

    from db import get_engine
    with get_engine().connect() as conn:
        health = forward_health(conn, now=datetime.now(timezone.utc), include_schwab=True,
                                ended_only=args.ended)
    failing = health["status"] in FAILING
    subject, body = alert_text(health)
    sent = None
    if failing and args.alert:
        try:
            from alerts.email import send_alert
            sent = bool(send_alert(subject, body, severity="critical"))
        except Exception:  # noqa: BLE001 - the verdict below still reaches the caller
            sent = False
    if args.json:
        print(json.dumps({**health, "alert_sent": sent}, indent=1))
    else:
        print(body if failing else health["message"])
        if sent is not None:
            print("alert e-mail sent" if sent else "alert e-mail NOT sent (disabled or failed)")
    return 1 if failing else 0


if __name__ == "__main__":
    sys.exit(main())
