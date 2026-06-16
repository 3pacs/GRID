#!/usr/bin/env python3
"""Poll active stepdad.finance price alerts and fire iMessages on a cross.

Run periodically (cron / systemd timer). Idempotent and cheap when idle: one
live price fetch per distinct ticker, one-shot alerts deactivate after firing so
they never double-text. A notification failure is logged but never blocks the
DB update — the alert still records as triggered.

Usage:  python -m scripts.check_price_alerts
"""

from __future__ import annotations

import sys

from loguru import logger as log


def _format_fire(ticker: str, direction: str, threshold: float, price: float) -> str:
    word = "above" if direction == "above" else "below"
    return (
        f"\U0001F4C8 {ticker} is now {word} ${threshold:,.2f} "
        f"— it's ${price:,.2f} right now. (stepdad.finance alert)"
    )


def main() -> int:
    from sqlalchemy import text

    from api.dependencies import get_db_engine
    from api.routers.price_alerts import current_price, ensure_alerts_table
    from scripts.sd_imessage import imessage_owner

    engine = get_db_engine()
    ensure_alerts_table(engine)

    with engine.connect() as conn:
        alerts = conn.execute(text(
            "SELECT id, owner, ticker, direction, threshold "
            "FROM sd_price_alerts WHERE active = true"
        )).fetchall()

    if not alerts:
        log.info("Price alerts: none active.")
        return 0

    prices: dict[str, float | None] = {}
    fired = 0

    for alert_id, owner, ticker, direction, threshold in alerts:
        tk = (ticker or "").upper()
        if tk not in prices:
            px, _src = current_price(tk, prefer_live=True)
            prices[tk] = px
        px = prices[tk]
        if px is None:
            log.warning("Price alerts: no price for {t}; skipping #{id}", t=tk, id=alert_id)
            continue

        met = (
            (direction == "above" and px >= float(threshold))
            or (direction == "below" and px <= float(threshold))
        )
        if not met:
            continue

        body = _format_fire(tk, direction, float(threshold), px)
        sent = imessage_owner(owner or "dad", body)
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE sd_price_alerts "
                "SET active = false, triggered_at = now(), last_price = :p "
                "WHERE id = :id"
            ), {"p": px, "id": alert_id})
        fired += 1
        log.info(
            "Price alerts: fired #{id} {t} {d} {th} @ {px} (sent={s}, owner={o})",
            id=alert_id, t=tk, d=direction, th=threshold, px=px, s=sent, o=owner,
        )

    log.info("Price alerts: checked {n}, fired {f}.", n=len(alerts), f=fired)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001
        log.error("Price alert checker crashed: {e}", e=str(exc))
        sys.exit(1)
