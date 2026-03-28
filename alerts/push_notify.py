"""
GRID Intelligence — Web Push notification system.

Sends PWA push notifications for trade recommendations, convergence alerts,
regime changes, red flags, and price alerts. Uses the Web Push protocol
with VAPID authentication.

Subscriptions are stored in PostgreSQL (push_subscriptions table).
Notification preferences are stored in notification_preferences table.

All sending is non-blocking (daemon thread) and never crashes the caller.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log


# ---------------------------------------------------------------------------
# Settings helper
# ---------------------------------------------------------------------------

def _get_settings() -> Any:
    from config import settings
    return settings


def _get_engine():
    from db import get_engine
    return get_engine()


# ---------------------------------------------------------------------------
# Database schema (idempotent creation)
# ---------------------------------------------------------------------------

_SCHEMA_APPLIED = False


def _ensure_schema() -> None:
    """Create push_subscriptions and notification_preferences tables if missing."""
    global _SCHEMA_APPLIED
    if _SCHEMA_APPLIED:
        return

    from sqlalchemy import text

    ddl = """
    CREATE TABLE IF NOT EXISTS push_subscriptions (
        id              SERIAL PRIMARY KEY,
        endpoint        TEXT NOT NULL UNIQUE,
        p256dh_key      TEXT NOT NULL,
        auth_key        TEXT NOT NULL,
        user_agent      TEXT DEFAULT '',
        created_at      TIMESTAMPTZ DEFAULT NOW(),
        last_success_at TIMESTAMPTZ,
        failure_count   INT DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS notification_preferences (
        id                  SERIAL PRIMARY KEY,
        endpoint            TEXT NOT NULL UNIQUE REFERENCES push_subscriptions(endpoint) ON DELETE CASCADE,
        trade_recommendations BOOLEAN DEFAULT TRUE,
        convergence_alerts    BOOLEAN DEFAULT TRUE,
        regime_changes        BOOLEAN DEFAULT TRUE,
        red_flags             BOOLEAN DEFAULT TRUE,
        price_alerts          BOOLEAN DEFAULT TRUE,
        price_alert_threshold FLOAT DEFAULT 5.0,
        created_at          TIMESTAMPTZ DEFAULT NOW(),
        updated_at          TIMESTAMPTZ DEFAULT NOW()
    );
    """
    try:
        engine = _get_engine()
        with engine.begin() as conn:
            for statement in ddl.strip().split(";"):
                statement = statement.strip()
                if statement:
                    conn.execute(text(statement))
        _SCHEMA_APPLIED = True
        log.debug("Push notification schema ensured")
    except Exception as exc:
        log.warning("Push notification schema creation failed: {e}", e=str(exc))


# ---------------------------------------------------------------------------
# Subscription management
# ---------------------------------------------------------------------------

def save_subscription(endpoint: str, p256dh_key: str, auth_key: str,
                      user_agent: str = "") -> bool:
    """Store a push subscription in the database.

    Returns True on success, False on failure.
    """
    _ensure_schema()
    from sqlalchemy import text

    try:
        engine = _get_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO push_subscriptions (endpoint, p256dh_key, auth_key, user_agent)
                VALUES (:endpoint, :p256dh, :auth, :ua)
                ON CONFLICT (endpoint)
                DO UPDATE SET p256dh_key = :p256dh, auth_key = :auth,
                              user_agent = :ua, failure_count = 0
            """), {
                "endpoint": endpoint,
                "p256dh": p256dh_key,
                "auth": auth_key,
                "ua": user_agent,
            })

            # Create default preferences for this subscription
            conn.execute(text("""
                INSERT INTO notification_preferences (endpoint)
                VALUES (:endpoint)
                ON CONFLICT (endpoint) DO NOTHING
            """), {"endpoint": endpoint})

        log.info("Push subscription saved: {e}", e=endpoint[:60])
        return True
    except Exception as exc:
        log.warning("Failed to save push subscription: {e}", e=str(exc))
        return False


def remove_subscription(endpoint: str) -> bool:
    """Remove a push subscription from the database.

    Returns True on success, False on failure.
    """
    _ensure_schema()
    from sqlalchemy import text

    try:
        engine = _get_engine()
        with engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM push_subscriptions WHERE endpoint = :endpoint"
            ), {"endpoint": endpoint})
        log.info("Push subscription removed: {e}", e=endpoint[:60])
        return True
    except Exception as exc:
        log.warning("Failed to remove push subscription: {e}", e=str(exc))
        return False


def get_all_subscriptions(category: str | None = None) -> list[dict]:
    """Retrieve all active push subscriptions.

    If category is provided, only returns subscriptions where that
    notification category is enabled in preferences.

    Categories: trade_recommendations, convergence_alerts, regime_changes,
                red_flags, price_alerts
    """
    _ensure_schema()
    from sqlalchemy import text

    try:
        engine = _get_engine()
        with engine.connect() as conn:
            if category and category in (
                "trade_recommendations", "convergence_alerts",
                "regime_changes", "red_flags", "price_alerts",
            ):
                rows = conn.execute(text(f"""
                    SELECT s.endpoint, s.p256dh_key, s.auth_key
                    FROM push_subscriptions s
                    JOIN notification_preferences p ON s.endpoint = p.endpoint
                    WHERE s.failure_count < 5
                      AND p.{category} = TRUE
                    ORDER BY s.created_at
                """)).fetchall()
            else:
                rows = conn.execute(text("""
                    SELECT endpoint, p256dh_key, auth_key
                    FROM push_subscriptions
                    WHERE failure_count < 5
                    ORDER BY created_at
                """)).fetchall()

            return [
                {"endpoint": r[0], "p256dh_key": r[1], "auth_key": r[2]}
                for r in rows
            ]
    except Exception as exc:
        log.warning("Failed to fetch push subscriptions: {e}", e=str(exc))
        return []


# ---------------------------------------------------------------------------
# Preference management
# ---------------------------------------------------------------------------

def get_preferences(endpoint: str) -> dict | None:
    """Get notification preferences for a subscription."""
    _ensure_schema()
    from sqlalchemy import text

    try:
        engine = _get_engine()
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT trade_recommendations, convergence_alerts, regime_changes,
                       red_flags, price_alerts, price_alert_threshold
                FROM notification_preferences
                WHERE endpoint = :endpoint
            """), {"endpoint": endpoint}).fetchone()

            if not row:
                return None

            return {
                "trade_recommendations": row[0],
                "convergence_alerts": row[1],
                "regime_changes": row[2],
                "red_flags": row[3],
                "price_alerts": row[4],
                "price_alert_threshold": row[5],
            }
    except Exception as exc:
        log.warning("Failed to fetch notification preferences: {e}", e=str(exc))
        return None


def update_preferences(endpoint: str, prefs: dict) -> bool:
    """Update notification preferences for a subscription.

    Accepted keys: trade_recommendations, convergence_alerts, regime_changes,
                   red_flags, price_alerts, price_alert_threshold
    """
    _ensure_schema()
    from sqlalchemy import text

    allowed = {
        "trade_recommendations", "convergence_alerts", "regime_changes",
        "red_flags", "price_alerts", "price_alert_threshold",
    }
    filtered = {k: v for k, v in prefs.items() if k in allowed}
    if not filtered:
        return False

    try:
        engine = _get_engine()
        set_clauses = ", ".join(f"{k} = :{k}" for k in filtered)
        filtered["endpoint"] = endpoint
        filtered["now"] = datetime.now(timezone.utc)

        with engine.begin() as conn:
            conn.execute(text(f"""
                UPDATE notification_preferences
                SET {set_clauses}, updated_at = :now
                WHERE endpoint = :endpoint
            """), filtered)

        log.info("Notification preferences updated for {e}", e=endpoint[:60])
        return True
    except Exception as exc:
        log.warning("Failed to update notification preferences: {e}", e=str(exc))
        return False


# ---------------------------------------------------------------------------
# Push sending
# ---------------------------------------------------------------------------

def _build_subscription_info(sub: dict) -> dict:
    """Build a pywebpush-compatible subscription info dict."""
    return {
        "endpoint": sub["endpoint"],
        "keys": {
            "p256dh": sub["p256dh_key"],
            "auth": sub["auth_key"],
        },
    }


def _record_failure(endpoint: str) -> None:
    """Increment failure count for a subscription."""
    from sqlalchemy import text

    try:
        engine = _get_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE push_subscriptions
                SET failure_count = failure_count + 1
                WHERE endpoint = :endpoint
            """), {"endpoint": endpoint})
    except Exception:
        pass


def _record_success(endpoint: str) -> None:
    """Record successful push delivery."""
    from sqlalchemy import text

    try:
        engine = _get_engine()
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE push_subscriptions
                SET last_success_at = NOW(), failure_count = 0
                WHERE endpoint = :endpoint
            """), {"endpoint": endpoint})
    except Exception:
        pass


def send_push(subscription: dict, title: str, body: str,
              tag: str = "", url: str = "/",
              require_interaction: bool = False) -> bool:
    """Send a push notification to a single subscription.

    Parameters:
        subscription: dict with endpoint, p256dh_key, auth_key
        title: Notification title
        body: Notification body text
        tag: Grouping tag (replaces older notifications with same tag)
        url: URL to open when notification is clicked
        require_interaction: If True, notification persists until dismissed

    Returns True on success, False on failure.
    """
    cfg = _get_settings()
    if not cfg.VAPID_PRIVATE_KEY or not cfg.VAPID_PUBLIC_KEY:
        log.debug("Push notification skipped — VAPID keys not configured")
        return False

    try:
        from pywebpush import webpush, WebPushException

        payload = json.dumps({
            "title": title,
            "body": body,
            "tag": tag,
            "url": url,
            "requireInteraction": require_interaction,
        })

        sub_info = _build_subscription_info(subscription)

        webpush(
            subscription_info=sub_info,
            data=payload,
            vapid_private_key=cfg.VAPID_PRIVATE_KEY,
            vapid_claims={"sub": cfg.VAPID_CLAIMS_EMAIL},
        )

        _record_success(subscription["endpoint"])
        return True

    except Exception as exc:
        exc_str = str(exc)
        # 410 Gone or 404 means subscription expired — remove it
        if "410" in exc_str or "404" in exc_str:
            log.info("Push subscription expired, removing: {e}", e=subscription["endpoint"][:60])
            remove_subscription(subscription["endpoint"])
        else:
            log.warning("Push notification failed: {e}", e=exc_str[:200])
            _record_failure(subscription["endpoint"])
        return False


def _send_push_in_thread(subscription: dict, title: str, body: str,
                         tag: str = "", url: str = "/") -> None:
    """Non-blocking push send in a daemon thread."""
    t = threading.Thread(
        target=send_push,
        args=(subscription, title, body, tag, url),
        daemon=True,
        name="grid-push",
    )
    t.start()


# ---------------------------------------------------------------------------
# Broadcast helpers — send to all subscribers of a category
# ---------------------------------------------------------------------------

def broadcast_push(title: str, body: str, tag: str = "", url: str = "/",
                   category: str | None = None) -> int:
    """Send a push notification to all subscribers of a category.

    Parameters:
        title: Notification title
        body: Notification body text
        tag: Grouping tag
        url: URL to open on click
        category: Optional preference category to filter subscribers

    Returns number of notifications queued.
    """
    subs = get_all_subscriptions(category=category)
    for sub in subs:
        _send_push_in_thread(sub, title, body, tag, url)
    if subs:
        log.info("Push broadcast: {n} notifications queued — {t}", n=len(subs), t=title)
    return len(subs)


# ---------------------------------------------------------------------------
# Event-specific notification senders
# ---------------------------------------------------------------------------

def notify_trade_recommendation(ticker: str, direction: str, strike: str = "",
                                expiry: str = "") -> int:
    """Push notification for a new trade recommendation."""
    detail = f"{strike} {expiry}".strip()
    body = f"{direction} {ticker}"
    if detail:
        body += f" — {detail}"

    return broadcast_push(
        title=f"Trade Rec: {ticker} {direction}",
        body=body,
        tag=f"trade-{ticker}",
        url="/options",
        category="trade_recommendations",
    )


def notify_convergence_alert(description: str, severity: str = "info") -> int:
    """Push notification for a convergence/signal alignment alert."""
    return broadcast_push(
        title="Convergence Alert",
        body=description[:200],
        tag="convergence",
        url="/signals",
        category="convergence_alerts",
    )


def notify_regime_change(from_regime: str, to_regime: str,
                         confidence: float = 0.0) -> int:
    """Push notification for a regime change event."""
    pct = f"{confidence:.0%}" if confidence else ""
    body = f"{from_regime} -> {to_regime}"
    if pct:
        body += f" ({pct} confidence)"

    return broadcast_push(
        title="Regime Change",
        body=body,
        tag="regime-change",
        url="/regime",
        category="regime_changes",
    )


def notify_red_flag(title: str, description: str) -> int:
    """Push notification for a red flag / warning event."""
    return broadcast_push(
        title=f"Red Flag: {title}",
        body=description[:200],
        tag="red-flag",
        url="/",
        category="red_flags",
    )


def notify_price_alert(ticker: str, price: float, change_pct: float,
                       threshold: float = 5.0) -> int:
    """Push notification for a significant price move.

    Only sends to subscribers whose price_alert_threshold is <= the actual move.
    """
    direction = "up" if change_pct > 0 else "down"
    body = f"{ticker} {direction} {abs(change_pct):.1f}% to ${price:.2f}"

    # For price alerts, we filter by threshold per-subscriber
    from sqlalchemy import text

    _ensure_schema()
    try:
        engine = _get_engine()
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT s.endpoint, s.p256dh_key, s.auth_key
                FROM push_subscriptions s
                JOIN notification_preferences p ON s.endpoint = p.endpoint
                WHERE s.failure_count < 5
                  AND p.price_alerts = TRUE
                  AND p.price_alert_threshold <= :change_pct
            """), {"change_pct": abs(change_pct)}).fetchall()

        count = 0
        for r in rows:
            sub = {"endpoint": r[0], "p256dh_key": r[1], "auth_key": r[2]}
            _send_push_in_thread(sub, f"Price Alert: {ticker}", body,
                                 tag=f"price-{ticker}", url="/")
            count += 1
        return count
    except Exception as exc:
        log.warning("Price alert push failed: {e}", e=str(exc))
        return 0


# ---------------------------------------------------------------------------
# Integration hooks — call from existing alert pathways
# ---------------------------------------------------------------------------

def integrate_with_email_alerts() -> None:
    """Monkey-patch email alert functions to also send push notifications.

    Called once at startup to add push notifications alongside emails.
    This is non-invasive — email alerts continue to work as before.
    """
    import alerts.email as email_mod

    # Wrap regime change alerts
    _original_regime = email_mod.alert_on_regime_change

    def _regime_with_push(from_regime: str, to_regime: str, confidence: float) -> None:
        _original_regime(from_regime, to_regime, confidence)
        notify_regime_change(from_regime, to_regime, confidence)

    email_mod.alert_on_regime_change = _regime_with_push

    # Wrap 100x opportunity alerts
    _original_100x = email_mod.alert_on_100x_opportunity

    def _100x_with_push(ticker: str, score: float, direction: str, thesis: str) -> None:
        _original_100x(ticker, score, direction, thesis)
        notify_trade_recommendation(ticker, direction)

    email_mod.alert_on_100x_opportunity = _100x_with_push

    # Wrap failure alerts as red flags
    _original_failure = email_mod.alert_on_failure

    def _failure_with_push(source: str, error: str) -> None:
        _original_failure(source, error)
        notify_red_flag(f"{source} Failure", error[:150])

    email_mod.alert_on_failure = _failure_with_push

    log.info("Push notifications integrated with email alert system")
