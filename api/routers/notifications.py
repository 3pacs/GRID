"""
GRID Intelligence — Push notification API endpoints.

Handles subscription management and notification preferences for PWA
push notifications.

Routes:
    POST   /api/v1/notifications/subscribe     — Register a push subscription
    DELETE /api/v1/notifications/unsubscribe    — Remove a push subscription
    GET    /api/v1/notifications/preferences    — Get notification preferences
    PUT    /api/v1/notifications/preferences    — Update notification preferences
    GET    /api/v1/notifications/vapid-key      — Get the public VAPID key
    POST   /api/v1/notifications/test           — Send a test push notification
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/v1/notifications", tags=["notifications"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class SubscriptionKeys(BaseModel):
    p256dh: str
    auth: str


class SubscribeRequest(BaseModel):
    endpoint: str
    keys: SubscriptionKeys
    user_agent: str = ""


class UnsubscribeRequest(BaseModel):
    endpoint: str


class PreferencesUpdate(BaseModel):
    endpoint: str
    trade_recommendations: bool | None = None
    convergence_alerts: bool | None = None
    regime_changes: bool | None = None
    red_flags: bool | None = None
    price_alerts: bool | None = None
    price_alert_threshold: float | None = Field(None, ge=0.1, le=50.0)


class PreferencesQuery(BaseModel):
    endpoint: str


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/vapid-key")
async def get_vapid_key():
    """Return the public VAPID key for push subscription."""
    from config import settings

    if not settings.VAPID_PUBLIC_KEY:
        raise HTTPException(
            status_code=503,
            detail="Push notifications not configured — VAPID keys missing",
        )

    return {"vapid_public_key": settings.VAPID_PUBLIC_KEY}


@router.post("/subscribe")
async def subscribe(req: SubscribeRequest):
    """Register a push notification subscription."""
    from alerts.push_notify import save_subscription

    success = save_subscription(
        endpoint=req.endpoint,
        p256dh_key=req.keys.p256dh,
        auth_key=req.keys.auth,
        user_agent=req.user_agent,
    )

    if not success:
        raise HTTPException(status_code=500, detail="Failed to save subscription")

    return {"status": "subscribed", "endpoint": req.endpoint[:60] + "..."}


@router.delete("/unsubscribe")
async def unsubscribe(req: UnsubscribeRequest):
    """Remove a push notification subscription."""
    from alerts.push_notify import remove_subscription

    success = remove_subscription(endpoint=req.endpoint)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to remove subscription")

    return {"status": "unsubscribed"}


@router.get("/preferences")
async def get_preferences(endpoint: str):
    """Get notification preferences for a subscription endpoint."""
    from alerts.push_notify import get_preferences as _get_prefs

    prefs = _get_prefs(endpoint)
    if prefs is None:
        raise HTTPException(status_code=404, detail="Subscription not found")

    return prefs


@router.put("/preferences")
async def update_preferences(req: PreferencesUpdate):
    """Update notification preferences for a subscription endpoint."""
    from alerts.push_notify import update_preferences as _update_prefs

    prefs = {k: v for k, v in req.model_dump().items()
             if k != "endpoint" and v is not None}

    if not prefs:
        raise HTTPException(status_code=400, detail="No preferences to update")

    success = _update_prefs(endpoint=req.endpoint, prefs=prefs)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to update preferences")

    return {"status": "updated", "preferences": prefs}


@router.post("/test")
async def test_push(req: SubscribeRequest):
    """Send a test push notification to verify the subscription works."""
    from alerts.push_notify import send_push

    sub = {
        "endpoint": req.endpoint,
        "p256dh_key": req.keys.p256dh,
        "auth_key": req.keys.auth,
    }

    success = send_push(
        subscription=sub,
        title="GRID Test",
        body="Push notifications are working.",
        tag="test",
        url="/settings",
    )

    if not success:
        raise HTTPException(
            status_code=500,
            detail="Test push failed — check VAPID configuration",
        )

    return {"status": "sent"}
