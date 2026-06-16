from __future__ import annotations

from datetime import datetime, timezone


class _State:
    last_daily_digest = None


def test_daily_digest_first_run_waits_for_digest_window(monkeypatch):
    from scripts import daily_digest

    sent: list[str] = []
    monkeypatch.setattr(
        daily_digest,
        "send_daily_digest",
        lambda *_args, **_kwargs: sent.append("sent") or {"sent": True},
    )

    result = daily_digest.maybe_send_daily_digest(
        _State(),
        engine=None,
        dry_run=False,
        now=datetime(2026, 5, 20, 3, 15, tzinfo=timezone.utc),
    )

    assert result is None
    assert sent == []


def test_daily_digest_uses_persisted_db_timestamp_after_restart(monkeypatch):
    from scripts import daily_digest

    class Engine:
        pass

    persisted = datetime(2026, 5, 20, 8, 10, tzinfo=timezone.utc)
    monkeypatch.setattr(daily_digest, "_latest_digest_sent_at", lambda _engine: persisted)

    sent: list[str] = []
    monkeypatch.setattr(
        daily_digest,
        "send_daily_digest",
        lambda *_args, **_kwargs: sent.append("sent") or {"sent": True},
    )

    result = daily_digest.maybe_send_daily_digest(
        _State(),
        Engine(),
        dry_run=False,
        now=datetime(2026, 5, 20, 8, 30, tzinfo=timezone.utc),
    )

    assert result is None
    assert sent == []
