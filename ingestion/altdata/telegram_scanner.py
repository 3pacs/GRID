"""
GRID Solana Telegram Scanner.

Connects to one or more Telegram user accounts via Telethon (MTProto)
and passively monitors channels/groups for Solana memecoin mentions.
Each message is classified by the memecoin_classifier and stored in
raw_series for downstream analysis.

Multi-user architecture:
- Each user provides their own Telegram API ID + hash + phone
- Sessions persist to disk (one auth per phone, then runs forever)
- All users' channel feeds are aggregated and cross-referenced
- When the same token appears across independent users' channels,
  the confidence multiplies

Requires: pip install telethon

Series stored:
- TGSCAN:{label}:{token_address_short}  (per-token classification)
- TGSCAN:agg_genuine_count              (daily genuine signal count)
- TGSCAN:agg_scam_count                 (daily scam count)
- TGSCAN:agg_paid_ad_count              (daily paid ad count)
- TGSCAN:hot_tokens_count               (cross-channel hot tokens)

Source: Telegram channels via user accounts (Telethon MTProto)
Schedule: Continuous (daemon) or periodic batch via pull_recent()
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

from ingestion.altdata.memecoin_classifier import (
    ClassifiedMessage,
    MentionTracker,
    SignalLabel,
    classify_full_message,
    extract_token_addresses,
)

# ── Configuration ────────────────────────────────────────────────────

# Session files stored here (one per phone number)
_SESSION_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "tg_sessions"

# Max messages to scan per channel in batch mode
_BATCH_LIMIT: int = 100

# Rate limit between channel reads (seconds)
_CHANNEL_DELAY: float = 1.0

# Default channels to monitor (Solana alpha channels — user can override)
DEFAULT_CHANNELS: list[str] = [
    # Popular Solana call channels (add your own)
    # These are examples — actual channel usernames will vary
]

# Known Solana trading bot channels (higher signal weight)
KNOWN_BOT_CHANNELS: set[str] = {
    "maestaborobot",
    "bonaborobot",
    "solaborobot",
    "trojanaborobot",
}


class TelegramUser:
    """Configuration for a single Telegram user account.

    Attributes:
        api_id: Telegram API ID (from my.telegram.org).
        api_hash: Telegram API hash.
        phone: Phone number (with country code).
        session_name: Name for the session file.
        channels: Channels this user monitors (empty = all joined groups).
    """

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        phone: str,
        session_name: str = "",
        channels: list[str] | None = None,
    ) -> None:
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self.session_name = session_name or f"tg_{phone.replace('+', '')}"
        self.channels = channels or []


class TelegramScanner(BasePuller):
    """Scans Telegram channels for Solana memecoin signals.

    Connects to one or more Telegram user accounts, reads messages
    from specified channels, classifies them, and stores results
    in raw_series.

    Multi-user: each TelegramUser gets its own Telethon client.
    Cross-referencing happens in the shared MentionTracker.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: source_catalog.id for this scanner.
        users: List of TelegramUser configs.
        tracker: Cross-channel mention tracker.
    """

    SOURCE_NAME: str = "Telegram_Solana_Scanner"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://telegram.org",
        "cost_tier": "FREE",
        "latency_class": "REALTIME",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "LOW",
        "priority_rank": 18,
    }

    def __init__(
        self,
        db_engine: Engine,
        users: list[TelegramUser] | None = None,
    ) -> None:
        super().__init__(db_engine)
        self.users = users or self._load_users_from_config()
        self.tracker = MentionTracker()
        log.info(
            "TelegramScanner initialised — source_id={sid}, users={n}",
            sid=self.source_id,
            n=len(self.users),
        )

    @staticmethod
    def _load_users_from_config() -> list[TelegramUser]:
        """Load Telegram user configs from environment/config.

        Reads TELEGRAM_USERS env var as JSON array:
        [
            {"api_id": 12345, "api_hash": "abc", "phone": "+1234567890",
             "channels": ["channel1", "channel2"]},
            ...
        ]

        Returns:
            List of TelegramUser objects.
        """
        raw = os.getenv("TELEGRAM_USERS", "")
        if not raw:
            # Fallback: single user from individual env vars
            api_id = os.getenv("TELEGRAM_API_ID", "")
            api_hash = os.getenv("TELEGRAM_API_HASH", "")
            phone = os.getenv("TELEGRAM_PHONE", "")
            if api_id and api_hash and phone:
                channels_raw = os.getenv("TELEGRAM_CHANNELS", "")
                channels = [c.strip() for c in channels_raw.split(",") if c.strip()]
                return [TelegramUser(
                    api_id=int(api_id),
                    api_hash=api_hash,
                    phone=phone,
                    channels=channels,
                )]
            return []

        try:
            users_data = json.loads(raw)
            users: list[TelegramUser] = []
            for u in users_data:
                users.append(TelegramUser(
                    api_id=int(u["api_id"]),
                    api_hash=u["api_hash"],
                    phone=u["phone"],
                    session_name=u.get("session_name", ""),
                    channels=u.get("channels", []),
                ))
            return users
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            log.warning("Failed to parse TELEGRAM_USERS: {e}", e=str(exc))
            return []

    def _ensure_session_dir(self) -> Path:
        """Create session directory if it doesn't exist."""
        _SESSION_DIR.mkdir(parents=True, exist_ok=True)
        return _SESSION_DIR

    # ── Batch pull (non-realtime) ───────────────────────────────────

    async def _scan_user_channels(
        self,
        user: TelegramUser,
        limit: int = _BATCH_LIMIT,
    ) -> list[ClassifiedMessage]:
        """Scan all channels for a single Telegram user.

        Connects to Telegram, reads recent messages from each channel,
        classifies them, and returns classified messages.

        Parameters:
            user: TelegramUser configuration.
            limit: Max messages per channel.

        Returns:
            List of classified messages.
        """
        try:
            from telethon import TelegramClient
            from telethon.tl.types import Channel, Chat
        except ImportError:
            log.error(
                "Telethon not installed. Run: pip install telethon"
            )
            return []

        session_dir = self._ensure_session_dir()
        session_path = str(session_dir / user.session_name)

        client = TelegramClient(session_path, user.api_id, user.api_hash)
        messages: list[ClassifiedMessage] = []

        try:
            await client.start(phone=user.phone)
            log.info(
                "TG connected — user={p}, session={s}",
                p=user.phone[-4:],  # Only log last 4 digits
                s=user.session_name,
            )

            # Get channels to scan
            channels_to_scan = user.channels
            if not channels_to_scan:
                # Auto-discover: scan all joined groups/channels
                async for dialog in client.iter_dialogs():
                    if isinstance(dialog.entity, (Channel, Chat)):
                        channels_to_scan.append(dialog.entity.username or str(dialog.entity.id))
                log.info(
                    "TG auto-discovered {n} channels for user {p}",
                    n=len(channels_to_scan),
                    p=user.phone[-4:],
                )

            for channel_ref in channels_to_scan:
                try:
                    entity = await client.get_entity(channel_ref)
                    channel_name = getattr(entity, "title", str(channel_ref))
                    channel_id = str(getattr(entity, "id", channel_ref))

                    async for msg in client.iter_messages(entity, limit=limit):
                        if not msg.text:
                            continue

                        # Only process messages with potential token references
                        if not _has_crypto_content(msg.text):
                            continue

                        classified = classify_full_message(
                            text=msg.text,
                            source="telegram",
                            channel_name=channel_name,
                            channel_id=channel_id,
                            user_id=str(msg.sender_id or ""),
                            username=_get_sender_name(msg),
                            timestamp=msg.date.replace(tzinfo=timezone.utc)
                            if msg.date
                            else None,
                            raw_payload={
                                "message_id": msg.id,
                                "views": msg.views,
                                "forwards": msg.forwards,
                                "reply_to": msg.reply_to_msg_id if msg.reply_to else None,
                                "scanner_user": user.phone[-4:],
                            },
                        )

                        # Run through cross-channel tracker
                        classified = self.tracker.add(classified)
                        messages.append(classified)

                    await asyncio.sleep(_CHANNEL_DELAY)

                except Exception as exc:
                    log.warning(
                        "TG: failed to scan channel {ch}: {e}",
                        ch=channel_ref,
                        e=str(exc),
                    )
                    continue

        except Exception as exc:
            log.error(
                "TG: client error for user {p}: {e}",
                p=user.phone[-4:],
                e=str(exc),
            )
        finally:
            await client.disconnect()

        return messages

    def _run_async(self, coro: Any) -> Any:
        """Run an async coroutine from sync context."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're already in an async context, create a new loop
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    return pool.submit(asyncio.run, coro).result()
            return loop.run_until_complete(coro)
        except RuntimeError:
            return asyncio.run(coro)

    def _store_classified_messages(
        self,
        messages: list[ClassifiedMessage],
    ) -> dict[str, int]:
        """Store classified messages in raw_series.

        Parameters:
            messages: List of classified messages.

        Returns:
            Dict with counts per label.
        """
        today = date.today()
        counts: dict[str, int] = {
            "GENUINE": 0,
            "PAID_AD": 0,
            "SCAM": 0,
            "NOISE": 0,
            "stored": 0,
            "skipped": 0,
        }

        with self.engine.begin() as conn:
            for msg in messages:
                counts[msg.label.value] = counts.get(msg.label.value, 0) + 1

                # Only store non-noise signals (noise would flood the DB)
                if msg.label == SignalLabel.NOISE:
                    counts["skipped"] += 1
                    continue

                # Skip if no token address (nothing actionable)
                if not msg.token_address:
                    counts["skipped"] += 1
                    continue

                addr_short = msg.token_address[:8]
                series_id = f"TGSCAN:{msg.label.value}:{addr_short}"

                if self._row_exists(series_id, today, conn, dedup_hours=4):
                    counts["skipped"] += 1
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=series_id,
                    obs_date=today,
                    value=msg.confidence,
                    raw_payload={
                        "source": msg.source,
                        "channel_name": msg.channel_name,
                        "channel_id": msg.channel_id,
                        "user_id": msg.user_id,
                        "username": msg.username,
                        "token_address": msg.token_address,
                        "token_symbol": msg.token_symbol,
                        "label": msg.label.value,
                        "confidence": msg.confidence,
                        "scores": msg.scores,
                        "mention_count": msg.mention_count,
                        "unique_sources": msg.unique_sources,
                        "first_seen": msg.first_seen.isoformat()
                        if msg.first_seen
                        else None,
                        "message_hash": msg.message_hash,
                        "timestamp": msg.timestamp.isoformat(),
                    },
                )
                counts["stored"] += 1

            # Store daily aggregates
            for label_name in ("GENUINE", "PAID_AD", "SCAM"):
                agg_series = f"TGSCAN:agg_{label_name.lower()}_count"
                if not self._row_exists(agg_series, today, conn, dedup_hours=1):
                    self._insert_raw(
                        conn=conn,
                        series_id=agg_series,
                        obs_date=today,
                        value=float(counts[label_name]),
                    )

            # Store hot tokens count
            hot_tokens = self.tracker.get_hot_tokens()
            hot_series = "TGSCAN:hot_tokens_count"
            if not self._row_exists(hot_series, today, conn, dedup_hours=1):
                self._insert_raw(
                    conn=conn,
                    series_id=hot_series,
                    obs_date=today,
                    value=float(len(hot_tokens)),
                    raw_payload={"hot_tokens": hot_tokens[:20]},  # Top 20
                )

        return counts

    # ── Public API ──────────────────────────────────────────────────

    def pull_recent(
        self,
        limit: int = _BATCH_LIMIT,
    ) -> dict[str, Any]:
        """Batch-pull recent messages from all users' Telegram channels.

        Scans each user's channels, classifies messages, cross-references,
        and stores results.

        Parameters:
            limit: Max messages per channel per user.

        Returns:
            Summary dict.
        """
        if not self.users:
            log.warning("TelegramScanner: no users configured — skipping")
            return {
                "source": "telegram",
                "status": "SKIPPED",
                "error": "No Telegram users configured. Set TELEGRAM_USERS env var.",
                "signals_found": 0,
                "rows_inserted": 0,
            }

        log.info(
            "TelegramScanner: scanning {n} user accounts",
            n=len(self.users),
        )

        all_messages: list[ClassifiedMessage] = []

        for user in self.users:
            try:
                user_messages = self._run_async(
                    self._scan_user_channels(user, limit=limit)
                )
                all_messages.extend(user_messages)
                log.info(
                    "TG user {p}: scanned {n} messages",
                    p=user.phone[-4:],
                    n=len(user_messages),
                )
            except Exception as exc:
                log.error(
                    "TG user {p} scan failed: {e}",
                    p=user.phone[-4:],
                    e=str(exc),
                )

        if not all_messages:
            return {
                "source": "telegram",
                "status": "SUCCESS",
                "signals_found": 0,
                "rows_inserted": 0,
                "hot_tokens": [],
            }

        # Store results
        counts = self._store_classified_messages(all_messages)
        hot_tokens = self.tracker.get_hot_tokens()

        log.info(
            "TelegramScanner complete — {total} messages: "
            "{g} genuine, {p} paid, {s} scam, {n} noise, "
            "{st} stored, {hot} hot tokens",
            total=len(all_messages),
            g=counts["GENUINE"],
            p=counts["PAID_AD"],
            s=counts["SCAM"],
            n=counts["NOISE"],
            st=counts["stored"],
            hot=len(hot_tokens),
        )

        return {
            "source": "telegram",
            "status": "SUCCESS",
            "signals_found": len(all_messages) - counts["NOISE"],
            "rows_inserted": counts["stored"],
            "classification_counts": {
                k: v for k, v in counts.items()
                if k not in ("stored", "skipped")
            },
            "hot_tokens": hot_tokens[:10],
        }

    def pull_all(self) -> list[dict[str, Any]]:
        """Main entry point for scheduler integration.

        Returns:
            List with single result dict.
        """
        result = self.pull_recent()
        return [result]

    # ── Real-time daemon mode ───────────────────────────────────────

    async def run_realtime(
        self,
        callback: Any | None = None,
    ) -> None:
        """Run in real-time mode — listens to new messages as they arrive.

        This is the preferred mode for production. Connects all user
        accounts and handles new messages via event handlers.

        Parameters:
            callback: Optional async callback(ClassifiedMessage) for
                      each classified message (e.g., push to WebSocket).
        """
        try:
            from telethon import TelegramClient, events
        except ImportError:
            log.error("Telethon not installed. Run: pip install telethon")
            return

        if not self.users:
            log.error("No Telegram users configured for realtime mode")
            return

        session_dir = self._ensure_session_dir()
        clients: list[TelegramClient] = []

        for user in self.users:
            session_path = str(session_dir / user.session_name)
            client = TelegramClient(session_path, user.api_id, user.api_hash)

            @client.on(events.NewMessage)
            async def handler(event: Any, _user: TelegramUser = user) -> None:
                """Handle incoming Telegram messages in real-time."""
                if not event.text:
                    return
                if not _has_crypto_content(event.text):
                    return

                try:
                    chat = await event.get_chat()
                    sender = await event.get_sender()

                    classified = classify_full_message(
                        text=event.text,
                        source="telegram",
                        channel_name=getattr(chat, "title", "DM"),
                        channel_id=str(chat.id),
                        user_id=str(sender.id) if sender else "",
                        username=_get_display_name(sender) if sender else "",
                        timestamp=event.date.replace(tzinfo=timezone.utc)
                        if event.date
                        else None,
                        raw_payload={
                            "message_id": event.id,
                            "scanner_user": _user.phone[-4:],
                            "realtime": True,
                        },
                    )

                    classified = self.tracker.add(classified)

                    # Store if actionable
                    if classified.label != SignalLabel.NOISE and classified.token_address:
                        self._store_classified_messages([classified])

                    if callback:
                        await callback(classified)

                except Exception as exc:
                    log.debug("TG realtime handler error: {e}", e=str(exc))

            await client.start(phone=user.phone)
            clients.append(client)
            log.info(
                "TG realtime connected — user {p}",
                p=user.phone[-4:],
            )

        log.info(
            "TelegramScanner realtime mode active — {n} clients",
            n=len(clients),
        )

        # Run until interrupted
        try:
            await asyncio.gather(
                *(client.run_until_disconnected() for client in clients)
            )
        finally:
            for client in clients:
                await client.disconnect()


# ── Helpers ─────────────────────────────────────────────────────────

def _has_crypto_content(text: str) -> bool:
    """Quick check if a message might contain crypto/token references.

    Fast pre-filter to avoid running full classification on every message.

    Parameters:
        text: Message text.

    Returns:
        True if the message likely contains crypto content.
    """
    lower = text.lower()
    # Check for common crypto indicators
    indicators = (
        "$", "0x", "sol", "solana", "token", "pump", "moon",
        "dex", "swap", "mint", "rug", "gem", "mcap", "liquidity",
        "bonding", "raydium", "jupiter", "birdeye", "dexscreener",
        "pump.fun", "ca:", "contract",
    )
    return any(ind in lower for ind in indicators)


def _get_sender_name(msg: Any) -> str:
    """Extract sender display name from a Telethon message."""
    if hasattr(msg, "sender") and msg.sender:
        return _get_display_name(msg.sender)
    return ""


def _get_display_name(entity: Any) -> str:
    """Get display name from a Telethon entity."""
    if hasattr(entity, "first_name"):
        name = entity.first_name or ""
        if hasattr(entity, "last_name") and entity.last_name:
            name += f" {entity.last_name}"
        return name.strip()
    if hasattr(entity, "title"):
        return entity.title or ""
    if hasattr(entity, "username"):
        return entity.username or ""
    return ""


if __name__ == "__main__":
    from db import get_engine

    scanner = TelegramScanner(db_engine=get_engine())
    results = scanner.pull_all()
    for r in results:
        print(json.dumps(r, indent=2, default=str))
