"""
GRID Solana Discord Scanner.

Connects to one or more Discord user accounts via the gateway (websocket)
and monitors servers/channels for Solana memecoin mentions. Uses the same
classifier as the Telegram scanner for consistent labelling.

Why user accounts instead of bot tokens:
- Most paid alpha/call servers don't allow bot tokens
- User accounts can read any channel the user has access to
- No server admin cooperation needed

Multi-user architecture:
- Each user provides their Discord user token
- All users' feeds are aggregated and cross-referenced
- Shared MentionTracker with the Telegram scanner (if both run)

Requires: pip install aiohttp websockets

Note: Using user tokens is against Discord TOS. This is for personal
research/automation on your own accounts. Use responsibly.

Series stored:
- DCSCAN:{label}:{token_address_short}   (per-token classification)
- DCSCAN:agg_genuine_count               (daily genuine signal count)
- DCSCAN:agg_scam_count                  (daily scam count)
- DCSCAN:agg_paid_ad_count               (daily paid ad count)
- DCSCAN:hot_tokens_count                (cross-channel hot tokens)

Source: Discord servers via user gateway connections
Schedule: Continuous (daemon) or periodic batch via pull_recent()
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import date, datetime, timezone
from typing import Any

import aiohttp
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

_DISCORD_API = "https://discord.com/api/v10"
_DISCORD_GATEWAY = "wss://gateway.discord.gg/?v=10&encoding=json"

# Max messages to fetch per channel in batch mode
_BATCH_LIMIT: int = 100

# Rate limit between channel reads
_CHANNEL_DELAY: float = 1.5  # Discord is strict on self-bot rate limits

# Heartbeat interval (ms) — updated from gateway HELLO
_DEFAULT_HEARTBEAT_MS: int = 41250


class DiscordUser:
    """Configuration for a single Discord user account.

    Attributes:
        token: Discord user token (NOT a bot token).
        label: Friendly label for logging.
        guild_ids: Optional list of guild (server) IDs to monitor.
                   Empty = monitor all guilds.
        channel_ids: Optional list of specific channel IDs to monitor.
    """

    def __init__(
        self,
        token: str,
        label: str = "",
        guild_ids: list[str] | None = None,
        channel_ids: list[str] | None = None,
    ) -> None:
        self.token = token
        self.label = label or token[:6] + "..."
        self.guild_ids = guild_ids or []
        self.channel_ids = channel_ids or []


class DiscordScanner(BasePuller):
    """Scans Discord servers for Solana memecoin signals.

    Connects to Discord using user tokens (NOT bot tokens) to read
    messages from alpha/call servers. Classifies each message and
    stores actionable signals in raw_series.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: source_catalog.id for this scanner.
        users: List of DiscordUser configs.
        tracker: Cross-channel mention tracker.
    """

    SOURCE_NAME: str = "Discord_Solana_Scanner"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://discord.com",
        "cost_tier": "FREE",
        "latency_class": "REALTIME",
        "pit_available": True,
        "revision_behavior": "APPEND_ONLY",
        "trust_score": "LOW",
        "priority_rank": 19,
    }

    def __init__(
        self,
        db_engine: Engine,
        users: list[DiscordUser] | None = None,
        tracker: MentionTracker | None = None,
    ) -> None:
        super().__init__(db_engine)
        self.users = users or self._load_users_from_config()
        # Share tracker with Telegram scanner if provided
        self.tracker = tracker or MentionTracker()
        log.info(
            "DiscordScanner initialised — source_id={sid}, users={n}",
            sid=self.source_id,
            n=len(self.users),
        )

    @staticmethod
    def _load_users_from_config() -> list[DiscordUser]:
        """Load Discord user configs from environment.

        Reads DISCORD_USERS env var as JSON array:
        [
            {"token": "user_token_here", "label": "main",
             "guild_ids": ["123", "456"],
             "channel_ids": ["789"]},
            ...
        ]

        Returns:
            List of DiscordUser objects.
        """
        raw = os.getenv("DISCORD_USERS", "")
        if not raw:
            # Fallback: single user from individual env var
            token = os.getenv("DISCORD_USER_TOKEN", "")
            if token:
                guild_ids_raw = os.getenv("DISCORD_GUILD_IDS", "")
                guild_ids = [g.strip() for g in guild_ids_raw.split(",") if g.strip()]
                channel_ids_raw = os.getenv("DISCORD_CHANNEL_IDS", "")
                channel_ids = [c.strip() for c in channel_ids_raw.split(",") if c.strip()]
                return [DiscordUser(
                    token=token,
                    label="primary",
                    guild_ids=guild_ids,
                    channel_ids=channel_ids,
                )]
            return []

        try:
            users_data = json.loads(raw)
            users: list[DiscordUser] = []
            for u in users_data:
                users.append(DiscordUser(
                    token=u["token"],
                    label=u.get("label", ""),
                    guild_ids=u.get("guild_ids", []),
                    channel_ids=u.get("channel_ids", []),
                ))
            return users
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            log.warning("Failed to parse DISCORD_USERS: {e}", e=str(exc))
            return []

    # ── HTTP API helpers ────────────────────────────────────────────

    @staticmethod
    def _auth_headers(token: str) -> dict[str, str]:
        """Build auth headers for Discord API requests.

        Parameters:
            token: Discord user token.

        Returns:
            Headers dict.
        """
        return {
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        }

    async def _api_get(
        self,
        session: aiohttp.ClientSession,
        path: str,
        token: str,
    ) -> dict[str, Any] | list[Any] | None:
        """Make a GET request to the Discord API.

        Parameters:
            session: aiohttp client session.
            path: API path (e.g., /users/@me/guilds).
            token: User token.

        Returns:
            JSON response or None on error.
        """
        url = f"{_DISCORD_API}{path}"
        try:
            async with session.get(
                url,
                headers=self._auth_headers(token),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 429:
                    # Rate limited — wait and skip
                    retry_after = (await resp.json()).get("retry_after", 5)
                    log.warning(
                        "Discord rate limited — waiting {s}s",
                        s=retry_after,
                    )
                    await asyncio.sleep(float(retry_after))
                    return None
                if resp.status != 200:
                    log.debug(
                        "Discord API {p} returned {s}",
                        p=path,
                        s=resp.status,
                    )
                    return None
                return await resp.json()
        except Exception as exc:
            log.warning("Discord API error: {p} — {e}", p=path, e=str(exc))
            return None

    # ── Batch pull ──────────────────────────────────────────────────

    async def _scan_user_channels(
        self,
        user: DiscordUser,
        limit: int = _BATCH_LIMIT,
    ) -> list[ClassifiedMessage]:
        """Scan channels for a single Discord user.

        Fetches recent messages from specified channels (or auto-discovers
        channels from guilds) and classifies them.

        Parameters:
            user: DiscordUser configuration.
            limit: Max messages per channel.

        Returns:
            List of classified messages.
        """
        messages: list[ClassifiedMessage] = []

        async with aiohttp.ClientSession() as session:
            # If specific channel IDs provided, use those
            channel_ids = list(user.channel_ids)

            # Otherwise discover channels from guilds
            if not channel_ids:
                guild_ids = list(user.guild_ids)

                # If no guild IDs, get all guilds
                if not guild_ids:
                    guilds = await self._api_get(
                        session, "/users/@me/guilds", user.token
                    )
                    if isinstance(guilds, list):
                        guild_ids = [g["id"] for g in guilds]
                        log.info(
                            "DC user {l}: discovered {n} guilds",
                            l=user.label,
                            n=len(guild_ids),
                        )

                # Get text channels from each guild
                for guild_id in guild_ids:
                    channels = await self._api_get(
                        session,
                        f"/guilds/{guild_id}/channels",
                        user.token,
                    )
                    if isinstance(channels, list):
                        for ch in channels:
                            # Type 0 = text channel
                            if ch.get("type") == 0:
                                channel_ids.append(ch["id"])
                    await asyncio.sleep(_CHANNEL_DELAY)

            log.info(
                "DC user {l}: scanning {n} channels",
                l=user.label,
                n=len(channel_ids),
            )

            # Read messages from each channel
            for ch_id in channel_ids:
                try:
                    msgs = await self._api_get(
                        session,
                        f"/channels/{ch_id}/messages?limit={min(limit, 100)}",
                        user.token,
                    )
                    if not isinstance(msgs, list):
                        continue

                    # Get channel info for naming
                    ch_info = await self._api_get(
                        session,
                        f"/channels/{ch_id}",
                        user.token,
                    )
                    ch_name = (
                        ch_info.get("name", ch_id)
                        if isinstance(ch_info, dict)
                        else ch_id
                    )
                    guild_id = (
                        ch_info.get("guild_id", "")
                        if isinstance(ch_info, dict)
                        else ""
                    )

                    for msg in msgs:
                        content = msg.get("content", "")
                        if not content:
                            continue
                        if not _has_crypto_content(content):
                            continue

                        author = msg.get("author", {})
                        timestamp_str = msg.get("timestamp", "")
                        timestamp = _parse_discord_timestamp(timestamp_str)

                        classified = classify_full_message(
                            text=content,
                            source="discord",
                            channel_name=f"{guild_id}/{ch_name}" if guild_id else ch_name,
                            channel_id=ch_id,
                            user_id=author.get("id", ""),
                            username=author.get("username", ""),
                            timestamp=timestamp,
                            raw_payload={
                                "message_id": msg.get("id", ""),
                                "guild_id": guild_id,
                                "channel_name": ch_name,
                                "attachments": len(msg.get("attachments", [])),
                                "embeds": len(msg.get("embeds", [])),
                                "scanner_user": user.label,
                            },
                        )

                        classified = self.tracker.add(classified)
                        messages.append(classified)

                    await asyncio.sleep(_CHANNEL_DELAY)

                except Exception as exc:
                    log.warning(
                        "DC: failed to scan channel {ch}: {e}",
                        ch=ch_id,
                        e=str(exc),
                    )

        return messages

    def _run_async(self, coro: Any) -> Any:
        """Run an async coroutine from sync context."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
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

                if msg.label == SignalLabel.NOISE:
                    counts["skipped"] += 1
                    continue
                if not msg.token_address:
                    counts["skipped"] += 1
                    continue

                addr_short = msg.token_address[:8]
                series_id = f"DCSCAN:{msg.label.value}:{addr_short}"

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
                agg_series = f"DCSCAN:agg_{label_name.lower()}_count"
                if not self._row_exists(agg_series, today, conn, dedup_hours=1):
                    self._insert_raw(
                        conn=conn,
                        series_id=agg_series,
                        obs_date=today,
                        value=float(counts[label_name]),
                    )

            # Store hot tokens count
            hot_tokens = self.tracker.get_hot_tokens()
            hot_series = "DCSCAN:hot_tokens_count"
            if not self._row_exists(hot_series, today, conn, dedup_hours=1):
                self._insert_raw(
                    conn=conn,
                    series_id=hot_series,
                    obs_date=today,
                    value=float(len(hot_tokens)),
                    raw_payload={"hot_tokens": hot_tokens[:20]},
                )

        return counts

    # ── Public API ──────────────────────────────────────────────────

    def pull_recent(
        self,
        limit: int = _BATCH_LIMIT,
    ) -> dict[str, Any]:
        """Batch-pull recent messages from all users' Discord channels.

        Parameters:
            limit: Max messages per channel per user.

        Returns:
            Summary dict.
        """
        if not self.users:
            log.warning("DiscordScanner: no users configured — skipping")
            return {
                "source": "discord",
                "status": "SKIPPED",
                "error": "No Discord users configured. Set DISCORD_USERS env var.",
                "signals_found": 0,
                "rows_inserted": 0,
            }

        log.info(
            "DiscordScanner: scanning {n} user accounts",
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
                    "DC user {l}: scanned {n} messages",
                    l=user.label,
                    n=len(user_messages),
                )
            except Exception as exc:
                log.error(
                    "DC user {l} scan failed: {e}",
                    l=user.label,
                    e=str(exc),
                )

        if not all_messages:
            return {
                "source": "discord",
                "status": "SUCCESS",
                "signals_found": 0,
                "rows_inserted": 0,
                "hot_tokens": [],
            }

        counts = self._store_classified_messages(all_messages)
        hot_tokens = self.tracker.get_hot_tokens()

        log.info(
            "DiscordScanner complete — {total} messages: "
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
            "source": "discord",
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
        """Run in real-time mode via Discord gateway websocket.

        Connects each user to the Discord gateway and listens for
        MESSAGE_CREATE events. Classifies and stores in real-time.

        Parameters:
            callback: Optional async callback(ClassifiedMessage).
        """
        import websockets

        if not self.users:
            log.error("No Discord users configured for realtime mode")
            return

        async def _user_gateway(user: DiscordUser) -> None:
            """Connect a single user to the Discord gateway."""
            while True:
                try:
                    async with websockets.connect(
                        _DISCORD_GATEWAY,
                        extra_headers={"User-Agent": "Mozilla/5.0"},
                    ) as ws:
                        # Receive HELLO
                        hello = json.loads(await ws.recv())
                        heartbeat_interval = (
                            hello.get("d", {}).get(
                                "heartbeat_interval", _DEFAULT_HEARTBEAT_MS
                            )
                            / 1000.0
                        )

                        # Send IDENTIFY
                        await ws.send(json.dumps({
                            "op": 2,
                            "d": {
                                "token": user.token,
                                "properties": {
                                    "os": "linux",
                                    "browser": "chrome",
                                    "device": "desktop",
                                },
                                "intents": 512 | 32768,  # GUILD_MESSAGES | MESSAGE_CONTENT
                            },
                        }))

                        log.info(
                            "DC gateway connected — user {l}",
                            l=user.label,
                        )

                        sequence: int | None = None

                        # Heartbeat task
                        async def heartbeat() -> None:
                            while True:
                                await asyncio.sleep(heartbeat_interval)
                                await ws.send(json.dumps({
                                    "op": 1, "d": sequence,
                                }))

                        hb_task = asyncio.create_task(heartbeat())

                        try:
                            async for raw_msg in ws:
                                data = json.loads(raw_msg)
                                sequence = data.get("s", sequence)

                                if data.get("t") != "MESSAGE_CREATE":
                                    continue

                                d = data.get("d", {})
                                content = d.get("content", "")
                                if not content or not _has_crypto_content(content):
                                    continue

                                # Filter by guild/channel if configured
                                guild_id = d.get("guild_id", "")
                                channel_id = d.get("channel_id", "")

                                if user.guild_ids and guild_id not in user.guild_ids:
                                    continue
                                if user.channel_ids and channel_id not in user.channel_ids:
                                    continue

                                author = d.get("author", {})
                                timestamp = _parse_discord_timestamp(
                                    d.get("timestamp", "")
                                )

                                classified = classify_full_message(
                                    text=content,
                                    source="discord",
                                    channel_name=f"{guild_id}/{channel_id}",
                                    channel_id=channel_id,
                                    user_id=author.get("id", ""),
                                    username=author.get("username", ""),
                                    timestamp=timestamp,
                                    raw_payload={
                                        "message_id": d.get("id", ""),
                                        "guild_id": guild_id,
                                        "scanner_user": user.label,
                                        "realtime": True,
                                    },
                                )

                                classified = self.tracker.add(classified)

                                if (
                                    classified.label != SignalLabel.NOISE
                                    and classified.token_address
                                ):
                                    self._store_classified_messages([classified])

                                if callback:
                                    await callback(classified)

                        finally:
                            hb_task.cancel()

                except Exception as exc:
                    log.warning(
                        "DC gateway disconnected — user {l}: {e}. Reconnecting...",
                        l=user.label,
                        e=str(exc),
                    )
                    await asyncio.sleep(5)

        await asyncio.gather(
            *(_user_gateway(user) for user in self.users)
        )


# ── Helpers ─────────────────────────────────────────────────────────

def _has_crypto_content(text: str) -> bool:
    """Quick pre-filter for crypto-related content."""
    lower = text.lower()
    indicators = (
        "$", "0x", "sol", "solana", "token", "pump", "moon",
        "dex", "swap", "mint", "rug", "gem", "mcap", "liquidity",
        "bonding", "raydium", "jupiter", "birdeye", "dexscreener",
        "pump.fun", "ca:", "contract",
    )
    return any(ind in lower for ind in indicators)


def _parse_discord_timestamp(ts: str) -> datetime:
    """Parse a Discord ISO timestamp string.

    Parameters:
        ts: ISO timestamp string from Discord API.

    Returns:
        datetime in UTC.
    """
    if not ts:
        return datetime.now(timezone.utc)
    try:
        # Discord uses ISO 8601 with timezone
        dt = datetime.fromisoformat(ts.replace("+00:00", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


if __name__ == "__main__":
    from db import get_engine

    scanner = DiscordScanner(db_engine=get_engine())
    results = scanner.pull_all()
    for r in results:
        print(json.dumps(r, indent=2, default=str))
