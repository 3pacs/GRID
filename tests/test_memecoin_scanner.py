"""
Tests for the Solana memecoin scanner pipeline.

Covers:
- memecoin_classifier: pattern classification, token extraction, cross-ref
- telegram_scanner: config loading, message storage, batch pull
- discord_scanner: config loading, message storage, batch pull
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.memecoin_classifier import (
    ClassifiedMessage,
    MentionTracker,
    SignalLabel,
    classify_full_message,
    classify_message,
    extract_token_addresses,
    extract_token_symbol,
    message_hash,
)


# ═══════════════════════════════════════════════════════════════════
# memecoin_classifier tests
# ═══════════════════════════════════════════════════════════════════


class TestTokenExtraction:
    """Test Solana token address and symbol extraction."""

    def test_extract_address_from_dexscreener_link(self) -> None:
        text = "check this out https://dexscreener.com/solana/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
        addrs = extract_token_addresses(text)
        assert len(addrs) >= 1
        assert "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr" in addrs

    def test_extract_address_from_pumpfun_link(self) -> None:
        text = "new gem https://pump.fun/coin/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
        addrs = extract_token_addresses(text)
        assert "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr" in addrs

    def test_extract_address_from_birdeye_link(self) -> None:
        text = "birdeye.so/token/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
        addrs = extract_token_addresses(text)
        assert "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr" in addrs

    def test_no_addresses_in_plain_text(self) -> None:
        text = "I think solana is going up this week"
        addrs = extract_token_addresses(text)
        assert len(addrs) == 0

    def test_extract_ticker_symbol(self) -> None:
        assert extract_token_symbol("just bought $BONK") == "BONK"
        assert extract_token_symbol("$WIF to the moon") == "WIF"
        assert extract_token_symbol("no ticker here") == ""

    def test_extract_ticker_case_insensitive(self) -> None:
        assert extract_token_symbol("$bonk is pumping") == "BONK"


class TestClassification:
    """Test message classification logic."""

    def test_scam_detected(self) -> None:
        text = "Send SOL to this address and double your money! Guaranteed profit!"
        label, confidence, scores = classify_message(text)
        assert label == SignalLabel.SCAM
        assert confidence >= 0.5

    def test_paid_ad_detected(self) -> None:
        text = (
            "🚀🚀🚀 NEXT 100X GEM! Just launched! SAFU, LP burned, dev doxxed! "
            "Buy now before it moons! Not financial advice DYOR! "
            "Contract address: abc123 Tokenomics: 1B supply Buy tax: 0% Sell tax: 0%"
        )
        label, confidence, scores = classify_message(text)
        assert label == SignalLabel.PAID_AD
        assert confidence >= 0.4

    def test_genuine_signal(self) -> None:
        text = (
            "Unusual volume spike on this token. Whale wallet just bought $50K. "
            "On-chain data shows holder count growing rapidly. "
            "Bonding curve nearly graduated."
        )
        label, confidence, scores = classify_message(text)
        assert label == SignalLabel.GENUINE
        assert confidence >= 0.3

    def test_noise_default(self) -> None:
        text = "gm"
        label, confidence, scores = classify_message(text)
        assert label == SignalLabel.NOISE

    def test_shill_with_emojis(self) -> None:
        text = "🚀🚀🚀🔥🔥🔥 MOON MOON MOON 100X EASY 🚀🚀🚀 BUY NOW!!! LAST CHANCE!!!"
        label, confidence, scores = classify_message(text)
        assert label in (SignalLabel.PAID_AD, SignalLabel.SCAM)

    def test_connect_wallet_scam(self) -> None:
        text = "Connect wallet to claim your free tokens! Limited whitelist spots!"
        label, confidence, scores = classify_message(text)
        assert label == SignalLabel.SCAM

    def test_classification_returns_scores(self) -> None:
        text = "some random token mention"
        label, confidence, scores = classify_message(text)
        assert "shill_score" in scores
        assert "scam_score" in scores
        assert "genuine_score" in scores
        assert 0 <= confidence <= 1


class TestClassifyFullMessage:
    """Test the full message classification pipeline."""

    def test_returns_classified_message(self) -> None:
        msg = classify_full_message(
            text="whale buying $BONK https://dexscreener.com/solana/7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
            source="telegram",
            channel_name="sol_alpha",
            channel_id="123",
            user_id="456",
            username="trader",
        )
        assert isinstance(msg, ClassifiedMessage)
        assert msg.source == "telegram"
        assert msg.token_symbol == "BONK"
        assert msg.token_address == "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr"
        assert msg.message_hash != ""

    def test_timestamp_defaults_to_utc(self) -> None:
        msg = classify_full_message(
            text="test",
            source="discord",
            channel_name="test",
            channel_id="1",
            user_id="2",
            username="user",
        )
        assert msg.timestamp.tzinfo == timezone.utc


class TestMessageHash:
    """Test message deduplication hashing."""

    def test_same_input_same_hash(self) -> None:
        h1 = message_hash("hello world", "ch1")
        h2 = message_hash("hello world", "ch1")
        assert h1 == h2

    def test_different_channel_different_hash(self) -> None:
        h1 = message_hash("hello world", "ch1")
        h2 = message_hash("hello world", "ch2")
        assert h1 != h2

    def test_hash_length(self) -> None:
        h = message_hash("test", "ch")
        assert len(h) == 12


class TestMentionTracker:
    """Test cross-channel mention tracking and correlation."""

    def _make_msg(
        self,
        token_address: str = "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
        channel_id: str = "ch1",
        user_id: str = "u1",
        label: SignalLabel = SignalLabel.GENUINE,
        text: str = "unique message",
    ) -> ClassifiedMessage:
        return ClassifiedMessage(
            source="telegram",
            channel_name=f"channel_{channel_id}",
            channel_id=channel_id,
            user_id=user_id,
            username=f"user_{user_id}",
            message_text=text,
            timestamp=datetime.now(timezone.utc),
            token_address=token_address,
            token_symbol="TEST",
            label=label,
            confidence=0.5,
            scores={},
            message_hash=message_hash(text, channel_id),
        )

    def test_single_mention(self) -> None:
        tracker = MentionTracker()
        msg = self._make_msg()
        result = tracker.add(msg)
        assert result.mention_count == 1
        assert result.unique_sources == 1

    def test_cross_channel_boost(self) -> None:
        tracker = MentionTracker()
        msg1 = self._make_msg(channel_id="ch1", text="msg1")
        msg2 = self._make_msg(channel_id="ch2", text="msg2")
        tracker.add(msg1)
        result = tracker.add(msg2)
        assert result.mention_count == 2
        assert result.unique_sources == 2
        # Genuine confidence should be boosted
        assert result.confidence > 0.5

    def test_template_detection(self) -> None:
        """Same exact message in 3+ channels = template = paid ad."""
        tracker = MentionTracker()
        same_text = "BUY NOW! 100x gem! SAFU!"
        for i in range(4):
            msg = self._make_msg(
                channel_id=f"ch{i}",
                user_id=f"u{i}",
                text=same_text,
                label=SignalLabel.NOISE,
            )
            result = tracker.add(msg)

        assert result.label == SignalLabel.PAID_AD

    def test_multi_user_genuine_promotion(self) -> None:
        """3+ unique users across 2+ channels with different text = genuine."""
        tracker = MentionTracker()
        for i in range(4):
            msg = self._make_msg(
                channel_id=f"ch{i % 2}",
                user_id=f"u{i}",
                text=f"unique message number {i}",
                label=SignalLabel.NOISE,
            )
            result = tracker.add(msg)

        assert result.label == SignalLabel.GENUINE

    def test_hot_tokens(self) -> None:
        tracker = MentionTracker()
        for i in range(5):
            msg = self._make_msg(
                channel_id=f"ch{i % 3}",
                user_id=f"u{i}",
                text=f"message {i}",
            )
            tracker.add(msg)

        hot = tracker.get_hot_tokens(min_mentions=3, min_channels=2)
        assert len(hot) >= 1
        assert hot[0]["total_mentions"] >= 3

    def test_clear_old(self) -> None:
        tracker = MentionTracker()
        msg = self._make_msg()
        tracker.add(msg)
        # Clear with 0 hours = remove everything
        removed = tracker.clear_old(max_age_hours=0)
        assert removed >= 1

    def test_no_token_address_skipped(self) -> None:
        tracker = MentionTracker()
        msg = self._make_msg(token_address="")
        result = tracker.add(msg)
        assert result.mention_count == 1  # Still returns but no tracking


# ═══════════════════════════════════════════════════════════════════
# telegram_scanner tests
# ═══════════════════════════════════════════════════════════════════


class TestTelegramUserConfig:
    """Test Telegram user configuration loading."""

    def test_load_single_user_from_env(self) -> None:
        from ingestion.altdata.telegram_scanner import TelegramUser

        with patch.dict(os.environ, {
            "TELEGRAM_API_ID": "12345",
            "TELEGRAM_API_HASH": "abc123",
            "TELEGRAM_PHONE": "+15551234567",
            "TELEGRAM_CHANNELS": "channel1,channel2",
            "TELEGRAM_USERS": "",
        }):
            from ingestion.altdata.telegram_scanner import TelegramScanner
            users = TelegramScanner._load_users_from_config()
            assert len(users) == 1
            assert users[0].api_id == 12345
            assert users[0].phone == "+15551234567"
            assert users[0].channels == ["channel1", "channel2"]

    def test_load_multi_user_from_json(self) -> None:
        users_json = json.dumps([
            {"api_id": 111, "api_hash": "aaa", "phone": "+1111"},
            {"api_id": 222, "api_hash": "bbb", "phone": "+2222", "channels": ["ch1"]},
        ])
        with patch.dict(os.environ, {"TELEGRAM_USERS": users_json}):
            from ingestion.altdata.telegram_scanner import TelegramScanner
            users = TelegramScanner._load_users_from_config()
            assert len(users) == 2
            assert users[1].channels == ["ch1"]

    def test_no_config_returns_empty(self) -> None:
        with patch.dict(os.environ, {
            "TELEGRAM_API_ID": "",
            "TELEGRAM_API_HASH": "",
            "TELEGRAM_PHONE": "",
            "TELEGRAM_USERS": "",
        }, clear=False):
            from ingestion.altdata.telegram_scanner import TelegramScanner
            users = TelegramScanner._load_users_from_config()
            assert users == []

    def test_invalid_json_returns_empty(self) -> None:
        with patch.dict(os.environ, {"TELEGRAM_USERS": "not valid json"}):
            from ingestion.altdata.telegram_scanner import TelegramScanner
            users = TelegramScanner._load_users_from_config()
            assert users == []


class TestTelegramScanner:
    """Test TelegramScanner pull logic (mocked DB)."""

    def test_pull_skips_when_no_users(self, mock_engine) -> None:
        # Need to mock _resolve_source_id
        with patch.dict(os.environ, {
            "TELEGRAM_API_ID": "",
            "TELEGRAM_API_HASH": "",
            "TELEGRAM_PHONE": "",
            "TELEGRAM_USERS": "",
        }):
            scanner = _make_telegram_scanner(mock_engine)
            result = scanner.pull_recent()
            assert result["status"] == "SKIPPED"

    def test_store_classified_messages(self, mock_engine) -> None:
        scanner = _make_telegram_scanner(mock_engine)

        # Create some test messages
        messages = [
            ClassifiedMessage(
                source="telegram",
                channel_name="alpha_channel",
                channel_id="ch1",
                user_id="u1",
                username="trader",
                message_text="whale bought 50k of this",
                timestamp=datetime.now(timezone.utc),
                token_address="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
                token_symbol="TEST",
                label=SignalLabel.GENUINE,
                confidence=0.7,
                scores={"genuine_score": 0.5},
                message_hash="abc123",
            ),
            ClassifiedMessage(
                source="telegram",
                channel_name="noise_channel",
                channel_id="ch2",
                user_id="u2",
                username="newbie",
                message_text="gm sol fam",
                timestamp=datetime.now(timezone.utc),
                label=SignalLabel.NOISE,
                confidence=0.5,
                scores={},
                message_hash="def456",
            ),
        ]

        counts = scanner._store_classified_messages(messages)
        assert counts["GENUINE"] == 1
        assert counts["NOISE"] == 1
        assert counts["skipped"] >= 1  # Noise is skipped


# ═══════════════════════════════════════════════════════════════════
# discord_scanner tests
# ═══════════════════════════════════════════════════════════════════


class TestDiscordUserConfig:
    """Test Discord user configuration loading."""

    def test_load_single_user_from_env(self) -> None:
        with patch.dict(os.environ, {
            "DISCORD_USER_TOKEN": "mytoken123",
            "DISCORD_GUILD_IDS": "guild1,guild2",
            "DISCORD_CHANNEL_IDS": "",
            "DISCORD_USERS": "",
        }):
            from ingestion.altdata.discord_scanner import DiscordScanner
            users = DiscordScanner._load_users_from_config()
            assert len(users) == 1
            assert users[0].token == "mytoken123"
            assert users[0].guild_ids == ["guild1", "guild2"]

    def test_load_multi_user_from_json(self) -> None:
        users_json = json.dumps([
            {"token": "tok1", "label": "user1", "guild_ids": ["g1"]},
            {"token": "tok2", "label": "user2"},
        ])
        with patch.dict(os.environ, {"DISCORD_USERS": users_json}):
            from ingestion.altdata.discord_scanner import DiscordScanner
            users = DiscordScanner._load_users_from_config()
            assert len(users) == 2
            assert users[0].guild_ids == ["g1"]

    def test_pull_skips_when_no_users(self, mock_engine) -> None:
        with patch.dict(os.environ, {
            "DISCORD_USER_TOKEN": "",
            "DISCORD_USERS": "",
        }):
            scanner = _make_discord_scanner(mock_engine)
            result = scanner.pull_recent()
            assert result["status"] == "SKIPPED"


class TestDiscordTimestamp:
    """Test Discord timestamp parsing."""

    def test_parse_valid_timestamp(self) -> None:
        from ingestion.altdata.discord_scanner import _parse_discord_timestamp

        dt = _parse_discord_timestamp("2024-01-15T10:30:00+00:00")
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15

    def test_parse_empty_timestamp(self) -> None:
        from ingestion.altdata.discord_scanner import _parse_discord_timestamp

        dt = _parse_discord_timestamp("")
        assert dt.tzinfo == timezone.utc

    def test_parse_invalid_timestamp(self) -> None:
        from ingestion.altdata.discord_scanner import _parse_discord_timestamp

        dt = _parse_discord_timestamp("not-a-timestamp")
        assert dt.tzinfo == timezone.utc


# ═══════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════


def _make_telegram_scanner(mock_engine) -> "TelegramScanner":
    """Create a TelegramScanner with mocked DB."""
    from ingestion.altdata.telegram_scanner import TelegramScanner

    with patch.object(TelegramScanner, "_resolve_source_id", return_value=99):
        scanner = TelegramScanner.__new__(TelegramScanner)
        scanner.engine = mock_engine
        scanner.source_id = 99
        scanner.users = []
        scanner.tracker = MentionTracker()
    return scanner


def _make_discord_scanner(mock_engine) -> "DiscordScanner":
    """Create a DiscordScanner with mocked DB."""
    from ingestion.altdata.discord_scanner import DiscordScanner

    with patch.object(DiscordScanner, "_resolve_source_id", return_value=100):
        scanner = DiscordScanner.__new__(DiscordScanner)
        scanner.engine = mock_engine
        scanner.source_id = 100
        scanner.users = []
        scanner.tracker = MentionTracker()
    return scanner
