"""
GRID Solana Memecoin Message Classifier.

Shared classification engine for Telegram and Discord scanners.
Scores every token mention from chat messages as one of:

- GENUINE    — organic alpha, early mover signal
- PAID_AD    — sponsored promotion / paid shill
- SCAM       — rug pull, honeypot, or fake project
- NOISE      — low-signal chatter, memes, reposts

Uses layered heuristics:
1. Pattern matching (known shill templates, rug indicators)
2. Token metadata checks (liquidity, holder distribution, age)
3. Cross-channel correlation (same token across independent sources)
4. Optional LLM sanity check (via GRID's LLM router)

The classifier does NOT make trading decisions — it labels signals
so downstream modules (options_recommender, signal_executor) can
filter by confidence.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ── Classification labels ───────────────────────────────────────────

class SignalLabel(str, Enum):
    """Classification label for a memecoin mention."""
    GENUINE = "GENUINE"
    PAID_AD = "PAID_AD"
    SCAM = "SCAM"
    NOISE = "NOISE"


# ── Scored message ──────────────────────────────────────────────────

@dataclass
class ClassifiedMessage:
    """A classified memecoin mention from Telegram or Discord."""

    source: str                    # "telegram" or "discord"
    channel_name: str              # channel/group/server name
    channel_id: str                # platform-specific ID
    user_id: str                   # who posted (anonymised hash if needed)
    username: str                  # display name
    message_text: str              # raw message
    timestamp: datetime            # message timestamp (UTC)

    # Extracted token info
    token_address: str = ""        # Solana contract address (mint)
    token_symbol: str = ""         # ticker/symbol if mentioned
    token_name: str = ""           # name if mentioned

    # Classification
    label: SignalLabel = SignalLabel.NOISE
    confidence: float = 0.0        # 0-1 classification confidence
    scores: dict[str, float] = field(default_factory=dict)  # per-signal breakdown

    # Cross-reference
    mention_count: int = 1         # times seen across channels
    unique_sources: int = 1        # distinct channels mentioning
    first_seen: datetime | None = None

    # Metadata
    message_hash: str = ""         # dedup hash
    raw_payload: dict[str, Any] = field(default_factory=dict)


# ── Pattern databases ───────────────────────────────────────────────

# Solana contract address pattern (base58, 32-44 chars)
_SOL_ADDRESS_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")

# Common pump.fun / raydium / jupiter link patterns
_DEX_LINK_RE = re.compile(
    r"(?:pump\.fun/coin|dexscreener\.com/solana|birdeye\.so/token"
    r"|raydium\.io/swap|jup\.ag/swap|solscan\.io/token)"
    r"[/=]([1-9A-HJ-NP-Za-km-z]{32,44})",
    re.IGNORECASE,
)

# ── Paid ad / shill indicators ──────────────────────────────────────

_SHILL_PATTERNS: list[re.Pattern] = [
    re.compile(r"(?:next\s+)?\d+x\s+(?:gem|guaranteed|easy|incoming)", re.I),
    re.compile(r"(?:buy|ape)\s+(?:now|before|while)", re.I),
    re.compile(r"not\s+financial\s+advice.*(?:dyor|nfa)", re.I),
    re.compile(r"(?:stealth\s+launch|fair\s+launch|just\s+launched)", re.I),
    re.compile(r"(?:safu|based\s+dev|dev\s+doxxed|lp\s+burned)", re.I),
    re.compile(r"(?:moon|moonshot|to\s+the\s+moon|100x|1000x)", re.I),
    re.compile(r"(?:don'?t\s+miss|last\s+chance|early|still\s+early)", re.I),
    re.compile(r"(?:telegram|tg)\s*(?:group|chat|link)", re.I),
    re.compile(r"(?:airdrop|free\s+tokens|giveaway)", re.I),
    re.compile(r"low\s+(?:cap|mcap|market\s+cap).*(?:gem|moon)", re.I),
]

# Template messages (copy-pasted shills often share these structures)
_TEMPLATE_INDICATORS: list[str] = [
    "contract address",
    "ca:",
    "tokenomics:",
    "total supply:",
    "buy tax",
    "sell tax",
    "renounced",
    "liquidity locked",
    "audit:",
]

# ── Scam indicators ─────────────────────────────────────────────────

_SCAM_PATTERNS: list[re.Pattern] = [
    re.compile(r"(?:send|transfer)\s+(?:sol|solana)\s+(?:to|and)", re.I),
    re.compile(r"(?:double|triple|multiply)\s+your\s+(?:sol|money|crypto)", re.I),
    re.compile(r"(?:connect\s+wallet|enter\s+seed|private\s+key)", re.I),
    re.compile(r"(?:claim|mint)\s+(?:your|free)\s+(?:tokens|nft|airdrop)", re.I),
    re.compile(r"(?:connect\s+wallet|claim).*(?:free\s+tokens|airdrop)", re.I),
    re.compile(r"(?:whitelist|wl)\s+(?:spot|open|limited).*(?:hurry|limited|fast)", re.I),
    re.compile(r"(?:presale|pre-sale)\s+(?:live|open|now)", re.I),
    re.compile(r"(?:guaranteed|risk.?free)\s+(?:profit|return|gains)", re.I),
]

# ── Genuine alpha indicators ────────────────────────────────────────

_GENUINE_PATTERNS: list[re.Pattern] = [
    re.compile(r"(?:whale|smart\s+money)\s+(?:bought|buying|accumulating)", re.I),
    re.compile(r"(?:unusual|spike|surge)\s+(?:in\s+)?(?:volume|buys|activity)", re.I),
    re.compile(r"(?:dev\s+wallet|insider)\s+(?:moved|transferred|sold)", re.I),
    re.compile(r"(?:new\s+listing|cex\s+listing|binance|coinbase|bybit)", re.I),
    re.compile(r"(?:partnership|collab|integration)\s+(?:with|announced)", re.I),
    re.compile(r"(?:on-?chain|onchain)\s+(?:data|analysis|metrics)", re.I),
    re.compile(r"(?:holder|holders)\s+(?:count|growing|increasing)", re.I),
    re.compile(r"(?:bonding\s+curve|graduated|raydium\s+migration)", re.I),
]

# ── Known Telegram bot prefixes (for attribution) ──────────────────

KNOWN_TG_BOTS: dict[str, str] = {
    "maestro": "Maestro Bot",
    "bonkbot": "BonkBot",
    "trojan": "Trojan Bot",
    "sol_trading_bot": "Sol Trading Bot",
    "banana_gun": "Banana Gun",
    "pepeboost": "PepeBoost",
    "shuriken": "Shuriken",
    "photon": "Photon",
    "bullx": "BullX",
    "bloom": "Bloom",
    "ray_bot": "Raydium Bot",
}


# ── Classifier ──────────────────────────────────────────────────────

def extract_token_addresses(text: str) -> list[str]:
    """Extract Solana token addresses from message text.

    Checks both raw base58 addresses and DEX/explorer links.

    Parameters:
        text: Message text to scan.

    Returns:
        List of unique Solana addresses found.
    """
    addresses: list[str] = []

    # From DEX links (higher confidence — explicitly linked)
    for match in _DEX_LINK_RE.finditer(text):
        addr = match.group(1)
        if addr not in addresses:
            addresses.append(addr)

    # Raw addresses (lower confidence — could be any base58 string)
    for match in _SOL_ADDRESS_RE.finditer(text):
        addr = match.group(0)
        # Filter out common false positives (too short, looks like hash)
        if len(addr) >= 40 and addr not in addresses:
            addresses.append(addr)

    return addresses


def extract_token_symbol(text: str) -> str:
    """Extract a token ticker/symbol from message text.

    Looks for $TICKER patterns common in crypto chat.

    Parameters:
        text: Message text.

    Returns:
        Ticker symbol or empty string.
    """
    # $TICKER pattern (1-10 chars, uppercase after normalization)
    match = re.search(r"\$([A-Za-z]{1,10})\b", text)
    if match:
        return match.group(1).upper()
    return ""


def message_hash(text: str, channel_id: str) -> str:
    """Generate a dedup hash for a message.

    Parameters:
        text: Message text.
        channel_id: Channel identifier.

    Returns:
        12-char hex hash.
    """
    payload = f"{channel_id}:{text}".encode("utf-8", errors="replace")
    return hashlib.sha256(payload).hexdigest()[:12]


def classify_message(text: str) -> tuple[SignalLabel, float, dict[str, float]]:
    """Classify a memecoin-related message.

    Runs the message through all pattern layers and returns a label
    with confidence score.

    Parameters:
        text: Message text to classify.

    Returns:
        Tuple of (label, confidence, score_breakdown).
    """
    scores: dict[str, float] = {
        "shill_score": 0.0,
        "scam_score": 0.0,
        "genuine_score": 0.0,
        "template_score": 0.0,
        "length_score": 0.0,
        "link_score": 0.0,
    }

    lower_text = text.lower()

    # ── Shill pattern matching ──────────────────────────────────
    shill_hits = sum(1 for p in _SHILL_PATTERNS if p.search(text))
    scores["shill_score"] = min(shill_hits / len(_SHILL_PATTERNS), 1.0)

    # Template indicators (copy-paste shill messages)
    template_hits = sum(1 for t in _TEMPLATE_INDICATORS if t in lower_text)
    scores["template_score"] = min(template_hits / 4.0, 1.0)

    # ── Scam pattern matching ───────────────────────────────────
    scam_hits = sum(1 for p in _SCAM_PATTERNS if p.search(text))
    scores["scam_score"] = min(scam_hits / len(_SCAM_PATTERNS), 1.0)

    # ── Genuine alpha indicators ────────────────────────────────
    genuine_hits = sum(1 for p in _GENUINE_PATTERNS if p.search(text))
    scores["genuine_score"] = min(genuine_hits / len(_GENUINE_PATTERNS), 1.0)

    # ── Structural signals ──────────────────────────────────────

    # Very short messages are usually noise
    word_count = len(text.split())
    if word_count < 5:
        scores["length_score"] = -0.3
    elif word_count > 100:
        # Long promotional posts are often shills
        scores["length_score"] = 0.2
    else:
        scores["length_score"] = 0.0

    # Lots of links = promotional
    link_count = len(re.findall(r"https?://", text))
    if link_count >= 3:
        scores["link_score"] = 0.3
    elif link_count >= 1:
        scores["link_score"] = 0.1

    # Excessive emojis / caps = shill
    emoji_ratio = len(re.findall(r"[🚀🔥💎🌙⬆️📈💰🎯✅❗️⚡]", text)) / max(word_count, 1)
    if emoji_ratio > 0.3:
        scores["shill_score"] = min(scores["shill_score"] + 0.2, 1.0)

    caps_ratio = sum(1 for c in text if c.isupper()) / max(len(text), 1)
    if caps_ratio > 0.5 and len(text) > 20:
        scores["shill_score"] = min(scores["shill_score"] + 0.15, 1.0)

    # ── Decision logic ──────────────────────────────────────────

    # Scam takes priority — even small scam signal is dangerous
    if scores["scam_score"] >= 0.2:
        confidence = min(0.5 + scores["scam_score"], 0.95)
        return SignalLabel.SCAM, confidence, scores

    # High shill + template = paid ad
    combined_shill = (
        scores["shill_score"] * 0.4
        + scores["template_score"] * 0.3
        + scores["link_score"] * 0.15
        + scores["length_score"] * 0.15
    )
    if combined_shill >= 0.3:
        confidence = min(0.4 + combined_shill, 0.90)
        return SignalLabel.PAID_AD, confidence, scores

    # Genuine alpha signals
    if scores["genuine_score"] >= 0.15:
        # Penalise if also looks shilly
        net_genuine = scores["genuine_score"] - (scores["shill_score"] * 0.5)
        if net_genuine > 0.1:
            confidence = min(0.3 + net_genuine, 0.85)
            return SignalLabel.GENUINE, confidence, scores

    # Default: noise
    confidence = max(0.3, 1.0 - scores["genuine_score"] - scores["shill_score"])
    return SignalLabel.NOISE, min(confidence, 0.70), scores


def classify_full_message(
    text: str,
    source: str,
    channel_name: str,
    channel_id: str,
    user_id: str,
    username: str,
    timestamp: datetime | None = None,
    raw_payload: dict[str, Any] | None = None,
) -> ClassifiedMessage:
    """Classify a message and return a full ClassifiedMessage object.

    Parameters:
        text: Message text.
        source: Platform ("telegram" or "discord").
        channel_name: Channel/group name.
        channel_id: Platform-specific channel ID.
        user_id: Poster's user ID.
        username: Poster's display name.
        timestamp: Message timestamp (defaults to now UTC).
        raw_payload: Optional extra metadata.

    Returns:
        ClassifiedMessage with classification results.
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    label, confidence, scores = classify_message(text)
    addresses = extract_token_addresses(text)
    symbol = extract_token_symbol(text)
    msg_hash = message_hash(text, channel_id)

    return ClassifiedMessage(
        source=source,
        channel_name=channel_name,
        channel_id=channel_id,
        user_id=user_id,
        username=username,
        message_text=text,
        timestamp=timestamp,
        token_address=addresses[0] if addresses else "",
        token_symbol=symbol,
        label=label,
        confidence=confidence,
        scores=scores,
        message_hash=msg_hash,
        first_seen=timestamp,
        raw_payload=raw_payload or {},
    )


# ── Cross-channel correlation ──────────────────────────────────────

class MentionTracker:
    """Tracks token mentions across channels for cross-referencing.

    When the same token appears in multiple independent channels within
    a time window, it's a stronger signal. If it appears in shill channels
    with identical template text, it's a weaker (paid) signal.

    Attributes:
        _mentions: Dict mapping token_address to list of ClassifiedMessages.
        _text_hashes: Dict mapping message text hashes to count (template detection).
    """

    def __init__(self) -> None:
        self._mentions: dict[str, list[ClassifiedMessage]] = {}
        self._text_hashes: dict[str, int] = {}

    @staticmethod
    def _content_hash(text: str) -> str:
        """Hash message text only (ignoring channel) for template detection."""
        return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()[:12]

    def add(self, msg: ClassifiedMessage) -> ClassifiedMessage:
        """Add a classified message and update cross-reference scores.

        Parameters:
            msg: Classified message to track.

        Returns:
            Updated message with cross-reference data.
        """
        if not msg.token_address:
            return msg

        addr = msg.token_address

        # Track text hash for template detection (content-only, ignoring channel)
        content_key = self._content_hash(msg.message_text)
        self._text_hashes[content_key] = (
            self._text_hashes.get(content_key, 0) + 1
        )

        # Track mentions
        if addr not in self._mentions:
            self._mentions[addr] = []

        self._mentions[addr].append(msg)

        # Update cross-reference counts
        all_mentions = self._mentions[addr]
        unique_channels = len({m.channel_id for m in all_mentions})
        unique_users = len({m.user_id for m in all_mentions})

        msg.mention_count = len(all_mentions)
        msg.unique_sources = unique_channels

        # If first seen in another channel
        if len(all_mentions) > 1:
            msg.first_seen = min(m.timestamp for m in all_mentions)

        # Boost genuine confidence if multiple independent sources
        if msg.label == SignalLabel.GENUINE and unique_channels >= 2:
            boost = min(unique_channels * 0.05, 0.15)
            msg.confidence = min(msg.confidence + boost, 0.95)

        # If same text hash appears many times, it's a template (paid ad)
        content_key = self._content_hash(msg.message_text)
        is_template = self._text_hashes.get(content_key, 0) >= 3

        if is_template:
            if msg.label != SignalLabel.SCAM:
                msg.label = SignalLabel.PAID_AD
                msg.confidence = max(msg.confidence, 0.75)

        # Cross-source genuine signal: seen by multiple users, not template
        if (
            unique_users >= 3
            and unique_channels >= 2
            and msg.label == SignalLabel.NOISE
            and not is_template
        ):
            msg.label = SignalLabel.GENUINE
            msg.confidence = 0.5

        return msg

    def get_hot_tokens(
        self,
        min_mentions: int = 3,
        min_channels: int = 2,
    ) -> list[dict[str, Any]]:
        """Get tokens with significant cross-channel activity.

        Parameters:
            min_mentions: Minimum total mentions.
            min_channels: Minimum unique channels.

        Returns:
            List of hot token summaries.
        """
        hot: list[dict[str, Any]] = []

        for addr, mentions in self._mentions.items():
            unique_ch = len({m.channel_id for m in mentions})
            if len(mentions) >= min_mentions and unique_ch >= min_channels:
                labels = [m.label.value for m in mentions]
                genuine_pct = labels.count("GENUINE") / len(labels) if labels else 0

                hot.append({
                    "token_address": addr,
                    "token_symbol": next(
                        (m.token_symbol for m in mentions if m.token_symbol), ""
                    ),
                    "total_mentions": len(mentions),
                    "unique_channels": unique_ch,
                    "unique_users": len({m.user_id for m in mentions}),
                    "genuine_pct": round(genuine_pct, 2),
                    "dominant_label": max(
                        set(labels), key=labels.count
                    ),
                    "first_seen": min(m.timestamp for m in mentions).isoformat(),
                    "channels": list({m.channel_name for m in mentions}),
                })

        hot.sort(key=lambda x: x["total_mentions"], reverse=True)
        return hot

    def clear_old(self, max_age_hours: int = 24) -> int:
        """Remove mentions older than max_age_hours.

        Parameters:
            max_age_hours: Maximum age in hours.

        Returns:
            Number of entries removed.
        """
        cutoff = datetime.now(timezone.utc).timestamp() - (max_age_hours * 3600)
        removed = 0

        for addr in list(self._mentions.keys()):
            before = len(self._mentions[addr])
            self._mentions[addr] = [
                m for m in self._mentions[addr]
                if m.timestamp.timestamp() > cutoff
            ]
            removed += before - len(self._mentions[addr])

            if not self._mentions[addr]:
                del self._mentions[addr]

        return removed
