"""Offline, content-addressed company documents. No network, DB or model calls.

This archive proves byte identity, not publisher authenticity or factual truth.
Only a trusted local archive directory should be used; it is not a hostile-user
filesystem sandbox. Derived HTML text is a navigation aid, never financial facts.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import tempfile
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit

SCHEMA = "company-document-v1"
PARSER = "company-text-v1"
MAX_BYTES = 16 * 1024 * 1024
FIELDS = {"company_id", "source_kind", "source_url", "title", "published_at",
          "retrieved_at", "media_type"}
KINDS = {"sec_filing", "issuer", "counterparty", "user_supplied"}


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True,
                      separators=(",", ":"), allow_nan=False).encode("ascii")


def _hash(value):
    return hashlib.sha256(value).hexdigest()


def _time(value):
    if not isinstance(value, str) or "T" not in value:
        raise ValueError("timestamp must be an ISO datetime with an explicit timezone")
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        raise ValueError("timestamp requires timezone")
    return stamp.astimezone(UTC)


def _metadata(metadata):
    if not isinstance(metadata, dict) or set(metadata) != FIELDS:
        raise ValueError("metadata must contain exactly the documented fields")
    result = dict(metadata)
    for key in FIELDS - {"published_at"}:
        value = result[key]
        if not isinstance(value, str) or not value.strip() or len(value) > 4096:
            raise ValueError(f"invalid {key}")
        if any(ord(c) < 32 for c in value):
            raise ValueError(f"control character in {key}")
        value.encode("utf-8", errors="strict")
    if result["source_kind"] not in KINDS:
        raise ValueError("unknown source_kind")
    url = urlsplit(result["source_url"])
    if url.scheme != "https" or not url.hostname or url.username or url.password:
        raise ValueError("source_url must be an HTTPS locator without credentials")
    if result["source_kind"] == "sec_filing" and url.hostname not in {"sec.gov", "www.sec.gov"}:
        raise ValueError("SEC filing locator must be on sec.gov")
    retrieved = _time(result["retrieved_at"])
    result["retrieved_at"] = retrieved.isoformat()
    if result["published_at"] is not None:
        published = _time(result["published_at"])
        if published > retrieved:
            raise ValueError("publication cannot follow retrieval")
        result["published_at"] = published.isoformat()
    return result


class _Text(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self.hidden += 1
        if not self.hidden and tag in {"p", "div", "br", "tr", "td", "th", "li", "h1", "h2", "h3"}:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in {"script", "style"} and self.hidden:
            self.hidden -= 1
        if not self.hidden and tag in {"p", "div", "tr", "td", "th", "li"}:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def _parse(content, media_type):
    if media_type not in {"text/plain", "text/html"}:
        return "unsupported_media_type", None
    try:
        text = content.decode("utf-8-sig", errors="strict")
    except UnicodeDecodeError:
        return "unsupported_encoding", None
    if "\x00" in text:
        return "invalid_text", None
    if media_type == "text/html":
        parser = _Text()
        parser.feed(text)
        parser.close()
        text = "".join(parser.parts)
    return ("parsed" if text.strip() else "empty_text"), text


def build_document(metadata, content):
    """Produce a deterministic immutable envelope; caller supplies genuine bytes.

    Publication time is caller-provided evidence metadata, not inferred from a
    fiscal period, retrieval time or HTML. Unknown publication remains None.
    """
    if not isinstance(content, bytes) or not content or len(content) > MAX_BYTES:
        raise ValueError("content must be nonempty bytes within MAX_BYTES")
    result = _metadata(metadata)
    status, text = _parse(content, result["media_type"])
    result.update(schema=SCHEMA, parser_version=PARSER, parse_status=status,
                  content_sha256=_hash(content), content_base64=base64.b64encode(content).decode("ascii"),
                  byte_length=len(content), text=text,
                  text_sha256=None if text is None else _hash(text.encode("utf-8")))
    result["snapshot_id"] = _hash(_json(result))
    return result


def ingest_document(root, metadata, content):
    """Publish one complete snapshot atomically, never replace an existing one.

    Repeated identical captures reuse the object. Changed bytes or metadata
    produce a separate snapshot. The caller owns archive permissions/retention.
    """
    result = build_document(metadata, content)
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    target = root / (result["snapshot_id"] + ".json")
    encoded = _json(result)
    fd, temporary = tempfile.mkstemp(prefix=".document-", dir=root)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, target)
        except FileExistsError:
            if target.read_bytes() != encoded:
                raise ValueError("existing snapshot failed integrity check") from None
    finally:
        os.unlink(temporary)
    return result


def read_document(root, snapshot_id):
    """Verify metadata, raw content, extracted text and all identities on read."""
    if not isinstance(snapshot_id, str) or not re.fullmatch(r"[0-9a-f]{64}", snapshot_id):
        raise ValueError("invalid snapshot_id")
    path = Path(root) / (snapshot_id + ".json")
    with path.open("rb") as stream:
        encoded = stream.read(MAX_BYTES * 8 + 1)
    if len(encoded) > MAX_BYTES * 8:
        raise ValueError("snapshot exceeds size limit")
    result = json.loads(encoded)
    try:
        content = base64.b64decode(result["content_base64"], validate=True)
        expected = build_document({key: result[key] for key in FIELDS}, content)
    except (KeyError, TypeError) as exc:
        raise ValueError("invalid snapshot envelope") from exc
    if result != expected or expected["snapshot_id"] != snapshot_id:
        raise ValueError("snapshot failed integrity check")
    return result


def availability(document, *, as_of, max_age_days):
    """Conservative PIT/freshness gate. Retrieval age is not publication age.

    Both publication and recorded retrieval must predate as_of; retrospective
    downloads therefore cannot pretend to be historically observed evidence.
    Freshness is only age under the caller's policy, not latest-source coverage.
    """
    if isinstance(max_age_days, bool) or not isinstance(max_age_days, int) or max_age_days < 0:
        raise ValueError("max_age_days must be a nonnegative integer")
    cutoff = _time(as_of)
    if _time(document["retrieved_at"]) > cutoff:
        return "not_yet_observed"
    if document["published_at"] is None:
        return "unknown_publication"
    published = _time(document["published_at"])
    if published > cutoff:
        return "not_yet_published"
    if document["parse_status"] != "parsed":
        return document["parse_status"]
    if (cutoff - published).total_seconds() > max_age_days * 86400:
        return "stale"
    return "available"
