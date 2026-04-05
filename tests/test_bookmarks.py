"""Tests for the bookmark intelligence pipeline."""
import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary bookmarks SQLite DB with test data."""
    db_path = tmp_path / "bookmarks.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE bookmarks (
            tweet_id TEXT PRIMARY KEY,
            author_id TEXT,
            author_username TEXT,
            author_name TEXT,
            text TEXT,
            created_at TEXT,
            urls TEXT,
            media TEXT,
            metrics TEXT,
            conversation_id TEXT,
            in_reply_to TEXT,
            referenced_tweets TEXT,
            raw_json TEXT,
            bookmarked_at TEXT DEFAULT (datetime('now')),
            tags TEXT DEFAULT '[]'
        );
    """)
    # Insert test bookmarks
    test_bookmarks = [
        ("1001", "alice", "Alice", "TurboQuant 6x KV cache compression", "2026-04-04T10:00:00Z", '["tools"]'),
        ("1002", "bob", "Bob", "SPY looking bearish, put/call ratio spiking", "2026-04-03T15:00:00Z", '["alpha"]'),
        ("1003", "carol", "Carol", "lol funny cat video", "2026-04-02T12:00:00Z", '["noise"]'),
    ]
    for tid, user, name, text, date, tags in test_bookmarks:
        conn.execute(
            "INSERT INTO bookmarks (tweet_id, author_username, author_name, text, created_at, tags, raw_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (tid, user, name, text, date, tags, json.dumps({"tweet_id": tid, "text": text})),
        )
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def tmp_vault(tmp_path):
    """Create a temporary Obsidian vault."""
    vault = tmp_path / "vault"
    vault.mkdir()
    return vault


class TestParseJsonResponse:
    def test_plain_json(self):
        from ingestion.altdata.bookmarks import _parse_json_response

        result = _parse_json_response('{"category": "tools", "relevance": 8}')
        assert result["category"] == "tools"
        assert result["relevance"] == 8

    def test_markdown_fenced_json(self):
        from ingestion.altdata.bookmarks import _parse_json_response

        text = '```json\n{"category": "alpha", "relevance": 7}\n```'
        result = _parse_json_response(text)
        assert result["category"] == "alpha"

    def test_json_with_surrounding_text(self):
        from ingestion.altdata.bookmarks import _parse_json_response

        text = 'Here is my analysis: {"category": "noise", "relevance": 2} hope that helps'
        result = _parse_json_response(text)
        assert result["category"] == "noise"

    def test_invalid_json_returns_none(self):
        from ingestion.altdata.bookmarks import _parse_json_response

        assert _parse_json_response("not json at all") is None


class TestCompareResults:
    def test_consensus(self):
        from ingestion.altdata.bookmarks import compare_results

        results = {
            "groq": {"result": {"category": "tools", "relevance": 8}, "status": "ok"},
            "gemini": {"result": {"category": "tools", "relevance": 7}, "status": "ok"},
        }
        comparison = compare_results(results)
        assert comparison["consensus"] is True
        assert comparison["category_agreement"] is True

    def test_disagreement(self):
        from ingestion.altdata.bookmarks import compare_results

        results = {
            "groq": {"result": {"category": "tools", "relevance": 9}, "status": "ok"},
            "gemini": {"result": {"category": "noise", "relevance": 2}, "status": "ok"},
        }
        comparison = compare_results(results)
        assert comparison["consensus"] is False
        assert comparison["category_agreement"] is False

    def test_single_backend(self):
        from ingestion.altdata.bookmarks import compare_results

        results = {
            "groq": {"result": {"category": "tools", "relevance": 8}, "status": "ok"},
            "gemini": {"result": None, "status": "no_key"},
        }
        comparison = compare_results(results)
        assert comparison["consensus"] is True
        assert comparison["active_llms"] == 1


class TestObsidianOutput:
    def test_write_inbox_entry(self, tmp_vault):
        from ingestion.altdata.bookmarks import write_inbox_entry, OBSIDIAN_VAULT

        # Patch the vault path
        with patch("ingestion.altdata.bookmarks.OBSIDIAN_VAULT", tmp_vault):
            bookmark = {
                "author_username": "testuser",
                "created_at": "2026-04-04T10:00:00Z",
                "text": "This is a test bookmark about AI tools",
                "raw_json": json.dumps({"tweet_url": "https://x.com/test/status/123"}),
            }
            results = {
                "groq": {
                    "result": {"category": "tools", "relevance": 8, "summary": "AI tool test", "tags": ["ai"]},
                    "status": "ok",
                },
            }
            comparison = {"consensus": True}

            write_inbox_entry(bookmark, results, comparison)

            inbox = tmp_vault / "01-Pipeline" / "Inbox.md"
            assert inbox.exists()
            content = inbox.read_text()
            assert "@testuser" in content
            assert "tools" in content

    def test_write_dashboard(self, tmp_db, tmp_vault):
        from ingestion.altdata.bookmarks import write_dashboard

        with patch("ingestion.altdata.bookmarks.OBSIDIAN_VAULT", tmp_vault), \
             patch("ingestion.altdata.bookmarks.BOOKMARKS_DB", tmp_db):
            write_dashboard()

            dashboard = tmp_vault / "00-DASHBOARD.md"
            assert dashboard.exists()
            content = dashboard.read_text()
            assert "Total Bookmarks" in content
            assert "3" in content  # 3 test bookmarks
