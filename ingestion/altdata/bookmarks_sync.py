"""
Twitter/X Bookmark Sync via Playwright.

Uses existing Chrome profile cookies to scrape bookmarks from x.com.
Stores in a local SQLite DB that the triage pipeline reads from.

Usage:
    python -m ingestion.altdata.bookmarks_sync                 # visible browser
    python -m ingestion.altdata.bookmarks_sync --headless      # background
    python -m ingestion.altdata.bookmarks_sync --max-scrolls 20  # quick recent

The SQLite DB lives at ~/.ft-bookmarks/bookmarks.db by default.
Override with BOOKMARKS_DB_PATH env var.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

DB_PATH = Path(
    os.environ.get(
        "BOOKMARKS_DB_PATH",
        os.path.expanduser("~/.ft-bookmarks/bookmarks.db"),
    )
)

BOOKMARKS_URL = "https://x.com/i/bookmarks"

CHROME_PROFILES = [
    os.path.expanduser("~/Library/Application Support/Google/Chrome"),
    os.path.expanduser("~/Library/Application Support/Google/Chrome/Default"),
    os.path.expanduser("~/.config/google-chrome"),
    os.path.expanduser("~/.config/google-chrome/Default"),
]

SCRAPER_JS = """
() => {
    const articles = document.querySelectorAll('article[data-testid="tweet"]');
    const results = [];
    articles.forEach(article => {
        try {
            const linkEl = article.querySelector('a[href*="/status/"]');
            if (!linkEl) return;
            const href = linkEl.getAttribute('href');
            const match = href.match(/\\/status\\/(\\d+)/);
            if (!match) return;

            const userLink = article.querySelector('a[href^="/"][role="link"] span');
            const handleEl = article.querySelector('a[href^="/"][tabindex="-1"]');
            const textEl = article.querySelector('[data-testid="tweetText"]');
            const timeEl = article.querySelector('time');

            const urls = [];
            if (textEl) {
                textEl.querySelectorAll('a[href]').forEach(a => {
                    const u = a.getAttribute('href');
                    if (u && !u.startsWith('/') && !u.includes('x.com')) urls.push(u);
                });
            }

            const metricEls = article.querySelectorAll('[data-testid$="count"]');
            const metrics = {};
            metricEls.forEach(el => {
                const testid = el.getAttribute('data-testid');
                const val = el.textContent.trim();
                if (testid) metrics[testid] = val;
            });

            results.push({
                tweet_id: match[1],
                author_username: handleEl ? handleEl.getAttribute('href').replace('/', '') : '',
                author_name: userLink ? userLink.textContent.trim() : '',
                text: textEl ? textEl.innerText : '',
                created_at: timeEl ? timeEl.getAttribute('datetime') : '',
                urls: urls,
                metrics: metrics,
                tweet_url: 'https://x.com' + href,
            });
        } catch(e) {}
    });
    return results;
}
"""


def init_db() -> None:
    """Initialize the bookmarks SQLite database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS bookmarks (
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

        CREATE INDEX IF NOT EXISTS idx_bookmarks_author
            ON bookmarks(author_username);
        CREATE INDEX IF NOT EXISTS idx_bookmarks_created
            ON bookmarks(created_at);

        CREATE VIRTUAL TABLE IF NOT EXISTS bookmarks_fts USING fts5(
            tweet_id UNINDEXED,
            author_username,
            author_name,
            text,
            content=bookmarks,
            content_rowid=rowid
        );

        CREATE TRIGGER IF NOT EXISTS bookmarks_ai AFTER INSERT ON bookmarks BEGIN
            INSERT INTO bookmarks_fts(rowid, tweet_id, author_username, author_name, text)
            VALUES (new.rowid, new.tweet_id, new.author_username, new.author_name, new.text);
        END;

        CREATE TRIGGER IF NOT EXISTS bookmarks_ad AFTER DELETE ON bookmarks BEGIN
            INSERT INTO bookmarks_fts(bookmarks_fts, rowid, tweet_id, author_username, author_name, text)
            VALUES ('delete', old.rowid, old.tweet_id, old.author_username, old.author_name, old.text);
        END;
    """)
    conn.close()


def upsert_bookmark(conn: sqlite3.Connection, item: dict) -> None:
    """Insert or update a single bookmark."""
    tweet_id = item.get("tweet_id") or item.get("id") or ""
    if not tweet_id:
        return

    urls = item.get("urls", [])
    urls_json = json.dumps(urls) if isinstance(urls, list) else json.dumps([])

    metrics = item.get("metrics", {})
    metrics_json = json.dumps(metrics) if isinstance(metrics, dict) else json.dumps({})

    conn.execute(
        """
        INSERT INTO bookmarks (
            tweet_id, author_username, author_name, text,
            created_at, urls, metrics, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tweet_id) DO UPDATE SET
            metrics = excluded.metrics,
            raw_json = excluded.raw_json,
            text = COALESCE(excluded.text, bookmarks.text)
        """,
        (
            tweet_id,
            item.get("author_username", ""),
            item.get("author_name", ""),
            item.get("text", ""),
            item.get("created_at", ""),
            urls_json,
            metrics_json,
            json.dumps(item),
        ),
    )


def find_chrome_profile() -> str | None:
    for path in CHROME_PROFILES:
        if os.path.isdir(path):
            return path
    return None


def sync_bookmarks(
    max_scrolls: int = 200,
    headless: bool = True,
) -> dict[str, int]:
    """Sync bookmarks from Twitter/X via browser automation.

    Returns:
        Dict with total seen, new count.
    """
    init_db()

    chrome_profile = find_chrome_profile()
    if not chrome_profile:
        print("ERROR: Chrome profile not found.")
        return {"total": 0, "new": 0}

    print(f"Chrome profile: {chrome_profile}")
    print(f"Mode: {'headless' if headless else 'visible'}")
    print(f"Max scrolls: {max_scrolls}\n")

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=chrome_profile,
            headless=headless,
            channel="chrome",
            args=["--disable-blink-features=AutomationControlled"],
            viewport={"width": 1280, "height": 900},
        )

        page = context.pages[0] if context.pages else context.new_page()
        print(f"Navigating to {BOOKMARKS_URL}...")
        page.goto(BOOKMARKS_URL, wait_until="networkidle", timeout=30000)
        time.sleep(3)

        if "login" in page.url.lower():
            print("ERROR: Not logged into X/Twitter in Chrome.")
            context.close()
            return {"total": 0, "new": 0}

        print("Logged in. Scraping...\n")

        conn = sqlite3.connect(str(DB_PATH))
        all_ids: set[str] = set()
        total_new = 0
        no_new_rounds = 0

        for scroll in range(1, max_scrolls + 1):
            tweets = page.evaluate(SCRAPER_JS)

            batch_new = 0
            for tweet in tweets:
                tid = tweet.get("tweet_id")
                if tid and tid not in all_ids:
                    all_ids.add(tid)
                    upsert_bookmark(conn, tweet)
                    batch_new += 1

            if batch_new > 0:
                conn.commit()
                total_new += batch_new
                no_new_rounds = 0
            else:
                no_new_rounds += 1

            print(f"  Scroll {scroll}: {len(all_ids)} total ({batch_new} new)")

            if no_new_rounds >= 5 and scroll > 10:
                print("\n  Reached end of bookmarks.")
                break

            page.evaluate("window.scrollBy(0, 2000)")
            time.sleep(1.5)

        # Log the sync
        conn.execute(
            """CREATE TABLE IF NOT EXISTS sync_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                synced_at TEXT DEFAULT (datetime('now')),
                tweet_count INTEGER,
                new_count INTEGER
            )"""
        )
        conn.execute(
            "INSERT INTO sync_log (tweet_count, new_count) VALUES (?, ?)",
            (len(all_ids), total_new),
        )
        conn.commit()
        conn.close()
        context.close()

    result = {"total": len(all_ids), "new": total_new}
    print(f"\nSync complete: {result}")
    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sync X/Twitter bookmarks")
    parser.add_argument("--max-scrolls", type=int, default=200)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--visible", action="store_true")
    args = parser.parse_args()

    headless = args.headless and not args.visible
    sync_bookmarks(max_scrolls=args.max_scrolls, headless=headless)
