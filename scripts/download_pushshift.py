#!/usr/bin/env python3
"""
Download Pushshift Reddit dump files from Academic Torrents.

Fetches zstandard-compressed NDJSON files for target finance subreddits.
Files are organized as individual subreddit dumps (submissions and comments).

Academic Torrents page: https://academictorrents.com/details/56aa49f9653ba545f48df2e33679f014d2829c10
Watchful1 mirror:      https://the-eye.eu/redarcs/

Usage:
    python scripts/download_pushshift.py
    python scripts/download_pushshift.py --data-dir /mnt/11tb/pushshift
    python scripts/download_pushshift.py --subreddits wallstreetbets,stocks
    python scripts/download_pushshift.py --method torrent
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from loguru import logger as log

# ── Target subreddits (same list as the puller) ─────────────────────

DEFAULT_SUBREDDITS: list[str] = [
    "wallstreetbets",
    "stocks",
    "investing",
    "cryptocurrency",
    "options",
    "thetagang",
    "SPACs",
    "Superstonk",
    "Bitcoin",
    "solana",
    "ethtrader",
    "algotrading",
]

DEFAULT_DATA_DIR = "/data/pushshift"

# ── Download URLs ────────────────────────────────────────────────────
# Watchful1's curated per-subreddit dumps (updated periodically)
# Format: submissions and comments as separate .zst files

_REDARCS_BASE = "https://the-eye.eu/redarcs/files"

# Academic Torrents magnet for the full Pushshift dump
_AT_MAGNET = (
    "magnet:?xt=urn:btih:7c0645c94321311bb05571"
    "3259a17e32b6e0b9e2&dn=reddit_dumps"
    "&tr=https%3A%2F%2Facademictorrents.com%2Fannounce.php"
)


def _build_urls(subreddit: str) -> list[dict[str, str]]:
    """Build download URLs for a subreddit's dumps.

    Parameters:
        subreddit: Subreddit name (case-sensitive).

    Returns:
        List of dicts with 'url', 'filename', and 'type' keys.
    """
    return [
        {
            "url": f"{_REDARCS_BASE}/{subreddit}_submissions.zst",
            "filename": f"{subreddit}_submissions.zst",
            "type": "submissions",
        },
        {
            "url": f"{_REDARCS_BASE}/{subreddit}_comments.zst",
            "filename": f"{subreddit}_comments.zst",
            "type": "comments",
        },
    ]


# ── HTTP download with progress ─────────────────────────────────────


def download_http(url: str, dest: Path, chunk_size: int = 2 ** 20) -> bool:
    """Download a file via HTTP with progress display.

    Parameters:
        url: Source URL.
        dest: Destination file path.
        chunk_size: Download chunk size (default 1 MiB).

    Returns:
        True if download succeeded, False otherwise.
    """
    import requests

    # Skip if already downloaded (resume not supported by all mirrors)
    if dest.exists() and dest.stat().st_size > 0:
        log.info("Already exists, skipping: {f}", f=dest.name)
        return True

    # Partial download support
    tmp = dest.with_suffix(dest.suffix + ".part")
    headers = {}
    start_byte = 0
    if tmp.exists():
        start_byte = tmp.stat().st_size
        headers["Range"] = f"bytes={start_byte}-"

    try:
        resp = requests.get(url, stream=True, timeout=30, headers=headers)

        if resp.status_code == 404:
            log.warning("Not found (404): {u}", u=url)
            return False

        resp.raise_for_status()

        total_size = int(resp.headers.get("content-length", 0)) + start_byte
        downloaded = start_byte

        try:
            from tqdm import tqdm
            pbar = tqdm(
                total=total_size,
                initial=start_byte,
                unit="B",
                unit_scale=True,
                desc=dest.name,
            )
        except ImportError:
            pbar = None

        mode = "ab" if start_byte > 0 else "wb"
        with open(tmp, mode) as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if pbar:
                        pbar.update(len(chunk))
                    elif downloaded % (50 * chunk_size) == 0:
                        pct = (downloaded / total_size * 100) if total_size else 0
                        log.info(
                            "  {f}: {d:.1f} MiB / {t:.1f} MiB ({p:.0f}%)",
                            f=dest.name,
                            d=downloaded / 2**20,
                            t=total_size / 2**20,
                            p=pct,
                        )

        if pbar:
            pbar.close()

        # Rename .part to final
        tmp.rename(dest)
        log.info(
            "Downloaded {f} — {s:.1f} MiB",
            f=dest.name, s=dest.stat().st_size / 2**20,
        )
        return True

    except Exception as exc:
        log.error("Download failed for {u}: {e}", u=url, e=str(exc))
        return False


# ── Torrent download ─────────────────────────────────────────────────


def download_torrent(
    data_dir: Path,
    subreddits: list[str],
    timeout_minutes: int = 720,
) -> bool:
    """Download Pushshift dumps via BitTorrent using libtorrent.

    Parameters:
        data_dir: Directory to save downloaded files.
        subreddits: List of target subreddits (for filtering after download).
        timeout_minutes: Max time to wait for torrent (default 12 hours).

    Returns:
        True if download started/completed, False on error.
    """
    try:
        import libtorrent as lt
    except ImportError:
        log.error(
            "libtorrent not installed. Install with: pip install libtorrent "
            "or apt install python3-libtorrent"
        )
        return False

    ses = lt.session()
    ses.listen_on(6881, 6891)

    params = lt.parse_magnet_uri(_AT_MAGNET)
    params.save_path = str(data_dir)
    handle = ses.add_torrent(params)

    log.info("Torrent added — waiting for metadata...")

    deadline = time.time() + timeout_minutes * 60

    # Wait for metadata
    while not handle.has_metadata():
        if time.time() > deadline:
            log.error("Torrent metadata timeout after {m} min", m=timeout_minutes)
            return False
        time.sleep(5)

    log.info("Torrent metadata received — {n} files", n=handle.torrent_file().num_files())

    # Filter files to only download target subreddits
    file_storage = handle.torrent_file().files()
    target_lower = {s.lower() for s in subreddits}

    for i in range(file_storage.num_files()):
        fname = file_storage.file_name(i).lower()
        should_download = any(sub in fname for sub in target_lower)
        if not should_download:
            handle.file_priority(i, 0)  # skip this file

    # Download loop
    last_log = 0
    while handle.status().state != lt.torrent_status.seeding:
        if time.time() > deadline:
            log.warning("Torrent download timeout — partial data may be available")
            break

        s = handle.status()
        if time.time() - last_log > 30:
            log.info(
                "Torrent: {p:.1f}% complete, {d:.1f} MiB/s down, {u:.1f} MiB/s up, peers: {n}",
                p=s.progress * 100,
                d=s.download_rate / 2**20,
                u=s.upload_rate / 2**20,
                n=s.num_peers,
            )
            last_log = time.time()

        if s.progress >= 1.0:
            break
        time.sleep(10)

    log.info("Torrent download complete")
    return True


# ── Main ─────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download Pushshift Reddit dumps for GRID backfill",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=DEFAULT_DATA_DIR,
        help=f"Directory to store downloaded dumps (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--subreddits",
        type=str,
        default=None,
        help="Comma-separated list of subreddits (default: all finance subs)",
    )
    parser.add_argument(
        "--method",
        choices=["http", "torrent"],
        default="http",
        help="Download method: http (Watchful1 mirror) or torrent (Academic Torrents)",
    )
    parser.add_argument(
        "--submissions-only",
        action="store_true",
        help="Only download submission dumps (skip comments)",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    subreddits = (
        args.subreddits.split(",") if args.subreddits else DEFAULT_SUBREDDITS
    )

    log.info(
        "Pushshift download — {n} subreddits, method={m}, dir={d}",
        n=len(subreddits), m=args.method, d=str(data_dir),
    )

    if args.method == "torrent":
        download_torrent(data_dir, subreddits)
        return

    # HTTP download
    succeeded = 0
    failed = 0

    for sub in subreddits:
        urls = _build_urls(sub)
        if args.submissions_only:
            urls = [u for u in urls if u["type"] == "submissions"]

        for entry in urls:
            dest = data_dir / entry["filename"]
            log.info("Downloading {f} ...", f=entry["filename"])

            ok = download_http(entry["url"], dest)
            if ok:
                succeeded += 1
            else:
                failed += 1

            # Rate limit between downloads
            time.sleep(2)

    log.info(
        "Download complete — {ok} succeeded, {fail} failed",
        ok=succeeded, fail=failed,
    )


if __name__ == "__main__":
    main()
