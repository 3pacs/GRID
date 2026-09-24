"""Bounded #639 API acceptance probe; run only after verifying the live release.

The probe has a fixed provider-free GET list and emits status/shape only. It
does not navigate the PWA, call refresh endpoints, or assert data freshness.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


PROBES = (
    ("health", "/api/v1/system/health", False),
    ("saved_options_unauth", "/api/v1/options/recommendations?ticker=AAPL", False),
    ("saved_options", "/api/v1/options/recommendations?ticker=AAPL", True),
    ("ticker_edge", "/api/v1/watchlist/AAPL/edge", True),
)


def validate_origin(origin: str) -> str:
    parsed = urlsplit(origin)
    if parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ("", "/"):
        raise ValueError("origin must contain only scheme and host, without credentials/path/query")
    local = parsed.scheme == "http" and parsed.hostname == "127.0.0.1" and parsed.port == 8000
    if not local:
        raise ValueError("origin must be local loopback HTTP on API port 8000, on the release host")
    return origin.rstrip("/")


def verify_release(release_path: Path, expected_sha: str, expected_tree: str) -> dict[str, str]:
    if not all(len(x) == 40 and all(c in "0123456789abcdef" for c in x.lower()) for x in (expected_sha, expected_tree)):
        raise ValueError("full 40-character SHA and tree are required")
    resolved = release_path.resolve(strict=True)
    def git(arg: str) -> str:
        return subprocess.check_output(
            ["git", "-C", str(resolved), "rev-parse", arg], text=True, stderr=subprocess.DEVNULL,
        ).strip().lower()
    actual_sha, actual_tree = git("HEAD"), git("HEAD^{tree}")
    if (actual_sha, actual_tree) != (expected_sha.lower(), expected_tree.lower()):
        raise ValueError("live release HEAD/tree does not match approved identity")
    return {"resolved_path": str(resolved), "head": actual_sha, "tree": actual_tree}


def verify_api_process(resolved_release_path: str) -> int:
    pid_text = subprocess.check_output(
        ["systemctl", "show", "-p", "MainPID", "--value", "grid-api"],
        text=True, stderr=subprocess.DEVNULL,
    ).strip()
    if not pid_text.isdecimal() or int(pid_text) <= 0:
        raise ValueError("grid-api has no live MainPID")
    pid = int(pid_text)
    cwd = Path(f"/proc/{pid}/cwd").resolve(strict=True)
    if cwd != Path(resolved_release_path):
        raise ValueError("grid-api MainPID cwd does not match verified live release")
    return pid


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def open_request(req: Request, timeout: float):
    # A redirect must not turn a fixed loopback GET into a provider/external GET.
    return build_opener(ProxyHandler({}), NoRedirect()).open(req, timeout=timeout)


def fetch_shape(origin: str, name: str, path: str, token: str | None, *, timeout: float = 10) -> dict:
    if (name, path, token is not None) not in PROBES:
        raise ValueError("request is outside the fixed #639 acceptance allowlist")
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = Request(origin + path, headers=headers, method="GET")
    try:
        with open_request(req, timeout=timeout) as response:
            status, body = response.status, response.read(256 * 1024)
    except HTTPError as error:
        status, body = error.code, error.read(256 * 1024)
    try:
        value = json.loads(body)
        keys = sorted(value) if isinstance(value, dict) else []
        health_status = value.get("status") if name == "health" and isinstance(value, dict) else None
        database = value.get("checks", {}).get("database") if name == "health" and isinstance(value, dict) and isinstance(value.get("checks"), dict) else None
    except (ValueError, UnicodeDecodeError):
        keys, health_status, database = [], None, None
    return {"name": name, "path": path, "http_status": status, "top_level_keys": keys,
            "health_status": health_status, "database_check": database}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--release-path", required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--expected-tree", required=True)
    args = parser.parse_args(argv)
    try:
        origin = validate_origin(args.origin)
        identity = verify_release(Path(args.release_path), args.expected_sha, args.expected_tree)
        api_pid = verify_api_process(identity["resolved_path"])
        token = os.environ.get("GRID_ACCEPTANCE_BEARER")
        if not token:
            raise ValueError("GRID_ACCEPTANCE_BEARER must be set in this process environment")
        results = []
        for index, (name, path, needs_auth) in enumerate(PROBES):
            if index:
                time.sleep(1)
            results.append(fetch_shape(origin, name, path, token if needs_auth else None))
        print(json.dumps({"release": identity, "api_pid": api_pid, "probes": results}, indent=2))
        expected_statuses = (200, 401, 200, 200)
        return 0 if tuple(r["http_status"] for r in results) == expected_statuses and results[0]["database_check"] is True else 1
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f"acceptance probe refused or failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
