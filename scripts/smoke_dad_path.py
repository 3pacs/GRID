#!/usr/bin/env python3
"""Live smoke test for the stepdad.finance "dad path".

Deterministic, repeatable evidence that the operator-facing dad workflow on
https://grid.stepdad.finance actually works: health, static PWA assets, a
minted contributor token, the NL composer + streaming verdict, the widget
data endpoints the home page cards call, DB freshness behind those widgets,
recent grid-api error logs, and the state of the deployed release tree.

Read-only against the server: GET/POST the application's own endpoints, mint
a short-lived token through the app's own `api.auth.create_token` helper, run
read-only SELECTs through the app's own DB settings, and read files. Never
writes into a repository tree or data directory, never restarts anything,
and never prints a token, password, key, or anything that looks like a
`.env` value — see `redact_secrets()`.

Usage:
    python3 scripts/smoke_dad_path.py \\
        --base-url http://127.0.0.1:8000 \\
        --release-dir /data/grid_v4/grid_release \\
        --json /tmp/dad_smoke.json \\
        [--budget-ms 5000] [--strict]

Exit codes: 0 nothing broken, 1 something broken AND --strict was passed,
2 something is blocked (regardless of --strict; blocked takes priority).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

# ── Constants shared with the app ────────────────────────────────────────

WIDGET_CATALOG = {
    "verdict",
    "ticker_pulse",
    "watchlist",
    "macro_regime",
    "news",
    "money_flow",
}

DAD_TICKER_SECTIONS = ("gold", "evidence", "chart", "options")

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_RELEASE_DIR = "/data/grid_v4/grid_release"
DEFAULT_BUDGET_MS = 5000

_STATUS_RANK = {"broken": 0, "blocked": 1, "degraded": 2, "ok": 3}


# ── Redaction ─────────────────────────────────────────────────────────────
# Anything that looks like a JWT, a bearer header, or a `KEY=value` /
# `"key": "value"` secret-shaped pair gets scrubbed before it can reach
# stdout, the JSON report, or the GitHub step summary.

_JWT_RE = re.compile(r"eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}")
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{8,}")
_SECRET_KV_RE = re.compile(
    r"(?im)\b((?:GRID_)?[A-Z0-9_]*"
    r"(?:PASSWORD|SECRET|TOKEN|API_KEY|APIKEY|PRIVATE_KEY|ACCESS_KEY)[A-Z0-9_]*)"
    r"(\s*[:=]\s*)([\"']?)([^\s\"'&]{3,})\3"
)
_JSON_SECRET_RE = re.compile(
    r'(?i)("(?:password|secret|token|api_key|apikey|access_key)")(\s*:\s*)"([^"]{3,})"'
)


def redact_secrets(text: str) -> str:
    """Scrub JWTs, bearer headers, and `KEY=value`/`"key": "value"` secrets.

    Best-effort by design — used on log lines and error strings before they
    are ever written to the report, stdout, or the step summary.
    """
    if not text:
        return text
    out = _JWT_RE.sub("[REDACTED]", text)
    out = _BEARER_RE.sub("Bearer [REDACTED]", out)
    out = _SECRET_KV_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}[REDACTED]", out)
    out = _JSON_SECRET_RE.sub(lambda m: f'{m.group(1)}{m.group(2)}"[REDACTED]"', out)
    return out


# ── Result model ──────────────────────────────────────────────────────────


@dataclass
class StepResult:
    name: str
    status: str  # ok | degraded | broken | blocked
    latency_ms: float | None = None
    note: str = ""
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "latency_ms": self.latency_ms,
            "note": redact_secrets(self.note),
            "data": self.data,
        }


# ── Pure helpers (unit-testable without a network) ──────────────────────


def validate_widget_types(widgets: list[dict[str, Any]]) -> list[str]:
    """Return the widget `type` values not present in WIDGET_CATALOG."""
    invalid = []
    for w in widgets or []:
        wtype = w.get("type") if isinstance(w, dict) else None
        if wtype not in WIDGET_CATALOG:
            invalid.append(wtype)
    return invalid


def compose_branch(payload: dict[str, Any]) -> str:
    """Classify a /chat/compose response into one of four branches."""
    if not isinstance(payload, dict):
        return "error"
    if payload.get("alert_created"):
        return "alert_created"
    if payload.get("cannot_fulfill"):
        return "cannot_fulfill"
    if payload.get("spoken_reply") is not None:
        return "normal"
    return "error"


def as_of_age_days(as_of: Any, *, now: datetime | None = None) -> float | None:
    """Age in days of an `as_of` date/datetime-ish value. None on missing/bad input."""
    if as_of in (None, ""):
        return None
    now = now or datetime.now(timezone.utc)
    dt: datetime | None = None
    if isinstance(as_of, datetime):
        dt = as_of
    elif isinstance(as_of, date):
        dt = datetime(as_of.year, as_of.month, as_of.day, tzinfo=timezone.utc)
    else:
        raw = str(as_of).strip()
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            try:
                dt = datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (now - dt).total_seconds() / 86400.0)


def iter_sse_events(buf: str) -> list[dict[str, Any]]:
    """Parse a raw SSE byte-stream chunk into a list of `data:` JSON events.

    Mirrors the parsing in pwa/src/api.js `askStream()`: split on blank
    lines, take `data:` lines, JSON-decode the remainder.
    """
    events: list[dict[str, Any]] = []
    for block in buf.split("\n\n"):
        line = block.strip()
        if not line.startswith("data:"):
            continue
        raw = line[len("data:"):].strip()
        if not raw:
            continue
        try:
            events.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return events


def find_index_asset(html: str) -> str | None:
    m = re.search(r"/assets/index-[^\"'\s]+\.js", html or "")
    return m.group(0) if m else None


def find_mascot_asset(*texts: str) -> str | None:
    """Scan built HTML/JS text for an image reference that looks like the
    stepdad.finance mascot (filename containing 'mascot' or 'stepdad')."""
    pattern = re.compile(
        r"[\"']([^\"'\s]*(?:mascot|stepdad)[^\"'\s]*\.(?:png|jpg|jpeg|svg|webp))[\"']",
        re.IGNORECASE,
    )
    for text in texts:
        m = pattern.search(text or "")
        if m:
            return m.group(1)
    return None


FALLBACK_MASCOT_PATH = "/stepdad-mascot.png"


def find_mascot_asset_on_disk(release_dir: str) -> str | None:
    """Scan built JS chunks under `<release_dir>/pwa_dist/assets` for a
    mascot image reference.

    The PWA's route chunks (e.g. a lazily-loaded `Home-*.js`) are never
    fetched by `step_static`'s HTTP-only scan, so a mascot reference that
    only appears in one of those chunks is invisible to `find_mascot_asset`.
    Reading the built files directly closes that gap without any extra
    network round-trips. Read-only; never touches the network.
    """
    assets_dir = Path(release_dir) / "pwa_dist" / "assets"
    if not assets_dir.is_dir():
        return None
    for js_path in sorted(assets_dir.glob("*.js")):
        try:
            text = js_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        found = find_mascot_asset(text)
        if found:
            return found
    return None


def worst_status(statuses: Iterable[str]) -> str:
    ranked = sorted(statuses, key=lambda s: _STATUS_RANK.get(s, 99))
    return ranked[0] if ranked else "ok"


# ── HTTP helpers ──────────────────────────────────────────────────────────


class Client:
    """Thin requests wrapper: base URL, optional bearer token, timing."""

    def __init__(self, base_url: str, token: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.token = token

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        h = {}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        if extra:
            h.update(extra)
        return h

    def request(
        self,
        method: str,
        path: str,
        *,
        timeout_s: float,
        json_body: dict[str, Any] | None = None,
        stream: bool = False,
    ):
        import requests

        url = path if path.startswith("http") else f"{self.base_url}{path}"
        t0 = time.perf_counter()
        resp = requests.request(
            method,
            url,
            headers=self._headers({"Content-Type": "application/json"} if json_body is not None else None),
            json=json_body,
            timeout=timeout_s,
            stream=stream,
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000
        return resp, elapsed_ms


# ── Steps ─────────────────────────────────────────────────────────────────


def step_health(client: Client, budget_ms: int) -> StepResult:
    try:
        resp, ms = client.request("GET", "/api/v1/system/health", timeout_s=budget_ms / 1000)
    except Exception as exc:
        return StepResult("health", "broken", None, f"request failed: {exc}")
    if resp.status_code != 200:
        return StepResult("health", "broken", ms, f"HTTP {resp.status_code}")
    try:
        payload = resp.json()
    except ValueError:
        return StepResult("health", "broken", ms, "non-JSON response")
    checks = payload.get("checks", {}) if isinstance(payload, dict) else {}
    not_ok = [k for k, v in checks.items() if v is False]
    status = payload.get("status", "unknown")
    result_status = "ok" if status == "ok" else "degraded" if status == "degraded" else "broken"
    note = f"status={status}"
    if not_ok:
        note += f"; not-ok subsystems: {', '.join(not_ok)}"
    return StepResult("health", result_status, ms, note, {"status": status, "not_ok": not_ok})


def step_static(client: Client, budget_ms: int, release_dir: str) -> StepResult:
    sub: list[StepResult] = []
    index_html = ""
    manifest: dict[str, Any] = {}
    sw_text = ""

    try:
        resp, ms = client.request("GET", "/", timeout_s=budget_ms / 1000)
        index_html = resp.text if resp.status_code == 200 else ""
        title_m = re.search(r"<title>([^<]*)</title>", index_html, re.IGNORECASE)
        asset = find_index_asset(index_html)
        note = f"title={title_m.group(1) if title_m else '<none>'}; index_asset={asset or '<none>'}"
        status = "ok" if resp.status_code == 200 and asset else "degraded" if resp.status_code == 200 else "broken"
        sub.append(StepResult("static:/", status, ms, note))
        if asset:
            try:
                aresp, ams = client.request("GET", asset, timeout_s=budget_ms / 1000)
                sub.append(StepResult("static:index-asset", "ok" if aresp.status_code == 200 else "degraded", ams, f"HTTP {aresp.status_code}"))
            except Exception as exc:
                sub.append(StepResult("static:index-asset", "degraded", None, f"fetch failed: {exc}"))
    except Exception as exc:
        sub.append(StepResult("static:/", "broken", None, f"request failed: {exc}"))

    try:
        resp, ms = client.request("GET", "/manifest.json", timeout_s=budget_ms / 1000)
        if resp.status_code == 200:
            manifest = resp.json()
            icons = manifest.get("icons", []) if isinstance(manifest, dict) else []
            icon_ok = 0
            for icon in icons[:6]:
                src = icon.get("src") if isinstance(icon, dict) else None
                if not src:
                    continue
                try:
                    iresp, _ims = client.request("GET", src, timeout_s=budget_ms / 1000)
                    if iresp.status_code == 200:
                        icon_ok += 1
                except Exception:
                    pass
            note = (
                f"name={manifest.get('name')!r} short_name={manifest.get('short_name')!r} "
                f"icons_ok={icon_ok}/{len(icons)}"
            )
            status = "ok" if manifest.get("name") and icon_ok == len(icons) and icons else "degraded"
            sub.append(StepResult("static:manifest.json", status, ms, note))
        else:
            sub.append(StepResult("static:manifest.json", "broken", ms, f"HTTP {resp.status_code}"))
    except Exception as exc:
        sub.append(StepResult("static:manifest.json", "broken", None, f"request failed: {exc}"))

    try:
        resp, ms = client.request("GET", "/service-worker.js", timeout_s=budget_ms / 1000)
        if resp.status_code == 200:
            sw_text = resp.text
            ver_m = re.search(r"(?:VERSION|CACHE_NAME)\s*=\s*['\"]([^'\"]+)['\"]", sw_text)
            note = f"version={ver_m.group(1) if ver_m else '<no version marker found>'}"
            sub.append(StepResult("static:service-worker.js", "ok" if ver_m else "degraded", ms, note))
        else:
            sub.append(StepResult("static:service-worker.js", "broken", ms, f"HTTP {resp.status_code}"))
    except Exception as exc:
        sub.append(StepResult("static:service-worker.js", "broken", None, f"request failed: {exc}"))

    mascot = find_mascot_asset_on_disk(release_dir) or FALLBACK_MASCOT_PATH
    mascot_url = mascot if mascot.startswith("/") else f"/{mascot}"
    try:
        mresp, mms = client.request("GET", mascot_url, timeout_s=budget_ms / 1000)
        sub.append(StepResult(
            "static:mascot", "ok" if mresp.status_code == 200 else "degraded",
            mms, f"{mascot_url} -> HTTP {mresp.status_code}",
        ))
    except Exception as exc:
        sub.append(StepResult("static:mascot", "degraded", None, f"fetch failed: {exc}"))

    total_ms = sum(s.latency_ms or 0 for s in sub)
    status = worst_status(s.status for s in sub)
    note = "; ".join(f"{s.name.split(':', 1)[-1]}={s.status}" for s in sub)
    return StepResult("static", status, total_ms, note, {"substeps": [s.to_dict() for s in sub]})


def mint_contributor_token(release_dir: str) -> tuple[str | None, str]:
    """Mint a 15-minute contributor JWT using the app's own helper.

    Returns (token_or_none, note). Never logs the token itself.
    """
    release_path = Path(release_dir)
    if not release_path.is_dir():
        return None, f"release dir not found: {release_dir}"

    original_cwd = os.getcwd()
    original_path = list(sys.path)
    for mod_name in list(sys.modules):
        if mod_name == "config" or mod_name.startswith("api."):
            del sys.modules[mod_name]
    try:
        os.chdir(release_path)
        sys.path.insert(0, str(release_path))
        try:
            from dotenv import load_dotenv  # type: ignore

            load_dotenv(release_path / ".env")
        except Exception:
            pass
        from api.auth import create_token  # type: ignore

        token = create_token(role="contributor", username="dad-smoke", expires_hours=0.25)
        if not token or not isinstance(token, str):
            return None, "create_token returned empty"
        return token, "minted 15-minute contributor token via api.auth.create_token"
    except Exception as exc:
        return None, f"token mint failed: {exc}"
    finally:
        os.chdir(original_cwd)
        sys.path[:] = original_path
        for mod_name in list(sys.modules):
            if mod_name == "config" or mod_name.startswith("api."):
                del sys.modules[mod_name]


def step_composer(client: Client, budget_ms: int) -> StepResult:
    sub: list[StepResult] = []
    llm_timeout_s = max(budget_ms * 10, 65_000) / 1000

    questions = [
        "Show me Apple and Tesla, what's gold doing, should I worry",
        "tell me when NVDA drops 5 percent",
    ]
    for q in questions:
        try:
            resp, ms = client.request(
                "POST", "/api/v1/chat/compose", timeout_s=llm_timeout_s,
                json_body={"question": q, "history": []},
            )
        except Exception as exc:
            sub.append(StepResult(f"compose:{q[:24]}", "broken", None, f"request failed: {exc}"))
            continue
        if resp.status_code != 200:
            sub.append(StepResult(f"compose:{q[:24]}", "broken", ms, f"HTTP {resp.status_code}"))
            continue
        try:
            payload = resp.json()
        except ValueError:
            sub.append(StepResult(f"compose:{q[:24]}", "broken", ms, "non-JSON response"))
            continue
        branch = compose_branch(payload)
        invalid = validate_widget_types(payload.get("widgets", []))
        reply_len = len(payload.get("spoken_reply") or "")
        status = "ok" if branch != "error" and not invalid else "degraded" if branch != "error" else "broken"
        note = f"branch={branch} widgets={len(payload.get('widgets', []))} reply_len={reply_len}"
        if invalid:
            note += f" invalid_widget_types={invalid}"
        sub.append(StepResult(f"compose:{q[:24]}", status, ms, note, {"branch": branch, "reply_len": reply_len}))

    # ask/stream — time to first token + total, capped at 120s.
    try:
        resp, _ = client.request(
            "POST", "/api/v1/chat/ask/stream", timeout_s=120,
            json_body={"question": questions[0], "history": []}, stream=True,
        )
        if resp.status_code != 200:
            sub.append(StepResult("ask_stream", "broken", None, f"HTTP {resp.status_code}"))
        else:
            t0 = time.perf_counter()
            ttft_ms: float | None = None
            got_delta = False
            got_error = False
            buf = ""
            for chunk in resp.iter_content(chunk_size=None, decode_unicode=True):
                if not chunk:
                    continue
                buf += chunk
                for evt in iter_sse_events(buf):
                    if evt.get("error"):
                        got_error = True
                    elif evt.get("delta") and ttft_ms is None:
                        ttft_ms = (time.perf_counter() - t0) * 1000
                        got_delta = True
                    if evt.get("done"):
                        break
                buf = ""
                if (time.perf_counter() - t0) > 120:
                    break
            total_ms = (time.perf_counter() - t0) * 1000
            status = "ok" if got_delta and not got_error else "broken" if got_error else "degraded"
            note = f"ttft_ms={ttft_ms} total_ms={round(total_ms, 1)}"
            sub.append(StepResult("ask_stream", status, total_ms, note, {"ttft_ms": ttft_ms}))
    except Exception as exc:
        sub.append(StepResult("ask_stream", "broken", None, f"stream failed: {exc}"))

    total_ms = sum(s.latency_ms or 0 for s in sub)
    status = worst_status(s.status for s in sub)
    note = "; ".join(f"{s.name}={s.status}" for s in sub)
    return StepResult("composer", status, total_ms, note, {"substeps": [s.to_dict() for s in sub]})


def step_widget_data(client: Client, budget_ms: int) -> StepResult:
    sub: list[StepResult] = []
    timeout_s = max(budget_ms, 15_000) / 1000

    for ticker in ("AAPL", "TSLA", "GLD"):
        try:
            resp, ms = client.request("GET", f"/api/v1/watchlist/{ticker}/quote", timeout_s=timeout_s)
        except Exception as exc:
            sub.append(StepResult(f"quote:{ticker}", "broken", None, f"request failed: {exc}"))
            continue
        if resp.status_code != 200:
            sub.append(StepResult(f"quote:{ticker}", "broken", ms, f"HTTP {resp.status_code}"))
            continue
        payload = resp.json()
        age = as_of_age_days(payload.get("as_of"))
        price = payload.get("price")
        change_null = payload.get("change_pct") is None
        status = "ok" if price is not None else "degraded"
        note = f"price={price} as_of_age_days={age} change_pct_null={change_null}"
        sub.append(StepResult(f"quote:{ticker}", status, ms, note, {"as_of_age_days": age}))

    try:
        resp, ms = client.request("GET", "/api/v1/flows/sectors", timeout_s=timeout_s)
        if resp.status_code != 200:
            sub.append(StepResult("flows/sectors", "broken", ms, f"HTTP {resp.status_code}"))
        else:
            payload = resp.json()
            sectors = payload.get("sectors", []) if isinstance(payload, dict) else []
            dates = []
            for s in sectors if isinstance(sectors, list) else []:
                for key in ("as_of", "date", "obs_date"):
                    if isinstance(s, dict) and s.get(key):
                        dates.append(str(s[key]))
            newest = max(dates) if dates else payload.get("as_of") if isinstance(payload, dict) else None
            status = "ok" if sectors else "degraded"
            sub.append(StepResult("flows/sectors", status, ms, f"count={len(sectors)} newest_date={newest}"))
    except Exception as exc:
        sub.append(StepResult("flows/sectors", "broken", None, f"request failed: {exc}"))

    try:
        resp, ms = client.request("GET", "/api/v1/alerts", timeout_s=timeout_s)
        if resp.status_code != 200:
            sub.append(StepResult("alerts", "broken", ms, f"HTTP {resp.status_code}"))
        else:
            payload = resp.json()
            count = len(payload.get("alerts", [])) if isinstance(payload, dict) else 0
            sub.append(StepResult("alerts", "ok", ms, f"count={count}"))
    except Exception as exc:
        sub.append(StepResult("alerts", "broken", None, f"request failed: {exc}"))

    for section in DAD_TICKER_SECTIONS:
        try:
            resp, ms = client.request("GET", f"/api/v1/dad/ticker/AAPL/{section}", timeout_s=timeout_s)
        except Exception as exc:
            sub.append(StepResult(f"dad:{section}", "broken", None, f"request failed: {exc}"))
            continue
        if resp.status_code != 200:
            sub.append(StepResult(f"dad:{section}", "broken", ms, f"HTTP {resp.status_code}"))
            continue
        payload = resp.json()
        keys = sorted(payload.keys()) if isinstance(payload, dict) else []
        error_field = payload.get("error") if isinstance(payload, dict) else None
        stale_field = payload.get("status") if isinstance(payload, dict) else None
        status = "ok" if not error_field else "degraded"
        sub.append(StepResult(
            f"dad:{section}", status, ms,
            f"keys={keys[:10]} status_field={stale_field!r} error={error_field!r}",
        ))

    try:
        resp, ms = client.request("GET", "/api/v1/regime/current", timeout_s=timeout_s)
        sub.append(StepResult(
            "macro_regime", "ok" if resp.status_code == 200 else "broken", ms, f"HTTP {resp.status_code}",
        ))
    except Exception as exc:
        sub.append(StepResult("macro_regime", "broken", None, f"request failed: {exc}"))

    try:
        resp, ms = client.request("GET", "/api/v1/physics/momentum?lookback_days=63", timeout_s=timeout_s)
        sub.append(StepResult(
            "news", "ok" if resp.status_code == 200 else "broken", ms, f"HTTP {resp.status_code}",
        ))
    except Exception as exc:
        sub.append(StepResult("news", "broken", None, f"request failed: {exc}"))

    total_ms = sum(s.latency_ms or 0 for s in sub)
    status = worst_status(s.status for s in sub)
    note = "; ".join(f"{s.name}={s.status}" for s in sub)
    return StepResult("widget_data", status, total_ms, note, {"substeps": [s.to_dict() for s in sub]})


def step_freshness(release_dir: str) -> StepResult:
    original_cwd = os.getcwd()
    original_path = list(sys.path)
    for mod_name in list(sys.modules):
        if mod_name in ("config", "db") or mod_name.startswith("api."):
            del sys.modules[mod_name]
    try:
        release_path = Path(release_dir)
        if not release_path.is_dir():
            return StepResult("freshness", "blocked", None, f"release dir not found: {release_dir}")
        os.chdir(release_path)
        sys.path.insert(0, str(release_path))
        try:
            from dotenv import load_dotenv  # type: ignore

            load_dotenv(release_path / ".env")
        except Exception:
            pass
        from db import get_engine  # type: ignore
        from sqlalchemy import text  # type: ignore

        engine = get_engine()
        data: dict[str, Any] = {}
        notes = []
        with engine.connect() as conn:
            for ticker in ("SPY", "AAPL", "TSLA"):
                row = conn.execute(
                    text(
                        "SELECT MAX(rs.obs_date) FROM resolved_series rs "
                        "JOIN feature_registry fr ON fr.id = rs.feature_id "
                        "WHERE fr.name ILIKE :pattern"
                    ),
                    {"pattern": f"{ticker.lower()}%"},
                ).fetchone()
                max_date = row[0] if row else None
                age = as_of_age_days(max_date)
                data[ticker] = {"max_obs_date": str(max_date) if max_date else None, "age_days": age}
                notes.append(f"{ticker} age_days={age}")

            row = conn.execute(text("SELECT MAX(obs_date) FROM regime_history")).fetchone()
            max_regime = row[0] if row else None
            age = as_of_age_days(max_regime)
            data["regime_history"] = {"max_obs_date": str(max_regime) if max_regime else None, "age_days": age}
            notes.append(f"regime_history age_days={age}")

        ages = [v["age_days"] for v in data.values() if v.get("age_days") is not None]
        status = "ok" if ages and max(ages) <= 7 else "degraded" if ages else "broken"
        return StepResult("freshness", status, None, "; ".join(notes), data)
    except Exception as exc:
        return StepResult("freshness", "blocked", None, f"freshness query failed: {exc}")
    finally:
        os.chdir(original_cwd)
        sys.path[:] = original_path
        for mod_name in list(sys.modules):
            if mod_name in ("config", "db") or mod_name.startswith("api."):
                del sys.modules[mod_name]


def step_logs() -> StepResult:
    try:
        proc = subprocess.run(
            ["journalctl", "-u", "grid-api", "--since", "-2h", "--no-pager"],
            capture_output=True, text=True, timeout=30, check=False,
        )
    except FileNotFoundError:
        return StepResult("logs", "blocked", None, "journalctl not available on this host")
    except Exception as exc:
        return StepResult("logs", "blocked", None, f"journalctl invocation failed: {exc}")

    if proc.returncode != 0:
        return StepResult(
            "logs", "blocked", None,
            f"journalctl exited {proc.returncode} (likely not permitted): {redact_secrets(proc.stderr[:300])}",
        )

    pattern = re.compile(r"error|traceback|timeout", re.IGNORECASE)
    matches = [line for line in proc.stdout.splitlines() if pattern.search(line)]
    tail = matches[-30:]
    redacted_tail = [redact_secrets(line) for line in tail]
    status = "degraded" if tail else "ok"
    note = f"{len(matches)} matching lines in last 2h (showing last {len(tail)})"
    return StepResult("logs", status, None, note, {"lines": redacted_tail})


def step_deploy_tree(release_dir: str) -> StepResult:
    release_path = Path(release_dir)
    if not release_path.is_dir():
        return StepResult("deploy_tree", "blocked", None, f"release dir not found: {release_dir}")

    data: dict[str, Any] = {}
    notes = []
    try:
        log_proc = subprocess.run(
            ["git", "-C", str(release_path), "log", "-1", "--oneline"],
            capture_output=True, text=True, timeout=15, check=False,
        )
        data["git_log"] = log_proc.stdout.strip()
        notes.append(f"HEAD={log_proc.stdout.strip()}")
    except Exception as exc:
        notes.append(f"git log failed: {exc}")

    try:
        status_proc = subprocess.run(
            ["git", "-C", str(release_path), "status", "--short"],
            capture_output=True, text=True, timeout=15, check=False,
        )
        dirty_count = len([ln for ln in status_proc.stdout.splitlines() if ln.strip()])
        data["dirty_files"] = dirty_count
        notes.append(f"dirty_files={dirty_count}")
    except Exception as exc:
        notes.append(f"git status failed: {exc}")
        dirty_count = None

    index_path = release_path / "pwa_dist" / "index.html"
    canonical_path = Path("/data/grid_v4/pwa_dist_canonical") / "index.html"
    if index_path.is_file():
        release_sha = hashlib.sha256(index_path.read_bytes()).hexdigest()
        data["release_index_sha256"] = release_sha
        if canonical_path.is_file():
            canonical_sha = hashlib.sha256(canonical_path.read_bytes()).hexdigest()
            data["canonical_index_sha256"] = canonical_sha
            match = release_sha == canonical_sha
            data["pwa_dist_matches_canonical"] = match
            notes.append(f"pwa_dist matches canonical: {match}")
        else:
            notes.append("no pwa_dist_canonical directory — skipped comparison")
    else:
        notes.append("release pwa_dist/index.html not found")

    status = "ok"
    if dirty_count and dirty_count > 0:
        status = "degraded"
    if not index_path.is_file():
        status = "degraded"
    return StepResult("deploy_tree", status, None, "; ".join(notes), data)


# ── Report rendering ──────────────────────────────────────────────────────


def render_report(steps: list[StepResult], meta: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Dad Path Smoke Report")
    lines.append("")
    lines.append(f"- base_url: `{meta.get('base_url')}`")
    lines.append(f"- release_dir: `{meta.get('release_dir')}`")
    lines.append(f"- generated_at: {meta.get('generated_at')}")
    lines.append("")

    works = [s for s in steps if s.status == "ok"]
    degraded = [s for s in steps if s.status == "degraded"]
    broken = [s for s in steps if s.status == "broken"]
    blocked = [s for s in steps if s.status == "blocked"]

    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **Works** ({len(works)}): {', '.join(s.name for s in works) or 'none'}")
    lines.append(f"- **Degraded** ({len(degraded)}): {', '.join(s.name for s in degraded) or 'none'}")
    lines.append(f"- **Broken** ({len(broken)}): {', '.join(s.name for s in broken) or 'none'}")
    lines.append(f"- **Blocked** ({len(blocked)}): {', '.join(s.name for s in blocked) or 'none'}")
    lines.append("")

    lines.append("## Evidence")
    lines.append("")
    lines.append("| Step | Status | Latency (ms) | Note |")
    lines.append("|---|---|---|---|")
    for s in steps:
        latency = f"{s.latency_ms:.0f}" if isinstance(s.latency_ms, (int, float)) else "-"
        note = redact_secrets(s.note).replace("|", "\\|")
        lines.append(f"| {s.name} | {s.status} | {latency} | {note} |")
    lines.append("")

    freshness = next((s for s in steps if s.name == "freshness"), None)
    if freshness and freshness.data:
        lines.append("## Freshness")
        lines.append("")
        for key, val in freshness.data.items():
            if isinstance(val, dict):
                lines.append(f"- {key}: max_obs_date={val.get('max_obs_date')} age_days={val.get('age_days')}")
        lines.append("")

    logs_step = next((s for s in steps if s.name == "logs"), None)
    lines.append("## Errors (grid-api, last 2h)")
    lines.append("")
    if logs_step and logs_step.status == "blocked":
        lines.append(f"journalctl unavailable: {redact_secrets(logs_step.note)}")
    elif logs_step and logs_step.data.get("lines"):
        for line in logs_step.data["lines"]:
            lines.append(f"    {line}")
    else:
        lines.append("No matching error/traceback/timeout lines in the last 2h.")
    lines.append("")

    if blocked:
        lines.append("## Blocked items")
        lines.append("")
        for s in blocked:
            lines.append(f"- **{s.name}**: {redact_secrets(s.note)}")
        lines.append("")

    problems = broken + degraded
    problems.sort(key=lambda s: _STATUS_RANK.get(s.status, 99))
    lines.append("## Top problems")
    lines.append("")
    if not problems:
        lines.append("None — every step reported ok.")
    else:
        for i, s in enumerate(problems, 1):
            lines.append(f"{i}. **[{s.status}] {s.name}** — {redact_secrets(s.note)}")
    lines.append("")

    return "\n".join(lines)


# ── Orchestration ─────────────────────────────────────────────────────────


def run(args: argparse.Namespace) -> tuple[int, str, dict[str, Any]]:
    client = Client(args.base_url)

    steps: list[StepResult] = []
    steps.append(step_health(client, args.budget_ms))
    steps.append(step_static(client, args.budget_ms, args.release_dir))

    token, auth_note = mint_contributor_token(args.release_dir)
    if token:
        client.token = token
        steps.append(StepResult("auth", "ok", None, auth_note))
    else:
        steps.append(StepResult("auth", "blocked", None, auth_note))

    steps.append(step_composer(client, args.budget_ms))
    steps.append(step_widget_data(client, args.budget_ms))
    steps.append(step_freshness(args.release_dir))
    steps.append(step_logs())
    steps.append(step_deploy_tree(args.release_dir))

    meta = {
        "base_url": args.base_url,
        "release_dir": args.release_dir,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    report = render_report(steps, meta)

    has_broken = any(s.status == "broken" for s in steps)
    has_blocked = any(s.status == "blocked" for s in steps)
    if has_blocked:
        exit_code = 2
    elif has_broken and args.strict:
        exit_code = 1
    else:
        exit_code = 0

    result = {
        "meta": meta,
        "steps": [s.to_dict() for s in steps],
        "exit_code": exit_code,
    }
    return exit_code, report, result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--release-dir", default=DEFAULT_RELEASE_DIR)
    parser.add_argument("--json", default=None, help="Path to write the structured JSON result.")
    parser.add_argument("--budget-ms", type=int, default=DEFAULT_BUDGET_MS)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args(argv)

    exit_code, report, result = run(args)

    print(report)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(report)
            f.write("\n")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, default=str)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
