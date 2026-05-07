#!/usr/bin/env python3
"""Bake a static GoDaddy mirror of grid.stepdad.finance.

What it does:
  1. Crawls the live PWA bundle (index.html + every /assets/* + /icons/*
     + manifest + service-worker) and mirrors them verbatim.
  2. Mints a short-lived admin JWT from the local ``GRID_JWT_SECRET`` and
     fetches every read-only API endpoint the live PWA references,
     writing each successful response to disk at the URL it was fetched
     from. Failed/private/POST endpoints are skipped.
  3. Bakes a fake ``/api/v1/auth/verify`` response so the PWA's bootstrap
     auth check succeeds in mirror mode (the mirror is gated by the
     surrounding cPanel basic-auth; the JWT layer is decorative inside
     that perimeter).
  4. Writes a SPA-fallback ``.htaccess`` and an injected snapshot banner.

Output: ``dist/godaddy/`` (~1-3 MB, varies with API payloads).

Run:
    python3 scripts/offline_bake/bake.py
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, urlencode, parse_qs

LIVE = "https://grid.stepdad.finance"
REPO = Path(__file__).resolve().parents[2]
OUT = REPO / "dist" / "godaddy"
ENV_FILE = REPO / ".env"

ROOT_PAGES = ["/"]
EXPLICIT_ASSETS = ["/manifest.json", "/service-worker.js", "/favicon.ico"]
ICON_SIZES = [76, 120, 152, 180, 192, 512]

# Endpoints baked with parameters to fetch a useful slice of the live data.
# Each entry: (method, path, output_filename, query_dict_or_None)
PARAMETERIZED_BAKES = [
    ("GET", "/api/v1/astrogrid/predictions/latest", "api/v1/astrogrid/predictions/latest.json", {"limit": 50}),
    ("GET", "/api/v1/journal", "api/v1/journal.json", {"limit": 100}),
    ("GET", "/api/v1/options/scan", "api/v1/options/scan.json", {"min_score": 5.0}),
    ("GET", "/api/v1/agents/runs", "api/v1/agents/runs.json", {"limit": 50}),
    ("GET", "/api/v1/agents/backtest/summary", "api/v1/agents/backtest/summary.json", {"days_back": 30}),
    ("GET", "/api/v1/regime/history", "api/v1/regime/history.json", {"days": 90}),
    ("GET", "/api/v1/associations/anomalies", "api/v1/associations/anomalies.json", {"sigma_threshold": 2.0}),
    ("GET", "/api/v1/associations/correlation-matrix", "api/v1/associations/correlation-matrix.json", {"days": 90}),
    ("GET", "/api/v1/associations/regime-features", "api/v1/associations/regime-features.json", {"days": 90}),
    # The default landing view (surfacer) calls this on boot — empty UI without it.
    ("GET", "/api/v1/surfacer/candidates", "api/v1/surfacer/candidates.json",
     {"limit": 18, "fresh_only": "false", "queue_missing_data": "false"}),
    ("GET", "/api/v1/intelligence/cross-reference/history", "api/v1/intelligence/cross-reference/history.json", {"days": 90}),
    ("GET", "/api/v1/trials/signals", "api/v1/trials/signals.json", {"limit": 50}),
    ("GET", "/api/v1/trials/sponsors", "api/v1/trials/sponsors.json", {"limit": 20}),
]

# Endpoints we deliberately *don't* bake — POST/PUT/DELETE or anything
# that mutates server state. Pattern-matched against the path.
SKIP_PATTERNS = [
    r"^/api/v1/auth/",        # auth flow handled separately by verify stub
    r"/(start|stop|run|publish|generate|refresh|subscribe|unsubscribe|test)$",
    r"/(create|delete|assign|simulate|update)\b",
    r"^/api/v1/notifications/",
    r"/login$", r"/logout$", r"/register$",
]

# Match both absolute (/assets/x) and relative (assets/x) asset references.
# Vite emits dynamic-import paths as "assets/Foo.js" without a leading slash
# inside the bundles — both must be captured or every code-split route 404s.
ASSET_RE = re.compile(
    r'''["'(]/?((?:assets|icons|fonts|images)/[A-Za-z0-9_./~-]+\.(?:js|mjs|css|png|svg|jpg|jpeg|webp|woff2?|ttf|eot|json|map))["')]'''
)


def load_env_var(key: str) -> str:
    if not ENV_FILE.exists():
        return ""
    for line in ENV_FILE.read_text().splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def mint_admin_jwt() -> str:
    secret = load_env_var("GRID_JWT_SECRET")
    if not secret or secret == "dev-secret-change-me":
        raise RuntimeError("GRID_JWT_SECRET missing in repo .env")
    try:
        from jose import jwt as jose_jwt
    except ImportError as exc:
        raise RuntimeError("python-jose not installed: pip install python-jose") from exc
    payload = {
        "sub": "mirror-baker",
        "role": "admin",
        "exp": datetime.now(timezone.utc) + timedelta(hours=24),
        "iat": datetime.now(timezone.utc),
    }
    return jose_jwt.encode(payload, secret, algorithm="HS256")


def fetch(url: str, *, token: str = "", max_bytes: int = 25 * 1024 * 1024,
          timeout: int = 15) -> tuple[int, dict, bytes]:
    headers = {"User-Agent": "grid-godaddy-mirror-baker/2.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read(max_bytes + 1)
        if len(data) > max_bytes:
            raise RuntimeError(f"{url}: exceeds max_bytes={max_bytes}")
        return resp.status, dict(resp.headers), data


def write(rel_path: str, body: bytes) -> int:
    target = OUT / rel_path.lstrip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)
    return len(body)


def discover_assets(text: str) -> set[str]:
    # Always normalize to an absolute path so the queue sees one shape.
    return {"/" + m.lstrip("/") for m in ASSET_RE.findall(text)}


def is_skipped(path: str) -> bool:
    return any(re.search(p, path) for p in SKIP_PATTERNS)


def crawl_pwa() -> int:
    """Returns the count of successfully fetched assets (excluding the
    icons we always try). Caller uses this to refuse deploy on near-empty
    output."""
    seen: set[str] = set()
    queue: list[str] = []

    for path in ROOT_PAGES:
        try:
            _, _, body = fetch(LIVE + path)
        except Exception as exc:
            print(f"  FAIL   page {path}: {exc}", file=sys.stderr)
            continue
        rel = "index.html" if path == "/" else path.lstrip("/")
        n = write(rel, body)
        print(f"  page   {rel:60s} {n:>8} bytes")
        text = body.decode("utf-8", errors="ignore")
        queue.extend(discover_assets(text))

    queue.extend(EXPLICIT_ASSETS)
    queue.extend(f"/icons/icon-{s}.png" for s in ICON_SIZES)

    asset_count = 0
    while queue:
        rel = queue.pop(0)
        rel = urlparse(rel).path
        if rel in seen or not rel.startswith("/"):
            continue
        seen.add(rel)
        try:
            _, headers, body = fetch(LIVE + rel)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                pass
            else:
                print(f"  FAIL   {rel}: HTTP {exc.code}", file=sys.stderr)
            continue
        except Exception as exc:
            print(f"  FAIL   {rel}: {exc}", file=sys.stderr)
            continue
        n = write(rel, body)
        ctype = headers.get("Content-Type", "")
        kind = "asset" if rel.startswith("/assets/") else (
            "icon" if rel.startswith("/icons/") else "static"
        )
        print(f"  {kind:6s} {rel:60s} {n:>8} bytes")
        if kind == "asset":
            asset_count += 1
        if "javascript" in ctype or "css" in ctype or "json" in ctype \
                or rel.endswith((".js", ".css", ".json", ".html", ".webmanifest")):
            text = body.decode("utf-8", errors="ignore")
            for child in discover_assets(text):
                if child not in seen:
                    queue.append(child)
    return asset_count


def discover_api_endpoints() -> list[str]:
    """Mine the freshly-crawled JS bundles for /api/v1/ string literals."""
    paths: set[str] = set()
    asset_dir = OUT / "assets"
    if not asset_dir.exists():
        return []
    api_re = re.compile(r'"(/api/v1/[A-Za-z0-9_/.-]+)"')
    for js in asset_dir.glob("*.js"):
        text = js.read_text(encoding="utf-8", errors="ignore")
        paths.update(api_re.findall(text))
    return sorted(paths)


def bake_endpoint(path: str, token: str, query: dict | None = None,
                  out_rel: str | None = None, retries: int = 2) -> tuple[str, int, int]:
    """Fetch one read-only endpoint and persist as JSON. Retries on
    network errors (which the live server returns frequently when busy)."""
    url = LIVE + path
    if query:
        url += "?" + urlencode(query)
    rel = out_rel or (path.lstrip("/") + ".json")
    last_code, last_size = 0, 0
    for attempt in range(retries + 1):
        try:
            code, _, body = fetch(url, token=token, timeout=20)
        except urllib.error.HTTPError as exc:
            return path, exc.code, 0
        except Exception:
            last_code, last_size = 0, 0
            time.sleep(1.5 * (attempt + 1))
            continue
        if code != 200 or len(body) < 3:
            return path, code, len(body)
        try:
            json.loads(body)
        except ValueError:
            return path, code, -1
        n = write(rel, body)
        return path, code, n
    return path, last_code, last_size


def bake_api(token: str) -> None:
    print("\n--- discovering API endpoints from PWA bundles ---")
    endpoints = discover_api_endpoints()
    print(f"  found {len(endpoints)} candidate /api/v1/* paths in JS")

    # Parameterized first (richer data), then everything else
    print("\n--- parameterized bakes ---")
    seen_targets: set[str] = set()
    for method, path, out_rel, query in PARAMETERIZED_BAKES:
        if method != "GET":
            continue
        seen_targets.add(out_rel)
        path_, code, n = bake_endpoint(path, token, query=query, out_rel=out_rel)
        flag = "ok " if code == 200 and n > 0 else "skip"
        print(f"  {flag} {code:>3} {n:>8}  {path_}?{urlencode(query)}")

    print("\n--- bulk bake (all discovered GET endpoints, parallel) ---")
    todo = [p for p in endpoints if not is_skipped(p)]
    print(f"  {len(todo)} endpoints to probe")

    successes = 0
    failures: dict[int, int] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        futures = {
            ex.submit(bake_endpoint, p, token, None, None): p for p in todo
        }
        for fut in concurrent.futures.as_completed(futures):
            path, code, n = fut.result()
            if code == 200 and n > 0:
                successes += 1
            else:
                failures[code] = failures.get(code, 0) + 1
    print(f"\n  baked: {successes}")
    print(f"  skipped by code: {dict(sorted(failures.items()))}")


_ROUTE_PATCH_BEFORE = (
    'function be(i){const e=ji[i];'
    'if(!e)throw new Error(`Route component not found: ${i}`);'
    'return te.lazy(e)}'
)
_ROUTE_PATCH_AFTER = (
    'function be(i){const e=ji[i]||'
    '(()=>{console.warn("[mirror] no chunk for "+i);'
    'return Promise.resolve({default:()=>null});});'
    'return te.lazy(e)}'
)


def patch_route_loader() -> None:
    """The deployed PWA build has a routes table referencing
    './views/EdgeScanner.jsx' that isn't in the import map, so the route
    loader throws at boot and nothing renders. We patch the loader to
    return an empty component instead of throwing — same fallback you'd
    want for any view we couldn't fetch a chunk for.

    This bug exists on the live site too; the patch is harmless there
    and necessary on the mirror. If/when the build is fixed upstream
    the substitution simply won't match and this is a no-op."""
    bundle_dir = OUT / "assets"
    if not bundle_dir.exists():
        return
    patched = 0
    for js in bundle_dir.glob("index-*.js"):
        text = js.read_text(encoding="utf-8")
        if _ROUTE_PATCH_BEFORE in text:
            js.write_text(text.replace(_ROUTE_PATCH_BEFORE, _ROUTE_PATCH_AFTER), encoding="utf-8")
            patched += 1
            print(f"  patched route loader in {js.name}")
    if patched == 0:
        print("  (route-loader pattern not found — bundle may have been rebuilt upstream)")


def write_auth_verify_stub(token: str) -> None:
    """The PWA calls /api/v1/auth/verify on boot. Bake a permanent OK so the
    UI doesn't bounce to login (the surrounding cPanel basic-auth is the
    real perimeter). The token returned in the body is the same admin JWT
    the bake used; it'll be valid as long as the live server's
    GRID_JWT_SECRET is unchanged."""
    expires = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
    payload = {
        "valid": True,
        "expires_at": expires,
        "role": "admin",
        "username": "mirror-guest",
    }
    write("api/v1/auth/verify.json", (json.dumps(payload, indent=2) + "\n").encode())


def overwrite_service_worker() -> None:
    """Replace the live PWA's service worker with a self-unregistering
    noop. Without this, browsers that visited the mirror earlier (or the
    live site, since the SW lives at the same origin) keep serving the
    old cached bundle — including pre-patch versions that crash on the
    EdgeScanner route. The noop SW also clears all caches so the browser
    falls back to the network on next reload."""
    body = b"""// stepdadfi mirror: unregister any installed SW + clear caches.
self.addEventListener('install', e => self.skipWaiting());
self.addEventListener('activate', e => {
  e.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.map(k => caches.delete(k)));
    const regs = (await self.registration.unregister()) ? [] : [];
    const clients = await self.clients.matchAll({ type: 'window' });
    clients.forEach(c => c.navigate(c.url));
  })());
});
self.addEventListener('fetch', e => {});
"""
    write("service-worker.js", body)


def write_health() -> None:
    payload = {
        "status": "degraded",
        "mode": "godaddy-mirror",
        "host": "stepdadfi.com",
        "live_host": "grid.stepdad.finance",
        "snapshot_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "note": "Static fallback for grid.stepdad.finance. POST endpoints "
                "are unavailable; GET data is point-in-time snapshot.",
    }
    write("api/health.json", (json.dumps(payload, indent=2) + "\n").encode())


def write_htaccess() -> None:
    body = b"""DirectoryIndex index.html
Options -Indexes -MultiViews

<IfModule mod_mime.c>
  AddType application/json .json
  AddType application/javascript .js
  AddType application/manifest+json .webmanifest
  AddType image/svg+xml .svg
</IfModule>

<IfModule mod_deflate.c>
  AddOutputFilterByType DEFLATE text/html text/css application/javascript application/json image/svg+xml
</IfModule>

<IfModule mod_headers.c>
  Header set X-Content-Type-Options "nosniff"
  Header set X-Frame-Options "SAMEORIGIN"
  Header set Referrer-Policy "strict-origin-when-cross-origin"
  <FilesMatch "^(assets|icons|fonts|images)/.+\\.(js|css|png|svg|woff2?|ttf|eot|jpg|jpeg|webp|avif)$">
    Header set Cache-Control "public, max-age=31536000, immutable"
  </FilesMatch>
  <FilesMatch "^(index\\.html|manifest\\.json|service-worker\\.js|api/.+\\.json)$">
    Header set Cache-Control "public, max-age=60, must-revalidate"
  </FilesMatch>
</IfModule>

RewriteEngine On

# Strip querystring on baked GET endpoints (?limit=N etc.)
RewriteRule ^api/v1/astrogrid/predictions/latest$ /api/v1/astrogrid/predictions/latest.json [L]
RewriteRule ^api/v1/journal$                       /api/v1/journal.json [L]
RewriteRule ^api/v1/options/scan$                  /api/v1/options/scan.json [L]
RewriteRule ^api/v1/agents/runs$                   /api/v1/agents/runs.json [L]
RewriteRule ^api/v1/agents/backtest/summary$       /api/v1/agents/backtest/summary.json [L]
RewriteRule ^api/v1/regime/history$                /api/v1/regime/history.json [L]
RewriteRule ^api/v1/associations/anomalies$        /api/v1/associations/anomalies.json [L]
RewriteRule ^api/v1/associations/correlation-matrix$  /api/v1/associations/correlation-matrix.json [L]
RewriteRule ^api/v1/associations/regime-features$  /api/v1/associations/regime-features.json [L]

# /api/v1/auth/verify -> baked stub (PWA boot check). [END] stops further passes.
RewriteRule ^api/v1/auth/verify$ /api/v1/auth/verify.json [END]

# Generic /api/v1/foo -> /api/v1/foo.json fallback (querystring-stripped)
RewriteCond %{REQUEST_URI} ^/api/v1/
RewriteCond %{REQUEST_FILENAME} !-f
RewriteCond %{REQUEST_FILENAME}.json -f
RewriteRule ^(.*)$ /$1.json [END]

# Disabled / write endpoints -> structured JSON 503 (skip verify which we serve)
RewriteCond %{REQUEST_URI} ^/api/v1/(auth|notifications)/
RewriteCond %{REQUEST_URI} !^/api/v1/auth/verify(\.json)?$
RewriteRule ^ /api/_disabled.json [END]

# Any /api/ path that wasn't matched above -> _disabled.json (NOT the SPA shell)
RewriteCond %{REQUEST_URI} ^/api/
RewriteCond %{REQUEST_FILENAME} !-f
RewriteRule ^ /api/_disabled.json [END]

# SPA fallback for client-side routes (anything not under /api/, not a real file)
RewriteCond %{REQUEST_URI} !^/api/
RewriteCond %{REQUEST_FILENAME} !-f
RewriteCond %{REQUEST_FILENAME} !-d
RewriteRule ^ /index.html [L]

# Basic-auth perimeter on the SHELL. The /api/ subdir overrides this
# with its own .htaccess that grants public access -- the PWA sends
# `Authorization: Bearer <jwt>` for those calls and HTTP allows only
# one Authorization header per request, so the PWA's Bearer overrides
# Basic, Apache 401s, the PWA assumes its token is dead, clears
# localStorage, and bounces to /login. Splitting auth keeps the PWA
# functional. Static JSON snapshots are reachable to anyone who guesses
# the URL, but the HTML/JS shell still requires the basic-auth gate.
AuthType Basic
AuthName "GRID Mirror"
AuthUserFile /home/h2hb4v1an7lh/.htpasswds/stepdadfi.com/.htpasswd
Require valid-user
"""
    write(".htaccess", body)
    # /api/ subdir: drop the basic-auth perimeter so PWA Bearer requests work.
    # Apache 2.4 syntax — `Satisfy Any`/`Allow from all` is mod_access_compat
    # only and silently no-ops on stock 2.4 builds.
    write("api/.htaccess", b"""AuthType None
Require all granted
""")
    write("api/_disabled.json", (json.dumps({
        "error": "endpoint_disabled_in_mirror",
        "mode": "godaddy-mirror",
        "live_host": "grid.stepdad.finance",
        "message": "This endpoint requires the live backend.",
    }, indent=2) + "\n").encode())


_BANNER_TEMPLATE = """<style>
.grid-mirror-banner{{
  position:fixed;top:0;left:0;right:0;z-index:99999;
  background:linear-gradient(90deg,#3a2a00,#5b4300 40%,#3a2a00);
  color:#ffd870;font:500 13px/1.45 'IBM Plex Sans',-apple-system,system-ui,sans-serif;
  padding:7px 16px;text-align:center;
  border-bottom:1px solid #806000;letter-spacing:.02em;
}}
.grid-mirror-banner a{{color:#fff;text-decoration:underline;}}
body{{padding-top:30px !important;}}
</style>
<div class="grid-mirror-banner">
  Snapshot mirror &middot; {snapshot_at} &middot; live API offline &middot;
  for current state see <a href="https://grid.stepdad.finance/">grid.stepdad.finance</a>
</div>
"""


def bust_asset_caches() -> None:
    """Append a per-bake querystring to every /assets/* URL referenced by
    index.html so no upstream HTTP cache (CF edge, browser, Playwright,
    Apache mod_brotli precompressed) can serve a previous bake's bytes
    even when the asset's hashed filename happens to match.

    The querystring is the bake timestamp; servers ignore unknown query
    params on static files but caches key by full URL."""
    idx = OUT / "index.html"
    if not idx.exists():
        return
    cb = time.strftime("%Y%m%d%H%M%S", time.gmtime())
    html = idx.read_text(encoding="utf-8")
    html = re.sub(
        r'(["\'])(/assets/[A-Za-z0-9_./~-]+\.(?:js|css|mjs))(?=\1)',
        rf'\1\2?cb={cb}',
        html,
    )
    idx.write_text(html, encoding="utf-8")


def inject_banner_and_token(token: str) -> None:
    """Inject the snapshot banner AND pre-populate the auth token in
    localStorage so the PWA boots already-logged-in. Login POST doesn't
    work in mirror mode, so the user would otherwise be stuck on the
    login form. The token is the same admin JWT the bake minted; it's
    valid for 24h against the live server's GRID_JWT_SECRET."""
    idx = OUT / "index.html"
    if not idx.exists():
        return
    html = idx.read_text(encoding="utf-8")
    snapshot_at = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
    banner = _BANNER_TEMPLATE.format(snapshot_at=snapshot_at)
    # Token-injection script must run BEFORE the PWA module loads so that
    # the API client picks it up on its first read of localStorage.
    token_script = (
        '<script>'
        '(function(){try{'
        f'var t={json.dumps(token)};'
        'if(!localStorage.getItem("grid_token"))localStorage.setItem("grid_token",t);'
        '}catch(e){}})();'
        '</script>'
    )
    # Inject banner + token script right after <body>
    if "<body" in html:
        html = re.sub(r"(<body[^>]*>)", r"\1" + banner + token_script, html, count=1)
    else:
        html = banner + token_script + html
    idx.write_text(html, encoding="utf-8")


def main() -> None:
    if OUT.exists():
        for f in sorted(OUT.rglob("*"), reverse=True):
            if f.is_file():
                f.unlink()
            else:
                try:
                    f.rmdir()
                except OSError:
                    pass
    OUT.mkdir(parents=True, exist_ok=True)

    print(f"Baking GoDaddy mirror of {LIVE}")
    print(f"  -> {OUT}")

    print("\n--- minting admin JWT from local GRID_JWT_SECRET ---")
    token = mint_admin_jwt()
    print(f"  jwt: {token[:40]}...")

    print("\n--- crawling PWA bundle ---")
    asset_count = crawl_pwa()
    if asset_count < 5:
        print(f"\nABORT: only {asset_count} JS/CSS chunks fetched. Live server "
              "is probably down. Refusing to deploy a broken bake — the existing "
              "mirror on the wire stays intact.", file=sys.stderr)
        sys.exit(2)

    bake_api(token)

    print("\n--- patching PWA route loader (fix EdgeScanner crash) ---")
    patch_route_loader()

    print("\n--- writing auth/verify stub + health + htaccess + sw cleanup ---")
    write_auth_verify_stub(token)
    write_health()
    write_htaccess()
    overwrite_service_worker()
    inject_banner_and_token(token)
    bust_asset_caches()

    files = sorted(p for p in OUT.rglob("*") if p.is_file())
    total = sum(p.stat().st_size for p in files)
    print(f"\nbaked {len(files)} files, {total/1024:.1f} KB")


if __name__ == "__main__":
    main()
