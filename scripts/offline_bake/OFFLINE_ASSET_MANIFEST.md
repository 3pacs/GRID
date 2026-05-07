# GoDaddy offline mirror — what gets baked

> **Purpose:** drive `bake.py`, the script that builds a static mirror of
> grid.stepdad.finance for the cPanel doc root at `public_html/stepdad.fi/`.
> When grid-svr or the Cloudflare Tunnel goes dark, `https://stepdad.fi/`
> stays up with a degraded snapshot of the dashboard.

## Why a mirror at all

`grid.stepdad.finance` is a single Cloudflare Tunnel into grid-svr. If the
server crashes (see 2026-03-28 server post-mortem), the entire dashboard
404s. The mirror is the always-on backup URL: `https://stepdad.fi/` shows
the same UI in degraded mode, so the public never sees a hard outage.

This mirror does **not** auto-fail-over. Both URLs stay live. The mirror is
the documented fallback URL anyone can visit when the live site is down.

## What the mirror serves

`bake.py` produces this tree under `dist/godaddy/`:

| Path                                     | Source                                                       |
| ---------------------------------------- | ------------------------------------------------------------ |
| `index.html`                             | Crawled from `https://grid.stepdad.finance/`, with a snapshot banner injected just after `<body>` |
| `manifest.json`, `service-worker.js`     | Fetched verbatim from live                                   |
| `favicon.ico`, `icons/icon-{76,120,152,180,192,512}.png` | Fetched verbatim                                  |
| `assets/*`                               | Every Vite-built JS/CSS chunk referenced by `index.html` (recursively scanned for child references) |
| `api/v1/astrogrid/predictions/latest.json` | Snapshot of `/api/v1/astrogrid/predictions/latest?limit=12` (used by the home grid feed) |
| `api/health.json`                        | `{"status":"degraded","mode":"godaddy-mirror",...}` so the live frontend or any monitor can detect mirror mode |
| `api/_disabled.json`                     | Returned by `.htaccess` for POST endpoints (`interpret`, `predictions`, `guru/ask`) so they fail with structured JSON instead of HTML |
| `.htaccess`                              | Apache config: SPA fallback, querystring-stripped predictions rewrite, MIME types, immutable-cache for hashed assets |

Total payload: ~870 KB.

## What the mirror deliberately doesn't serve

These endpoints require the live FastAPI backend and have no honest static
representation:

| Endpoint                                  | Method | Mirror behavior                        |
| ----------------------------------------- | ------ | -------------------------------------- |
| `/api/v1/astrogrid/interpret`             | POST   | 200 JSON `{error:"endpoint_disabled_in_mirror",...}` (would otherwise be HTML 404) |
| `/api/v1/astrogrid/predictions`           | POST   | same                                   |
| `/api/v1/astrogrid/guru/ask`              | POST   | same                                   |
| Anything under `/api/` not baked          | any    | Falls through to filesystem → Apache 404 |
| `/data/*`                                 | GET    | Not baked. Live frontend hits these as relative fallbacks; they 404 in mirror mode and the SPA shows whatever empty-state it has. |

## Cadence

Re-bake whenever the live frontend changes shape (new Vite hash, new asset
chunk, new route) or whenever the snapshot prediction feed grows stale
enough to look misleading. There is no cron — running it on demand is the
intended workflow until we wire it to a Hermes scheduler entry.

```bash
# Bake + zip only:
scripts/offline_bake/package_godaddy.sh

# Bake + zip + push to godaddy:public_html/stepdad.fi/
scripts/offline_bake/package_godaddy.sh --upload

# Just push the existing dist/godaddy/ without re-baking:
scripts/offline_bake/package_godaddy.sh --upload-only
```

The zip lands in `output/deploy/grid-godaddy-cpanel-<UTC-timestamp>.zip`
and is also useful as a manual fallback if `--upload` ever fails: drop the
zip in cPanel → File Manager → Extract.

## SSH/host config

`package_godaddy.sh` ssh-aliases to `godaddy` (defined in `~/.ssh/config`,
points at `p3plzcpnl506011.prod.phx3.secureserver.net`, user
`h2hb4v1an7lh`, key `~/.ssh/storymill_godaddy_cpanel_ed25519`). The doc
root for the `stepdad.fi` addon domain is
`~/public_html/stepdad.fi/`. The same shared host serves boogerbots.com
out of `~/public_html/boogerbots/` — same key, separate doc root.

## Future hooks

* Auto-failover via Cloudflare DNS — modeled on
  `~/.config/cloudflare/cf-flip` (the boogerbots flipper). Needs a CF API
  token scoped to the `stepdad.finance` zone before it can flip
  `grid.stepdad.finance` → GoDaddy.
* Live-site banner that links to `https://stepdad.fi` when
  `/api/health.json` reports `degraded`. Lives in the PWA, not in this
  bake.
* A Hermes scheduler entry to re-bake nightly so the snapshot doesn't drift
  more than 24h behind reality.
