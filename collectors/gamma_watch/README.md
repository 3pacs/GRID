# Gamma Watch deployed-source baseline

This is a version-control import for coordinator review, not a deployment or
activation. GRID does not import or execute this directory. Existing remote
services and files remain unchanged. The independent P1 design proposes a
future canonical receipt adapter; this baseline does not implement it.

## Provenance and scope

Linux modules, UI and contract configuration were copied read-only from
`grid-svr:/data/agent-home/anikdang/gex-watch` on 2026-09-28. Windows collectors
and subscription topics were copied from ANIK's `SPY-GEX-Feed` and
`Structural-Feed` directories. `IMPORT-MANIFEST.json` records file SHA256s.
Offline tests are retained from the original local development directory.
Preserved host-specific paths describe the existing installation, not portable
defaults or authorization to contact those hosts.

`contracts.json` and Windows `topics.json` are dated subscription configuration,
not fresh market data. They include September 25/28/30 and October 16 expiries;
they must not be treated as a current daily subscription roll. This import
deliberately preserves existing collector behavior and its limitations.

Excluded: live SQLite journals, overwritten snapshots, logs, chain.csv market
data, tape recordings, credentials, broker accounts and tunnel credentials.
The frozen GEX-levels v1 research inputs/log are not modified or imported.

## Runtime boundaries

The existing server runs as `grid` under `gex-watch.service`, on port 8769,
from the standalone directory above. `gex-tunnel.service` exposes it through
Cloudflare. No GRID install hooks, unit replacements, restarts or deployment
scripts are added here. Importing server.py can initialize a local journal;
do not import or run it merely to inspect this source.

External runtime inputs include `chain.csv`, ANIK RTD snapshot files and network
provider responses. Windows code depends on thinkorswim RTD and the installed
Excel interop assembly; its launchers are reference source, not CI entrypoints.
Starting them subscribes to broker data and writes snapshot files. No launch
was performed for this import.

Providers include Yahoo public prices, ZeroGEX delayed levels, Cboe delayed
chains, Treasury/New York Fed public data and entitlement-restricted brokerage
RTD. No paid API or LLM is enabled. Public endpoint availability does not prove
redistribution rights or exchange real-time entitlement. Models assume dealer
signs; they do not observe inventory. Receipt clocks do not prove quote age.

## Offline validation

From the repository root:

```text
python -m pytest tests/test_gamma_watch_collector_baseline.py -q --noconftest
```

The wrapper runs only fixture-based unittest modules in a subprocess, with
outbound socket connections blocked. It does not import server.py, start RTD,
fetch providers or modify production. Syntax checks cover imported Python.
Windows COM execution and production deployment are deliberately untested.

Coordinator review/merge only. No self-merge. The September 29 13:30Z freeze
lasts until the owner confirms GEM containment; merging source alone does not
authorize deployment, migration, paid feeds or activation.
