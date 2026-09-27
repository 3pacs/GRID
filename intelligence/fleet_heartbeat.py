"""Fleet heartbeat monitor — hermes-driven, full-fleet liveness + auto-heal.

Runs every hermes cycle (wired into hermes_operator.run_cycle). Probes the
whole fleet, records to ``fleet_heartbeats``, sends debounced (2-strike) email
alerts on DOWN transitions, and — per operator policy — attempts auto-heal,
including remote recovery over SSH (hermes has passwordless sudo on grid-svr and
to the GPU nodes).

Created 2026-05-25 after a session where p9d (offline 4d), the retired panda GPU host (PCIe
bus errors), The Meter (stale days), and several systemd units all failed
SILENTLY — nothing watched nodes, GPUs, units, or freshness. The existing
/home/grid/grid_health/llm_probe.py only watched LLM endpoints. This unifies all
of it under hermes.

Probe kinds:
  node      — tailscale reachability (online/offline)
  gpu       — `ssh <node> nvidia-smi` : all expected cards enumerate, no errors
  unit      — `systemctl --failed` on grid-svr
  endpoint  — LLM/API endpoints (health + listener)
  freshness — The Meter output age, key table recency

Auto-heal (cooldown-guarded to avoid loops):
  unit offline      -> sudo systemctl restart <unit>
  gpu wedged        -> ssh node: stop ollama; modprobe -r/-a nvidia*; reprobe
  endpoint down     -> restart owning systemd service (local or via ssh)
  node offline      -> cannot self-heal (it's unreachable); alert only
"""
from __future__ import annotations

import json
import shlex
import subprocess
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log

# ── Registry ────────────────────────────────────────────────────────────────
# host = ssh target (must be reachable from grid-svr). gpu = expected GPU count
# (0 = no GPU probe). heal = allow auto-heal actions on this node.
FLEET = {
    "grid-svr": {"host": None, "gpu": 2, "heal": True, "critical": True},   # local
    "gridz4":   {"host": "gridz4", "gpu": 2, "heal": True, "critical": True},
    "redbox":   {"host": "redbox", "gpu": 2, "heal": True, "critical": False},
    "koala":    {"host": "koala", "gpu": 2, "heal": True, "critical": False},
    "ocr-node": {"host": "ocr-node", "gpu": 2, "heal": True, "critical": False},  # OCR GPU node, often off
}
# Pruned 2026-08-08 (owner: "prune them theyre gone"): p9d and z400 were decommissioned
# but stayed in this registry, accumulating 11,550 and 6,739 consecutive failures as
# "not in tailnet peer list". Dead entries are not free: with re-notification enabled
# they would have produced a daily alert forever and buried the real signal. Verified
# before removal that no live systemd unit or bin/ script targets either host.
# Note: the NAME "z400" now resolves to a different machine (asus-ws), so any future
# reference to it would silently talk to the wrong box — do not re-add it.

# LLM/API endpoints to probe (name -> (url, kind, owning systemd unit or None)).
# kind: "http_ok" any 2xx/401; "ollama" expects /api/tags 200; "llamacpp" /health 200.
ENDPOINTS = {
    "grid-api":          ("http://localhost:8000/api/v1/intelligence/news", "http_ok", "grid-api"),
    "oracle-27b":        ("http://localhost:8081/health", "llamacpp", "grid-llamacpp-oracle"),
    "gemma-classifier":  ("http://localhost:8082/health", "llamacpp", "grid-micro-classifier"),
    "gemma-narrator":    ("http://localhost:8083/health", "llamacpp", "grid-micro-narrator"),
    "gemma-extractor":   ("http://localhost:8084/health", "llamacpp", "grid-micro-extractor"),
    "gemma-mapper":      ("http://localhost:8085/health", "llamacpp", "grid-micro-mapper"),
}

# Freshness targets: name -> (path or sql, max_age_minutes).
FRESHNESS = {
    "the-meter": ("/data/the-meter/data/automation/grid-ingest-latest.json", 24 * 60),
}

ALERT_STRIKES = 2            # consecutive fails before alerting (debounce)
RENOTIFY_EVERY_SWEEPS = 288  # re-alert a still-down entity this often (~daily @ 5min)
HEAL_COOLDOWN_MIN = 30       # min minutes between heal attempts per entity
HEAL_MAX_PER_DAY = 6         # give up + escalate after this many heals/entity/day
SSH_OPTS = ["-o", "ConnectTimeout=6", "-o", "BatchMode=yes"]


# ── Schema ──────────────────────────────────────────────────────────────────
def ensure_schema(engine: Any) -> None:
    with engine.begin() as c:
        c.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS fleet_heartbeats (
                id            BIGSERIAL PRIMARY KEY,
                entity        TEXT NOT NULL,
                kind          TEXT NOT NULL,
                status        TEXT NOT NULL,           -- up | down | degraded
                detail        TEXT,
                latency_ms    INTEGER,
                consecutive_fail INTEGER NOT NULL DEFAULT 0,
                healed        BOOLEAN NOT NULL DEFAULT FALSE,
                heal_detail   TEXT,
                checked_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
        c.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_fleet_hb_entity_time "
            "ON fleet_heartbeats (entity, checked_at DESC)")


def _last(engine: Any, entity: str) -> dict[str, Any] | None:
    with engine.connect() as c:
        row = c.exec_driver_sql(
            "SELECT status, consecutive_fail, checked_at FROM fleet_heartbeats "
            "WHERE entity = %s ORDER BY checked_at DESC LIMIT 1", (entity,)).fetchone()
    if not row:
        return None
    return {"status": row[0], "consecutive_fail": row[1], "checked_at": row[2]}


def _heals_today(engine: Any, entity: str) -> int:
    with engine.connect() as c:
        return c.exec_driver_sql(
            "SELECT COUNT(*) FROM fleet_heartbeats WHERE entity = %s AND healed "
            "AND checked_at > NOW() - INTERVAL '24 hours'", (entity,)).fetchone()[0]


def _last_heal_age_min(engine: Any, entity: str) -> float | None:
    with engine.connect() as c:
        row = c.exec_driver_sql(
            "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(checked_at)))/60 "
            "FROM fleet_heartbeats WHERE entity = %s AND healed", (entity,)).fetchone()
    return float(row[0]) if row and row[0] is not None else None


# Signatures meaning "automated reload cannot fix this — needs reboot / hands-on".
# Once we see one of these from a recent heal, stop retrying (don't bounce a busy
# node's services pointlessly) and let the standing alert carry it.
_FUTILE = ("is in use", "timeout", "version mismatch", "fell off", "no route")


def _heal_is_futile(engine: Any, entity: str) -> bool:
    """Sticky: once a heal returns a futile signature, NEVER auto-retry until the
    entity has recovered (seen 'up') on its own. Re-running modprobe -r on a
    wedged GPU node can hard-hang the box, so a failed GPU reload must not be
    retried on a timer — it needs a reboot / hands-on, which clears it when the
    node comes back 'up'.
    """
    with engine.connect() as c:
        row = c.exec_driver_sql(
            "SELECT heal_detail, checked_at FROM fleet_heartbeats "
            "WHERE entity = %s AND healed ORDER BY checked_at DESC LIMIT 1",
            (entity,)).fetchone()
        if not (row and row[0] and any(p in row[0].lower() for p in _FUTILE)):
            return False
        # Futile heal on record — re-arm ONLY if the entity has been 'up' since.
        up_since = c.exec_driver_sql(
            "SELECT 1 FROM fleet_heartbeats WHERE entity = %s AND status = 'up' "
            "AND checked_at > %s LIMIT 1", (entity, row[1])).fetchone()
    return up_since is None


# ── Shell helpers ─────────────────────────────────────────────────────────────
def _run(cmd: list[str], timeout: int = 20) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout + p.stderr).strip()
    except subprocess.TimeoutExpired:
        return 124, "timeout"
    except Exception as e:  # pragma: no cover
        return 1, str(e)


def _ssh(host: str, remote_cmd: str, timeout: int = 25) -> tuple[int, str]:
    return _run(["ssh", *SSH_OPTS, host, remote_cmd], timeout=timeout)


# ── Probes (each returns list of (entity, kind, status, detail, latency_ms)) ──
def probe_nodes() -> list[tuple]:
    rc, out = _run(["tailscale", "status", "--json"], timeout=15)
    results = []
    if rc != 0:
        return [("tailscale", "node", "down", f"tailscale status rc={rc}", None)]
    try:
        data = json.loads(out)
        peers = data.get("Peer", {}) or {}
        seen = {}
        for p in peers.values():
            name = (p.get("HostName") or "").split(".")[0].lower()
            seen[name] = bool(p.get("Online"))
        for node, cfg in FLEET.items():
            if node == "grid-svr":
                continue  # self
            online = seen.get(node)
            if online is None:
                results.append((node, "node", "down", "not in tailnet peer list", None))
            else:
                results.append((node, "node", "up" if online else "down",
                                "tailscale online" if online else "tailscale offline", None))
    except Exception as e:  # pragma: no cover
        results.append(("tailscale", "node", "down", f"parse error: {e}", None))
    return results


def probe_gpus() -> list[tuple]:
    results = []
    for node, cfg in FLEET.items():
        if cfg["gpu"] <= 0:
            continue
        q = ("nvidia-smi --query-gpu=index,name,memory.used,temperature.gpu "
             "--format=csv,noheader")
        rc, out = (_run(["bash", "-lc", q], timeout=20) if cfg["host"] is None
                   else _ssh(cfg["host"], q, timeout=20))
        if rc != 0:
            # Distinguish "node unreachable" (node probe owns that — skip to avoid
            # double-alerting) from "reachable but GPU/driver errored" (real gpu down).
            unreachable = any(m in out for m in (
                "Connection timed out", "Connection refused", "port 22",
                "Could not resolve", "No route to host", "timeout"))
            if unreachable:
                continue
            results.append((f"{node}:gpu", "gpu", "down", out[:300], None))
            continue
        cards = [l for l in out.splitlines() if l.strip()]
        if len(cards) < cfg["gpu"]:
            results.append((f"{node}:gpu", "gpu", "degraded",
                            f"{len(cards)}/{cfg['gpu']} cards enumerate", None))
        else:
            results.append((f"{node}:gpu", "gpu", "up",
                            f"{len(cards)} cards ok", None))
    return results


def probe_units() -> list[tuple]:
    rc, out = _run(["systemctl", "--failed", "--no-legend", "--plain"], timeout=15)
    failed = [l.split()[0] for l in out.splitlines() if l.strip() and l.split()[0].endswith(".service")]
    if not failed:
        return [("grid-svr:units", "unit", "up", "0 failed units", None)]
    return [(u.replace(".service", ""), "unit", "down", "systemd failed", None) for u in failed]


def probe_endpoints() -> list[tuple]:
    import time
    import urllib.error
    import urllib.request
    results = []
    # Codes that mean "listening and healthy enough" (auth/method gating != down).
    LISTENING = {200, 401, 403, 405}
    for name, (url, kind, _unit) in ENDPOINTS.items():
        t0 = time.time()
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=8) as r:
                code = r.status
            lat = int((time.time() - t0) * 1000)
            results.append((name, "endpoint", "up" if code in LISTENING else "degraded",
                            f"HTTP {code}", lat))
        except urllib.error.HTTPError as he:
            # urllib raises on 4xx/5xx — a 401/403/405 still means it's listening.
            lat = int((time.time() - t0) * 1000)
            results.append((name, "endpoint", "up" if he.code in LISTENING else "degraded",
                            f"HTTP {he.code}", lat))
        except Exception as e:
            lat = int((time.time() - t0) * 1000)
            results.append((name, "endpoint", "down", str(e)[:200], lat))
    return results


def probe_freshness() -> list[tuple]:
    import os
    import time
    results = []
    for name, (path, max_age_min) in FRESHNESS.items():
        try:
            age_min = (time.time() - os.path.getmtime(path)) / 60
            status = "up" if age_min <= max_age_min else "degraded"
            results.append((name, "freshness", status,
                            f"age={age_min:.0f}m (max {max_age_min}m)", None))
        except FileNotFoundError:
            results.append((name, "freshness", "down", "output file missing", None))
        except Exception as e:  # pragma: no cover
            results.append((name, "freshness", "down", str(e)[:200], None))
    return results


# ── Auto-heal ─────────────────────────────────────────────────────────────────
def _heal(entity: str, kind: str, detail: str) -> tuple[bool, str]:
    """Attempt recovery. Returns (attempted, result_detail)."""
    if kind == "unit":
        rc, out = _run(["sudo", "-n", "systemctl", "restart", f"{entity}.service"], timeout=30)
        return True, f"systemctl restart -> rc={rc} {out[:120]}"

    if kind == "gpu":
        node = entity.split(":")[0]
        cfg = FLEET.get(node, {})
        if not cfg.get("heal"):
            return False, "heal disabled for node"
        # stop ollama (frees module refs), reload nvidia stack, re-probe
        seq = ("sudo -n systemctl stop ollama 2>/dev/null; sleep 2; "
               "sudo -n timeout 30 modprobe -r nvidia_uvm nvidia_drm nvidia_modeset nvidia; "
               "sudo -n timeout 30 modprobe nvidia; sudo -n modprobe nvidia_uvm; sleep 3; "
               "sudo -n systemctl start ollama 2>/dev/null; "
               "nvidia-smi --query-gpu=index --format=csv,noheader | wc -l")
        rc, out = (_run(["bash", "-lc", seq], timeout=90) if cfg.get("host") is None
                   else _ssh(cfg["host"], seq, timeout=90))
        return True, f"gpu module reload -> rc={rc} cards={out.splitlines()[-1] if out else '?'}"

    if kind == "endpoint":
        unit = ENDPOINTS.get(entity, (None, None, None))[2]
        if not unit:
            return False, "no owning unit (handled via gpu/node heal)"
        rc, out = _run(["sudo", "-n", "systemctl", "restart", f"{unit}.service"], timeout=30)
        return True, f"restart {unit} -> rc={rc} {out[:120]}"

    # node offline / freshness -> not directly healable here
    return False, "no auto-heal for this kind"


def _email(subject: str, body: str, severity: str) -> None:
    try:
        from alerts.email import send_alert
        send_alert(subject, body, severity=severity)
    except Exception as e:  # pragma: no cover
        log.warning("fleet_heartbeat: email send failed: {e}", e=str(e))


# ── Orchestrator ──────────────────────────────────────────────────────────────
def run_fleet_heartbeat(engine: Any, *, dry_run: bool = False,
                        do_heal: bool = True) -> dict[str, Any]:
    """One full fleet sweep. Returns summary dict for the hermes cycle_result."""
    ensure_schema(engine)
    probes = []
    for fn in (probe_nodes, probe_gpus, probe_units, probe_endpoints, probe_freshness):
        try:
            probes.extend(fn())
        except Exception as e:  # pragma: no cover - never let one probe kill the sweep
            log.warning("fleet_heartbeat probe {f} failed: {e}", f=fn.__name__, e=str(e))

    summary = {"checked": len(probes), "down": 0, "degraded": 0,
               "alerts": 0, "heals": 0}
    alerts: list[str] = []
    bad_now: list[tuple] = []   # every not-up entity this sweep, for the alert roster

    for entity, kind, status, detail, lat in probes:
        prev = _last(engine, entity)
        is_bad = status in ("down", "degraded")
        consec = ((prev["consecutive_fail"] if prev else 0) + 1) if is_bad else 0

        healed, heal_detail = False, None
        if is_bad:
            summary["down" if status == "down" else "degraded"] += 1
            bad_now.append((entity, kind, status, consec, detail))

        # auto-heal: only after the debounce strike, with cooldown + daily cap
        if is_bad and do_heal and not dry_run and consec >= ALERT_STRIKES:
            last_age = _last_heal_age_min(engine, entity)
            under_cooldown = last_age is not None and last_age < HEAL_COOLDOWN_MIN
            over_cap = _heals_today(engine, entity) >= HEAL_MAX_PER_DAY
            futile = _heal_is_futile(engine, entity)  # reboot/hands-on needed
            if not under_cooldown and not over_cap and not futile:
                attempted, heal_detail = _heal(entity, kind, detail or "")
                healed = attempted
                if attempted:
                    summary["heals"] += 1
                    log.info("fleet_heartbeat HEAL {e} ({k}): {d}", e=entity, k=kind, d=heal_detail)

        # record
        if not dry_run:
            with engine.begin() as c:
                c.exec_driver_sql(
                    "INSERT INTO fleet_heartbeats "
                    "(entity, kind, status, detail, latency_ms, consecutive_fail, healed, heal_detail) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (entity, kind, status, detail, lat, consec, healed, heal_detail))

        # Alert on the strike transition (debounced), and KEEP alerting on a cadence
        # while the entity stays bad. Firing only at `consec == ALERT_STRIKES` meant a
        # single missed/unread notice bought permanent silence: on 2026-08-08 this swept
        # 37 down entities and raised 0 alerts, because 30 of them were already past
        # strike 2 — grid-pg-backup at 1252 strikes (~4d) and plane-backup at 2240 (~8d)
        # had been failing unannounced. Detection without repeated escalation is silence.
        renotify = (consec > ALERT_STRIKES
                    and consec % RENOTIFY_EVERY_SWEEPS == 0)
        if is_bad and (consec == ALERT_STRIKES or renotify):
            tag = "STILL " + status.upper() if renotify else status.upper()
            line = f"[{tag}] {entity} ({kind}): {detail}"
            if renotify:
                line += f"  | {consec} consecutive sweeps (~{consec // 288}d)"
            if healed:
                line += f"  | auto-heal: {heal_detail}"
            alerts.append(line)
        # recovery notice
        elif status == "up" and prev and prev["status"] in ("down", "degraded") \
                and (prev["consecutive_fail"] or 0) >= ALERT_STRIKES:
            alerts.append(f"[RECOVERED] {entity} ({kind}): {detail}")

    if alerts and not dry_run:
        summary["alerts"] = len(alerts)
        # Every alert carries the FULL current down-roster, not just what changed this
        # sweep. Transition-only mail meant an entity that went silent (see the
        # re-notify comment above) never reappeared in any message, so a reader could
        # not tell "one thing broke" from "thirty things are broken". State, not deltas.
        roster = [f"  - {e} ({k}): {d}" for e, k, s, c, d in sorted(
            bad_now, key=lambda r: -(r[3] or 0))]
        body = "Fleet heartbeat — state changes:\n\n" + "\n".join(alerts)
        if roster:
            body += (f"\n\nCurrently not-up ({len(roster)} of {summary['checked']}), "
                     f"worst first:\n" + "\n".join(roster))
        body += (f"\n\nSwept {summary['checked']} entities at "
                 f"{datetime.now(timezone.utc).isoformat()}")
        # "[STILL DOWN]" must stay critical — a thing that has been down for days is
        # not less urgent than one that just fell over. Substring "DOWN]" covers both.
        sev = "critical" if any("DOWN]" in a for a in alerts) else "warning"
        _email(f"GRID fleet: {len(alerts)} change(s)", body, sev)

    summary["alert_lines"] = alerts
    return summary


if __name__ == "__main__":
    import argparse
    from sqlalchemy import create_engine
    import os

    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="probe only, no record/heal/email")
    ap.add_argument("--no-heal", action="store_true", help="probe+record+alert, no heal")
    args = ap.parse_args()

    dsn = os.environ.get("DATABASE_URL") or \
        f"postgresql+psycopg2://{os.environ.get('DB_USER','grid')}:" \
        f"{os.environ.get('DB_PASSWORD','')}@{os.environ.get('DB_HOST','localhost')}:" \
        f"{os.environ.get('DB_PORT','5432')}/{os.environ.get('DB_NAME','griddb')}"
    eng = create_engine(dsn)
    res = run_fleet_heartbeat(eng, dry_run=args.dry_run, do_heal=not args.no_heal)
    print(json.dumps({k: v for k, v in res.items() if k != "alert_lines"}, indent=2))
    for ln in res.get("alert_lines", []):
        print("  ", ln)
