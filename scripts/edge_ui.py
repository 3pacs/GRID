#!/usr/bin/env python3
"""GRID — Human-in-the-Loop Edge Worker with Web UI.

Runs on any machine (Windows/Mac/Linux). Serves a local web UI where users
can see pending LLM prompts, copy them to ChatGPT/Gemini/Claude, and paste
responses back. Results flow to the GRID coordinator → PostgreSQL.

Usage:
    python3 edge_ui.py                                    # defaults
    python3 edge_ui.py --coordinator http://100.75.185.36:8100
    python3 edge_ui.py --port 3200 --name "Anik-PC"

The web UI runs on http://localhost:3200
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from typing import Optional
from loguru import logger as log

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

import secrets
import hmac
import hashlib

DEFAULT_COORDINATOR = "http://100.75.185.36:8100"
DEFAULT_PORT = 3200
POLL_INTERVAL = 10  # seconds between job checks
HEARTBEAT_INTERVAL = 30
MAX_RESPONSE_LENGTH = 100_000  # 100KB max response
MAX_MODEL_NAME_LENGTH = 100

# ── State ──────────────────────────────────────────────────────

worker_id: Optional[int] = None
coordinator_url: str = DEFAULT_COORDINATOR
pending_jobs: list[dict] = []
completed_jobs: list[dict] = []
worker_name: str = platform.node()
_lock = threading.Lock()
_csrf_secret: str = secrets.token_hex(32)


def _make_csrf_token(job_id: int) -> str:
    """Generate a CSRF token tied to a specific job."""
    msg = f"{_csrf_secret}:{job_id}".encode()
    return hmac.new(_csrf_secret.encode(), msg, hashlib.sha256).hexdigest()[:32]


def _verify_csrf_token(token: str, job_id: int) -> bool:
    """Verify CSRF token."""
    expected = _make_csrf_token(job_id)
    return hmac.compare_digest(token, expected)


# ── HTML UI ────────────────────────────────────────────────────

def render_html() -> str:
    with _lock:
        jobs = list(pending_jobs)
        done = list(completed_jobs[-20:])

    pending_html = ""
    if not jobs:
        pending_html = '<div class="empty">No pending queries. Waiting for jobs from GRID...</div>'
    else:
        for j in jobs:
            prompt = j.get("params", {}).get("prompt", "No prompt")
            target = j.get("params", {}).get("target_llm", "Any LLM")
            context = j.get("params", {}).get("context", "")
            job_id = j["id"]
            pending_html += f'''
            <div class="job-card">
                <div class="job-header">
                    <span class="job-id">Job #{job_id}</span>
                    <span class="target-llm">{_esc(target)}</span>
                </div>
                {f'<div class="context">{_esc(context)}</div>' if context else ''}
                <div class="prompt-box">
                    <div class="prompt-label">Copy this prompt:</div>
                    <pre class="prompt" id="prompt-{job_id}">{_esc(prompt)}</pre>
                    <button class="btn btn-copy" onclick="copyPrompt({job_id})">Copy to Clipboard</button>
                </div>
                <form method="POST" action="/submit">
                    <input type="hidden" name="job_id" value="{job_id}">
                    <input type="hidden" name="csrf_token" value="{_make_csrf_token(job_id)}">
                    <div class="response-label">Paste the response here:</div>
                    <textarea name="response" rows="10" maxlength="{MAX_RESPONSE_LENGTH}" placeholder="Paste the LLM response here..." required></textarea>
                    <div class="form-row">
                        <input type="text" name="model_used" maxlength="{MAX_MODEL_NAME_LENGTH}" placeholder="Model used (e.g. GPT-4o, Gemini Pro)" class="model-input">
                        <button type="submit" class="btn btn-submit">Submit Response</button>
                    </div>
                </form>
            </div>
            '''

    completed_html = ""
    for d in reversed(done):
        completed_html += f'''
        <div class="done-card">
            <span class="job-id">Job #{d["job_id"]}</span>
            <span class="model-tag">{_esc(d.get("model", ""))}</span>
            <span class="time">{d.get("time", "")}</span>
        </div>
        '''

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GRID Edge Worker — {_esc(worker_name)}</title>
<meta http-equiv="refresh" content="15">
<style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
        font-family: 'Segoe UI', -apple-system, sans-serif;
        background: #0a0e14;
        color: #c8d8e8;
        min-height: 100vh;
        padding: 20px;
    }}
    .header {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 16px 24px;
        background: #111820;
        border-radius: 12px;
        margin-bottom: 24px;
        border: 1px solid #1a2530;
    }}
    .header h1 {{
        font-size: 20px;
        color: #1a6ebf;
        letter-spacing: 2px;
    }}
    .status {{
        display: flex;
        gap: 16px;
        align-items: center;
        font-size: 13px;
    }}
    .status-dot {{
        width: 8px; height: 8px;
        background: #22c55e;
        border-radius: 50%;
        display: inline-block;
    }}
    .empty {{
        text-align: center;
        padding: 60px 20px;
        color: #556;
        font-size: 16px;
    }}
    .job-card {{
        background: #111820;
        border: 1px solid #1a2530;
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 20px;
    }}
    .job-header {{
        display: flex;
        justify-content: space-between;
        margin-bottom: 16px;
    }}
    .job-id {{
        font-family: monospace;
        color: #1a6ebf;
        font-weight: bold;
    }}
    .target-llm {{
        background: #1a2530;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 12px;
        color: #7ab;
    }}
    .context {{
        background: #0d1117;
        padding: 12px 16px;
        border-radius: 8px;
        margin-bottom: 16px;
        font-size: 13px;
        color: #8a9ab0;
        border-left: 3px solid #1a6ebf;
    }}
    .prompt-box {{
        margin-bottom: 20px;
    }}
    .prompt-label, .response-label {{
        font-size: 12px;
        color: #667;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 8px;
    }}
    .prompt {{
        background: #0d1117;
        padding: 16px;
        border-radius: 8px;
        font-size: 14px;
        line-height: 1.6;
        white-space: pre-wrap;
        word-wrap: break-word;
        max-height: 300px;
        overflow-y: auto;
        border: 1px solid #1a2530;
    }}
    textarea {{
        width: 100%;
        background: #0d1117;
        color: #c8d8e8;
        border: 1px solid #1a2530;
        border-radius: 8px;
        padding: 16px;
        font-size: 14px;
        line-height: 1.6;
        resize: vertical;
        font-family: inherit;
        margin-bottom: 12px;
    }}
    textarea:focus {{
        outline: none;
        border-color: #1a6ebf;
    }}
    .form-row {{
        display: flex;
        gap: 12px;
        align-items: center;
    }}
    .model-input {{
        flex: 1;
        background: #0d1117;
        color: #c8d8e8;
        border: 1px solid #1a2530;
        border-radius: 8px;
        padding: 10px 16px;
        font-size: 14px;
    }}
    .model-input:focus {{
        outline: none;
        border-color: #1a6ebf;
    }}
    .btn {{
        padding: 10px 20px;
        border: none;
        border-radius: 8px;
        cursor: pointer;
        font-size: 14px;
        font-weight: 500;
        transition: all 0.2s;
    }}
    .btn-copy {{
        background: #1a2530;
        color: #7ab;
        margin-top: 8px;
    }}
    .btn-copy:hover {{ background: #243040; }}
    .btn-submit {{
        background: #1a6ebf;
        color: white;
        min-width: 160px;
    }}
    .btn-submit:hover {{ background: #2080df; }}
    .section-title {{
        font-size: 14px;
        color: #556;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin: 32px 0 16px;
    }}
    .done-card {{
        display: flex;
        gap: 12px;
        align-items: center;
        padding: 10px 16px;
        background: #111820;
        border-radius: 8px;
        margin-bottom: 6px;
        font-size: 13px;
    }}
    .model-tag {{
        background: #1a2530;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 11px;
    }}
    .time {{ color: #556; margin-left: auto; font-size: 12px; }}
    .flash {{
        background: #143020;
        border: 1px solid #22c55e;
        color: #22c55e;
        padding: 12px 20px;
        border-radius: 8px;
        margin-bottom: 20px;
        text-align: center;
    }}
</style>
</head>
<body>
    <div class="header">
        <h1>GRID EDGE WORKER</h1>
        <div class="status">
            <span class="status-dot"></span>
            <span>{_esc(worker_name)}</span>
            <span style="color:#556">|</span>
            <span>{len(jobs)} pending</span>
            <span style="color:#556">|</span>
            <span>{len(done)} completed</span>
        </div>
    </div>

    <div id="flash"></div>

    {pending_html}

    {f'<div class="section-title">Recently Completed</div>' if done else ''}
    {completed_html}

    <script>
    function copyPrompt(jobId) {{
        const el = document.getElementById('prompt-' + jobId);
        navigator.clipboard.writeText(el.textContent).then(() => {{
            const flash = document.getElementById('flash');
            flash.innerHTML = '<div class="flash">Prompt copied to clipboard! Paste it into your LLM.</div>';
            setTimeout(() => flash.innerHTML = '', 3000);
        }});
    }}
    </script>
</body>
</html>'''


def _esc(s: str) -> str:
    """Escape HTML."""
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))


# ── HTTP Handler ───────────────────────────────────────────────

class EdgeHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" or self.path.startswith("/?"):
            html = render_html()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html.encode())
        elif self.path == "/api/status":
            with _lock:
                data = {
                    "worker_id": worker_id,
                    "worker_name": worker_name,
                    "pending": len(pending_jobs),
                    "completed": len(completed_jobs),
                }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/submit":
            length = int(self.headers.get("Content-Length", 0))

            # Reject oversized payloads
            if length > MAX_RESPONSE_LENGTH + 10_000:
                self.send_response(413)
                self.end_headers()
                return

            body = self.rfile.read(length).decode("utf-8", errors="replace")

            # Parse form data safely
            from urllib.parse import unquote_plus
            params: dict[str, str] = {}
            for pair in body.split("&"):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    params[unquote_plus(k)] = unquote_plus(v)

            # Validate job_id is a positive integer
            try:
                job_id = int(params.get("job_id", "0"))
                if job_id <= 0:
                    raise ValueError
            except (ValueError, TypeError):
                self.send_response(400)
                self.end_headers()
                return

            # Verify CSRF token
            csrf_token = params.get("csrf_token", "")
            if not _verify_csrf_token(csrf_token, job_id):
                self.send_response(403)
                self.end_headers()
                return

            # Verify this job is actually in our pending list
            with _lock:
                valid_ids = {j["id"] for j in pending_jobs}
            if job_id not in valid_ids:
                self.send_response(400)
                self.end_headers()
                return

            # Sanitize inputs
            response_text = params.get("response", "").strip()[:MAX_RESPONSE_LENGTH]
            model_used = params.get("model_used", "unknown").strip()[:MAX_MODEL_NAME_LENGTH]
            # Strip any null bytes
            response_text = response_text.replace("\x00", "")
            model_used = model_used.replace("\x00", "")

            if not response_text:
                self.send_response(400)
                self.end_headers()
                return

            submit_response(job_id, response_text, model_used)

            # Redirect back
            self.send_response(303)
            self.send_header("Location", "/")
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress default logging


def submit_response(job_id: int, response_text: str, model_used: str) -> None:
    """Submit the human-provided LLM response back to the coordinator."""
    global pending_jobs

    try:
        # Mark job as started (if not already)
        requests.post(
            f"{coordinator_url}/jobs/{job_id}/start",
            params={"worker_id": worker_id},
            timeout=10,
        )
    except Exception as exc:
        log.warning("Failed to mark job #{j} as started: {e}", j=job_id, e=exc)

    # Submit the result
    try:
        requests.post(
            f"{coordinator_url}/jobs/{job_id}/complete",
            json={
                "job_id": job_id,
                "worker_id": worker_id,
                "output": {
                    "response": response_text,
                    "model_used": model_used,
                    "submitted_by": worker_name,
                    "submitted_at": datetime.now(timezone.utc).isoformat(),
                },
                "metrics": {
                    "model_used": model_used,
                    "response_length": len(response_text),
                },
                "error": None,
            },
            timeout=30,
        )

        with _lock:
            pending_jobs = [j for j in pending_jobs if j["id"] != job_id]
            completed_jobs.append({
                "job_id": job_id,
                "model": model_used,
                "time": datetime.now().strftime("%H:%M"),
            })

        log.info("Submitted response for job #{} ({})", job_id, model_used)

    except Exception as e:
        log.error("ERROR submitting job #{}: {}", job_id, e)


# ── Background Poller ──────────────────────────────────────────

def poll_loop():
    """Background thread: polls coordinator for HUMAN_LLM_QUERY jobs."""
    global pending_jobs
    last_heartbeat = time.time()

    while True:
        try:
            # Heartbeat
            if time.time() - last_heartbeat >= HEARTBEAT_INTERVAL:
                try:
                    requests.post(
                        f"{coordinator_url}/workers/{worker_id}/heartbeat",
                        timeout=5,
                    )
                    last_heartbeat = time.time()
                except Exception as exc:
                    log.warning("Worker heartbeat failed: {e}", e=exc)

            # Check for HUMAN_LLM_QUERY jobs
            try:
                r = requests.get(
                    f"{coordinator_url}/jobs",
                    params={"state": "QUEUED", "job_type": "HUMAN_LLM_QUERY", "limit": 10},
                    timeout=10,
                )
                r.raise_for_status()
                queued = r.json()
            except Exception:
                time.sleep(POLL_INTERVAL)
                continue

            with _lock:
                current_ids = {j["id"] for j in pending_jobs}

            for job in queued:
                if job["id"] not in current_ids:
                    # Claim it (only HUMAN_LLM_QUERY jobs)
                    try:
                        r = requests.post(
                            f"{coordinator_url}/jobs/claim",
                            params={
                                "worker_id": worker_id,
                                "gpu_available": False,
                                "ollama_available": False,
                                "job_type": "HUMAN_LLM_QUERY",
                            },
                            timeout=10,
                        )
                        r.raise_for_status()
                        claimed = r.json()
                        if claimed.get("id"):
                            with _lock:
                                pending_jobs.append(claimed)
                            log.info("New query: Job #{} — {}", claimed['id'], claimed.get('name', ''))
                    except Exception as exc:
                        log.warning("Failed to claim job: {e}", e=exc)

        except Exception as e:
            log.error("Poll error: {}", e)

        time.sleep(POLL_INTERVAL)


# ── Registration ───────────────────────────────────────────────

def register_worker(coord_url: str, name: str) -> int:
    """Register this edge node with the coordinator."""
    try:
        ts_ip = "0.0.0.0"
        try:
            import subprocess
            result = subprocess.run(
                ["tailscale", "ip", "-4"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                ts_ip = result.stdout.strip()
        except Exception:
            ts_ip = socket.gethostbyname(socket.gethostname())

        r = requests.post(
            f"{coord_url}/workers/register",
            json={
                "hostname": name,
                "tailscale_ip": ts_ip,
                "cpu_cores": os.cpu_count() or 1,
                "ram_gb": 1.0,  # not important for human workers
                "gpu_model": None,
                "gpu_vram_gb": None,
                "has_ollama": False,
                "has_docker": False,
                "max_concurrent": 5,  # humans can handle multiple queries
            },
            timeout=10,
        )
        r.raise_for_status()
        return r.json()["id"]
    except Exception as e:
        log.error("Could not register with coordinator at {}", coord_url)
        log.error("  {}", e)
        log.error("Is the coordinator running? Try: curl {}/health", coord_url)
        sys.exit(1)


# ── Main ───────────────────────────────────────────────────────

def main():
    global worker_id, coordinator_url, worker_name

    parser = argparse.ArgumentParser(description="GRID Human-in-the-Loop Edge Worker")
    parser.add_argument("--coordinator", default=DEFAULT_COORDINATOR, help="Coordinator URL")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Web UI port")
    parser.add_argument("--name", default=platform.node(), help="Worker name")
    args = parser.parse_args()

    coordinator_url = args.coordinator.rstrip("/")
    worker_name = args.name

    print()
    print("  ╔══════════════════════════════════════════════╗")
    print("  ║         GRID EDGE WORKER                     ║")
    print("  ║         Human-in-the-Loop LLM Proxy          ║")
    print("  ╠══════════════════════════════════════════════╣")
    print(f"  ║  Web UI:      http://localhost:{args.port:<14}║")
    print(f"  ║  Coordinator: {coordinator_url:<31}║")
    print(f"  ║  Worker:      {worker_name:<31}║")
    print("  ╚══════════════════════════════════════════════╝")
    print()

    # Register
    worker_id = register_worker(coordinator_url, worker_name)
    print(f"  Registered as worker #{worker_id}")
    print(f"  Polling for HUMAN_LLM_QUERY jobs...")
    print()

    # Start background poller
    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()

    # Start web server
    # Bind to localhost only — this UI should not be exposed to the network
    server = HTTPServer(("127.0.0.1", args.port), EdgeHandler)
    print(f"  Web UI ready at http://localhost:{args.port}")
    print()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Edge worker shutting down.")
        server.server_close()


if __name__ == "__main__":
    main()
