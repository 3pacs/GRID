"""
Langfuse end-to-end smoke test.

Verifies the trace fan-out wired in llm/feedback_loop.py is reaching the
self-hosted Langfuse instance. Runs a real LLM call through the router
(exercising the full path including the fan-out), flushes the Langfuse
client, then queries the Langfuse public API to confirm the trace landed.

Usage (from grid_repo/):
    LANGFUSE_PUBLIC_KEY=pk-lf-... \\
    LANGFUSE_SECRET_KEY=sk-lf-... \\
    LANGFUSE_HOST=http://grid-svr:3000 \\
    python -m scripts.smoke_langfuse

Exits 0 on success, 1 on any failure. Prints a step-by-step verdict so
it's obvious which step broke if it does.
"""
from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import datetime, timezone

# Load .env so the keys persisted by the operator are picked up automatically.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass


def _step(label: str, ok: bool, detail: str = "") -> None:
    icon = "✓" if ok else "✗"
    print(f"  {icon} {label}" + (f" — {detail}" if detail else ""))


def main() -> int:
    print("Langfuse smoke test")
    print("===================\n")

    # ----- 1. env sanity -----
    pk = os.getenv("LANGFUSE_PUBLIC_KEY")
    sk = os.getenv("LANGFUSE_SECRET_KEY")
    host = os.getenv("LANGFUSE_HOST", "http://grid-svr:3000")
    if not pk or not sk:
        _step("env", False, "LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY not set")
        return 1
    _step("env", True, f"host={host}")

    # ----- 2. langfuse SDK auth -----
    try:
        from langfuse import Langfuse
    except ImportError as e:
        _step("langfuse import", False, str(e))
        return 1
    client = Langfuse(public_key=pk, secret_key=sk, host=host)
    try:
        auth = client.auth_check()
    except Exception as e:
        _step("auth_check", False, str(e))
        return 1
    if not auth:
        _step("auth_check", False, "credentials rejected by host")
        return 1
    _step("auth_check", True)

    # ----- 3. fire an LLM call through the router -----
    # Use a unique tag so we can find this exact trace afterwards.
    smoke_tag = f"smoke-{uuid.uuid4().hex[:8]}"
    started_at = datetime.now(timezone.utc)
    try:
        from llm.router import get_llm, Tier
    except Exception as e:
        _step("router import", False, str(e))
        return 1
    llm = get_llm(Tier.LOCAL)
    if llm is None or not getattr(llm, "is_available", False):
        _step("llm router", False, "no provider available")
        return 1
    _step("llm router", True, f"provider={getattr(llm, '_health_provider', type(llm).__name__)}")

    # Embed the smoke_tag in the user prompt so we can verify round-trip.
    response = llm.chat(
        messages=[
            {"role": "system", "content": "You are a smoke test."},
            {"role": "user", "content": f"Echo this tag: {smoke_tag}"},
        ],
        temperature=0.0,
        num_predict=64,
    )
    if response is None:
        _step("llm call", False, "provider returned None")
        return 1
    _step("llm call", True, f"resp_len={len(response)}, tag={smoke_tag}")

    # ----- 4. flush so the event is sent -----
    try:
        client.flush()
    except Exception as e:
        _step("flush", False, str(e))
        return 1
    _step("flush", True)

    # ----- 5. query the public API for recent traces -----
    # Langfuse API: GET /api/public/traces?fromTimestamp=...&limit=...
    # We poll briefly because flush is async on the server.
    import requests
    auth_pair = (pk, sk)
    url = f"{host.rstrip('/')}/api/public/traces"
    params = {
        "fromTimestamp": started_at.isoformat().replace("+00:00", "Z"),
        "limit": 50,
    }
    found = None
    for attempt in range(10):  # up to ~20 seconds
        time.sleep(2)
        try:
            r = requests.get(url, auth=auth_pair, params=params, timeout=10)
        except Exception as e:
            _step("api query", False, str(e))
            return 1
        if r.status_code != 200:
            _step("api query", False, f"HTTP {r.status_code}: {r.text[:150]}")
            return 1
        data = r.json()
        for trace in data.get("data", []):
            inp = trace.get("input")
            if inp and smoke_tag in str(inp):
                found = trace
                break
            # Also check observations input (the smoke_tag lives on the generation)
            for obs in trace.get("observations", []) or []:
                if smoke_tag in str(obs):
                    found = trace
                    break
            if found:
                break
        if found:
            break
        print(f"    … attempt {attempt+1}/10, no match yet, retrying")

    if not found:
        _step("trace verified", False, "no trace containing smoke_tag found in 20s")
        print(f"\nDebug: search recent traces in Langfuse UI for '{smoke_tag}'")
        return 1
    _step(
        "trace verified",
        True,
        f"trace_id={found.get('id', '?')}, name={found.get('name', '?')}",
    )

    # ----- 6. print URL for click-through -----
    project_id = found.get("projectId", "")
    trace_id = found.get("id", "")
    if project_id and trace_id:
        ui_url = f"{host.rstrip('/')}/project/{project_id}/traces/{trace_id}"
        print(f"\nView trace: {ui_url}")
    print("\nAll checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
