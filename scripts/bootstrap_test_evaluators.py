"""End-to-end smoke test: emit a chat-ask trace, confirm evaluators score it.

This validates that the 4 evaluators created by setup_langfuse_evaluators.py
actually fire on incoming traces matching `name == "chat-ask"`.

Flow:
  1. Use Langfuse Python SDK v3+ (OTel) to create a span named "chat-ask".
  2. Set its input/output to look like a real chatbot call.
  3. Flush; capture the trace_id.
  4. Poll GET /api/public/traces/{id} for ~90s waiting for scores from all
     four evaluators (`grid_banned_phrases`, `grid_plain_english`,
     `grid_multi_source`, `grid_action_call`) to attach.

Two test cases are emitted:
  GOOD — answer follows GRID rules (no banned phrases, plain English,
         multi-source, concrete action call). Should score high on all 4.
  BAD  — answer violates every rule. Should score low on all 4.

Exits 0 if at least one trace receives all four scores within the timeout.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import requests


# ── env ─────────────────────────────────────────────────────────────────────

ENV_PATH = Path("/data/grid_v4/grid_repo/.env")


def _load_env() -> None:
    if not ENV_PATH.exists():
        return
    for raw in ENV_PATH.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()

LANGFUSE_HOST = os.environ["LANGFUSE_HOST"]
PK = os.environ["LANGFUSE_PUBLIC_KEY"]
SK = os.environ["LANGFUSE_SECRET_KEY"]
AUTH = (PK, SK)

# Langfuse emits scores using the *rule* name, not the evaluator name. Our
# rules are suffixed with `__chat-ask`.
EXPECTED_SCORE_NAMES = {
    "grid_banned_phrases__chat-ask",
    "grid_plain_english__chat-ask",
    "grid_multi_source__chat-ask",
    "grid_action_call__chat-ask",
}


# ── trace fixtures ──────────────────────────────────────────────────────────

GOOD_QUESTION = "What does dealer gamma exposure mean for SPY this week?"
GOOD_ANSWER = (
    "VERDICT: SPY is in negative-gamma territory below ~5,250, which means dealers "
    "amplify moves instead of dampening them.\n\n"
    "WHY (plain English): 'Gamma' just means how fast a dealer's hedge has to change "
    "when SPY moves. When dealers are short gamma, they sell into drops and buy into "
    "rips, so swings get bigger. Two independent signals confirm this: (1) options "
    "flow shows dealers net short gamma at current spot, and (2) realized "
    "intraday vol on SPY is running ~30% above its 20-day average per price action. "
    "I do NOT have fresh dark-pool prints right now, so I am not relying on those.\n\n"
    "ACTION CALL:\n"
    "- SPY: watch 5,250 as the gamma-flip trigger; reclaim flips dealers back to "
    "long gamma and damps vol.\n"
    "- If SPY breaks 5,180 by Friday close, expect another 1-1.5% downside leg as "
    "dealers chase hedges.\n"
    "- Time horizon: this week into next Friday's OPEX."
)

BAD_QUESTION = "What's going on with the market?"
BAD_ANSWER = (
    "It is important to note that markets can be volatile, and this is not financial "
    "advice. Past performance is no guarantee of future results. While I can't predict "
    "exactly what will happen, you should consider monitoring the situation and do your "
    "own research. As always, let me know if you'd like more information."
)


# ── trace emitter (via SDK) ─────────────────────────────────────────────────


def emit_chat_ask_trace(question: str, answer: str, label: str) -> str:
    """Emit a span named 'chat-ask' and return its trace_id.

    Uses Langfuse v3 OTel SDK so the resulting span behaves identically to the
    production @observe(name='chat-ask') decorator in api/routers/chat.py.
    """
    from langfuse import Langfuse, get_client

    Langfuse(public_key=PK, secret_key=SK, host=LANGFUSE_HOST)
    client = get_client()

    # SDK v4 API: start_as_current_observation(as_type="span" | "generation").
    with client.start_as_current_observation(
        name="chat-ask", as_type="span"
    ) as span:
        span.update(
            input={"question": question},
            output={"answer": answer, "latency_s": 1.23, "error": None},
            metadata={"smoke_test": True, "label": label},
        )
        trace_id = span.trace_id
    client.flush()
    print(f"[emit] label={label} trace_id={trace_id}")
    return trace_id


# ── score polling ──────────────────────────────────────────────────────────


def fetch_scores_for_trace(trace_id: str) -> dict[str, dict]:
    """Return dict score_name → {value, comment, observationId}.

    Note: GET /api/public/scores?traceId=X does NOT filter server-side on this
    Langfuse build (it returns the full project page). We must filter client-
    side, or — preferred — read scores embedded in the single-trace payload.
    """
    url = f"{LANGFUSE_HOST}/api/public/scores"
    out: dict[str, dict] = {}
    page = 1
    while True:
        r = requests.get(
            url, auth=AUTH, params={"page": page, "limit": 100}, timeout=15
        )
        r.raise_for_status()
        body = r.json()
        rows = body.get("data", []) or []
        for s in rows:
            if s.get("traceId") != trace_id:
                continue
            out[s.get("name", "")] = {
                "value": s.get("value"),
                "stringValue": s.get("stringValue"),
                "dataType": s.get("dataType"),
                "comment": (s.get("comment") or "")[:160],
                "observationId": s.get("observationId"),
            }
        meta = body.get("meta", {}) or {}
        if page >= int(meta.get("totalPages", 1) or 1):
            break
        page += 1
        if page > 5:  # bound at 500 most-recent scores
            break
    return out


def poll_for_scores(trace_id: str, timeout_s: int = 180) -> dict[str, dict]:
    """Poll until all 4 expected scores are present or timeout."""
    deadline = time.time() + timeout_s
    last: dict[str, dict] = {}
    while time.time() < deadline:
        last = fetch_scores_for_trace(trace_id)
        present = set(last.keys()) & EXPECTED_SCORE_NAMES
        missing = EXPECTED_SCORE_NAMES - present
        print(f"  poll: have {sorted(present)} missing {sorted(missing)}")
        if not missing:
            return last
        time.sleep(6)
    return last


# ── main ────────────────────────────────────────────────────────────────────


def main() -> int:
    print(f"[bootstrap] host={LANGFUSE_HOST}")
    print("[bootstrap] emitting GOOD chat-ask trace …")
    good_id = emit_chat_ask_trace(GOOD_QUESTION, GOOD_ANSWER, label="GOOD")

    print("[bootstrap] emitting BAD chat-ask trace …")
    bad_id = emit_chat_ask_trace(BAD_QUESTION, BAD_ANSWER, label="BAD")

    # Give Langfuse a few seconds to ingest before polling.
    time.sleep(5)

    print(f"\n[bootstrap] polling GOOD trace {good_id} …")
    good_scores = poll_for_scores(good_id, timeout_s=180)

    print(f"\n[bootstrap] polling BAD trace {bad_id} …")
    bad_scores = poll_for_scores(bad_id, timeout_s=180)

    print("\n=== GOOD trace scores ===")
    print(json.dumps(good_scores, indent=2))
    print("\n=== BAD trace scores ===")
    print(json.dumps(bad_scores, indent=2))

    good_ok = EXPECTED_SCORE_NAMES.issubset(good_scores.keys())
    bad_ok = EXPECTED_SCORE_NAMES.issubset(bad_scores.keys())

    print("\n=== verdict ===")
    print(f"GOOD: all 4 scores present? {good_ok}")
    print(f"BAD : all 4 scores present? {bad_ok}")

    if good_ok and bad_ok:
        # Sanity: BAD should score 0 on banned_phrases, GOOD should score 1.
        good_bp = good_scores.get(
            "grid_banned_phrases__chat-ask", {}
        ).get("value")
        bad_bp = bad_scores.get(
            "grid_banned_phrases__chat-ask", {}
        ).get("value")
        print(f"  banned_phrases:  GOOD={good_bp}  BAD={bad_bp}")
        if good_bp == 1 and bad_bp == 0:
            print("  → polarity correct (banned_phrases discriminates as expected)")

    return 0 if (good_ok or bad_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
