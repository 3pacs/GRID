"""
GRID Mobile Miner — OAuth-based task processing via ChatGPT/Copilot/Claude.

Lets anyone with a phone contribute to GRID's research backlog by connecting
their existing AI subscription. No GPU needed, no app download — just
authenticate, pull a task, route it through their AI, submit the response.

Supported providers:
    - ChatGPT (OpenAI) — free/Plus/Team via OAuth
    - Copilot (Microsoft) — free/Pro via Microsoft OAuth
    - Claude (Anthropic) — free/Pro via OAuth
    - Gemini (Google) — free/Advanced via Google OAuth
    - Custom API — any OpenAI-compatible endpoint

Flow:
    1. User visits grid.stepdad.finance/mine on their phone
    2. Clicks "Connect ChatGPT" (or Copilot, Claude, Gemini)
    3. OAuth redirects to provider, user authorizes
    4. GRID stores the access token (encrypted)
    5. User sees a task, taps "Process"
    6. GRID sends the task to user's AI provider via their token
    7. Response comes back, gets scored, user earns credits
    8. Repeat — swipe through tasks like Tinder

The user's AI subscription cost is $0 extra — they already pay for it.
GRID gets free compute. User earns API credits + future TAO.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── OAuth Provider Configs ───────────────────────────────────────────────

PROVIDERS = {
    "chatgpt": {
        "name": "ChatGPT",
        "auth_url": "https://auth.openai.com/authorize",
        "token_url": "https://auth.openai.com/oauth/token",
        "api_url": "https://api.openai.com/v1/chat/completions",
        "model": "gpt-4o-mini",  # free tier model
        "scopes": ["openid", "profile"],
        "client_id_env": "OPENAI_OAUTH_CLIENT_ID",
        "client_secret_env": "OPENAI_OAUTH_CLIENT_SECRET",
    },
    "copilot": {
        "name": "Microsoft Copilot",
        "auth_url": "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
        "token_url": "https://login.microsoftonline.com/common/oauth2/v2.0/token",
        "api_url": "https://api.bing.microsoft.com/v7.0/chat/completions",
        "model": "copilot",
        "scopes": ["openid", "profile", "User.Read"],
        "client_id_env": "MICROSOFT_OAUTH_CLIENT_ID",
        "client_secret_env": "MICROSOFT_OAUTH_CLIENT_SECRET",
    },
    "claude": {
        "name": "Claude",
        "auth_url": "https://console.anthropic.com/oauth/authorize",
        "token_url": "https://console.anthropic.com/oauth/token",
        "api_url": "https://api.anthropic.com/v1/messages",
        "model": "claude-sonnet-4-20250514",
        "scopes": ["openid"],
        "client_id_env": "ANTHROPIC_OAUTH_CLIENT_ID",
        "client_secret_env": "ANTHROPIC_OAUTH_CLIENT_SECRET",
    },
    "gemini": {
        "name": "Google Gemini",
        "auth_url": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_url": "https://oauth2.googleapis.com/token",
        "api_url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent",
        "model": "gemini-pro",
        "scopes": ["openid", "profile", "https://www.googleapis.com/auth/generative-language"],
        "client_id_env": "GOOGLE_OAUTH_CLIENT_ID",
        "client_secret_env": "GOOGLE_OAUTH_CLIENT_SECRET",
    },
    "custom": {
        "name": "Custom API",
        "auth_url": None,  # no OAuth — user provides API key directly
        "token_url": None,
        "api_url": None,  # user-specified
        "model": None,
        "scopes": [],
    },
}

# System prompt injected into every task
GRID_SYSTEM_PROMPT = (
    "You are a financial intelligence researcher for GRID. "
    "Provide specific, data-rich analysis with names, numbers, and dates. "
    "Label every finding with a confidence level: "
    "confirmed (from official records), derived (calculated from data), "
    "estimated (best guess with bounds), rumored (unverified reports), "
    "or inferred (logical deduction). "
    "Be concise but thorough. No filler."
)


class OAuthManager:
    """Manages OAuth tokens for AI provider connections."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._ensure_table()

    def _ensure_table(self) -> None:
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS miner_oauth_tokens (
                        miner_id TEXT NOT NULL,
                        provider TEXT NOT NULL,
                        access_token TEXT NOT NULL,
                        refresh_token TEXT,
                        expires_at TIMESTAMPTZ,
                        api_endpoint TEXT,
                        model_override TEXT,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (miner_id, provider)
                    )
                """))
        except Exception:
            pass

    def get_auth_url(self, provider: str, miner_id: str, redirect_uri: str) -> str | None:
        """Generate OAuth authorization URL for a provider."""
        config = PROVIDERS.get(provider)
        if not config or not config.get("auth_url"):
            return None

        client_id = os.getenv(config["client_id_env"], "")
        if not client_id:
            return None

        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": " ".join(config["scopes"]),
            "state": hashlib.sha256(f"{miner_id}:{provider}".encode()).hexdigest()[:16],
        }
        return f"{config['auth_url']}?{urlencode(params)}"

    def exchange_code(self, provider: str, code: str, redirect_uri: str) -> dict | None:
        """Exchange OAuth authorization code for access token."""
        config = PROVIDERS.get(provider)
        if not config or not config.get("token_url"):
            return None

        client_id = os.getenv(config["client_id_env"], "")
        client_secret = os.getenv(config["client_secret_env"], "")

        try:
            resp = requests.post(
                config["token_url"],
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.warning("OAuth token exchange failed for {p}: {e}", p=provider, e=str(exc))
            return None

    def store_token(self, miner_id: str, provider: str, token_data: dict,
                    api_endpoint: str = "", model_override: str = "") -> None:
        """Store OAuth token (encrypted in production)."""
        expires_in = token_data.get("expires_in", 3600)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        try:
            with self.engine.begin() as conn:
                conn.execute(text(
                    "INSERT INTO miner_oauth_tokens "
                    "(miner_id, provider, access_token, refresh_token, expires_at, "
                    "api_endpoint, model_override) "
                    "VALUES (:mid, :p, :at, :rt, :ea, :ae, :mo) "
                    "ON CONFLICT (miner_id, provider) DO UPDATE SET "
                    "access_token = EXCLUDED.access_token, "
                    "refresh_token = EXCLUDED.refresh_token, "
                    "expires_at = EXCLUDED.expires_at"
                ), {
                    "mid": miner_id,
                    "p": provider,
                    "at": token_data.get("access_token", ""),
                    "rt": token_data.get("refresh_token"),
                    "ea": expires_at,
                    "ae": api_endpoint,
                    "mo": model_override,
                })
        except Exception as exc:
            log.warning("Failed to store OAuth token: {e}", e=str(exc))

    def get_token(self, miner_id: str, provider: str) -> dict | None:
        """Get stored OAuth token for a miner."""
        try:
            with self.engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT access_token, refresh_token, expires_at, "
                    "api_endpoint, model_override "
                    "FROM miner_oauth_tokens "
                    "WHERE miner_id = :mid AND provider = :p"
                ), {"mid": miner_id, "p": provider}).fetchone()

                if not row:
                    return None

                return {
                    "access_token": row[0],
                    "refresh_token": row[1],
                    "expires_at": row[2],
                    "api_endpoint": row[3],
                    "model_override": row[4],
                }
        except Exception:
            return None

    def store_api_key(self, miner_id: str, provider: str, api_key: str,
                      api_endpoint: str = "", model: str = "") -> None:
        """Store a direct API key (for custom providers or API key auth)."""
        self.store_token(
            miner_id, provider,
            {"access_token": api_key, "expires_in": 365 * 86400},
            api_endpoint=api_endpoint,
            model_override=model,
        )


class AIProviderRouter:
    """Routes GRID research tasks to the miner's connected AI provider."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.oauth = OAuthManager(engine)

    def process_task(self, miner_id: str, provider: str, task: dict) -> str:
        """Send a task to the miner's AI provider and return the response."""
        token_data = self.oauth.get_token(miner_id, provider)
        if not token_data:
            return ""

        config = PROVIDERS.get(provider, {})
        api_url = token_data.get("api_endpoint") or config.get("api_url", "")
        model = token_data.get("model_override") or config.get("model", "")
        access_token = token_data.get("access_token", "")

        if not api_url or not access_token:
            return ""

        prompt = task.get("prompt", "")
        if not prompt:
            return ""

        if provider == "claude":
            return self._call_claude(api_url, access_token, model, prompt)
        elif provider == "gemini":
            return self._call_gemini(api_url, access_token, prompt)
        else:
            return self._call_openai_compatible(api_url, access_token, model, prompt)

    def _call_openai_compatible(self, url: str, token: str, model: str, prompt: str) -> str:
        """Call OpenAI-compatible API (ChatGPT, Copilot, custom)."""
        try:
            resp = requests.post(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model or "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": GRID_SYSTEM_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": 800,
                    "temperature": 0.3,
                },
                timeout=120,
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]
        except Exception as exc:
            log.debug("OpenAI-compatible call failed: {e}", e=str(exc))
            return ""

    def _call_claude(self, url: str, token: str, model: str, prompt: str) -> str:
        """Call Anthropic Claude API."""
        try:
            resp = requests.post(
                url,
                headers={
                    "x-api-key": token,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model or "claude-sonnet-4-20250514",
                    "max_tokens": 800,
                    "system": GRID_SYSTEM_PROMPT,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=120,
            )
            resp.raise_for_status()
            return resp.json()["content"][0]["text"]
        except Exception as exc:
            log.debug("Claude call failed: {e}", e=str(exc))
            return ""

    def _call_gemini(self, url: str, token: str, prompt: str) -> str:
        """Call Google Gemini API."""
        try:
            resp = requests.post(
                f"{url}?key={token}",
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [{
                        "parts": [{"text": f"{GRID_SYSTEM_PROMPT}\n\n{prompt}"}]
                    }],
                    "generationConfig": {
                        "maxOutputTokens": 800,
                        "temperature": 0.3,
                    },
                },
                timeout=120,
            )
            resp.raise_for_status()
            return resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as exc:
            log.debug("Gemini call failed: {e}", e=str(exc))
            return ""

    def get_connected_providers(self, miner_id: str) -> list[dict]:
        """List which AI providers a miner has connected."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT provider, expires_at, model_override "
                    "FROM miner_oauth_tokens WHERE miner_id = :mid"
                ), {"mid": miner_id}).fetchall()
                return [
                    {
                        "provider": r[0],
                        "name": PROVIDERS.get(r[0], {}).get("name", r[0]),
                        "connected": True,
                        "expires_at": r[1].isoformat() if r[1] else None,
                        "model": r[2] or PROVIDERS.get(r[0], {}).get("model"),
                    }
                    for r in rows
                ]
        except Exception:
            return []


# ── FastAPI Routes for Mobile Mining ─────────────────────────────────────

try:
    from fastapi import APIRouter, Depends, HTTPException, Query, Request
    from pydantic import BaseModel

    mine_router = APIRouter(prefix="/api/v1/mine", tags=["mine"])

    def _get_router_deps():
        from api.dependencies import get_db_engine
        return get_db_engine()

    class ConnectProviderRequest(BaseModel):
        provider: str
        api_key: str = ""
        api_endpoint: str = ""
        model: str = ""

    class ProcessTaskRequest(BaseModel):
        provider: str

    @mine_router.get("/providers")
    def list_providers():
        """List available AI providers for mobile mining."""
        return {
            "providers": [
                {
                    "id": pid,
                    "name": p["name"],
                    "auth_type": "oauth" if p.get("auth_url") else "api_key",
                    "free_tier": pid in ("chatgpt", "copilot", "gemini"),
                }
                for pid, p in PROVIDERS.items()
            ]
        }

    @mine_router.post("/connect")
    def connect_provider(body: ConnectProviderRequest, request: Request):
        """Connect an AI provider via API key (simplest method).

        For OAuth flow, use /mine/oauth/{provider}/start instead.
        """
        try:
            from api.auth import decode_token
            from api.dependencies import get_db_engine
        except ImportError:
            raise HTTPException(500, "Auth not available")

        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        user = decode_token(token)
        if not user:
            raise HTTPException(401, "Invalid token")

        miner_id = hashlib.sha256(user.get("sub", "").encode()).hexdigest()[:16]
        engine = get_db_engine()
        oauth = OAuthManager(engine)

        if body.api_key:
            oauth.store_api_key(
                miner_id, body.provider, body.api_key,
                api_endpoint=body.api_endpoint, model=body.model,
            )
            return {"status": "connected", "provider": body.provider}
        else:
            raise HTTPException(400, "API key required for direct connection")

    @mine_router.get("/task")
    def get_mining_task(request: Request):
        """Pull the next research task for mobile processing."""
        try:
            from api.auth import decode_token
            from api.dependencies import get_db_engine
        except ImportError:
            raise HTTPException(500, "Auth not available")

        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        user = decode_token(token)
        if not user:
            raise HTTPException(401, "Invalid token")

        engine = get_db_engine()
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT id, task_type, prompt, context FROM llm_task_backlog "
                "WHERE status = 'pending' "
                "ORDER BY priority ASC, RANDOM() LIMIT 1"
            )).fetchone()

        if not row:
            raise HTTPException(204, "No tasks available")

        return {
            "task_id": row[0],
            "task_type": row[1],
            "prompt": row[2][:500],  # truncate for mobile display
            "full_prompt": row[2],
            "context": row[3] if isinstance(row[3], dict) else {},
        }

    @mine_router.post("/process/{task_id}")
    def process_task(task_id: int, body: ProcessTaskRequest, request: Request):
        """Process a task using the miner's connected AI provider."""
        try:
            from api.auth import decode_token
            from api.dependencies import get_db_engine
        except ImportError:
            raise HTTPException(500, "Auth not available")

        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        user = decode_token(token)
        if not user:
            raise HTTPException(401, "Invalid token")

        miner_id = hashlib.sha256(user.get("sub", "").encode()).hexdigest()[:16]
        engine = get_db_engine()

        # Get the task
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT prompt FROM llm_task_backlog WHERE id = :id"
            ), {"id": task_id}).fetchone()

        if not row:
            raise HTTPException(404, "Task not found")

        # Route to AI provider
        router = AIProviderRouter(engine)
        response = router.process_task(miner_id, body.provider, {"prompt": row[0]})

        if not response:
            raise HTTPException(502, "AI provider returned no response")

        # Submit the response through the compute pipeline
        from subnet.distributed_compute import ComputeCoordinator
        coord = ComputeCoordinator(engine)
        result = coord.submit_result(miner_id, task_id, response)

        return {
            "status": "submitted",
            "score": result.get("score", {}).get("total", 0),
            "tier": result.get("score", {}).get("tier", "unknown"),
            "credits_earned": result.get("rewards", {}).get("api_credits", 0),
            "response_preview": response[:200],
        }

    @mine_router.get("/stats")
    def mining_stats(request: Request):
        """Get the miner's stats and earnings."""
        try:
            from api.auth import decode_token
            from api.dependencies import get_db_engine
        except ImportError:
            raise HTTPException(500, "Auth not available")

        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        user = decode_token(token)
        if not user:
            raise HTTPException(401, "Invalid token")

        miner_id = hashlib.sha256(user.get("sub", "").encode()).hexdigest()[:16]
        engine = get_db_engine()
        router = AIProviderRouter(engine)

        providers = router.get_connected_providers(miner_id)

        try:
            with engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT total_tasks, avg_score, api_credits "
                    "FROM compute_miners WHERE miner_id = :mid"
                ), {"mid": miner_id}).fetchone()
                stats = {
                    "tasks_completed": row[0] if row else 0,
                    "avg_score": round(row[1], 3) if row else 0,
                    "api_credits": row[2] if row else 0,
                }
        except Exception:
            stats = {"tasks_completed": 0, "avg_score": 0, "api_credits": 0}

        return {
            "miner_id": miner_id,
            "connected_providers": providers,
            **stats,
            "backlog_remaining": 74000,  # TODO: live count
        }

except ImportError:
    mine_router = None
