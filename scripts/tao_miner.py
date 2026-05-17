#!/usr/bin/env python3
"""
GRID TAO Miner — earns Bittensor TAO by serving LLM inference.

Wraps the existing llama.cpp server (Qwen 32B) as a Bittensor miner.
When validators send prompts, we forward them to llama-server and
return the response. GRID requests take priority via a simple queue.

Architecture:
  Validator → TAO Miner → llama-server:8080 → response → Validator
                              ↑
                          GRID API also uses this

The miner yields to GRID when the API is actively using the LLM
(detected by checking llama-server /health endpoint queue depth).

Usage:
  python scripts/tao_miner.py --wallet.name grid_miner --netuid 1

Prerequisites:
  1. btcli wallet create --wallet.name grid_miner
  2. Fund the wallet with TAO for registration
  3. btcli subnet register --wallet.name grid_miner --netuid <SUBNET>
"""

# NOTE: do NOT use `from __future__ import annotations` here —
# bittensor's axon.attach() inspects type annotations at runtime
# and needs them to be real classes, not strings.

import argparse
import asyncio
import time
from typing import Any

import requests
from loguru import logger as log

# Check bittensor availability
try:
    import bittensor as bt
    HAS_BITTENSOR = True
except ImportError:
    HAS_BITTENSOR = False
    log.warning("bittensor not installed — run: pip install bittensor bittensor-cli")


from config import settings
LLAMA_URL = settings.LLAMACPP_BASE_URL
GRID_PRIORITY_CHECK_URL = f"{LLAMA_URL}/health"

# How long to wait if GRID is actively using the LLM
GRID_YIELD_SECONDS = 5
MAX_TOKENS = 1024
TEMPERATURE = 0.7


def is_grid_busy() -> bool:
    """Check if GRID is actively using the LLM server."""
    try:
        resp = requests.get(GRID_PRIORITY_CHECK_URL, timeout=2)
        data = resp.json()
        # If there are pending requests, GRID has priority
        slots_processing = data.get("slots_processing", 0)
        return slots_processing > 0
    except Exception:
        return False


def query_llama(prompt: str, max_tokens: int = MAX_TOKENS, temperature: float = TEMPERATURE) -> str:
    """Forward a prompt to the local llama.cpp server."""
    # Wait if GRID is busy
    retries = 0
    while is_grid_busy() and retries < 10:
        log.debug("GRID active — yielding for {s}s", s=GRID_YIELD_SECONDS)
        time.sleep(GRID_YIELD_SECONDS)
        retries += 1

    try:
        resp = requests.post(
            f"{LLAMA_URL}/v1/chat/completions",
            json={
                "model": "qwen",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "temperature": temperature,
                "stream": False,
            },
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]
    except Exception as exc:
        log.warning("llama query failed: {e}", e=str(exc))
        return f"Error: {str(exc)}"


# ── Synapse Protocol ──────────────────────────────────────────
# Bittensor v10 requires a proper Synapse subclass with typed fields.

class TextPrompting(bt.Synapse):
    """Simple text prompting protocol for subnet 1."""
    prompt: str = ""
    completion: str = ""

    def deserialize(self) -> str:
        return self.completion


# ── Miner state ──────────────────────────────────────────────

_requests_served = 0
_total_tokens = 0


def forward_fn(synapse: TextPrompting) -> TextPrompting:
    """Handle incoming inference request from validator."""
    global _requests_served, _total_tokens

    prompt = synapse.prompt
    if not prompt:
        return synapse

    log.info("TAO request #{n}: {p}", n=_requests_served + 1, p=prompt[:80])

    response = query_llama(prompt)
    synapse.completion = response

    _requests_served += 1
    _total_tokens += len(response.split())

    log.info("TAO response: {n} tokens (total served: {t})",
             n=len(response.split()), t=_requests_served)
    return synapse


from typing import Tuple

def blacklist_fn(synapse: TextPrompting) -> Tuple[bool, str]:
    """Accept all requests."""
    return False, ""


def priority_fn(synapse: TextPrompting) -> float:
    """All requests equal priority."""
    return 1.0


def setup_wallet(wallet_name: str) -> dict:
    """Check wallet status and provide setup instructions if needed."""
    info = {"wallet_name": wallet_name, "exists": False, "registered": False}

    try:
        wallet = bt.Wallet(name=wallet_name, path="~/.bittensor/wallets/")
        info["exists"] = True
        info["coldkey"] = str(wallet.coldkeypub.ss58_address)
        info["hotkey"] = str(wallet.hotkey.ss58_address)
        log.info("Wallet found: {w}", w=wallet_name)
    except Exception as exc:
        log.debug("Wallet check error: {e}", e=str(exc))
        log.warning("Wallet '{w}' not found", w=wallet_name)
        print(f"""
╔══════════════════════════════════════════════════════════╗
║  GRID TAO MINER — WALLET SETUP REQUIRED                ║
╠══════════════════════════════════════════════════════════╣
║                                                          ║
║  1. Create wallet:                                       ║
║     btcli wallet create --wallet.name {wallet_name:<14}  ║
║                                                          ║
║  2. Fund wallet with TAO for registration                ║
║     (check registration cost on taostats.io)             ║
║                                                          ║
║  3. Register on a subnet:                                ║
║     btcli subnet register --wallet.name {wallet_name:<14}║
║       --netuid 1                                         ║
║                                                          ║
║  4. Run this script again                                ║
╚══════════════════════════════════════════════════════════╝
""")

    return info


def main():
    parser = argparse.ArgumentParser(description="GRID TAO Miner")
    parser.add_argument("--wallet.name", dest="wallet_name", default="grid_miner")
    parser.add_argument("--netuid", type=int, default=1)
    parser.add_argument("--check-only", action="store_true", help="Just check setup, don't mine")
    parser.add_argument("--test", action="store_true", help="Test llama connection")
    args = parser.parse_args()

    log.info("GRID TAO Miner starting")
    log.info("Wallet: {w}, Subnet: {n}", w=args.wallet_name, n=args.netuid)

    # Test llama connection
    if args.test:
        log.info("Testing llama.cpp connection...")
        response = query_llama("What is 2+2? Answer in one word.")
        log.info("Response: {}", response)
        return

    if not HAS_BITTENSOR:
        log.error("bittensor not installed")
        return

    # Check wallet
    wallet_info = setup_wallet(args.wallet_name)
    if args.check_only:
        log.info("Wallet: {}", wallet_info)
        return

    if not wallet_info["exists"]:
        return

    # Set up bittensor axon
    try:
        wallet = bt.Wallet(name=args.wallet_name, path="~/.bittensor/wallets/")
        subtensor = bt.Subtensor(network="finney")

        log.info("Connected to Bittensor network")
        log.info("Subnet: {n}", n=args.netuid)

        uid = subtensor.get_uid_for_hotkey_on_subnet(wallet.hotkey.ss58_address, args.netuid)
        log.info("UID: {u}", u=uid)

        # Create and configure axon
        axon = bt.Axon(wallet=wallet)
        axon.attach(
            forward_fn=forward_fn,
            blacklist_fn=blacklist_fn,
            priority_fn=priority_fn,
        )

        # Serve and start
        axon.serve(netuid=args.netuid, subtensor=subtensor)
        axon.start()

        log.info("TAO Miner axon started — serving on subnet {n}, UID {u}", n=args.netuid, u=uid)
        log.info("Waiting for validator requests...")

        while True:
            time.sleep(30)
            if _requests_served > 0 and _requests_served % 10 == 0:
                log.info("Stats: {r} requests, {t} tokens served",
                         r=_requests_served, t=_total_tokens)

    except KeyboardInterrupt:
        log.info("Shutting down TAO miner")
    except Exception as exc:
        log.error("TAO miner failed: {e}", e=str(exc))
        raise


if __name__ == "__main__":
    main()
