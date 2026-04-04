"""
GRID Bittensor Subnet Miner.

Edge client that runs on user GPUs. Pulls research tasks from the
GRID validator, runs inference with a local model (Qwen 7B or Hermes 8B),
and submits responses to earn TAO + GRID API credits.

Requirements:
    - Any GPU with 8GB+ VRAM (runs quantized 7-8B models)
    - Python 3.10+
    - llama-cpp-python or ollama

Usage:
    python subnet/miner.py --wallet.name miner1 --model hermes-8b
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from typing import Any

from loguru import logger as log


# ── Local Inference ──────────────────────────────────────────────────────

class LocalInference:
    """Run inference on the miner's local GPU."""

    def __init__(self, model_path: str | None = None, backend: str = "llamacpp") -> None:
        self.backend = backend
        self.model_path = model_path
        self._client = None
        from config import settings
        self.llm_url = settings.LLAMACPP_BASE_URL

    def generate(self, prompt: str, max_tokens: int = 500) -> str:
        """Generate a response using the local model."""
        if self.backend == "llamacpp":
            return self._generate_llamacpp(prompt, max_tokens)
        elif self.backend == "ollama":
            return self._generate_ollama(prompt, max_tokens)
        else:
            raise ValueError(f"Unknown backend: {self.backend}")

    def _generate_llamacpp(self, prompt: str, max_tokens: int) -> str:
        """Use llama.cpp server (must be running locally)."""
        import requests
        try:
            resp = requests.post(
                f"{self.llm_url}/v1/chat/completions",
                json={
                    "model": "local",
                    "messages": [
                        {"role": "system", "content": (
                            "You are a financial intelligence researcher for GRID. "
                            "Provide specific, data-rich analysis with names, numbers, and dates. "
                            "Label every finding: confirmed, derived, estimated, rumored, or inferred."
                        )},
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": max_tokens,
                    "temperature": 0.3,
                },
                timeout=120,
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]
        except Exception as exc:
            log.warning("llama.cpp inference failed: {e}", e=str(exc))
            return ""

    def _generate_ollama(self, prompt: str, max_tokens: int) -> str:
        """Use Ollama API."""
        import requests
        try:
            resp = requests.post(
                "http://localhost:11434/api/generate",
                json={
                    "model": "hermes3:8b",
                    "prompt": prompt,
                    "stream": False,
                    "options": {"num_predict": max_tokens, "temperature": 0.3},
                },
                timeout=120,
            )
            resp.raise_for_status()
            return resp.json().get("response", "")
        except Exception as exc:
            log.warning("Ollama inference failed: {e}", e=str(exc))
            return ""


# ── Miner Core ───────────────────────────────────────────────────────────

class GRIDMiner:
    """GRID subnet miner — processes financial research tasks for TAO rewards.

    Lifecycle:
        1. Receive task from validator via Bittensor axon
        2. Run inference on local GPU
        3. Return structured response
        4. Earn TAO based on response quality
        5. Accumulate API credits for GRID intelligence access
    """

    def __init__(
        self,
        model_path: str | None = None,
        backend: str = "llamacpp",
    ) -> None:
        self.inference = LocalInference(model_path=model_path, backend=backend)
        self.tasks_completed = 0
        self.total_tokens = 0

    async def forward(self, synapse: dict) -> str:
        """Process a research task from the validator.

        This is called by the Bittensor axon when the validator
        sends a task. The miner runs inference and returns the response.
        """
        prompt = synapse.get("prompt", "")
        task_type = synapse.get("task_type", "unknown")

        if not prompt:
            return ""

        log.info("Processing {t} task ({n} chars)", t=task_type, n=len(prompt))

        # Run inference
        start = time.monotonic()
        response = self.inference.generate(prompt, max_tokens=500)
        elapsed = time.monotonic() - start

        self.tasks_completed += 1
        self.total_tokens += len(response.split())

        log.info(
            "Task {n} complete — {t}s, {w} words",
            n=self.tasks_completed, t=round(elapsed, 1),
            w=len(response.split()),
        )

        return response

    def get_stats(self) -> dict:
        """Return miner performance stats."""
        return {
            "tasks_completed": self.tasks_completed,
            "total_tokens": self.total_tokens,
            "uptime_hours": 0,  # TODO: track
        }


# ── Standalone Mode (no Bittensor) ───────────────────────────────────────

class StandaloneMiner:
    """Run as a standalone worker without Bittensor network.

    Pulls tasks directly from the GRID API and submits results.
    Useful for testing or running on machines without TAO setup.
    """

    def __init__(
        self,
        grid_url: str = "http://localhost:8000",
        api_key: str = "",
        backend: str = "llamacpp",
    ) -> None:
        self.grid_url = grid_url.rstrip("/")
        self.api_key = api_key
        self.miner = GRIDMiner(backend=backend)

    async def pull_and_process(self) -> dict:
        """Pull a task from GRID API, process it, submit result."""
        import requests

        headers = {"Authorization": f"Bearer {self.api_key}"}

        # Pull task
        try:
            resp = requests.get(
                f"{self.grid_url}/api/v1/subnet/task",
                headers=headers,
                timeout=10,
            )
            if resp.status_code != 200:
                return {"status": "no_tasks"}
            task = resp.json()
        except Exception as exc:
            return {"status": "error", "error": str(exc)}

        # Process
        response = await self.miner.forward(task)

        # Submit
        try:
            resp = requests.post(
                f"{self.grid_url}/api/v1/subnet/submit",
                headers=headers,
                json={
                    "task_id": task.get("task_id"),
                    "response": response,
                },
                timeout=10,
            )
            return {"status": "submitted", "task_type": task.get("task_type")}
        except Exception as exc:
            return {"status": "submit_error", "error": str(exc)}

    async def run_forever(self, interval: int = 5) -> None:
        """Continuously pull and process tasks."""
        log.info("GRID Standalone Miner starting")
        while True:
            try:
                result = await self.pull_and_process()
                if result["status"] == "submitted":
                    log.info("Submitted: {t}", t=result.get("task_type"))
                elif result["status"] == "no_tasks":
                    await asyncio.sleep(30)  # wait longer if no tasks
                    continue
            except KeyboardInterrupt:
                break
            except Exception as exc:
                log.error("Miner error: {e}", e=str(exc))

            await asyncio.sleep(interval)


# ── CLI ──────────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="GRID Subnet Miner")
    parser.add_argument("--backend", default="llamacpp", choices=["llamacpp", "ollama"])
    parser.add_argument("--standalone", action="store_true", help="Run without Bittensor")
    parser.add_argument("--grid-url", default="http://localhost:8000")
    parser.add_argument("--api-key", default=os.getenv("GRID_API_KEY", ""))
    args = parser.parse_args()

    if args.standalone:
        miner = StandaloneMiner(
            grid_url=args.grid_url,
            api_key=args.api_key,
            backend=args.backend,
        )
        asyncio.run(miner.run_forever())
    else:
        log.info("Bittensor miner mode — requires bittensor package")
        log.info("Install: pip install bittensor")
        log.info("For standalone mode: python subnet/miner.py --standalone")


if __name__ == "__main__":
    main()
