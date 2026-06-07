"""Hermes Codex backend.

Reaches the user's ChatGPT/Codex **subscription** (GPT-5.5 is subscription-only
— there is no API-key path to it) by shelling out to the Codex CLI in headless
mode::

    codex exec --sandbox read-only --skip-git-repo-check \
        --output-last-message <file> - < <prompt-on-stdin>

Design choices that keep this safe and robust:
  - prompt is fed on **stdin** (the trailing ``-``), never as an argv element,
    so there is no argument-injection or length limit;
  - ``subprocess.run`` with a **list** argv and no ``shell=True``;
  - **read-only sandbox** + a throwaway temp working directory, so Codex's
    agentic tools cannot touch the GRID repo or write anything;
  - ``--output-last-message`` captures the clean final answer in a file (more
    reliable than scraping the event stream);
  - any failure (binary missing, not logged in, non-zero exit, timeout, empty
    output) logs a warning and returns ``None`` so :class:`HermesAgent` falls
    back to the local analyst.

Auth is whatever ``codex login`` established on the host (Sign in with ChatGPT
for the subscription). This provider never handles a key. Usage is billed
against the subscription's rate limits, so there is no per-token USD accounting
here (``cost_usd`` is 0; the spend ledger/cap apply only to the OpenAI backend).
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
import time

from loguru import logger as log

from .config import HermesConfig, load_hermes_config
from .provider import HermesResponse, TokenUsage


class CodexProvider:
    """Subscription-backed analyst provider via the Codex CLI."""

    def __init__(self, config: HermesConfig | None = None) -> None:
        self.config = config or load_hermes_config()

    @property
    def is_available(self) -> bool:
        """True when enabled, selected as the backend, and the CLI is on PATH.

        (Whether ``codex login`` has been run can't be checked without a real
        call — a logged-out CLI just makes ``complete`` return ``None``.)
        """
        if not (self.config.enabled and self.config.backend == "codex"):
            return False
        return shutil.which(self.config.codex_bin) is not None

    @staticmethod
    def _flatten(messages: list[dict[str, str]]) -> str:
        """Collapse chat messages into a single prompt (Codex has no roles)."""
        parts = [m.get("content", "") for m in messages if m.get("content")]
        return "\n\n".join(parts)

    def _build_argv(self, out_path: str, model: str | None) -> list[str]:
        argv = [
            self.config.codex_bin,
            "exec",
            "--sandbox",
            "read-only",
            "--skip-git-repo-check",
            "--output-last-message",
            out_path,
        ]
        mdl = model or self.config.codex_model
        if mdl:
            argv += ["--model", mdl]
        if self.config.codex_extra_args:
            argv += shlex.split(self.config.codex_extra_args)
        argv.append("-")  # read the prompt from stdin
        return argv

    def complete(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float | None = None,  # noqa: ARG002 - Codex ignores these
        max_completion_tokens: int | None = None,  # noqa: ARG002
    ) -> HermesResponse | None:
        """Run one headless Codex turn. Returns ``None`` on any failure."""
        if not (self.config.enabled and self.config.backend == "codex"):
            return None
        if shutil.which(self.config.codex_bin) is None:
            log.warning("Codex CLI '{b}' not found on PATH — is it installed?",
                        b=self.config.codex_bin)
            return None

        prompt = self._flatten(messages)
        mdl = model or self.config.codex_model
        start = time.monotonic()
        try:
            with tempfile.TemporaryDirectory(prefix="hermes_codex_") as td:
                out_path = os.path.join(td, "last_message.txt")
                argv = self._build_argv(out_path, mdl)
                proc = subprocess.run(  # noqa: S603 - list argv, no shell, read-only sandbox
                    argv,
                    input=prompt,
                    capture_output=True,
                    text=True,
                    timeout=self.config.codex_timeout_seconds,
                    cwd=td,
                )
                text = self._read_output(out_path, proc)
        except FileNotFoundError:
            log.warning("Codex CLI '{b}' not found", b=self.config.codex_bin)
            return None
        except subprocess.TimeoutExpired:
            log.warning("Codex exec timed out after {t}s",
                        t=self.config.codex_timeout_seconds)
            return None
        except Exception as exc:
            log.warning("Codex exec failed: {e}", e=str(exc))
            return None

        latency = (time.monotonic() - start) * 1000
        if proc.returncode != 0:
            log.warning("Codex exec exited {c}: {err}",
                        c=proc.returncode, err=(proc.stderr or "")[:300])
            return None
        if not text:
            log.warning("Codex exec produced no output")
            return None

        log.info("Codex ok — model={m} {l:.0f}ms (subscription)",
                 m=mdl or "default", l=latency)
        return HermesResponse(
            text=text,
            model=f"codex:{mdl or 'subscription'}",
            usage=TokenUsage(),  # subscription billing — no per-token usage
            cost_usd=0.0,
            latency_ms=latency,
            provider="codex",
        )

    @staticmethod
    def _read_output(out_path: str, proc: subprocess.CompletedProcess) -> str:
        """Prefer the --output-last-message file; fall back to stdout."""
        try:
            if os.path.exists(out_path):
                content = open(out_path, encoding="utf-8").read().strip()
                if content:
                    return content
        except OSError:
            pass
        return (proc.stdout or "").strip()
