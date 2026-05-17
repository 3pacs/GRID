"""Langfuse observability for the AstroGrid API.

Mirrors the pattern in /home/grid/storymill/app/observability.py but
adapted to AstroGrid's single LLM call site (`interpret_snapshot`).

All spans/traces produced here are tagged with ``app:astrogrid`` so they can
be filtered out of the shared ``grid`` Langfuse project.

Configuration is read from environment variables (loaded by
``astrogrid_api.dependencies`` via the systemd ``EnvironmentFile``):

- ``LANGFUSE_PUBLIC_KEY``
- ``LANGFUSE_SECRET_KEY``
- ``LANGFUSE_HOST`` (defaults to http://grid-svr:3000)
- ``LANGFUSE_ENABLED`` ("true"/"false", defaults to true when keys present)
- ``LANGFUSE_CAPTURE_IO`` ("true"/"false", defaults to true)
- ``APP_ENV`` (defaults to "production")
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

APP_TAG = "app:astrogrid"
DEFAULT_HOST = "http://grid-svr:3000"

_LANGFUSE_CLIENTS: list[Any] = []


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class LangfuseTracer:
    """Thin wrapper around the Langfuse v4 client.

    Always returns a no-op when not configured so callers can use the same
    code paths whether Langfuse is on or off.
    """

    enabled: bool
    capture_io: bool
    app_env: str
    public_key: str | None = None
    secret_key: str | None = None
    host: str | None = None
    default_tags: tuple[str, ...] = field(default_factory=lambda: (APP_TAG,))
    client: Any | None = None

    @classmethod
    def from_env(cls) -> "LangfuseTracer":
        public_key = os.environ.get("LANGFUSE_PUBLIC_KEY") or None
        secret_key = os.environ.get("LANGFUSE_SECRET_KEY") or None
        host = os.environ.get("LANGFUSE_HOST") or DEFAULT_HOST
        # Default to enabled if both keys are present.
        enabled = _env_bool("LANGFUSE_ENABLED", bool(public_key and secret_key))
        capture_io = _env_bool("LANGFUSE_CAPTURE_IO", True)
        app_env = os.environ.get("APP_ENV") or os.environ.get("ENV") or "production"
        return cls(
            enabled=enabled,
            capture_io=capture_io,
            app_env=app_env,
            public_key=public_key,
            secret_key=secret_key,
            host=host,
        )

    @property
    def is_configured(self) -> bool:
        return self.enabled and bool(self.public_key and self.secret_key)

    def get_client(self) -> Any | None:
        if not self.is_configured:
            return None
        if self.client is not None:
            return self.client
        try:
            from langfuse import Langfuse
        except ImportError:
            logger.warning(
                "LANGFUSE_ENABLED=true but the 'langfuse' package is not installed"
            )
            return None
        try:
            self.client = Langfuse(
                public_key=self.public_key,
                secret_key=self.secret_key,
                host=self.host or None,
                environment=self.app_env,
            )
            _LANGFUSE_CLIENTS.append(self.client)
            logger.info(
                "Langfuse tracer initialized (host=%s, env=%s)",
                self.host,
                self.app_env,
            )
        except Exception:  # noqa: BLE001 - never block app startup
            logger.exception("Failed to initialize Langfuse client")
            return None
        return self.client

    @contextmanager
    def start_generation(
        self,
        *,
        name: str,
        model: str,
        input: Any | None = None,  # noqa: A002 - Langfuse keyword
        metadata: dict[str, Any] | None = None,
        extra_tags: tuple[str, ...] | list[str] | None = None,
    ) -> Iterator[Any]:
        """Open a generation observation; yields a no-op object when disabled.

        Trace-level tags are applied via ``langfuse.propagate_attributes`` so
        the ``app:astrogrid`` filter works in the shared Langfuse project.
        """
        client = self.get_client()
        if client is None:
            yield _NoopObservation()
            return

        tags = list(self.default_tags)
        if extra_tags:
            for tag in extra_tags:
                if tag and tag not in tags:
                    tags.append(tag)

        try:
            from langfuse import propagate_attributes
        except ImportError:  # pragma: no cover - SDK guaranteed when client exists
            propagate_attributes = None  # type: ignore[assignment]

        attrs_ctx = None
        observation_ctx = None
        observation: Any = _NoopObservation()
        try:
            observation_ctx = client.start_as_current_observation(
                as_type="generation",
                name=name,
                model=model,
                input=input if self.capture_io else None,
                metadata=metadata,
            )
            observation = observation_ctx.__enter__()
            # Tags must be propagated AFTER the trace's first span is active,
            # otherwise propagate_attributes has no span to attach to and the
            # trace lands without our tag.
            if propagate_attributes is not None:
                attrs_ctx = propagate_attributes(tags=tags)
                attrs_ctx.__enter__()
        except Exception:  # noqa: BLE001
            logger.exception("Failed to start Langfuse observation")
            try:
                if attrs_ctx is not None:
                    attrs_ctx.__exit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass
            try:
                if observation_ctx is not None:
                    observation_ctx.__exit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass
            yield _NoopObservation()
            return

        exc_info = (None, None, None)
        try:
            yield observation
        except BaseException:  # pragma: no cover - preserve caller exceptions
            exc_info = sys.exc_info()
            raise
        finally:
            # LIFO cleanup: attrs context was entered last, exit first.
            try:
                if attrs_ctx is not None:
                    attrs_ctx.__exit__(*exc_info)
            except Exception:  # noqa: BLE001
                logger.exception("Failed to finish Langfuse attribute context")
            try:
                if observation_ctx is not None:
                    observation_ctx.__exit__(*exc_info)
            except Exception:  # noqa: BLE001
                logger.exception("Failed to finish Langfuse observation")

    def update(self, observation: Any, **kwargs: Any) -> None:
        if observation is None:
            return
        if not self.capture_io:
            kwargs.pop("input", None)
            kwargs.pop("output", None)
        if not kwargs:
            return
        try:
            observation.update(**kwargs)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to update Langfuse observation")


class _NoopObservation:
    def update(self, **_kwargs: Any) -> None:
        return None


# Module-level singleton — mirrors how storymill/grid wire the tracer.
_TRACER: LangfuseTracer | None = None


def get_tracer() -> LangfuseTracer:
    global _TRACER
    if _TRACER is None:
        _TRACER = LangfuseTracer.from_env()
    return _TRACER


def flush_langfuse() -> None:
    """Flush any open Langfuse clients. Safe to call multiple times."""
    flushed_any = False
    for client in list(_LANGFUSE_CLIENTS):
        try:
            client.flush()
            flushed_any = True
        except Exception:  # noqa: BLE001
            logger.exception("Failed to flush Langfuse client")
    if flushed_any:
        return
    try:
        from langfuse import get_client as _get_global_client
    except ImportError:
        return
    try:
        _get_global_client().flush()
    except Exception:  # noqa: BLE001
        logger.exception("Failed to flush global Langfuse client")
