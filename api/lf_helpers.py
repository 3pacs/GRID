"""Best-effort Langfuse helpers shared by API routers.

All helpers no-op when Langfuse is not installed or its keys are absent —
they must NEVER raise. Importing this module is cheap; nothing here calls
the network at import time.

Public surface:

* ``observe`` — re-export of ``langfuse.observe`` (or a no-op decorator).
* ``set_input(**kwargs)`` — explicit input for the active span. Use this
  instead of ``capture_input=True`` when arguments may include secrets,
  full history, or other sensitive data.
* ``propagate_attributes(...)`` — context manager. Sets ``user_id``,
  ``session_id``, and ``tags`` on the active trace AND propagates to
  every child span created inside the ``with`` block.
* ``user_id_from_token(token)`` — best-effort JWT subject extraction.
"""

from __future__ import annotations

from contextlib import nullcontext
from typing import Any, Iterable

try:
    from langfuse import (
        get_client as _lf_get_client,
        observe as _lf_observe,
        propagate_attributes as _lf_propagate_attributes,
    )

    def observe(*args: Any, **kwargs: Any):  # type: ignore[no-redef]
        """Pass-through to langfuse.observe with the same calling shape."""
        return _lf_observe(*args, **kwargs)

except Exception:  # pragma: no cover — optional dep missing entirely
    def observe(*args: Any, **kwargs: Any):  # type: ignore[no-redef]
        def _decorator(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _decorator

    def _lf_get_client():  # type: ignore[no-redef]
        return None

    def _lf_propagate_attributes(**_kwargs: Any):  # type: ignore[no-redef]
        return nullcontext()


def set_input(**kwargs: Any) -> None:
    """Explicitly set the input field on the currently active Langfuse span.

    Prefer this over ``capture_input=True`` whenever the function arguments
    might include secrets, large payloads, or PII (e.g. user chat history).
    """
    try:
        client = _lf_get_client()
        if client is not None:
            client.update_current_span(input=kwargs)
    except Exception:
        pass


def set_output(value: Any) -> None:
    """Set the output field on the currently active Langfuse span."""
    try:
        client = _lf_get_client()
        if client is not None:
            client.update_current_span(output=value)
    except Exception:
        pass


def propagate_attributes(
    *,
    user_id: str | None = None,
    session_id: str | None = None,
    tags: Iterable[str] | None = None,
    metadata: dict[str, str] | None = None,
):
    """Context manager that propagates trace attributes to all child spans.

    Drops empty values; never raises. Falls back to ``nullcontext`` when the
    Langfuse SDK is missing or any attribute fails validation.
    """
    cleaned: dict[str, Any] = {}
    if user_id:
        cleaned["user_id"] = user_id
    if session_id:
        cleaned["session_id"] = session_id
    if tags:
        tag_list = [t for t in tags if t]
        if tag_list:
            cleaned["tags"] = tag_list
    if metadata:
        meta_clean = {k: v for k, v in metadata.items() if v not in (None, "")}
        if meta_clean:
            cleaned["metadata"] = meta_clean
    if not cleaned:
        return nullcontext()
    try:
        return _lf_propagate_attributes(**cleaned)
    except Exception:
        return nullcontext()


def user_id_from_token(token: str | None) -> str | None:
    """Decode a JWT bearer token and return its subject (user id), if any.

    Imports ``decode_token`` lazily to avoid import-cycle risk.
    """
    if not token:
        return None
    try:
        from api.auth import decode_token

        payload = decode_token(token)
        if payload:
            sub = payload.get("sub")
            if isinstance(sub, str) and sub:
                return sub[:200]  # Langfuse user_id <= 200 chars
    except Exception:
        return None
    return None
