"""Pinned main e8d68da7 post-hoc emitter, dummy-only regression fixture."""

import logging

log = logging.getLogger("dummy_posthoc_fixture")

def _get_langfuse():
    return None

def _emit_to_langfuse(
    *,
    call_id: str,
    module: str,
    tier: str,
    model: str,
    provider: str,
    system_prompt: str,
    user_prompt: str,
    output: str,
    context_tokens: int,
    output_tokens: int,
    latency_ms: int,
    metadata: dict,
) -> None:
    """Best-effort fan-out to Langfuse. Silently swallows all errors."""
    client = _get_langfuse()
    if client is None:
        return
    try:
        obs = client.start_observation(
            name=module,
            as_type="generation",
            model=model or None,
            input={
                "system": system_prompt,
                "user": user_prompt,
            },
            output=output,
            usage_details={
                "input": int(context_tokens) if context_tokens else 0,
                "output": int(output_tokens) if output_tokens else 0,
            },
            metadata={
                "tier": tier,
                "provider": provider,
                "module": module,
                "latency_ms": int(latency_ms) if latency_ms else 0,
                "call_id": call_id,
                **(metadata or {}),
            },
        )
        obs.end()
    except Exception as e:
        log.debug(f"Langfuse emit failed: {e}")
