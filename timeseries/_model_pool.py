"""
Shared TimesFM model pool.

Supports both TimesFM 2.5 (preferred) and 1.0 (fallback).
Thread-safe singleton — call get_timesfm_model() from any thread.
"""

from __future__ import annotations

import os
import threading
from typing import Any

from loguru import logger as log

_lock = threading.Lock()
_model: Any = None
_model_version: str | None = None


def get_timesfm_model(
    context_len: int = 512,
    horizon_len: int = 128,
    batch_size: int = 32,
) -> tuple[Any, str]:
    """Return a cached TimesFM model and version string.

    Tries TimesFM 2.5 first, falls back to 1.0 if unavailable.
    Thread-safe via a module-level Lock.

    Returns:
        (model, version_string)
    """
    global _model, _model_version

    if _model is not None:
        return _model, _model_version  # type: ignore[return-value]

    with _lock:
        if _model is not None:
            return _model, _model_version  # type: ignore[return-value]

        try:
            import torch
            torch.set_float32_matmul_precision("high")
        except ImportError:
            log.error("PyTorch not installed — cannot run TimesFM")
            raise

        # Try TimesFM 2.5 first
        model, version = _try_load_v25(context_len, horizon_len, batch_size)
        if model is None:
            model, version = _try_load_v1(context_len, horizon_len, batch_size)
        if model is None:
            raise RuntimeError("Could not load any TimesFM model")

        _model = model
        _model_version = version
        log.info("Shared TimesFM loaded: {v}", v=version)

        return _model, _model_version


def _try_load_v25(
    context_len: int,
    horizon_len: int,
    batch_size: int,
) -> tuple[Any | None, str]:
    """Load TimesFM 2.5 (200M, 16K context, safetensors)."""
    repo = "google/timesfm-2.5-200m-pytorch"
    try:
        import timesfm
        if not hasattr(timesfm, "TimesFM_2p5_200M_torch"):
            return None, ""

        # Context + horizon must fit within 16384
        max_ctx = min(context_len, 16384 - horizon_len)
        max_hz = min(horizon_len, 1024)

        log.info("Loading TimesFM 2.5 from {repo}...", repo=repo)
        model = timesfm.TimesFM_2p5_200M_torch.from_pretrained(repo)

        config = timesfm.ForecastConfig(
            max_context=max_ctx,
            max_horizon=max_hz,
            per_core_batch_size=batch_size,
        )
        model.compile(config)

        return model, f"v2.5:{repo}"
    except Exception as exc:
        log.warning("TimesFM 2.5 load failed: {e} — trying v1", e=str(exc))
        return None, ""


def _try_load_v1(
    context_len: int,
    horizon_len: int,
    batch_size: int,
) -> tuple[Any | None, str]:
    """Load TimesFM 1.0 (200M, 512 context, ckpt format)."""
    repo = "google/timesfm-1.0-200m-pytorch"
    try:
        import timesfm
        log.info("Loading TimesFM 1.0 from {repo}...", repo=repo)

        hparams = timesfm.TimesFmHparams(
            per_core_batch_size=batch_size,
            horizon_len=horizon_len,
            context_len=context_len,
            input_patch_len=32,
            output_patch_len=128,
            num_layers=20,
            model_dims=1280,
        )
        ckpt = timesfm.TimesFmCheckpoint(huggingface_repo_id=repo)
        model = timesfm.TimesFm(hparams=hparams, checkpoint=ckpt)
        model.load_from_checkpoint(ckpt)

        return model, f"v1:{repo}"
    except Exception as exc:
        log.warning("TimesFM 1.0 load failed: {e}", e=str(exc))
        return None, ""
