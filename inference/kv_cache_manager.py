"""
KV Cache Manager — transparent compress/decompress lifecycle for TurboQuant.

Wraps the TurboQuant quantize/dequantize API and adds metrics logging,
so the rest of the inference pipeline sees the same interface.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np
from loguru import logger as log


@dataclass
class CacheMetrics:
    """Accumulated metrics for a KV cache manager session."""
    total_quantize_calls: int = 0
    total_dequantize_calls: int = 0
    total_quantize_ms: float = 0.0
    total_dequantize_ms: float = 0.0
    total_original_bytes: int = 0
    total_compressed_bytes: int = 0
    avg_compression_ratio: float = 0.0
    avg_snr_db: float = 0.0
    _ratios: list[float] = field(default_factory=list, repr=False)
    _snrs: list[float] = field(default_factory=list, repr=False)

    def record_quantize(self, elapsed_ms: float, ratio: float, snr_db: float,
                        orig_bytes: int, comp_bytes: int) -> None:
        self.total_quantize_calls += 1
        self.total_quantize_ms += elapsed_ms
        self.total_original_bytes += orig_bytes
        self.total_compressed_bytes += comp_bytes
        self._ratios.append(ratio)
        self._snrs.append(snr_db)
        self.avg_compression_ratio = sum(self._ratios) / len(self._ratios)
        self.avg_snr_db = sum(self._snrs) / len(self._snrs)

    def record_dequantize(self, elapsed_ms: float) -> None:
        self.total_dequantize_calls += 1
        self.total_dequantize_ms += elapsed_ms

    def summary(self) -> dict[str, Any]:
        return {
            "quantize_calls": self.total_quantize_calls,
            "dequantize_calls": self.total_dequantize_calls,
            "quantize_ms": round(self.total_quantize_ms, 2),
            "dequantize_ms": round(self.total_dequantize_ms, 2),
            "avg_compression_ratio": round(self.avg_compression_ratio, 2),
            "avg_snr_db": round(self.avg_snr_db, 2),
            "original_MB": round(self.total_original_bytes / 1e6, 2),
            "compressed_MB": round(self.total_compressed_bytes / 1e6, 2),
        }


class KVCacheManager:
    """Manages KV cache compression using TurboQuant.

    Stores compressed caches keyed by layer index, and transparently
    compresses on store / decompresses on retrieve.

    Usage:
        mgr = KVCacheManager(bits=3)
        mgr.store(layer_idx=0, kv_tensor=tensor)
        restored = mgr.retrieve(layer_idx=0)
    """

    def __init__(self, bits: int = 3, mode: str = "mse", enabled: bool = True) -> None:
        self.bits = bits
        self.mode = mode
        self.enabled = enabled
        self._cache: dict[int, Any] = {}  # layer_idx -> CompressedKV
        self.metrics = CacheMetrics()

        if enabled:
            log.info("KVCacheManager active — bits={b}, mode={m}", b=bits, m=mode)
        else:
            log.debug("KVCacheManager disabled — passthrough mode")

    def store(self, layer_idx: int, kv_tensor: np.ndarray) -> None:
        """Compress and store a KV cache tensor for a layer.

        Args:
            layer_idx: Transformer layer index.
            kv_tensor: Shape [num_heads, seq_len, head_dim], fp16 or fp32.
        """
        if not self.enabled:
            self._cache[layer_idx] = kv_tensor
            return

        from inference.turboquant import quantize_kv, compression_ratio, distortion

        start = time.monotonic()
        compressed = quantize_kv(kv_tensor, bits=self.bits, mode=self.mode)
        elapsed_ms = (time.monotonic() - start) * 1000

        ratio = compression_ratio(compressed)
        dist = distortion(kv_tensor, compressed)

        orig_bytes = kv_tensor.nbytes
        index_bits = compressed.indices.size * compressed.bits
        comp_bytes = (index_bits + 7) // 8 + compressed.norms.nbytes + compressed.codebook.nbytes

        self.metrics.record_quantize(elapsed_ms, ratio, dist["snr_db"], orig_bytes, comp_bytes)
        self._cache[layer_idx] = compressed

        log.debug(
            "KV cache layer {l}: {r:.1f}x compression, SNR={s:.1f}dB, {t:.1f}ms",
            l=layer_idx, r=ratio, s=dist["snr_db"], t=elapsed_ms,
        )

    def retrieve(self, layer_idx: int) -> np.ndarray | None:
        """Decompress and return a KV cache tensor for a layer.

        Args:
            layer_idx: Transformer layer index.

        Returns:
            Decompressed tensor, or None if layer not cached.
        """
        cached = self._cache.get(layer_idx)
        if cached is None:
            return None

        if not self.enabled:
            return cached

        from inference.turboquant import dequantize_kv

        start = time.monotonic()
        restored = dequantize_kv(cached)
        elapsed_ms = (time.monotonic() - start) * 1000

        self.metrics.record_dequantize(elapsed_ms)
        return restored

    def clear(self) -> None:
        """Clear all cached layers."""
        self._cache.clear()

    def layer_count(self) -> int:
        """Number of layers currently cached."""
        return len(self._cache)

    def get_metrics(self) -> dict[str, Any]:
        """Return accumulated metrics summary."""
        return self.metrics.summary()
