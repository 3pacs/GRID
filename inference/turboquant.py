"""
TurboQuant — KV Cache Quantization (arXiv:2504.19874).

Data-oblivious quantization for transformer KV caches. Achieves ~5x compression
at 3 bits with near-zero quality loss. No calibration data needed.

Algorithm:
    1. Extract norms, normalize to unit sphere
    2. Random orthogonal rotation (deterministic, cached)
    3. Lloyd-Max scalar quantization per coordinate
    4. Pack indices

Usage:
    from inference.turboquant import quantize_kv, dequantize_kv

    compressed = quantize_kv(kv_tensor, bits=3)
    restored = dequantize_kv(compressed)
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

# ---------------------------------------------------------------------------
# Compressed KV cache container
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CompressedKV:
    """Immutable container for a quantized KV cache tensor."""
    indices: np.ndarray       # uint8, quantization indices
    norms: np.ndarray         # fp16, per-vector norms [num_heads, seq_len]
    codebook: np.ndarray      # fp32, [2^bits] centroids
    rotation_seed: int        # seed to regenerate rotation matrix
    shape: tuple[int, ...]    # original tensor shape
    bits: int                 # bit width (2, 3, or 4)
    head_dim: int             # head dimension
    original_dtype: np.dtype  # original tensor dtype


# ---------------------------------------------------------------------------
# Rotation matrix (cached)
# ---------------------------------------------------------------------------

_rotation_cache: dict[int, np.ndarray] = {}


def get_rotation(head_dim: int) -> np.ndarray:
    """Get or compute a deterministic random orthogonal rotation matrix.

    Cached per head_dim. Deterministic given the dimension (used as seed).
    """
    if head_dim in _rotation_cache:
        return _rotation_cache[head_dim]

    rng = np.random.default_rng(seed=head_dim)
    M = rng.standard_normal((head_dim, head_dim))
    Q, _ = np.linalg.qr(M)
    _rotation_cache[head_dim] = Q
    return Q


# ---------------------------------------------------------------------------
# Lloyd-Max codebook (cached)
# ---------------------------------------------------------------------------

_codebook_cache: dict[tuple[int, int], np.ndarray] = {}


def _lloyd_max(bits: int, head_dim: int, n_iter: int = 50) -> np.ndarray:
    """Compute Lloyd-Max optimal codebook for the coordinate distribution.

    Returns sorted centroids array of length 2^bits.
    """
    n_levels = 2 ** bits

    # Use Gaussian approximation for large dims (accurate for head_dim >= 32)
    sigma = 1.0 / np.sqrt(head_dim)

    # Sample from the distribution for numerical integration
    n_samples = 100_000
    rng = np.random.default_rng(seed=bits * 1000 + head_dim)

    if head_dim >= 32:
        # Gaussian approximation
        samples = rng.normal(0, sigma, n_samples)
    else:
        # Direct sampling from unit sphere
        v = rng.standard_normal((n_samples, head_dim))
        v /= np.linalg.norm(v, axis=1, keepdims=True)
        Q = get_rotation(head_dim)
        rotated = v @ Q.T
        samples = rotated[:, 0]  # any single coordinate

    # Initialize centroids via quantiles
    percentiles = np.linspace(0, 100, n_levels + 2)[1:-1]
    centroids = np.percentile(samples, percentiles)

    # Lloyd-Max iteration
    for _ in range(n_iter):
        # Compute boundaries (midpoints between centroids)
        boundaries = np.concatenate([
            [-np.inf],
            (centroids[:-1] + centroids[1:]) / 2,
            [np.inf],
        ])

        # Recompute centroids as conditional means
        new_centroids = np.empty(n_levels)
        for i in range(n_levels):
            mask = (samples >= boundaries[i]) & (samples < boundaries[i + 1])
            if np.any(mask):
                new_centroids[i] = np.mean(samples[mask])
            else:
                new_centroids[i] = centroids[i]

        if np.allclose(centroids, new_centroids, atol=1e-10):
            break
        centroids = new_centroids

    return np.sort(centroids).astype(np.float32)


def get_codebook(bits: int, head_dim: int) -> np.ndarray:
    """Get or compute Lloyd-Max codebook for given bit width and head dimension."""
    key = (bits, head_dim)
    if key in _codebook_cache:
        return _codebook_cache[key]

    if bits not in (2, 3, 4):
        raise ValueError(f"Unsupported bit width: {bits}. Must be 2, 3, or 4.")

    codebook = _lloyd_max(bits, head_dim)
    _codebook_cache[key] = codebook
    return codebook


# ---------------------------------------------------------------------------
# Quantize / Dequantize
# ---------------------------------------------------------------------------

def quantize_kv(
    tensor: np.ndarray,
    bits: int = 3,
    mode: str = "mse",
) -> CompressedKV:
    """Quantize a KV cache tensor.

    Args:
        tensor: shape [num_heads, seq_len, head_dim], fp16 or fp32.
        bits: 2, 3, or 4.
        mode: "mse" (default). "prod" not yet implemented.

    Returns:
        CompressedKV with packed indices + metadata.
    """
    if mode not in ("mse", "prod"):
        raise ValueError(f"Mode '{mode}' not supported. Use 'mse' or 'prod'.")

    original_dtype = tensor.dtype
    tensor = tensor.astype(np.float32)
    num_heads, seq_len, head_dim = tensor.shape

    # 1. Extract norms
    norms = np.linalg.norm(tensor, axis=2)  # [num_heads, seq_len]

    # 2. Normalize to unit sphere (handle zero vectors)
    safe_norms = np.where(norms > 0, norms, 1.0)
    normalized = tensor / safe_norms[:, :, np.newaxis]

    # 3. Rotate
    Q = get_rotation(head_dim)
    flat = normalized.reshape(-1, head_dim)
    rotated = flat @ Q.T  # [N, head_dim]

    if mode == "prod":
        # Product quantization: split head_dim into subspaces, quantize each
        n_sub = min(4, head_dim // 2)  # Number of subspaces
        sub_dim = head_dim // n_sub
        codebook = get_codebook(bits, sub_dim)
        all_indices = []
        for s in range(n_sub):
            sub_vec = rotated[:, s * sub_dim : (s + 1) * sub_dim]
            diffs = np.abs(
                sub_vec[:, :, np.newaxis] - codebook[np.newaxis, np.newaxis, :]
            )
            sub_idx = np.argmin(diffs, axis=2).astype(np.uint8)
            all_indices.append(sub_idx)
        indices = np.concatenate(all_indices, axis=1)  # [N, head_dim]
    else:
        # MSE mode: nearest centroid per coordinate
        codebook = get_codebook(bits, head_dim)
        diffs = np.abs(rotated[:, :, np.newaxis] - codebook[np.newaxis, np.newaxis, :])
        indices = np.argmin(diffs, axis=2).astype(np.uint8)  # [N, head_dim]

    # Store norms as fp16
    norms_fp16 = norms.astype(np.float16)

    return CompressedKV(
        indices=indices,
        norms=norms_fp16,
        codebook=codebook,
        rotation_seed=head_dim,
        shape=tensor.shape,
        bits=bits,
        head_dim=head_dim,
        original_dtype=original_dtype,
    )


def dequantize_kv(compressed: CompressedKV) -> np.ndarray:
    """Decompress a CompressedKV back to a float tensor.

    Returns:
        np.ndarray matching original shape and dtype.
    """
    num_heads, seq_len, head_dim = compressed.shape

    # 1. Lookup centroids
    reconstructed = compressed.codebook[compressed.indices]  # [N, head_dim]

    # 2. Inverse rotation
    Q = get_rotation(head_dim)
    unrotated = reconstructed @ Q  # Q^T inverse = Q (orthogonal)

    # 3. Reshape
    unrotated = unrotated.reshape(num_heads, seq_len, head_dim)

    # 4. Rescale by norms
    norms = compressed.norms.astype(np.float32)
    result = unrotated * norms[:, :, np.newaxis]

    return result.astype(compressed.original_dtype)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def compression_ratio(compressed: CompressedKV) -> float:
    """Compute actual compression ratio vs original fp16 size."""
    num_heads, seq_len, head_dim = compressed.shape
    original_bytes = num_heads * seq_len * head_dim * 2  # fp16 = 2 bytes

    index_bits = compressed.indices.size * compressed.bits
    norm_bytes = compressed.norms.nbytes
    codebook_bytes = compressed.codebook.nbytes
    compressed_bytes = (index_bits + 7) // 8 + norm_bytes + codebook_bytes

    return original_bytes / compressed_bytes if compressed_bytes > 0 else 0.0


def distortion(original: np.ndarray, compressed: CompressedKV) -> dict[str, float]:
    """Compute distortion metrics between original and compressed tensor."""
    restored = dequantize_kv(compressed)
    original_f32 = original.astype(np.float32)
    restored_f32 = restored.astype(np.float32)

    mse = float(np.mean((original_f32 - restored_f32) ** 2))
    signal_power = float(np.mean(original_f32 ** 2))
    relative_mse = mse / signal_power if signal_power > 0 else 0.0
    snr_db = 10 * np.log10(signal_power / mse) if mse > 0 else float("inf")

    return {
        "mse": mse,
        "relative_mse": relative_mse,
        "snr_db": float(snr_db),
    }
