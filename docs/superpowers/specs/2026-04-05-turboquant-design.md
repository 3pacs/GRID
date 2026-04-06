# TurboQuant — KV Cache Quantization

**Date:** 2026-04-05
**Status:** Approved
**Scope:** Data-oblivious KV cache quantization for transformer inference compression
**Paper:** arXiv:2504.19874 (Google Research, ICLR 2026)

---

## Problem

Local LLM inference on GRID hardware (z4 AMD RX 580 8GB, grid-svr GPU) is VRAM-constrained. Long context windows blow up KV cache memory. TurboQuant compresses KV cache 5-6x with near-zero quality loss, no calibration data needed.

## Solution

Standalone module `inference/turboquant.py` implementing the TurboQuant algorithm. Config toggle (`TURBOQUANT_ENABLED`) to switch on/off per [[deployment]]. Works on any model's KV cache tensors.

---

## 1. Algorithm

### Pipeline

```
Input: KV tensor [num_heads, seq_len, head_dim] (fp16 or fp32)
  1. Extract norms: store ||v||₂ per vector as fp16
  2. Normalize: v_unit = v / ||v||₂
  3. Rotate: y = Π · v_unit (random orthogonal matrix, cached per head_dim)
  4. Quantize: idx_j = argmin_k |y_j - codebook[k]| per coordinate
  5. Pack: indices into uint8 array
Output: CompressedKV(indices, norms, rotation, codebook, shape, bits)

Decompress:
  1. Unpack indices
  2. Lookup: y_hat_j = codebook[idx_j]
  3. Inverse rotate: v_hat = Π^T · y_hat
  4. Rescale: v_approx = v_hat * norm
Output: tensor [num_heads, seq_len, head_dim] (fp16)
```

### Rotation Matrix

Generated via QR decomposition of random Gaussian matrix. Deterministic given a seed. Cached per unique head_dim value (computed once, reused across all calls with same dimension).

```python
rng = np.random.default_rng(seed=head_dim)  # deterministic per dimension
M = rng.standard_normal((head_dim, head_dim))
Q, R = np.linalg.qr(M)
rotation = Q  # orthogonal matrix
```

### Lloyd-Max Codebook

Precomputed at module load for each bit width (2, 3, 4) by running Lloyd-Max iteration on the Beta-derived distribution for high-dimensional unit sphere coordinates.

The PDF for each coordinate after rotation:
```
f(x) = Γ(d/2) / (√π · Γ((d-1)/2)) · (1 - x²)^((d-3)/2)
```

For high d (typical head_dim=64-128), this converges to N(0, 1/d). The codebook is computed by iterating:
1. Initialize 2^b centroids uniformly in [-1, 1]
2. Update boundaries: b_i = (c_i + c_{i+1}) / 2
3. Recompute centroids as conditional expectations
4. Repeat until convergence (~20 iterations)

Codebooks are a module-level dict: `_CODEBOOKS[bits][head_dim]`.

### Two Modes

**MSE mode** (`TURBOQUANT_MODE="mse"`):
- Uses b bits per coordinate
- Minimizes reconstruction error
- Best for general compression

**Prod mode** (`TURBOQUANT_MODE="prod"`):
- Stage 1: MSE quantizer at (b-1) bits
- Stage 2: QJL (1-bit sign of random projection of residual)
- Unbiased inner products — better attention score accuracy
- Slightly more storage (residual norms)

Default: MSE mode (simpler, sufficient for most use cases).

### Compression Ratios

| Bit width | vs fp16 | vs fp32 |
|-----------|---------|---------|
| 2 bits | 8x | 16x |
| 3 bits | 5.3x | 10.7x |
| 4 bits | 4x | 8x |

Plus ~0.1 bits overhead for norms (fp16 per vector).

---

## 2. Data Structures

```python
@dataclass(frozen=True)
class CompressedKV:
    indices: np.ndarray      # uint8, shape depends on packing
    norms: np.ndarray        # fp16, [num_heads, seq_len]
    codebook: np.ndarray     # fp32, [2^bits] centroids
    rotation_seed: int       # seed to regenerate rotation matrix
    shape: tuple[int, ...]   # original tensor shape
    bits: int                # bit width (2, 3, or 4)
    head_dim: int            # for rotation matrix regeneration
```

Rotation matrix is NOT stored — regenerated from seed (deterministic). This saves significant memory for large head_dim.

---

## 3. Public API

```python
def quantize_kv(
    tensor: np.ndarray,
    bits: int = 3,
    mode: str = "mse",
) -> CompressedKV:
    """Quantize a KV cache tensor.

    Args:
        tensor: shape [num_heads, seq_len, head_dim], fp16 or fp32.
        bits: 2, 3, or 4.
        mode: "mse" or "prod".

    Returns:
        CompressedKV with packed indices + metadata.
    """

def dequantize_kv(compressed: CompressedKV) -> np.ndarray:
    """Decompress a CompressedKV back to a float tensor.

    Returns:
        np.ndarray shape matching original, dtype fp16.
    """

def compression_ratio(compressed: CompressedKV) -> float:
    """Compute actual compression ratio vs fp16."""

def distortion(original: np.ndarray, compressed: CompressedKV) -> dict:
    """Compute MSE and inner-product distortion metrics."""
```

---

## 4. Config

```python
# KV Cache Quantization (TurboQuant — arXiv:2504.19874)
TURBOQUANT_ENABLED: bool = False    # off by default
TURBOQUANT_BITS: int = 3            # 2, 3, or 4
TURBOQUANT_MODE: str = "mse"        # "mse" or "prod"
```

Toggle pattern (used at inference callsite):
```python
if settings.TURBOQUANT_ENABLED:
    kv_cache = quantize_kv(kv_cache, bits=settings.TURBOQUANT_BITS, mode=settings.TURBOQUANT_MODE)
```

---

## 5. File Inventory

| File | Purpose | Est. LoC |
|------|---------|----------|
| `inference/turboquant.py` | Core: codebook, quantize, dequantize, compress, decompress | 300 |
| `tests/test_turboquant.py` | Round-trip accuracy, distortion bounds, compression ratio, edge cases | 150 |
| `config.py` | 3 new settings | 5 |

---

## 6. Constraints

- NumPy + SciPy only (no PyTorch, no CUDA kernels)
- Codebook precomputation < 1 second at module load
- No calibration data — algorithm is data-oblivious
- Immutable CompressedKV (frozen dataclass)
- Rotation matrix regenerated from seed, never stored
- Must handle edge cases: zero vectors, single-token sequences, different dtypes
