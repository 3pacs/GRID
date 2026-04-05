"""Tests for TurboQuant KV cache quantization."""

from __future__ import annotations

import numpy as np
import pytest


class TestCodebook:
    def test_codebook_has_correct_size(self):
        from inference.turboquant import get_codebook
        for bits in (2, 3, 4):
            cb = get_codebook(bits, head_dim=64)
            assert len(cb) == 2 ** bits

    def test_codebook_centroids_in_range(self):
        from inference.turboquant import get_codebook
        cb = get_codebook(3, head_dim=64)
        assert np.all(cb >= -1.0)
        assert np.all(cb <= 1.0)

    def test_codebook_is_sorted(self):
        from inference.turboquant import get_codebook
        cb = get_codebook(3, head_dim=64)
        assert np.all(np.diff(cb) > 0)

    def test_codebook_cached(self):
        from inference.turboquant import get_codebook
        cb1 = get_codebook(3, head_dim=64)
        cb2 = get_codebook(3, head_dim=64)
        assert cb1 is cb2  # same object, not recomputed


class TestRotation:
    def test_rotation_is_orthogonal(self):
        from inference.turboquant import get_rotation
        Q = get_rotation(64)
        eye = np.eye(64)
        np.testing.assert_allclose(Q @ Q.T, eye, atol=1e-10)

    def test_rotation_deterministic(self):
        from inference.turboquant import get_rotation
        Q1 = get_rotation(64)
        Q2 = get_rotation(64)
        np.testing.assert_array_equal(Q1, Q2)

    def test_different_dims_different_rotations(self):
        from inference.turboquant import get_rotation
        Q64 = get_rotation(64)
        Q128 = get_rotation(128)
        assert Q64.shape != Q128.shape


class TestQuantizeDequantize:
    def test_round_trip_shape_preserved(self):
        from inference.turboquant import quantize_kv, dequantize_kv
        tensor = np.random.randn(4, 16, 64).astype(np.float32)
        compressed = quantize_kv(tensor, bits=3)
        restored = dequantize_kv(compressed)
        assert restored.shape == tensor.shape

    def test_round_trip_low_distortion(self):
        from inference.turboquant import quantize_kv, dequantize_kv
        rng = np.random.default_rng(42)
        tensor = rng.standard_normal((4, 32, 64)).astype(np.float32)
        compressed = quantize_kv(tensor, bits=3)
        restored = dequantize_kv(compressed)
        # Relative MSE should be small for 3-bit
        mse = np.mean((tensor - restored) ** 2)
        signal_power = np.mean(tensor ** 2)
        relative_mse = mse / signal_power
        assert relative_mse < 0.15, f"Relative MSE {relative_mse:.4f} too high for 3-bit"

    def test_4bit_better_than_3bit(self):
        from inference.turboquant import quantize_kv, dequantize_kv
        rng = np.random.default_rng(42)
        tensor = rng.standard_normal((4, 32, 64)).astype(np.float32)
        c3 = quantize_kv(tensor, bits=3)
        c4 = quantize_kv(tensor, bits=4)
        r3 = dequantize_kv(c3)
        r4 = dequantize_kv(c4)
        mse3 = np.mean((tensor - r3) ** 2)
        mse4 = np.mean((tensor - r4) ** 2)
        assert mse4 < mse3, "4-bit should have lower distortion than 3-bit"

    def test_zero_vectors_handled(self):
        from inference.turboquant import quantize_kv, dequantize_kv
        tensor = np.zeros((2, 4, 64), dtype=np.float32)
        compressed = quantize_kv(tensor, bits=3)
        restored = dequantize_kv(compressed)
        np.testing.assert_allclose(restored, 0.0, atol=1e-6)

    def test_single_token(self):
        from inference.turboquant import quantize_kv, dequantize_kv
        tensor = np.random.randn(4, 1, 64).astype(np.float32)
        compressed = quantize_kv(tensor, bits=3)
        restored = dequantize_kv(compressed)
        assert restored.shape == (4, 1, 64)

    def test_fp16_input(self):
        from inference.turboquant import quantize_kv, dequantize_kv
        tensor = np.random.randn(2, 8, 64).astype(np.float16)
        compressed = quantize_kv(tensor, bits=3)
        restored = dequantize_kv(compressed)
        assert restored.dtype == np.float16


class TestCompressedKV:
    def test_immutable(self):
        from inference.turboquant import quantize_kv
        tensor = np.random.randn(2, 4, 64).astype(np.float32)
        compressed = quantize_kv(tensor, bits=3)
        with pytest.raises(AttributeError):
            compressed.bits = 4

    def test_stores_metadata(self):
        from inference.turboquant import quantize_kv
        tensor = np.random.randn(2, 4, 64).astype(np.float32)
        compressed = quantize_kv(tensor, bits=3)
        assert compressed.bits == 3
        assert compressed.shape == (2, 4, 64)
        assert compressed.head_dim == 64


class TestCompressionRatio:
    def test_3bit_compression(self):
        from inference.turboquant import quantize_kv, compression_ratio
        tensor = np.random.randn(4, 128, 64).astype(np.float16)
        compressed = quantize_kv(tensor, bits=3)
        ratio = compression_ratio(compressed)
        # 3 bits vs 16 bits = ~5.3x, minus overhead
        assert ratio > 3.0, f"Compression ratio {ratio:.1f}x too low"
        assert ratio < 8.0, f"Compression ratio {ratio:.1f}x unexpectedly high"


class TestDistortion:
    def test_distortion_returns_metrics(self):
        from inference.turboquant import quantize_kv, distortion
        tensor = np.random.randn(2, 8, 64).astype(np.float32)
        compressed = quantize_kv(tensor, bits=3)
        metrics = distortion(tensor, compressed)
        assert "mse" in metrics
        assert "relative_mse" in metrics
        assert "snr_db" in metrics
        assert metrics["mse"] >= 0
        assert metrics["snr_db"] > 0  # signal should be stronger than noise
