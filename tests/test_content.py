"""Tests for the content generation pipeline.

These tests mock heavy dependencies (torch, diffusers, kokoro) so they
run fast without GPU or model downloads.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Ensure grid root is importable
# ---------------------------------------------------------------------------
GRID_ROOT = Path(__file__).resolve().parent.parent
if str(GRID_ROOT) not in sys.path:
    sys.path.insert(0, str(GRID_ROOT))


# ===================================================================
# image_gen tests
# ===================================================================


class TestImageGenerator:
    """Tests for content.image_gen.ImageGenerator."""

    def test_available_models(self):
        from content.image_gen import ImageGenerator

        models = ImageGenerator.available_models()
        assert "flux-schnell" in models
        assert "flux-dev" in models
        assert "sdxl" in models

    def test_unknown_model_raises(self):
        from content.image_gen import ImageGenerator

        gen = ImageGenerator(model_name="nonexistent-model")
        # Mock torch so _ensure_imports doesn't fail on missing package
        mock_torch = MagicMock()
        mock_torch.cuda.is_available.return_value = False
        with patch.dict("content.image_gen.__dict__", {"_torch": mock_torch}):
            with pytest.raises(ValueError, match="Unknown model"):
                gen.load_model()

    def test_default_model_is_flux_schnell(self):
        from content.image_gen import ImageGenerator, DEFAULT_MODEL

        gen = ImageGenerator()
        assert gen.model_name == DEFAULT_MODEL
        assert gen.model_name == "flux-schnell"

    def test_models_have_required_keys(self):
        from content.image_gen import MODELS

        required_keys = {"repo", "pipeline", "default_steps", "dtype", "license"}
        for name, spec in MODELS.items():
            missing = required_keys - set(spec.keys())
            assert not missing, f"Model '{name}' missing keys: {missing}"

    def test_generate_calls_pipeline(self, tmp_path):
        """Verify generate() calls the pipeline and returns an image."""
        from content.image_gen import ImageGenerator

        mock_image = MagicMock()
        mock_image.save = MagicMock()

        mock_pipe = MagicMock()
        mock_pipe.return_value = SimpleNamespace(images=[mock_image])

        gen = ImageGenerator(
            model_name="flux-schnell",
            device="cpu",
            output_dir=str(tmp_path),
        )
        gen._pipe = mock_pipe
        gen._loaded = True

        # Patch torch for the generator
        with patch.dict("content.image_gen.__dict__", {"_torch": MagicMock()}):
            result = gen.generate("test prompt", save=False)

        assert result is mock_image
        mock_pipe.assert_called_once()

    def test_is_loaded_property(self):
        from content.image_gen import ImageGenerator

        gen = ImageGenerator()
        assert gen.is_loaded is False
        gen._loaded = True
        assert gen.is_loaded is True


# ===================================================================
# video_gen tests
# ===================================================================


class TestVideoGenerator:
    """Tests for content.video_gen.VideoGenerator."""

    def test_available_models(self):
        from content.video_gen import VideoGenerator

        models = VideoGenerator.available_models()
        assert "wan2.1-t2v-14b" in models
        assert "wan2.1-t2v-1.3b" in models
        assert "cogvideox-5b" in models

    def test_unknown_model_raises(self):
        from content.video_gen import VideoGenerator

        gen = VideoGenerator(model_name="nonexistent")
        mock_torch = MagicMock()
        mock_torch.cuda.is_available.return_value = False
        with patch.dict("content.video_gen.__dict__", {"_torch": mock_torch}):
            with pytest.raises(ValueError, match="Unknown model"):
                gen.load_model()

    def test_default_model(self):
        from content.video_gen import VideoGenerator, DEFAULT_MODEL

        gen = VideoGenerator()
        assert gen.model_name == DEFAULT_MODEL
        assert gen.model_name == "wan2.1-t2v-1.3b"

    def test_models_have_required_keys(self):
        from content.video_gen import MODELS

        required_keys = {"repo", "pipeline", "mode", "default_steps", "dtype", "vram_gb", "license"}
        for name, spec in MODELS.items():
            missing = required_keys - set(spec.keys())
            assert not missing, f"Model '{name}' missing keys: {missing}"

    def test_save_video_creates_frames_fallback(self, tmp_path):
        """When export_to_video is None, save individual frames."""
        from content.video_gen import VideoGenerator

        gen = VideoGenerator(output_dir=str(tmp_path))

        mock_frame = MagicMock()
        mock_frame.save = MagicMock()

        with patch("content.video_gen._export_to_video", None):
            result = gen.save_video([mock_frame, mock_frame], prompt="test")

        assert result.exists()
        assert mock_frame.save.call_count == 2


# ===================================================================
# tts tests
# ===================================================================


class TestNarrator:
    """Tests for content.tts.Narrator."""

    def test_available_models(self):
        from content.tts import Narrator

        models = Narrator.available_models()
        assert "kokoro" in models
        assert "xtts-v2" in models

    def test_available_voices(self):
        from content.tts import Narrator

        voices = Narrator.available_voices()
        assert "kokoro" in voices
        assert len(voices["kokoro"]) > 0

    def test_unknown_model_raises(self):
        from content.tts import Narrator

        narrator = Narrator(model_name="nonexistent")
        with pytest.raises(ValueError, match="Unknown model"):
            narrator.load_model()

    def test_default_model_is_kokoro(self):
        from content.tts import Narrator, DEFAULT_MODEL

        narrator = Narrator()
        assert narrator.model_name == DEFAULT_MODEL
        assert narrator.model_name == "kokoro"

    def test_split_text(self):
        from content.tts import Narrator

        text = "First sentence. Second sentence. Third sentence. Fourth one."
        chunks = Narrator._split_text(text, max_chars=40)
        assert len(chunks) >= 2
        # All text should be preserved
        reassembled = " ".join(chunks)
        assert "First" in reassembled
        assert "Fourth" in reassembled

    def test_split_text_single_chunk(self):
        from content.tts import Narrator

        text = "Short text."
        chunks = Narrator._split_text(text, max_chars=500)
        assert len(chunks) == 1
        assert chunks[0] == text


# ===================================================================
# lipsync tests
# ===================================================================


class TestLipSyncDriver:
    """Tests for content.lipsync.LipSyncDriver."""

    def test_available_backends(self):
        from content.lipsync import LipSyncDriver

        backends = LipSyncDriver.available_backends()
        assert "sadtalker" in backends
        assert "liveportrait" in backends
        assert "wav2lip" in backends

    def test_unknown_backend_raises(self):
        from content.lipsync import LipSyncDriver

        driver = LipSyncDriver(backend="nonexistent")
        with pytest.raises(ValueError, match="Unknown backend"):
            driver.load_model()

    def test_check_installation(self):
        from content.lipsync import LipSyncDriver

        status = LipSyncDriver.check_installation()
        assert isinstance(status, dict)
        assert "sadtalker" in status
        assert isinstance(status["sadtalker"], bool)

    def test_generate_without_setup_raises(self, tmp_path):
        from content.lipsync import LipSyncDriver

        driver = LipSyncDriver(backend="sadtalker")
        # Not installed → should raise
        with pytest.raises(RuntimeError, match="not installed"):
            driver.generate(
                portrait=tmp_path / "face.png",
                audio=tmp_path / "audio.wav",
            )

    def test_generate_missing_portrait_raises(self, tmp_path):
        from content.lipsync import LipSyncDriver

        driver = LipSyncDriver(backend="sadtalker")
        driver._ready = True
        driver._backend_path = tmp_path

        with pytest.raises(FileNotFoundError, match="Portrait not found"):
            driver.generate(
                portrait=tmp_path / "nonexistent.png",
                audio=tmp_path / "audio.wav",
            )


# ===================================================================
# pipeline tests
# ===================================================================


class TestContentPipeline:
    """Tests for content.pipeline.ContentPipeline."""

    def test_default_config(self):
        from content.pipeline import ContentPipeline

        pipe = ContentPipeline()
        assert pipe.image_model == "flux-schnell"
        assert pipe.video_model == "wan2.1-t2v-1.3b"
        assert pipe.tts_model == "kokoro"
        assert pipe.lipsync_backend == "sadtalker"
        assert pipe.auto_unload is True

    def test_style_prompt(self):
        from content.pipeline import ContentPipeline

        prompt = ContentPipeline._style_prompt("bull run", "dark_trading")
        assert "bull run" in prompt
        assert "neon" in prompt.lower() or "dark" in prompt.lower()

    def test_style_prompt_unknown_style(self):
        from content.pipeline import ContentPipeline

        prompt = ContentPipeline._style_prompt("test", "unknown_style")
        assert prompt == "test"

    def test_content_result_immutable(self):
        from content.pipeline import ContentResult

        result = ContentResult(elapsed_seconds=1.5)
        with pytest.raises(AttributeError):
            result.elapsed_seconds = 2.0  # type: ignore[misc]

    def test_content_result_defaults(self):
        from content.pipeline import ContentResult

        result = ContentResult()
        assert result.image_path is None
        assert result.audio_path is None
        assert result.video_path is None
        assert result.elapsed_seconds == 0.0
        assert result.metadata == {}

    def test_status(self):
        from content.pipeline import ContentPipeline

        pipe = ContentPipeline()
        status = pipe.status()
        assert status["image_model"] == "flux-schnell"
        assert "lipsync_installed" in status


# ===================================================================
# config integration test
# ===================================================================


class TestContentConfig:
    """Verify content settings are present in config.py."""

    def test_content_settings_exist(self):
        from config import Settings

        fields = Settings.model_fields
        assert "CONTENT_ENABLED" in fields
        assert "CONTENT_IMAGE_MODEL" in fields
        assert "CONTENT_VIDEO_MODEL" in fields
        assert "CONTENT_TTS_MODEL" in fields
        assert "CONTENT_LIPSYNC_BACKEND" in fields
        assert "CONTENT_DEVICE" in fields

    def test_content_defaults(self):
        from config import settings

        assert settings.CONTENT_ENABLED is True
        assert settings.CONTENT_IMAGE_MODEL == "flux-schnell"
        assert settings.CONTENT_TTS_MODEL == "kokoro"
