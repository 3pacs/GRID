"""Text-to-speech narration via locally-hosted models.

Primary:  Kokoro (82M params, Apache 2.0, CPU-friendly)
Fallback: XTTS v2 (Coqui TTS, voice cloning capable)

Usage::

    narrator = Narrator()
    narrator.load_model()
    narrator.speak("The market just broke through resistance.", "output.wav")

    # With voice cloning (XTTS only)
    narrator = Narrator(model_name="xtts-v2")
    narrator.load_model()
    narrator.speak(
        "Breaking: Fed holds rates steady.",
        "output.wav",
        reference_audio="my_voice.wav",
    )
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loguru import logger as log

# Lazy imports
_kokoro: Any = None
_TTS: Any = None
_np: Any = None
_sf: Any = None


def _ensure_numpy() -> None:
    global _np, _sf
    if _np is not None:
        return
    import numpy as np

    _np = np
    try:
        import soundfile as sf

        _sf = sf
    except ImportError:
        _sf = None


# ---------------------------------------------------------------------------
# Supported TTS models
# ---------------------------------------------------------------------------
MODELS: dict[str, dict[str, Any]] = {
    "kokoro": {
        "package": "kokoro-onnx",
        "engine": "kokoro",
        "supports_cloning": False,
        "cpu_friendly": True,
        "license": "Apache-2.0",
    },
    "xtts-v2": {
        "package": "TTS",
        "engine": "coqui",
        "repo": "tts_models/multilingual/multi-dataset/xtts_v2",
        "supports_cloning": True,
        "cpu_friendly": False,
        "license": "MPL-2.0",
    },
}

DEFAULT_MODEL = "kokoro"


@dataclass
class Narrator:
    """Self-hosted text-to-speech engine.

    Parameters:
        model_name: Key from ``MODELS`` dict.
        output_dir: Where generated audio files are saved.
        voice:      Voice preset name (model-dependent).
        language:   Language code (e.g., ``"en"``, ``"ja"``).
        speed:      Speech speed multiplier (1.0 = normal).
    """

    model_name: str = DEFAULT_MODEL
    output_dir: str = "outputs/content/audio"
    voice: str = "af_heart"
    language: str = "en"
    speed: float = 1.0
    _engine: Any = field(default=None, repr=False, init=False)
    _loaded: bool = field(default=False, repr=False, init=False)

    def load_model(self) -> None:
        """Load the TTS model."""
        if self._loaded:
            return

        spec = MODELS.get(self.model_name)
        if spec is None:
            raise ValueError(
                f"Unknown model '{self.model_name}'. "
                f"Available: {list(MODELS.keys())}"
            )

        log.info("Loading TTS model: {model}", model=self.model_name)
        t0 = time.monotonic()

        if spec["engine"] == "kokoro":
            self._load_kokoro()
        elif spec["engine"] == "coqui":
            self._load_coqui(spec)

        elapsed = time.monotonic() - t0
        self._loaded = True
        log.info("TTS model loaded in {t:.1f}s", t=elapsed)

    def _load_kokoro(self) -> None:
        """Load Kokoro ONNX model."""
        global _kokoro
        try:
            import kokoro_onnx

            _kokoro = kokoro_onnx
        except ImportError:
            raise ImportError(
                "Kokoro not available. Install: pip install kokoro-onnx"
            )
        self._engine = _kokoro.Kokoro("kokoro-v1.0.onnx", "voices-v1.0.bin")

    def _load_coqui(self, spec: dict[str, Any]) -> None:
        """Load Coqui TTS (XTTS v2) model."""
        global _TTS
        try:
            from TTS.api import TTS

            _TTS = TTS
        except ImportError:
            raise ImportError(
                "Coqui TTS not available. Install: pip install TTS"
            )
        self._engine = _TTS(model_name=spec["repo"])

    def speak(
        self,
        text: str,
        output_path: str | None = None,
        *,
        reference_audio: str | None = None,
        voice: str | None = None,
        speed: float | None = None,
    ) -> Path:
        """Synthesize speech from text.

        Args:
            text:            The text to speak.
            output_path:     Where to save the audio. Auto-generated if None.
            reference_audio: Path to reference audio for voice cloning (XTTS only).
            voice:           Override voice preset.
            speed:           Override speech speed.

        Returns:
            Path to the saved audio file.
        """
        self.load_model()
        _ensure_numpy()

        spec = MODELS[self.model_name]
        voice_id = voice or self.voice
        spd = speed or self.speed

        if output_path is None:
            out_dir = Path(self.output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            slug = "".join(
                c if c.isalnum() or c in " -_" else "" for c in text[:40]
            )
            slug = slug.strip().replace(" ", "_")
            ts = int(time.time())
            output_path = str(out_dir / f"{ts}_{slug}.wav")

        log.info("Synthesizing: {text:.80s}", text=text)
        t0 = time.monotonic()

        if spec["engine"] == "kokoro":
            self._speak_kokoro(text, output_path, voice_id, spd)
        elif spec["engine"] == "coqui":
            self._speak_coqui(text, output_path, reference_audio, spd)

        elapsed = time.monotonic() - t0
        log.info("TTS done in {t:.1f}s → {path}", t=elapsed, path=output_path)
        return Path(output_path)

    def _speak_kokoro(
        self, text: str, output_path: str, voice: str, speed: float
    ) -> None:
        """Generate audio with Kokoro."""
        samples, sample_rate = self._engine.create(
            text, voice=voice, speed=speed, lang=self.language
        )
        if _sf is not None:
            _sf.write(output_path, samples, sample_rate)
        else:
            # Fallback: save as raw numpy
            _ensure_numpy()
            _np.save(output_path.replace(".wav", ".npy"), samples)
            log.warning(
                "soundfile not installed — saved raw numpy array. "
                "Install: pip install soundfile"
            )

    def _speak_coqui(
        self,
        text: str,
        output_path: str,
        reference_audio: str | None,
        speed: float,
    ) -> None:
        """Generate audio with Coqui TTS (XTTS v2)."""
        kwargs: dict[str, Any] = {
            "text": text,
            "file_path": output_path,
            "language": self.language,
            "speed": speed,
        }
        if reference_audio:
            kwargs["speaker_wav"] = reference_audio
        self._engine.tts_to_file(**kwargs)

    def speak_long(
        self,
        text: str,
        output_path: str | None = None,
        *,
        max_chars_per_chunk: int = 500,
        **kwargs: Any,
    ) -> Path:
        """Synthesize long text by chunking into sentences.

        Splits text at sentence boundaries and concatenates the audio.

        Args:
            text:                Full text to narrate.
            output_path:         Where to save the final audio.
            max_chars_per_chunk: Max characters per TTS call.
            **kwargs:            Passed to ``speak()``.

        Returns:
            Path to the concatenated audio file.
        """
        self.load_model()
        _ensure_numpy()

        chunks = self._split_text(text, max_chars_per_chunk)
        log.info("Long text: {n} chunks, {c} chars total", n=len(chunks), c=len(text))

        if output_path is None:
            out_dir = Path(self.output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            ts = int(time.time())
            output_path = str(out_dir / f"{ts}_long_narration.wav")

        all_samples: list[Any] = []
        sample_rate = 24000  # default

        for i, chunk in enumerate(chunks):
            tmp_path = output_path.replace(".wav", f"_chunk{i}.wav")
            self.speak(chunk, tmp_path, **kwargs)

            if _sf is not None:
                data, sr = _sf.read(tmp_path)
                all_samples.append(data)
                sample_rate = sr
                Path(tmp_path).unlink(missing_ok=True)

        if all_samples and _sf is not None:
            combined = _np.concatenate(all_samples)
            _sf.write(output_path, combined, sample_rate)
            log.info("Combined {n} chunks → {path}", n=len(chunks), path=output_path)
        else:
            log.warning("Could not concatenate — individual chunks saved separately")

        return Path(output_path)

    @staticmethod
    def _split_text(text: str, max_chars: int) -> list[str]:
        """Split text at sentence boundaries respecting max length."""
        import re

        sentences = re.split(r"(?<=[.!?])\s+", text)
        chunks: list[str] = []
        current: list[str] = []
        current_len = 0

        for sent in sentences:
            if current_len + len(sent) > max_chars and current:
                chunks.append(" ".join(current))
                current = []
                current_len = 0
            current.append(sent)
            current_len += len(sent) + 1

        if current:
            chunks.append(" ".join(current))

        return chunks

    def unload(self) -> None:
        """Free resources."""
        if self._engine is not None:
            del self._engine
            self._engine = None
            self._loaded = False
            log.info("TTS model unloaded")

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    @staticmethod
    def available_models() -> list[str]:
        return list(MODELS.keys())

    @staticmethod
    def available_voices() -> dict[str, list[str]]:
        """Return known voice presets per model."""
        return {
            "kokoro": [
                "af_heart", "af_bella", "af_nicole", "af_sarah", "af_sky",
                "am_adam", "am_michael",
                "bf_emma", "bf_isabella",
                "bm_george", "bm_lewis",
            ],
            "xtts-v2": ["(uses reference audio for voice cloning)"],
        }
