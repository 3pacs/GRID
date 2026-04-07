"""Content pipeline orchestrator.

Chains together image generation, video generation, TTS narration, and
lip-sync into reusable content workflows.

Usage::

    pipe = ContentPipeline()

    # Market briefing video: TTS + talking head
    pipe.market_briefing(
        text="The S&P broke through 5800 resistance today...",
        portrait="presenter.png",
        output="briefing.mp4",
    )

    # Social media graphic
    pipe.social_image(
        text="GRID Oracle: 78% probability of rate cut in September",
        style="dark_trading",
    )

    # Full content package: image + narration + talking head
    result = pipe.full_package(
        headline="Fed Holds Rates Steady",
        body="The Federal Reserve held rates at...",
        portrait="presenter.png",
    )
    # result.image, result.audio, result.video
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loguru import logger as log


@dataclass(frozen=True)
class ContentResult:
    """Immutable result from a pipeline run."""

    image_path: Path | None = None
    audio_path: Path | None = None
    video_path: Path | None = None
    elapsed_seconds: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ContentPipeline:
    """Orchestrates multi-step content generation workflows.

    Each component (image, video, TTS, lipsync) is loaded lazily on first
    use and unloaded between heavy steps to conserve VRAM.

    Parameters:
        output_dir:   Root output directory.
        image_model:  Image model key (see ``image_gen.MODELS``).
        video_model:  Video model key (see ``video_gen.MODELS``).
        tts_model:    TTS model key (see ``tts.MODELS``).
        lipsync_backend: Lip-sync backend key (see ``lipsync.BACKENDS``).
        device:       Device override (``"auto"`` to auto-detect).
        auto_unload:  Unload models between pipeline steps to save VRAM.
    """

    output_dir: str = "outputs/content"
    image_model: str = "flux-schnell"
    video_model: str = "wan2.1-t2v-1.3b"
    tts_model: str = "kokoro"
    lipsync_backend: str = "sadtalker"
    device: str = "auto"
    auto_unload: bool = True

    def _get_image_gen(self) -> Any:
        from content.image_gen import ImageGenerator

        return ImageGenerator(
            model_name=self.image_model,
            device=self.device,
            output_dir=f"{self.output_dir}/images",
        )

    def _get_video_gen(self) -> Any:
        from content.video_gen import VideoGenerator

        return VideoGenerator(
            model_name=self.video_model,
            device=self.device,
            output_dir=f"{self.output_dir}/videos",
        )

    def _get_narrator(self) -> Any:
        from content.tts import Narrator

        return Narrator(
            model_name=self.tts_model,
            output_dir=f"{self.output_dir}/audio",
        )

    def _get_lipsync(self) -> Any:
        from content.lipsync import LipSyncDriver

        return LipSyncDriver(
            backend=self.lipsync_backend,
            device=self.device,
            output_dir=f"{self.output_dir}/lipsync",
        )

    # -------------------------------------------------------------------
    # High-level workflows
    # -------------------------------------------------------------------

    def social_image(
        self,
        text: str,
        *,
        style: str = "dark_trading",
        width: int = 1024,
        height: int = 1024,
        seed: int | None = None,
    ) -> ContentResult:
        """Generate a social media graphic.

        Args:
            text:   Description or caption for the image.
            style:  Prompt prefix style.
            width:  Image width.
            height: Image height.
            seed:   Reproducibility seed.

        Returns:
            ContentResult with ``image_path`` set.
        """
        t0 = time.monotonic()
        prompt = self._style_prompt(text, style)

        gen = self._get_image_gen()
        image = gen.generate(prompt, width=width, height=height, seed=seed)

        if self.auto_unload:
            gen.unload()

        # Get the saved path from the output dir
        images_dir = Path(f"{self.output_dir}/images")
        saved = sorted(images_dir.glob("*.png"), key=lambda p: p.stat().st_mtime)
        img_path = saved[-1] if saved else None

        return ContentResult(
            image_path=img_path,
            elapsed_seconds=time.monotonic() - t0,
            metadata={"prompt": prompt, "style": style},
        )

    def narrate(
        self,
        text: str,
        *,
        voice: str | None = None,
        speed: float = 1.0,
    ) -> ContentResult:
        """Generate a narration audio file.

        Args:
            text:  The text to narrate.
            voice: Voice preset override.
            speed: Speech speed multiplier.

        Returns:
            ContentResult with ``audio_path`` set.
        """
        t0 = time.monotonic()
        narrator = self._get_narrator()
        audio_path = narrator.speak_long(text, voice=voice, speed=speed)

        if self.auto_unload:
            narrator.unload()

        return ContentResult(
            audio_path=audio_path,
            elapsed_seconds=time.monotonic() - t0,
            metadata={"text_length": len(text)},
        )

    def market_briefing(
        self,
        text: str,
        portrait: str | Path,
        output: str | Path | None = None,
        *,
        voice: str | None = None,
    ) -> ContentResult:
        """Generate a talking-head market briefing video.

        Pipeline: text → TTS audio → lip-sync with portrait → video

        Args:
            text:     The briefing script.
            portrait: Path to the presenter's face image.
            output:   Output video path. Auto-generated if None.
            voice:    TTS voice preset.

        Returns:
            ContentResult with ``audio_path`` and ``video_path`` set.
        """
        t0 = time.monotonic()
        log.info("Starting market briefing pipeline")

        # Step 1: TTS
        log.info("Step 1/2: Generating narration")
        narrator = self._get_narrator()
        audio_path = narrator.speak_long(text, voice=voice)
        if self.auto_unload:
            narrator.unload()

        # Step 2: Lip sync
        log.info("Step 2/2: Generating talking head")
        driver = self._get_lipsync()
        video_path = driver.generate(
            portrait=portrait,
            audio=str(audio_path),
            output=output,
        )

        elapsed = time.monotonic() - t0
        log.info("Market briefing complete in {t:.1f}s", t=elapsed)

        return ContentResult(
            audio_path=audio_path,
            video_path=video_path,
            elapsed_seconds=elapsed,
            metadata={"text_length": len(text), "pipeline": "market_briefing"},
        )

    def full_package(
        self,
        headline: str,
        body: str,
        portrait: str | Path,
        *,
        image_style: str = "dark_trading",
        voice: str | None = None,
        seed: int | None = None,
    ) -> ContentResult:
        """Generate a complete content package: image + narration + video.

        Pipeline:
        1. Generate social image from headline
        2. Generate TTS narration from body text
        3. Generate talking-head video from portrait + narration

        Args:
            headline:    Short text for image generation.
            body:        Full text for narration.
            portrait:    Path to the presenter's face image.
            image_style: Prompt style for image.
            voice:       TTS voice preset.
            seed:        Image generation seed.

        Returns:
            ContentResult with all paths set.
        """
        t0 = time.monotonic()
        log.info("Starting full content package pipeline")

        # Step 1: Image
        log.info("Step 1/3: Generating social image")
        img_gen = self._get_image_gen()
        prompt = self._style_prompt(headline, image_style)
        img_gen.generate(prompt, seed=seed)
        images_dir = Path(f"{self.output_dir}/images")
        saved_imgs = sorted(images_dir.glob("*.png"), key=lambda p: p.stat().st_mtime)
        img_path = saved_imgs[-1] if saved_imgs else None
        if self.auto_unload:
            img_gen.unload()

        # Step 2: TTS
        log.info("Step 2/3: Generating narration")
        narrator = self._get_narrator()
        audio_path = narrator.speak_long(body, voice=voice)
        if self.auto_unload:
            narrator.unload()

        # Step 3: Lip sync
        log.info("Step 3/3: Generating talking head")
        driver = self._get_lipsync()
        video_path = driver.generate(portrait=portrait, audio=str(audio_path))

        elapsed = time.monotonic() - t0
        log.info("Full package complete in {t:.1f}s", t=elapsed)

        return ContentResult(
            image_path=img_path,
            audio_path=audio_path,
            video_path=video_path,
            elapsed_seconds=elapsed,
            metadata={
                "headline": headline,
                "text_length": len(body),
                "pipeline": "full_package",
            },
        )

    def clip(
        self,
        prompt: str,
        *,
        num_frames: int = 49,
        width: int = 480,
        height: int = 320,
        seed: int | None = None,
    ) -> ContentResult:
        """Generate a short video clip from a text prompt.

        Args:
            prompt:     Text description of the video.
            num_frames: Number of frames.
            width:      Output width.
            height:     Output height.
            seed:       Reproducibility seed.

        Returns:
            ContentResult with ``video_path`` set.
        """
        t0 = time.monotonic()

        gen = self._get_video_gen()
        gen.generate(
            prompt,
            num_frames=num_frames,
            width=width,
            height=height,
            seed=seed,
        )

        videos_dir = Path(f"{self.output_dir}/videos")
        saved = sorted(videos_dir.glob("*.mp4"), key=lambda p: p.stat().st_mtime)
        vid_path = saved[-1] if saved else None

        if self.auto_unload:
            gen.unload()

        return ContentResult(
            video_path=vid_path,
            elapsed_seconds=time.monotonic() - t0,
            metadata={"prompt": prompt},
        )

    # -------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------

    @staticmethod
    def _style_prompt(text: str, style: str) -> str:
        """Prepend a style prefix to the user's text prompt."""
        styles = {
            "dark_trading": (
                "Professional dark-themed financial visualization, "
                "neon green and blue accents on black background, "
                "clean modern design, trading dashboard aesthetic. "
            ),
            "clean_white": (
                "Clean minimalist white background, professional "
                "business infographic style, modern sans-serif typography. "
            ),
            "cinematic": (
                "Cinematic wide-angle shot, dramatic lighting, "
                "film grain, moody atmosphere. "
            ),
            "chart": (
                "Professional financial chart, candlestick patterns, "
                "volume bars, technical indicators, dark theme. "
            ),
            "news": (
                "Breaking news graphic, bold red and white, "
                "professional broadcast quality, ticker tape style. "
            ),
        }
        prefix = styles.get(style, "")
        return f"{prefix}{text}"

    def status(self) -> dict[str, Any]:
        """Return current pipeline configuration and readiness."""
        from content.lipsync import LipSyncDriver

        return {
            "image_model": self.image_model,
            "video_model": self.video_model,
            "tts_model": self.tts_model,
            "lipsync_backend": self.lipsync_backend,
            "device": self.device,
            "auto_unload": self.auto_unload,
            "output_dir": self.output_dir,
            "lipsync_installed": LipSyncDriver.check_installation(),
        }
