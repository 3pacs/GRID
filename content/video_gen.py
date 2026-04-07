"""Video generation via locally-hosted models.

Supports Wan2.1 (text-to-video and image-to-video) and CogVideoX through
the HuggingFace ``diffusers`` library.  Runs entirely on local GPU.

Usage::

    gen = VideoGenerator()
    gen.load_model()
    frames = gen.generate("a stock ticker scrolling across a trading floor")
    gen.save_video(frames, "output.mp4")

    # Image-to-video
    from PIL import Image
    img = Image.open("chart.png")
    frames = gen.img2video(img, "chart animating with green candles")
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loguru import logger as log

# Lazy imports — torch / diffusers are heavy
_torch: Any = None
_WanPipeline: Any = None
_CogVideoXPipeline: Any = None
_export_to_video: Any = None


def _ensure_imports() -> None:
    """Import heavy dependencies on first use."""
    global _torch, _WanPipeline, _CogVideoXPipeline, _export_to_video
    if _torch is not None:
        return
    import torch

    _torch = torch
    try:
        from diffusers import WanPipeline

        _WanPipeline = WanPipeline
    except ImportError:
        _WanPipeline = None
    try:
        from diffusers import CogVideoXPipeline

        _CogVideoXPipeline = CogVideoXPipeline
    except ImportError:
        _CogVideoXPipeline = None
    try:
        from diffusers.utils import export_to_video as _etv

        _export_to_video = _etv
    except ImportError:
        _export_to_video = None


# ---------------------------------------------------------------------------
# Supported models
# ---------------------------------------------------------------------------
MODELS: dict[str, dict[str, Any]] = {
    "wan2.1-t2v-14b": {
        "repo": "Wan-AI/Wan2.1-T2V-14B",
        "pipeline": "wan",
        "mode": "t2v",
        "default_steps": 50,
        "dtype": "bfloat16",
        "vram_gb": 24,
        "license": "Apache-2.0",
    },
    "wan2.1-t2v-1.3b": {
        "repo": "Wan-AI/Wan2.1-T2V-1.3B",
        "pipeline": "wan",
        "mode": "t2v",
        "default_steps": 50,
        "dtype": "bfloat16",
        "vram_gb": 8,
        "license": "Apache-2.0",
    },
    "wan2.1-i2v-14b": {
        "repo": "Wan-AI/Wan2.1-I2V-14B-480P",
        "pipeline": "wan",
        "mode": "i2v",
        "default_steps": 50,
        "dtype": "bfloat16",
        "vram_gb": 24,
        "license": "Apache-2.0",
    },
    "cogvideox-5b": {
        "repo": "THUDM/CogVideoX-5b",
        "pipeline": "cogvideox",
        "mode": "t2v",
        "default_steps": 50,
        "dtype": "bfloat16",
        "vram_gb": 24,
        "license": "Apache-2.0",
    },
    "cogvideox-2b": {
        "repo": "THUDM/CogVideoX-2b",
        "pipeline": "cogvideox",
        "mode": "t2v",
        "default_steps": 50,
        "dtype": "float16",
        "vram_gb": 12,
        "license": "Apache-2.0",
    },
}

DEFAULT_MODEL = "wan2.1-t2v-1.3b"


@dataclass
class VideoGenerator:
    """Self-hosted video generation engine.

    Parameters:
        model_name: Key from ``MODELS`` dict.
        device:     ``"cuda"``, ``"cpu"``, or ``"auto"``.
        output_dir: Where generated videos are saved.
        cache_dir:  HuggingFace model cache directory.
    """

    model_name: str = DEFAULT_MODEL
    device: str = "auto"
    output_dir: str = "outputs/content/videos"
    cache_dir: str | None = None
    _pipe: Any = field(default=None, repr=False, init=False)
    _loaded: bool = field(default=False, repr=False, init=False)

    def _resolve_device(self) -> str:
        _ensure_imports()
        if self.device != "auto":
            return self.device
        if _torch.cuda.is_available():
            return "cuda"
        return "cpu"

    def load_model(self) -> None:
        """Download (if needed) and load the video model onto the device."""
        if self._loaded:
            return
        _ensure_imports()

        spec = MODELS.get(self.model_name)
        if spec is None:
            raise ValueError(
                f"Unknown model '{self.model_name}'. "
                f"Available: {list(MODELS.keys())}"
            )

        device = self._resolve_device()
        dtype_map = {
            "bfloat16": _torch.bfloat16,
            "float16": _torch.float16,
            "float32": _torch.float32,
        }
        dtype = dtype_map.get(spec["dtype"], _torch.float32)

        log.info(
            "Loading video model {model} on {device} (~{vram}GB VRAM)",
            model=spec["repo"],
            device=device,
            vram=spec["vram_gb"],
        )

        kwargs: dict[str, Any] = {"torch_dtype": dtype}
        if self.cache_dir:
            kwargs["cache_dir"] = self.cache_dir

        t0 = time.monotonic()

        if spec["pipeline"] == "wan":
            if _WanPipeline is None:
                raise ImportError(
                    "WanPipeline not available. Install diffusers >= 0.32: "
                    "pip install diffusers[torch] transformers accelerate"
                )
            self._pipe = _WanPipeline.from_pretrained(spec["repo"], **kwargs)
        elif spec["pipeline"] == "cogvideox":
            if _CogVideoXPipeline is None:
                raise ImportError(
                    "CogVideoXPipeline not available. Install diffusers >= 0.30: "
                    "pip install diffusers[torch] transformers accelerate"
                )
            self._pipe = _CogVideoXPipeline.from_pretrained(spec["repo"], **kwargs)
        else:
            raise ValueError(f"Unknown pipeline type: {spec['pipeline']}")

        self._pipe = self._pipe.to(device)

        # Memory optimizations
        if device == "cuda":
            try:
                self._pipe.enable_model_cpu_offload()
            except (AttributeError, NotImplementedError):
                pass
            try:
                self._pipe.enable_attention_slicing()
            except AttributeError:
                pass

        elapsed = time.monotonic() - t0
        self._loaded = True
        log.info("Video model loaded in {t:.1f}s", t=elapsed)

    def generate(
        self,
        prompt: str,
        *,
        negative_prompt: str = "",
        num_frames: int = 49,
        width: int = 480,
        height: int = 320,
        steps: int | None = None,
        guidance_scale: float = 6.0,
        seed: int | None = None,
        fps: int = 16,
        save: bool = True,
    ) -> list[Any]:
        """Generate a video from a text prompt.

        Args:
            prompt:          Text description of the desired video.
            negative_prompt: What to avoid.
            num_frames:      Number of frames to generate.
            width:           Output width in pixels.
            height:          Output height in pixels.
            steps:           Inference steps (None = model default).
            guidance_scale:  Classifier-free guidance strength.
            seed:            Reproducibility seed.
            fps:             Frames per second for saved video.
            save:            Whether to auto-save to ``output_dir``.

        Returns:
            List of PIL.Image.Image frames.
        """
        self.load_model()
        spec = MODELS[self.model_name]
        num_steps = steps or spec["default_steps"]

        gen_kwargs: dict[str, Any] = {
            "prompt": prompt,
            "num_frames": num_frames,
            "height": height,
            "width": width,
            "num_inference_steps": num_steps,
            "guidance_scale": guidance_scale,
        }
        if negative_prompt:
            gen_kwargs["negative_prompt"] = negative_prompt

        if seed is not None:
            gen_kwargs["generator"] = _torch.Generator(
                device=self._resolve_device()
            ).manual_seed(seed)

        log.info(
            "Generating video: {prompt:.80s} ({f} frames, {w}x{h})",
            prompt=prompt,
            f=num_frames,
            w=width,
            h=height,
        )
        t0 = time.monotonic()
        result = self._pipe(**gen_kwargs)
        frames = result.frames[0] if hasattr(result, "frames") else result.images
        elapsed = time.monotonic() - t0
        log.info(
            "Video generated in {t:.1f}s ({n} frames)",
            t=elapsed,
            n=len(frames) if frames else 0,
        )

        if save and frames:
            out_path = self.save_video(frames, prompt=prompt, fps=fps)
            log.info("Saved to {path}", path=out_path)

        return frames

    def img2video(
        self,
        image: Any,
        prompt: str = "",
        *,
        num_frames: int = 49,
        steps: int | None = None,
        guidance_scale: float = 6.0,
        seed: int | None = None,
        fps: int = 16,
        save: bool = True,
    ) -> list[Any]:
        """Generate video from an input image + optional prompt.

        Args:
            image:  PIL.Image.Image to animate.
            prompt: Optional text guidance.
            **kwargs: Same as ``generate()``.

        Returns:
            List of PIL.Image.Image frames.
        """
        self.load_model()
        spec = MODELS[self.model_name]

        if spec["mode"] != "i2v":
            log.warning(
                "Model {m} is text-to-video, not image-to-video. "
                "Consider using wan2.1-i2v-14b instead.",
                m=self.model_name,
            )

        num_steps = steps or spec["default_steps"]
        gen_kwargs: dict[str, Any] = {
            "image": image,
            "prompt": prompt if prompt else "",
            "num_frames": num_frames,
            "num_inference_steps": num_steps,
            "guidance_scale": guidance_scale,
        }

        if seed is not None:
            gen_kwargs["generator"] = _torch.Generator(
                device=self._resolve_device()
            ).manual_seed(seed)

        log.info("Generating image-to-video ({f} frames)", f=num_frames)
        t0 = time.monotonic()
        result = self._pipe(**gen_kwargs)
        frames = result.frames[0] if hasattr(result, "frames") else result.images
        elapsed = time.monotonic() - t0
        log.info("Image-to-video done in {t:.1f}s", t=elapsed)

        if save and frames:
            out_path = self.save_video(frames, prompt=prompt or "i2v", fps=fps)
            log.info("Saved to {path}", path=out_path)

        return frames

    def save_video(
        self,
        frames: list[Any],
        prompt: str = "video",
        fps: int = 16,
    ) -> Path:
        """Save frames as an MP4 file.

        Falls back to saving individual frames as PNGs if export_to_video
        is unavailable.
        """
        out_dir = Path(self.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        slug = "".join(c if c.isalnum() or c in " -_" else "" for c in prompt[:60])
        slug = slug.strip().replace(" ", "_")
        ts = int(time.time())

        if _export_to_video is not None:
            filename = f"{ts}_{slug}.mp4"
            path = out_dir / filename
            _export_to_video(frames, str(path), fps=fps)
            return path

        # Fallback: save individual frames
        frame_dir = out_dir / f"{ts}_{slug}_frames"
        frame_dir.mkdir(parents=True, exist_ok=True)
        for i, frame in enumerate(frames):
            frame.save(frame_dir / f"frame_{i:04d}.png")
        log.warning(
            "export_to_video unavailable — saved {n} frames to {d}",
            n=len(frames),
            d=frame_dir,
        )
        return frame_dir

    def unload(self) -> None:
        """Free GPU memory by unloading the model."""
        if self._pipe is not None:
            del self._pipe
            self._pipe = None
            self._loaded = False
            _ensure_imports()
            if _torch.cuda.is_available():
                _torch.cuda.empty_cache()
            log.info("Video model unloaded")

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    @staticmethod
    def available_models() -> list[str]:
        return list(MODELS.keys())
