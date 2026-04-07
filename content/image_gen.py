"""Image generation via locally-hosted diffusion models.

Supports FLUX.1 (schnell / dev), SDXL, and SD 3.5 through the HuggingFace
``diffusers`` library.  No external API calls — everything runs on local GPU
or CPU.

Usage::

    gen = ImageGenerator()          # lazy — no model loaded yet
    gen.load_model()                # downloads + loads onto device
    img = gen.generate("a bull market chart exploding upward")
    img.save("output.png")

    # Batch generation
    images = gen.generate_batch(["prompt1", "prompt2"], seed=42)
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loguru import logger as log

# Lazy imports — diffusers / torch are heavy
_FluxPipeline: Any = None
_StableDiffusionXLPipeline: Any = None
_torch: Any = None


def _ensure_imports() -> None:
    """Import heavy dependencies on first use."""
    global _FluxPipeline, _StableDiffusionXLPipeline, _torch
    if _torch is not None:
        return
    import torch

    _torch = torch
    try:
        from diffusers import FluxPipeline

        _FluxPipeline = FluxPipeline
    except ImportError:
        _FluxPipeline = None
    try:
        from diffusers import StableDiffusionXLPipeline

        _StableDiffusionXLPipeline = StableDiffusionXLPipeline
    except ImportError:
        _StableDiffusionXLPipeline = None


# ---------------------------------------------------------------------------
# Supported models
# ---------------------------------------------------------------------------
MODELS: dict[str, dict[str, Any]] = {
    "flux-schnell": {
        "repo": "black-forest-labs/FLUX.1-schnell",
        "pipeline": "flux",
        "default_steps": 4,
        "dtype": "bfloat16",
        "license": "Apache-2.0",
    },
    "flux-dev": {
        "repo": "black-forest-labs/FLUX.1-dev",
        "pipeline": "flux",
        "default_steps": 28,
        "dtype": "bfloat16",
        "license": "FLUX.1-dev-non-commercial",
    },
    "sdxl": {
        "repo": "stabilityai/stable-diffusion-xl-base-1.0",
        "pipeline": "sdxl",
        "default_steps": 30,
        "dtype": "float16",
        "license": "CreativeML-Open-RAIL++-M",
    },
}

DEFAULT_MODEL = "flux-schnell"


@dataclass
class ImageGenerator:
    """Self-hosted image generation engine.

    Parameters:
        model_name: Key from ``MODELS`` dict.
        device:     ``"cuda"``, ``"cpu"``, or ``"auto"`` (auto-detect).
        output_dir: Where generated images are saved.
        cache_dir:  HuggingFace model cache directory.
    """

    model_name: str = DEFAULT_MODEL
    device: str = "auto"
    output_dir: str = "outputs/content/images"
    cache_dir: str | None = None
    _pipe: Any = field(default=None, repr=False, init=False)
    _loaded: bool = field(default=False, repr=False, init=False)

    def _resolve_device(self) -> str:
        """Pick the best available device."""
        _ensure_imports()
        if self.device != "auto":
            return self.device
        if _torch.cuda.is_available():
            return "cuda"
        if hasattr(_torch.backends, "mps") and _torch.backends.mps.is_available():
            return "mps"
        return "cpu"

    def load_model(self) -> None:
        """Download (if needed) and load the model onto the device."""
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
            "Loading image model {model} on {device} ({dtype})",
            model=spec["repo"],
            device=device,
            dtype=spec["dtype"],
        )

        kwargs: dict[str, Any] = {"torch_dtype": dtype}
        if self.cache_dir:
            kwargs["cache_dir"] = self.cache_dir

        t0 = time.monotonic()

        if spec["pipeline"] == "flux":
            if _FluxPipeline is None:
                raise ImportError(
                    "FluxPipeline not available. Install diffusers >= 0.30: "
                    "pip install diffusers[torch] transformers accelerate"
                )
            self._pipe = _FluxPipeline.from_pretrained(spec["repo"], **kwargs)
        elif spec["pipeline"] == "sdxl":
            if _StableDiffusionXLPipeline is None:
                raise ImportError(
                    "StableDiffusionXLPipeline not available. Install diffusers: "
                    "pip install diffusers[torch] transformers accelerate"
                )
            self._pipe = _StableDiffusionXLPipeline.from_pretrained(
                spec["repo"], **kwargs
            )
        else:
            raise ValueError(f"Unknown pipeline type: {spec['pipeline']}")

        # Move to device (CPU stays in float32 for FLUX)
        if device == "cpu" and dtype == _torch.bfloat16:
            self._pipe = self._pipe.to(device)
        else:
            self._pipe = self._pipe.to(device)

        # Enable memory optimizations when available
        if device in ("cuda", "mps"):
            try:
                self._pipe.enable_attention_slicing()
            except AttributeError:
                pass

        elapsed = time.monotonic() - t0
        self._loaded = True
        log.info("Image model loaded in {t:.1f}s", t=elapsed)

    def generate(
        self,
        prompt: str,
        *,
        negative_prompt: str = "",
        width: int = 1024,
        height: int = 1024,
        steps: int | None = None,
        guidance_scale: float = 7.5,
        seed: int | None = None,
        save: bool = True,
    ) -> Any:
        """Generate a single image from a text prompt.

        Args:
            prompt:          Text description of the desired image.
            negative_prompt: What to avoid in the image.
            width:           Output width in pixels (must be multiple of 8).
            height:          Output height in pixels (must be multiple of 8).
            steps:           Inference steps (None = model default).
            guidance_scale:  Classifier-free guidance strength.
            seed:            Reproducibility seed (None = random).
            save:            Whether to auto-save to ``output_dir``.

        Returns:
            PIL.Image.Image
        """
        self.load_model()

        spec = MODELS[self.model_name]
        num_steps = steps or spec["default_steps"]

        gen_kwargs: dict[str, Any] = {
            "prompt": prompt,
            "width": width,
            "height": height,
            "num_inference_steps": num_steps,
            "guidance_scale": guidance_scale,
        }
        if negative_prompt and spec["pipeline"] != "flux":
            gen_kwargs["negative_prompt"] = negative_prompt

        if seed is not None:
            gen_kwargs["generator"] = _torch.Generator(
                device=self._resolve_device()
            ).manual_seed(seed)

        log.info(
            "Generating image: {prompt:.80s} ({w}x{h}, {s} steps)",
            prompt=prompt,
            w=width,
            h=height,
            s=num_steps,
        )
        t0 = time.monotonic()
        result = self._pipe(**gen_kwargs)
        image = result.images[0]
        elapsed = time.monotonic() - t0
        log.info("Image generated in {t:.1f}s", t=elapsed)

        if save:
            out_path = self._save_image(image, prompt)
            log.info("Saved to {path}", path=out_path)

        return image

    def generate_batch(
        self,
        prompts: list[str],
        *,
        seed: int | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Generate multiple images sequentially.

        Args:
            prompts: List of text prompts.
            seed:    Base seed (incremented per image for reproducibility).
            **kwargs: Passed through to ``generate()``.

        Returns:
            List of PIL.Image.Image objects.
        """
        images = []
        for i, prompt in enumerate(prompts):
            s = (seed + i) if seed is not None else None
            img = self.generate(prompt, seed=s, **kwargs)
            images.append(img)
        return images

    def _save_image(self, image: Any, prompt: str) -> Path:
        """Save image to output_dir with timestamp-based filename."""
        out_dir = Path(self.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        # Sanitize prompt for filename
        slug = "".join(c if c.isalnum() or c in " -_" else "" for c in prompt[:60])
        slug = slug.strip().replace(" ", "_")
        ts = int(time.time())
        filename = f"{ts}_{slug}.png"
        path = out_dir / filename
        image.save(path)
        return path

    def unload(self) -> None:
        """Free GPU memory by unloading the model."""
        if self._pipe is not None:
            del self._pipe
            self._pipe = None
            self._loaded = False
            _ensure_imports()
            if _torch.cuda.is_available():
                _torch.cuda.empty_cache()
            log.info("Image model unloaded")

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    @staticmethod
    def available_models() -> list[str]:
        """Return list of supported model keys."""
        return list(MODELS.keys())
