"""Per-host hardware profiles and model-fit estimation.

The fleet runs models on very different boxes (a Blackwell server, a 24 GB
node, several 12 GB cards including old Maxwell TITAN Xs). Upgrading every
host to a Qwen 3.6+ model means picking the largest variant + quant that
*fits the VRAM and still clears the quality bar*. This module estimates
whether a candidate model fits a host and flags hardware caveats (Maxwell
has no flash-attention / FP8), so the autoresearch loop only proposes
configs that can actually run.

VRAM figures are seeded from config.py host comments and are operator-
editable — treat them as a starting point, not gospel.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Bytes-per-weight by quant scheme (approximate, for VRAM estimation).
QUANT_BPW: dict[str, float] = {
    "f16": 16.0,
    "q8_0": 8.5,
    "q6_k": 6.6,
    "q5_k_m": 5.7,
    "q4_k_m": 4.8,
    "q4_0": 4.5,
    "q3_k_m": 3.9,
    "iq4_xs": 4.3,
    "iq3_xxs": 3.1,
}

# KV-cache + activation + context overhead headroom (GB) to keep free.
DEFAULT_OVERHEAD_GB = 2.5


@dataclass(frozen=True)
class HostProfile:
    """Hardware profile for one fleet host.

    Attributes:
        host: Logical name (matches Endpoint.host).
        vram_gb: Usable VRAM per inference card, in GB.
        gpus: Number of GPUs available for one model.
        arch: GPU architecture (affects feature support).
        flash_attn: Whether Flash Attention is supported.
        fp8: Whether FP8 weights/KV are supported.
        notes: Free-text caveats.
    """

    host: str
    vram_gb: float
    gpus: int = 1
    arch: str = "unknown"
    flash_attn: bool = True
    fp8: bool = False
    notes: str = ""

    @property
    def total_vram_gb(self) -> float:
        return self.vram_gb * self.gpus


# Seeded from config.py host comments (2026-05). Edit as hardware changes.
HOST_PROFILES: dict[str, HostProfile] = {
    "grid-svr": HostProfile("grid-svr", vram_gb=48.0, gpus=1, arch="blackwell",
                            flash_attn=True, fp8=True,
                            notes="Blackwell — FP8/flash-attn; main GPU inference"),
    "panda": HostProfile("panda", vram_gb=24.0, gpus=1, arch="ampere",
                         flash_attn=True, fp8=False,
                         notes="runs qwen3.6:27b-q4_K_M today"),
    "ocr-node": HostProfile("ocr-node", vram_gb=16.0, gpus=1, arch="ampere",
                            flash_attn=True, fp8=False,
                            notes="vision/OCR node; gemma3:12b today"),
    "koala": HostProfile("koala", vram_gb=12.0, gpus=2, arch="maxwell",
                        flash_attn=False, fp8=False,
                        notes="2x GTX TITAN X Maxwell 12GB — NO flash-attn/FP8; "
                              "split a model across both cards or run 12GB-class"),
    "z400": HostProfile("z400", vram_gb=12.0, gpus=1, arch="pascal",
                        flash_attn=False, fp8=False,
                        notes="workstation 12GB GPU; qwen2.5:7b today"),
}


@dataclass(frozen=True)
class ModelSpec:
    """A candidate model variant to consider deploying.

    Attributes:
        name: Display name / ollama tag / gguf alias.
        params_b: Parameter count in billions.
        quant: Quant scheme key (see QUANT_BPW).
        meets_bar: Whether this model clears the quality bar (caller sets).
        repo: Optional HF repo / source hint.
    """

    name: str
    params_b: float
    quant: str
    meets_bar: bool = True
    repo: str = ""

    def estimated_vram_gb(self, overhead_gb: float = DEFAULT_OVERHEAD_GB) -> float:
        """Rough VRAM needed: weights + fixed overhead."""
        bpw = QUANT_BPW.get(self.quant.lower(), 5.0)
        weights_gb = (self.params_b * 1e9 * bpw / 8.0) / 1e9
        return round(weights_gb + overhead_gb, 1)


def fits_on(spec: ModelSpec, host: HostProfile, *, allow_multi_gpu: bool = True) -> tuple[bool, str]:
    """Decide whether ``spec`` fits ``host`` and note any caveats."""
    need = spec.estimated_vram_gb()
    budget = host.total_vram_gb if allow_multi_gpu else host.vram_gb
    if need > budget:
        return False, f"needs ~{need}GB > {budget}GB available on {host.host}"
    caveats = []
    if not host.flash_attn:
        caveats.append("no flash-attn (slower KV)")
    if spec.params_b >= 20 and host.gpus > 1 and need > host.vram_gb:
        caveats.append("requires tensor-split across GPUs")
    note = f"fits (~{need}GB / {budget}GB)" + (" — " + "; ".join(caveats) if caveats else "")
    return True, note
