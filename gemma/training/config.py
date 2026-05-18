"""
Training configuration for GRID Gemma fine-tuning.

Defines task types, model choices, LoRA parameters, and training
hyperparameters. Supports both Gemma 3 (270M-27B) and Gemma 4
(E2B, E4B, 26B-A4B MoE, 31B dense) on a single GPU
(T4/L4/A100 via Colab or local RTX 3090).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class TaskType(str, Enum):
    """GRID micro model task specialisations."""

    SIGNAL_CLASSIFIER = "signal_classifier"
    ANOMALY_NARRATOR = "anomaly_narrator"
    EDGAR_EXTRACTOR = "edgar_extractor"
    KNOWLEDGE_MAPPER = "knowledge_mapper"
    HERMES_OPERATOR = "hermes_operator"


# Base models available for fine-tuning (Unsloth pre-quantized)
BASE_MODELS: dict[str, str] = {
    # Gemma 4 (preferred — newer architecture, better quality)
    "gemma4-e2b": "unsloth/gemma-4-E2B-it",
    "gemma4-e4b": "unsloth/gemma-4-E4B-it",
    "gemma4-26b": "unsloth/gemma-4-26B-A4B-it",
    "gemma4-31b": "unsloth/gemma-4-31B-it-unsloth-bnb-4bit",
    # Gemma 3 (legacy — still useful for 270M CPU deployment)
    "gemma3-270m": "unsloth/gemma-3-270m-it",
    "gemma3-1b": "unsloth/gemma-3-1b-it-unsloth-bnb-4bit",
    "gemma3-4b": "unsloth/gemma-3-4b-it-unsloth-bnb-4bit",
}

# Chat template tokens for train_on_responses_only masking
# Gemma 3 and 4 share the same chat template structure
GEMMA_INSTRUCTION_PART = "<start_of_turn>user\n"
GEMMA_RESPONSE_PART = "<start_of_turn>model\n"

# Legacy alias
GEMMA3_INSTRUCTION_PART = GEMMA_INSTRUCTION_PART
GEMMA3_RESPONSE_PART = GEMMA_RESPONSE_PART

# Task-specific system prompts (must match gemma/micro.py for consistency)
TASK_SYSTEM_PROMPTS: dict[TaskType, str] = {
    TaskType.SIGNAL_CLASSIFIER: (
        "You are a financial signal classifier. Given a signal description, "
        "classify it into exactly one category and urgency level.\n\n"
        "Categories: rates, credit, equity, volatility, flows, macro, "
        "geopolitical, insider, options, crypto, commodities, fx\n\n"
        "Urgency: critical (act now), high (within hours), "
        "medium (within day), low (informational)\n\n"
        "Respond in exactly this format:\n"
        "CATEGORY: <category>\nURGENCY: <urgency>\nREASON: <one sentence>"
    ),
    TaskType.ANOMALY_NARRATOR: (
        "You are an anomaly narrator for a trading system. "
        "Given anomaly data (z-scores, values, context), write a single "
        "concise sentence describing what happened and why it matters. "
        "Be specific about numbers and direction. No hedging."
    ),
    TaskType.EDGAR_EXTRACTOR: (
        "You are a structured data extractor for SEC EDGAR filings. "
        "Extract the requested fields from the filing text and return "
        "them as a JSON object. Only include fields that are explicitly "
        "stated in the text. Use null for missing fields."
    ),
    TaskType.KNOWLEDGE_MAPPER: (
        "You are a knowledge mapper for a trading intelligence system. "
        "Given a piece of content (signal, analysis, actor profile, event, or concept), "
        "generate a wiki-style entry with:\n\n"
        "1. A concise summary (1-2 sentences)\n"
        "2. [[Backlinks]] to related concepts, actors, signals, and events using [[double bracket]] notation\n"
        "3. A 'Connections' section listing non-obvious relationships and degrees of separation\n"
        "4. A 'See Also' section with related entries\n\n"
        "Surface hidden connections that would otherwise go unnoticed — "
        "trace money flows, policy chains, supply chain dependencies, "
        "and actor relationships across domains.\n\n"
        "Format:\n"
        "## <Title>\n"
        "<Summary with [[backlinks]] inline>\n\n"
        "### Connections\n"
        "- <connection 1: explain the link>\n"
        "- <connection 2: explain the link>\n\n"
        "### See Also\n"
        "[[Related1]], [[Related2]], [[Related3]]"
    ),
    TaskType.HERMES_OPERATOR: (
        "You are GRID Hermes, the operator for Anik's local research, render, "
        "and agent fleet. Be concise, evidence-first, and safety-bound. Prefer "
        "read-only inspection, durable Obsidian/GitHub-backed records, explicit "
        "approval for risky mutations, and concrete next actions."
    ),
}


@dataclass(frozen=True)
class LoRAConfig:
    """LoRA adapter configuration."""

    r: int = 8
    lora_alpha: int = 8
    lora_dropout: float = 0.0
    bias: str = "none"
    finetune_language_layers: bool = True
    finetune_attention_modules: bool = True
    finetune_mlp_modules: bool = True
    finetune_vision_layers: bool = False
    random_state: int = 3407


@dataclass(frozen=True)
class TrainingConfig:
    """Full training configuration.

    Attributes:
        task: Which GRID micro model task to train.
        base_model: Key into BASE_MODELS or a HuggingFace model ID.
        max_seq_length: Maximum sequence length for training.
        load_in_4bit: Use 4-bit QLoRA quantization.
        lora: LoRA adapter configuration.
        per_device_train_batch_size: Batch size per GPU.
        gradient_accumulation_steps: Gradient accumulation.
        num_train_epochs: Number of training epochs (use max_steps to override).
        max_steps: Max training steps (-1 = use num_train_epochs).
        learning_rate: Peak learning rate.
        warmup_steps: Linear warmup steps.
        weight_decay: AdamW weight decay.
        lr_scheduler_type: LR scheduler (linear, cosine, constant).
        optim: Optimizer name.
        logging_steps: Log every N steps.
        seed: Random seed for reproducibility.
        output_dir: Directory for checkpoints and logs.
        dataset_path: Path to pre-built JSONL dataset (optional).
        train_on_responses_only: Mask instruction tokens during training.
        packing: Enable sequence packing for efficiency.
    """

    task: TaskType = TaskType.SIGNAL_CLASSIFIER
    base_model: str = "gemma4-e4b"
    max_seq_length: int = 2048
    load_in_4bit: bool = True

    lora: LoRAConfig = field(default_factory=LoRAConfig)

    per_device_train_batch_size: int = 2
    gradient_accumulation_steps: int = 4
    num_train_epochs: int = 3
    max_steps: int = -1
    learning_rate: float = 2e-4
    warmup_steps: int = 10
    weight_decay: float = 0.001
    lr_scheduler_type: str = "linear"
    optim: str = "adamw_8bit"
    logging_steps: int = 1
    seed: int = 3407
    output_dir: str = "outputs/gemma_training"
    dataset_path: str | None = None
    train_on_responses_only: bool = True
    packing: bool = False

    @property
    def resolved_model_name(self) -> str:
        """Resolve base_model key to HuggingFace model ID."""
        return BASE_MODELS.get(self.base_model, self.base_model)

    @property
    def model_output_dir(self) -> Path:
        """Output directory for this task's trained model."""
        return Path(self.output_dir) / self.task.value

    @property
    def system_prompt(self) -> str:
        """System prompt for this task."""
        return TASK_SYSTEM_PROMPTS[self.task]


# ---------------------------------------------------------------------------
# Recommended presets by model size (from Unsloth official notebooks)
#
# Key findings:
#   - 270M models: higher LoRA rank (128), float32/16-bit (NOT 4-bit),
#     lower LR (5e-5). Small models can't tolerate aggressive quantization.
#   - 1B-4B models: LoRA r=8-16, 4-bit QLoRA, LR 2e-4. Standard config.
#   - 27B+ models: LoRA r=8 is sufficient, 4-bit QLoRA required, LR 2e-4.
#   - LoRA alpha should always >= r (1:1 ratio minimum).
#   - LoRA dropout should always be 0 (non-zero is slower, unhelpful).
#   - adamw_8bit for SFT, adamw_torch_fused for RL/GRPO.
#   - GGUF: use Q8_0 or Q6_K for sub-1B models (can't tolerate Q4),
#     Q4_K_M acceptable for 4B+.
# ---------------------------------------------------------------------------

MODEL_PRESETS: dict[str, dict] = {
    "gemma3-270m": {
        "lora_r": 128,
        "lora_alpha": 128,
        "load_in_4bit": False,      # Sub-1B models need 16-bit
        "learning_rate": 5e-5,      # Lower for small models
        "per_device_train_batch_size": 4,
        "gradient_accumulation_steps": 1,
        "recommended_gguf": "q8_0", # Don't go below Q8 for 270M
    },
    "gemma3-1b": {
        "lora_r": 16,
        "lora_alpha": 16,
        "load_in_4bit": True,
        "learning_rate": 2e-4,
        "per_device_train_batch_size": 2,
        "gradient_accumulation_steps": 4,
        "recommended_gguf": "q8_0",
    },
    "gemma4-e2b": {
        "lora_r": 16,
        "lora_alpha": 16,
        "load_in_4bit": True,
        "learning_rate": 2e-4,
        "per_device_train_batch_size": 2,
        "gradient_accumulation_steps": 4,
        "recommended_gguf": "q8_0",
    },
    "gemma4-e4b": {
        "lora_r": 8,
        "lora_alpha": 8,
        "load_in_4bit": True,
        "learning_rate": 2e-4,
        "per_device_train_batch_size": 2,
        "gradient_accumulation_steps": 4,
        "recommended_gguf": "q4_k_m",
    },
    "gemma4-26b": {
        "lora_r": 8,
        "lora_alpha": 8,
        "load_in_4bit": True,
        "learning_rate": 2e-4,
        "per_device_train_batch_size": 2,
        "gradient_accumulation_steps": 4,
        "recommended_gguf": "q4_k_m",
    },
    "gemma4-31b": {
        "lora_r": 8,
        "lora_alpha": 8,
        "load_in_4bit": True,
        "learning_rate": 2e-4,
        "per_device_train_batch_size": 2,
        "gradient_accumulation_steps": 4,
        "recommended_gguf": "q4_k_m",
    },
}


def get_preset_config(base_model: str, task: TaskType) -> TrainingConfig:
    """Get a recommended TrainingConfig preset for a given model + task.

    Applies model-size-specific best practices from Unsloth notebooks:
    higher LoRA rank for small models, appropriate quantization, etc.

    Parameters:
        base_model: Model key (e.g., "gemma4-e4b", "gemma3-270m").
        task: Which task to train.

    Returns:
        TrainingConfig with model-appropriate defaults.
    """
    preset = MODEL_PRESETS.get(base_model, MODEL_PRESETS["gemma4-e4b"])
    return TrainingConfig(
        task=task,
        base_model=base_model,
        load_in_4bit=preset["load_in_4bit"],
        lora=LoRAConfig(
            r=preset["lora_r"],
            lora_alpha=preset["lora_alpha"],
        ),
        learning_rate=preset["learning_rate"],
        per_device_train_batch_size=preset["per_device_train_batch_size"],
        gradient_accumulation_steps=preset["gradient_accumulation_steps"],
    )
