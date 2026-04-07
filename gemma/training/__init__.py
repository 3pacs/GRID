"""
GRID Gemma Training — Fine-tune Gemma 4 / Gemma 3 for GRID-specific tasks.

Uses Unsloth for 2x faster training with 70% less VRAM. Supports:
  - Signal classifier: domain + urgency classification
  - Anomaly narrator: one-line anomaly summaries
  - EDGAR extractor: structured SEC filing extraction

Models: Gemma 4 E2B/E4B/26B-A4B/31B (preferred) or Gemma 3 270M-27B.

Training pipeline:
  1. Generate task-specific datasets from GRID data
  2. Fine-tune with LoRA via Unsloth + SFTTrainer
  3. Export to GGUF for llama.cpp deployment
  4. Deploy as micro model pool (see gemma/micro.py)
"""

from gemma.training.config import TrainingConfig, TaskType

__all__ = ["TrainingConfig", "TaskType"]
