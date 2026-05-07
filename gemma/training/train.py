"""
GRID Gemma Fine-Tuning with Unsloth.

Fine-tunes Gemma 4 (E2B, E4B, 26B-A4B MoE, 31B) or Gemma 3 (270M-27B)
for GRID-specific tasks using Unsloth's FastModel + LoRA + SFTTrainer.
Designed to run on:
  - Google Colab (free T4) for E2B / Gemma 3 270M/1B
  - Colab Pro (L4/A100) for E4B / 26B-A4B
  - Local RTX 3090 for any size up to 31B (4-bit)

Usage:
    # Train signal classifier with defaults (Gemma 4 E4B, LoRA r=8)
    python -m gemma.training.train --task signal_classifier

    # Train with Gemma 4 E2B (smaller, fits free Colab T4)
    python -m gemma.training.train --task anomaly_narrator --base-model gemma4-e2b

    # Train with legacy Gemma 3 270M for CPU deployment
    python -m gemma.training.train --task anomaly_narrator --base-model gemma3-270m

    # Train with custom LoRA rank and learning rate
    python -m gemma.training.train --task edgar_extractor --lora-r 16 --lr 1e-4

    # Export to GGUF after training
    python -m gemma.training.train --task signal_classifier --export-gguf q8_0

Best Practices Applied:
  - train_on_responses_only: Masks instruction tokens so the model only
    learns to generate responses, not parrot system/user prompts.
  - LoRA r=8 with alpha=8 (ratio 1:1): Balanced adapter capacity without
    overfitting on small datasets. Increase to r=16 for >1000 examples.
  - adamw_8bit: Saves ~30% optimizer memory vs standard AdamW.
  - gradient_checkpointing="unsloth": Unsloth's custom implementation
    saves more VRAM than standard HF gradient checkpointing.
  - Linear LR schedule with warmup: Stable training for small datasets.
  - seed=3407: Reproducible results (Unsloth's recommended seed).
  - Gemma 4 E4B + QLoRA preferred over E2B + full LoRA (better quality).
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger as log

if TYPE_CHECKING:
    from gemma.training.config import TrainingConfig


def train(config: "TrainingConfig") -> Path:
    """Run the full training pipeline.

    Parameters:
        config: Training configuration.

    Returns:
        Path to the saved model directory.

    Raises:
        ImportError: If unsloth is not installed.
        RuntimeError: If CUDA is not available.
    """
    # ---------------------------------------------------------------
    # 1. Validate environment
    # ---------------------------------------------------------------
    try:
        import torch
    except ImportError:
        raise ImportError("PyTorch is required. Install with: pip install torch")

    if not torch.cuda.is_available():
        log.warning(
            "CUDA not available — training will be extremely slow on CPU. "
            "Use Google Colab or a GPU machine for practical training times."
        )

    try:
        from unsloth import FastModel
    except ImportError:
        raise ImportError(
            "Unsloth is required for training. Install with:\n"
            "  pip install unsloth\n"
            "Or for Colab:\n"
            "  pip install unsloth"
        )

    from unsloth.chat_templates import (
        get_chat_template,
        standardize_data_formats,
        train_on_responses_only,
    )
    from trl import SFTConfig, SFTTrainer

    from gemma.training.config import GEMMA_INSTRUCTION_PART, GEMMA_RESPONSE_PART
    from gemma.training.datasets import load_dataset_for_training

    # ---------------------------------------------------------------
    # 2. Load base model + tokenizer
    # ---------------------------------------------------------------
    model_name = config.resolved_model_name
    log.info("Loading model: {m} (max_seq_length={s})", m=model_name, s=config.max_seq_length)

    model, tokenizer = FastModel.from_pretrained(
        model_name=model_name,
        max_seq_length=config.max_seq_length,
        load_in_4bit=config.load_in_4bit,
        load_in_8bit=False,
        full_finetuning=False,
        dtype=None,  # Auto-detect best dtype
    )

    # ---------------------------------------------------------------
    # 3. Attach LoRA adapters
    # ---------------------------------------------------------------
    lora = config.lora
    log.info(
        "Attaching LoRA adapters (r={r}, alpha={a})",
        r=lora.r, a=lora.lora_alpha,
    )

    model = FastModel.get_peft_model(
        model,
        finetune_vision_layers=lora.finetune_vision_layers,
        finetune_language_layers=lora.finetune_language_layers,
        finetune_attention_modules=lora.finetune_attention_modules,
        finetune_mlp_modules=lora.finetune_mlp_modules,
        r=lora.r,
        lora_alpha=lora.lora_alpha,
        lora_dropout=lora.lora_dropout,
        bias=lora.bias,
        random_state=lora.random_state,
    )

    # Print trainable parameter count
    trainable = sum(p.numel() for p in model.parameters() if p.requires_grad)
    total = sum(p.numel() for p in model.parameters())
    log.info(
        "Trainable params: {t:,} / {a:,} ({p:.2f}%)",
        t=trainable, a=total, p=100 * trainable / total,
    )

    # ---------------------------------------------------------------
    # 4. Set up chat template
    # ---------------------------------------------------------------
    tokenizer = get_chat_template(tokenizer, chat_template="gemma-3")

    # ---------------------------------------------------------------
    # 5. Load and format dataset
    # ---------------------------------------------------------------
    log.info("Loading dataset for task: {t}", t=config.task.value)
    dataset = load_dataset_for_training(config.task, config.dataset_path)

    # Standardize to Unsloth format
    dataset = standardize_data_formats(dataset)

    def _format_conversations(examples):
        """Apply chat template to conversations."""
        convos = examples["conversations"]
        texts = [
            tokenizer.apply_chat_template(
                convo, tokenize=False, add_generation_prompt=False,
            ).removeprefix("<bos>")
            for convo in convos
        ]
        return {"text": texts}

    dataset = dataset.map(_format_conversations, batched=True)
    log.info("Dataset ready: {n} examples", n=len(dataset))

    # ---------------------------------------------------------------
    # 6. Configure SFTTrainer
    # ---------------------------------------------------------------
    output_dir = str(config.model_output_dir)
    log.info("Output directory: {d}", d=output_dir)

    sft_config = SFTConfig(
        dataset_text_field="text",
        per_device_train_batch_size=config.per_device_train_batch_size,
        gradient_accumulation_steps=config.gradient_accumulation_steps,
        warmup_steps=config.warmup_steps,
        num_train_epochs=config.num_train_epochs,
        max_steps=config.max_steps,
        learning_rate=config.learning_rate,
        logging_steps=config.logging_steps,
        optim=config.optim,
        weight_decay=config.weight_decay,
        lr_scheduler_type=config.lr_scheduler_type,
        seed=config.seed,
        output_dir=output_dir,
        report_to="none",
        packing=config.packing,
        fp16=not torch.cuda.is_bf16_supported(),
        bf16=torch.cuda.is_bf16_supported(),
    )

    trainer = SFTTrainer(
        model=model,
        tokenizer=tokenizer,
        train_dataset=dataset,
        eval_dataset=None,
        args=sft_config,
    )

    # Mask instruction tokens — model only learns responses
    if config.train_on_responses_only:
        log.info("Enabling train_on_responses_only (masking instruction tokens)")
        trainer = train_on_responses_only(
            trainer,
            instruction_part=GEMMA_INSTRUCTION_PART,
            response_part=GEMMA_RESPONSE_PART,
        )

    # ---------------------------------------------------------------
    # 7. Train
    # ---------------------------------------------------------------
    log.info("Starting training...")
    stats = trainer.train()
    log.info(
        "Training complete — loss={l:.4f}, runtime={r:.1f}s",
        l=stats.training_loss,
        r=stats.metrics.get("train_runtime", 0),
    )

    # ---------------------------------------------------------------
    # 8. Save LoRA adapter
    # ---------------------------------------------------------------
    lora_dir = Path(output_dir) / "lora"
    model.save_pretrained(str(lora_dir))
    tokenizer.save_pretrained(str(lora_dir))
    log.info("LoRA adapter saved to {d}", d=lora_dir)

    return lora_dir


def export_gguf(
    config: "TrainingConfig",
    quantization: str = "q8_0",
) -> Path:
    """Export a trained model to GGUF for llama.cpp deployment.

    Must be called after train() or with an existing LoRA adapter.

    Parameters:
        config: Training config (uses output_dir to find the model).
        quantization: GGUF quantization method. Options:
            - q8_0: 8-bit (best quality, ~1GB for 270M)
            - q4_k_m: 4-bit K-quant medium (good balance)
            - q5_k_m: 5-bit K-quant medium
            - f16: Float16 (largest, highest quality)
            - bf16: BFloat16

    Returns:
        Path to the exported GGUF directory.
    """
    try:
        from unsloth import FastModel
    except ImportError:
        raise ImportError("Unsloth is required for GGUF export.")

    output_dir = config.model_output_dir
    lora_dir = output_dir / "lora"
    gguf_dir = output_dir / "gguf"

    if not lora_dir.exists():
        raise FileNotFoundError(
            f"LoRA adapter not found at {lora_dir}. Run train() first."
        )

    log.info("Loading model for GGUF export...")
    model, tokenizer = FastModel.from_pretrained(
        model_name=str(lora_dir),
        max_seq_length=config.max_seq_length,
        load_in_4bit=config.load_in_4bit,
    )

    log.info(
        "Exporting GGUF ({q}) to {d}",
        q=quantization, d=gguf_dir,
    )
    model.save_pretrained_gguf(
        str(gguf_dir),
        tokenizer,
        quantization_method=quantization,
    )

    log.info("GGUF export complete: {d}", d=gguf_dir)
    return gguf_dir


def merge_and_save(config: "TrainingConfig") -> Path:
    """Merge LoRA adapter into base model and save full weights.

    Parameters:
        config: Training config.

    Returns:
        Path to the merged model directory.
    """
    try:
        from unsloth import FastModel
    except ImportError:
        raise ImportError("Unsloth is required for model merging.")

    output_dir = config.model_output_dir
    lora_dir = output_dir / "lora"
    merged_dir = output_dir / "merged"

    if not lora_dir.exists():
        raise FileNotFoundError(
            f"LoRA adapter not found at {lora_dir}. Run train() first."
        )

    log.info("Loading model for merging...")
    model, tokenizer = FastModel.from_pretrained(
        model_name=str(lora_dir),
        max_seq_length=config.max_seq_length,
        load_in_4bit=config.load_in_4bit,
    )

    log.info("Merging LoRA and saving to {d}", d=merged_dir)
    model.save_pretrained_merged(str(merged_dir), tokenizer)

    log.info("Merged model saved: {d}", d=merged_dir)
    return merged_dir


def test_inference(config: "TrainingConfig", prompt: str | None = None) -> str:
    """Run a quick inference test on the trained model.

    Parameters:
        config: Training config.
        prompt: Test prompt. If None, uses a task-appropriate default.

    Returns:
        Model's generated response.
    """
    try:
        import torch
        from unsloth import FastModel
    except ImportError:
        raise ImportError("Unsloth + PyTorch required for inference.")

    from transformers import TextStreamer

    output_dir = config.model_output_dir
    lora_dir = output_dir / "lora"

    log.info("Loading model for inference test...")
    model, tokenizer = FastModel.from_pretrained(
        model_name=str(lora_dir),
        max_seq_length=config.max_seq_length,
        load_in_4bit=config.load_in_4bit,
    )

    # Default test prompts per task
    default_prompts = {
        "signal_classifier": (
            "Breaking: Federal Reserve announced emergency 50bp rate cut. "
            "Treasury yields dropping sharply. Equity futures surging."
        ),
        "anomaly_narrator": (
            "Feature: SPX_DAILY_RETURN\nValue: -6.8%\nExpected: -0.1%\n"
            "Z-score: -5.2\nPeriod: 2026-04-05\nContext: Largest single-day "
            "drop since March 2020. VIX above 40."
        ),
        "edgar_extractor": (
            "Extract these fields: company_name, filing_type, total_revenue, "
            "net_income, filing_date\n\nFiling text:\n"
            "AMAZON.COM INC\nFORM 10-Q\nQuarter ended March 31, 2026\n"
            "Net revenue: $155.7 billion\nNet income: $12.3 billion\n"
            "Filed: April 30, 2026"
        ),
        "knowledge_mapper": (
            "BlackRock increased its Bitcoin ETF holdings to $45B while simultaneously "
            "lobbying the SEC for spot Ethereum ETF approval. Larry Fink reversed his "
            "2017 anti-crypto stance. BlackRock also manages $2.3T in retirement assets "
            "for state pension funds."
        ),
    }

    if prompt is None:
        prompt = default_prompts.get(config.task.value, "Hello, what can you do?")

    messages = [
        {"role": "system", "content": config.system_prompt},
        {"role": "user", "content": prompt},
    ]

    inputs = tokenizer.apply_chat_template(
        messages,
        add_generation_prompt=True,
        tokenize=True,
        return_tensors="pt",
        return_dict=True,
    ).to("cuda" if torch.cuda.is_available() else "cpu")

    log.info("Generating response...")
    streamer = TextStreamer(tokenizer, skip_prompt=True)
    outputs = model.generate(
        **inputs,
        max_new_tokens=256,
        temperature=0.1,
        top_p=0.95,
        streamer=streamer,
    )

    response = tokenizer.decode(outputs[0][inputs["input_ids"].shape[1]:], skip_special_tokens=True)
    return response


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="GRID Gemma Fine-Tuning with Unsloth",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--task",
        type=str,
        choices=["signal_classifier", "anomaly_narrator", "edgar_extractor", "knowledge_mapper"],
        default="signal_classifier",
        help="Which micro model task to train.",
    )
    parser.add_argument(
        "--base-model",
        type=str,
        default="gemma4-e4b",
        help="Base model key (gemma4-e2b, gemma4-e4b, gemma4-26b, gemma4-31b, gemma3-270m, gemma3-1b, gemma3-4b) or HF model ID.",
    )
    parser.add_argument(
        "--max-seq-length",
        type=int,
        default=2048,
        help="Maximum sequence length.",
    )
    parser.add_argument(
        "--no-4bit",
        action="store_true",
        help="Disable 4-bit quantization (uses more VRAM).",
    )
    parser.add_argument(
        "--lora-r",
        type=int,
        default=8,
        help="LoRA rank. Use 8 for <1000 examples, 16 for larger datasets.",
    )
    parser.add_argument(
        "--lora-alpha",
        type=int,
        default=None,
        help="LoRA alpha. Defaults to same as lora-r (1:1 ratio).",
    )
    parser.add_argument(
        "--lr",
        type=float,
        default=2e-4,
        help="Learning rate. Use 2e-4 for short runs, 2e-5 for long runs.",
    )
    parser.add_argument(
        "--epochs",
        type=int,
        default=3,
        help="Number of training epochs.",
    )
    parser.add_argument(
        "--max-steps",
        type=int,
        default=-1,
        help="Max training steps (-1 = use epochs).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2,
        help="Per-device training batch size.",
    )
    parser.add_argument(
        "--grad-accum",
        type=int,
        default=4,
        help="Gradient accumulation steps. Effective batch = batch_size * grad_accum.",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default=None,
        help="Path to custom JSONL dataset. If not set, uses built-in synthetic data.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="outputs/gemma_training",
        help="Output directory for checkpoints and exports.",
    )
    parser.add_argument(
        "--export-gguf",
        type=str,
        default=None,
        metavar="QUANT",
        help="Export to GGUF after training. Quantization: q8_0, q4_k_m, q5_k_m, f16, bf16.",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge LoRA into base model after training.",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run inference test after training.",
    )
    parser.add_argument(
        "--test-prompt",
        type=str,
        default=None,
        help="Custom prompt for inference test.",
    )

    parser.add_argument(
        "--preset",
        action="store_true",
        help="Use recommended preset config for the chosen model size. "
             "Overrides lora-r, lora-alpha, lr, batch-size, grad-accum, "
             "and 4bit settings with model-appropriate best practices.",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """CLI entry point."""
    args = _parse_args(argv)

    from gemma.training.config import LoRAConfig, TaskType, TrainingConfig, get_preset_config

    task = TaskType(args.task)

    if args.preset:
        # Use model-size-specific best practices
        log.info("Using preset config for {m}", m=args.base_model)
        cfg = get_preset_config(args.base_model, task)
    else:
        lora_alpha = args.lora_alpha if args.lora_alpha is not None else args.lora_r
        cfg = TrainingConfig(
            task=task,
            base_model=args.base_model,
            max_seq_length=args.max_seq_length,
            load_in_4bit=not args.no_4bit,
            lora=LoRAConfig(r=args.lora_r, lora_alpha=lora_alpha),
            per_device_train_batch_size=args.batch_size,
            gradient_accumulation_steps=args.grad_accum,
            num_train_epochs=args.epochs,
            max_steps=args.max_steps,
            learning_rate=args.lr,
            output_dir=args.output_dir,
            dataset_path=args.dataset,
        )

    # Train
    train(cfg)

    # Optional: merge LoRA into base model
    if args.merge:
        merge_and_save(cfg)

    # Optional: export GGUF
    if args.export_gguf:
        export_gguf(cfg, quantization=args.export_gguf)

    # Optional: inference test
    if args.test:
        response = test_inference(cfg, args.test_prompt)
        print(f"\n{'='*60}")
        print("INFERENCE TEST RESULT:")
        print(f"{'='*60}")
        print(response)

    log.info("All done! Model artifacts in: {d}", d=cfg.model_output_dir)


if __name__ == "__main__":
    main()
