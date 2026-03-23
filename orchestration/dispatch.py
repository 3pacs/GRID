#!/usr/bin/env python3
"""
GRID Task Dispatcher — generates ready-to-paste prompts for external models.

No flags, no config. Just:
    python orchestration/dispatch.py

Walks you through it interactively.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

GRID_ROOT = Path(__file__).parent.parent
BRIEFS = GRID_ROOT / "orchestration" / "briefs"
INBOX = GRID_ROOT / "orchestration" / "inbox"
DISPATCH_LOG = GRID_ROOT / "orchestration" / "dispatch_log.json"

MODELS = {
    "1": {
        "name": "Gemini",
        "brief": "ux",
        "strengths": "UI components, design systems, layout, visual polish",
        "url": "https://gemini.google.com",
    },
    "2": {
        "name": "ChatGPT / Codex",
        "brief": "algo",
        "strengths": "algorithms, math, feature engineering, logic puzzles",
        "url": "https://chat.openai.com",
    },
    "3": {
        "name": "Perplexity",
        "brief": "perplexity",
        "strengths": "data source research, API discovery, finance questions",
        "url": "https://perplexity.ai",
    },
    "4": {
        "name": "Copilot",
        "brief": "copilot",
        "strengths": "test stubs, converters, validators, boilerplate",
        "url": "https://copilot.microsoft.com",
    },
}


def load_brief(name: str) -> str:
    """Load a brief template."""
    path = BRIEFS / f"{name}.md"
    if not path.exists():
        return ""
    return path.read_text()


def log_dispatch(model: str, task: str, output_file: str) -> None:
    """Log a dispatch for tracking."""
    log: list = []
    if DISPATCH_LOG.exists():
        try:
            log = json.loads(DISPATCH_LOG.read_text())
        except Exception:
            log = []

    log.append({
        "timestamp": datetime.now().isoformat(),
        "model": model,
        "task": task[:200],
        "expected_output": output_file,
        "status": "dispatched",
    })

    DISPATCH_LOG.write_text(json.dumps(log, indent=2))


def build_prompt(brief_content: str, task: str) -> str:
    """Insert the task into the brief template."""
    # Replace the "Your Task" placeholder
    if "<!-- PASTE YOUR SPECIFIC" in brief_content:
        # Find the Your Task section and replace placeholder lines
        lines = brief_content.splitlines()
        result = []
        in_task_section = False
        replaced = False
        for line in lines:
            if "## Your Task" in line:
                in_task_section = True
                result.append(line)
                result.append(task)
                replaced = True
                continue
            if in_task_section and line.startswith("<!--"):
                continue  # skip placeholder comments
            if in_task_section and line.startswith("## "):
                in_task_section = False  # next section
            result.append(line)
        return "\n".join(result)

    # Fallback: append task at the end
    return brief_content + f"\n\n## Your Task\n{task}\n"


def run_interactive() -> None:
    """Interactive dispatch flow."""
    print("=" * 50)
    print("  GRID Task Dispatcher")
    print("=" * 50)
    print()
    print("Which model should handle this?")
    print()
    for key, m in MODELS.items():
        print(f"  {key}. {m['name']:20s} — {m['strengths']}")
    print(f"  5. {'Claude (me)':20s} — I'll handle it right here")
    print()

    choice = input("Pick [1-5]: ").strip()

    if choice == "5":
        print("\nDescribe the task and I'll do it in this session.")
        return

    model = MODELS.get(choice)
    if not model:
        print(f"Invalid choice: {choice}")
        sys.exit(1)

    print(f"\n--- {model['name']} selected ---")
    print()
    task = input("Describe the task (be specific):\n> ").strip()
    if not task:
        print("No task provided.")
        return

    # Build the prompt
    brief = load_brief(model["brief"])
    prompt = build_prompt(brief, task)

    # Determine output filename
    slug = task.lower()[:40].replace(" ", "_").replace("/", "_")
    slug = "".join(c for c in slug if c.isalnum() or c == "_")
    ext = ".jsx" if model["brief"] == "ux" else ".py"
    if model["brief"] in ("perplexity", "research"):
        ext = ".md"
    output_name = f"{slug}{ext}"

    # Log it
    log_dispatch(model["name"], task, output_name)

    # Print instructions
    print()
    print("=" * 50)
    print(f"  STEP 1: Open {model['name']}")
    print(f"  {model['url']}")
    print("=" * 50)
    print()
    print("  STEP 2: Paste this entire prompt:")
    print()
    print("-" * 50)
    print(prompt)
    print("-" * 50)
    print()
    print("  STEP 3: Copy the response and save it to:")
    print(f"  grid/orchestration/inbox/{output_name}")
    print()
    print("  STEP 4: Come back here and tell Claude:")
    print(f'  "integrate {output_name}"')
    print()
    print("=" * 50)

    # Also write the prompt to a temp file for easy copy
    prompt_file = INBOX / f"_prompt_{output_name}.txt"
    prompt_file.write_text(prompt)
    print(f"\nPrompt also saved to: {prompt_file.relative_to(GRID_ROOT)}")
    print("(You can open this file and copy from there if easier)")


def run_with_args(args: list[str]) -> None:
    """Non-interactive mode: dispatch.py <model_num> <task>"""
    choice = args[0]
    task = " ".join(args[1:])

    model = MODELS.get(choice)
    if not model:
        print(f"Invalid model: {choice}")
        print("Options: " + ", ".join(f"{k}={m['name']}" for k, m in MODELS.items()))
        sys.exit(1)

    brief = load_brief(model["brief"])
    prompt = build_prompt(brief, task)

    slug = task.lower()[:40].replace(" ", "_").replace("/", "_")
    slug = "".join(c for c in slug if c.isalnum() or c == "_")
    ext = ".jsx" if model["brief"] == "ux" else ".py"
    if model["brief"] in ("perplexity", "research"):
        ext = ".md"
    output_name = f"{slug}{ext}"

    log_dispatch(model["name"], task, output_name)

    prompt_file = INBOX / f"_prompt_{output_name}.txt"
    prompt_file.write_text(prompt)

    print(f"Model:  {model['name']} ({model['url']})")
    print(f"Prompt: {prompt_file.relative_to(GRID_ROOT)}")
    print(f"Save response to: orchestration/inbox/{output_name}")
    print(f'\nThen tell Claude: "integrate {output_name}"')


if __name__ == "__main__":
    if len(sys.argv) > 2:
        run_with_args(sys.argv[1:])
    else:
        run_interactive()
