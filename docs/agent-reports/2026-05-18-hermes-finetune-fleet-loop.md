# Hermes Fine-Tune and Agent Fleet Loop

## What changed

- Added an allowlisted `scripts/hermes_imessage_bridge.py` command router for owner-only Hermes input. It queues slash commands to Obsidian JSONL and approval-gates risky restarts instead of exposing shell.
- Added `scripts/hermes_finetune_dataset.py` to build privacy-scrubbed chat-style SFT JSONL from the Hermes command queue plus synced agent reports.
- Added `hermes_operator` as a first-class Gemma/Unsloth training task so the generated JSONL can be used by the existing training runner.
- Added `docs/hermes-agent-fleet.json` and `scripts/hermes_agent_fleet_audit.py` to check that registered Hermes agent prompts, runbooks, tests, and service units exist and are recently maintained.
- Added `server_setup/hermes-agent-fleet-audit.{service,timer}` for a read-only 30-minute audit loop on `grid-svr`.
- Added runbooks for the iMessage bridge and fine-tune/fleet loop.

## Verification

- `python3 -m py_compile scripts/hermes_imessage_bridge.py scripts/hermes_finetune_dataset.py scripts/hermes_agent_fleet_audit.py gemma/training/config.py gemma/training/datasets.py gemma/training/train.py`
- `python3 -m pytest tests/test_hermes_imessage_bridge.py tests/test_hermes_finetune_dataset.py tests/test_hermes_agent_fleet_audit.py tests/test_hermes_operator_training_task.py` -> 17 passed.
- Built first-pass SFT JSONL: `/Volumes/4tb Backup/agent-scratch/hermes_finetune/hermes_sft.jsonl` with 13 examples.
- Ran secret-pattern smoke on the SFT JSONL; no matching `sk-`, `hf_`, bearer token, or env secret patterns remained.
- Ran agent fleet audit: `/Volumes/4tb Backup/agent-scratch/hermes_finetune/hermes_agent_fleet_audit.md`; result was 5 current, 0 missing, 0 stale.

## Unresolved blockers

- The new systemd timer is not installed on `grid-svr` yet. It should be enabled only after the branch lands in the deployed GRID checkout.
- The SFT JSONL is a starter dataset, not an approved training corpus. Review it before any LoRA/SFT job.

## Manual asks

- Deploy and enable `server_setup/hermes-agent-fleet-audit.timer` on `grid-svr` after merge, then confirm `/data/grid/reports/hermes-agent-fleet-audit.{json,md}` updates every 30 minutes.
