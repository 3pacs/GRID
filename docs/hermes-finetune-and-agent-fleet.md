# Hermes Fine-Tune and Agent Fleet Loop

This is the first durable loop for making Hermes more useful as the local fleet
operator without giving it unsafe authority too early.

## Operating model

1. Owner input enters through safe channels: Obsidian, agent reports, GitHub
   issues/PRs, or the allowlisted iMessage bridge.
2. Hermes queues work as structured JSONL, audits the decision, and prefers
   read-only inspection before mutation.
3. The fine-tune dataset builder turns approved commands and completed reports
   into privacy-scrubbed SFT examples.
4. The agent-fleet audit checks whether the registered agent prompts, scripts,
   tests, and systemd units are present and recently maintained.
5. A training run should only start after reviewing the generated JSONL for
   secrets, stale guidance, and low-quality examples.

## Build the starting SFT dataset

Run on a machine with access to the synced vault:

```bash
cd /Users/anikdang/dev/GRID
python3 scripts/hermes_finetune_dataset.py \
  --queue ~/dev/obsidian-vault/Inbox/Hermes-Command-Queue.jsonl \
  --reports-dir ~/dev/obsidian-vault/00-Agent-Reports \
  --output /Volumes/4tb\ Backup/agent-scratch/hermes_finetune/hermes_sft.jsonl
```

On `grid-svr`, prefer:

```bash
python3 scripts/hermes_finetune_dataset.py \
  --output /data/agent-home/anikdang/hermes_finetune/hermes_sft.jsonl
```

Then start a small Hermes operator LoRA with the existing Gemma/Unsloth runner:

```bash
python3 -m gemma.training.train \
  --task hermes_operator \
  --base-model gemma4-e2b \
  --dataset /data/agent-home/anikdang/hermes_finetune/hermes_sft.jsonl \
  --output-dir /data/agent-home/anikdang/hermes_finetune/runs \
  --max-steps 25 \
  --lora-r 8 \
  --lora-alpha 8
```

Use this as a smoke LoRA only. Do not promote it over the live Hermes model
until held-out operator scenarios pass.

The JSONL uses chat-style records:

```json
{"messages":[{"role":"system","content":"..."},{"role":"user","content":"..."},{"role":"assistant","content":"..."}],"source":"queue:imsg-...","tags":["queue","fleet"]}
```

## Keep agents current

The registry is:

```text
docs/hermes-agent-fleet.json
```

Manual run:

```bash
python3 scripts/hermes_agent_fleet_audit.py \
  --registry docs/hermes-agent-fleet.json \
  --output-json output/hermes_agent_fleet_audit.json \
  --output-md output/hermes_agent_fleet_audit.md
```

Systemd timer files:

```text
server_setup/hermes-agent-fleet-audit.service
server_setup/hermes-agent-fleet-audit.timer
```

The audit is read-only. It reports missing or stale agent assets; it does not
restart services, edit prompts, or mutate tailnet state.

## Next hardening step

After the first reviewed dataset exists, run a small LoRA/SFT on `grid-svr` or a
larger GPU node, then compare Hermes responses against held-out operator
scenarios:

- fleet audit scope request
- crash recovery request
- risky service restart request
- stale agent prompt detection
- Storymill render-lane request with large-file storage constraints
