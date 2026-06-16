# Hermes iMessage Bridge

The bridge lets one allowlisted iMessage sender talk to Hermes without exposing
raw shell access. It reads Messages on the Mac, accepts only slash commands,
writes every decision to an audit log, and queues accepted work in Obsidian.

## Safety model

- Sender allowlist is required. Set `HERMES_IMESSAGE_ALLOWED_SENDERS` to your
  phone number and/or Apple ID email.
- Direct chats only by default. Group chats are rejected.
- Plain prose is ignored. Commands must be explicit slash commands.
- Risky commands require a second approval text from the same sender.
- The bridge writes queue items; it does not run arbitrary shell.

## Commands

- `/help`
- `/status`
- `/queue`
- `/ask <message for Hermes>`
- `/todo <task for Hermes>`
- `/fleet [scope]`
- `/render <job description>`
- `/restart <grid-hermes|grid-api|grid-llamacpp|storymill|storymill-comfyui>`

Restart requests reply with a token:

```text
Confirm Hermes /restart grid-hermes with: approve 492100
```

Only the same allowlisted sender can approve the token.

## Run on the Mac

Messages.app must be signed in and the terminal/service account must have Full
Disk Access so it can read `~/Library/Messages/chat.db`.

```bash
cd /Users/anikdang/dev/GRID
export HERMES_IMESSAGE_ALLOWED_SENDERS="+15551112222,anik@example.com"
python3 scripts/hermes_imessage_bridge.py --once --print-replies
```

For the real bridge:

```bash
python3 scripts/hermes_imessage_bridge.py --send-replies --poll-interval 15
```

Default outputs:

- Queue: `~/dev/obsidian-vault/Inbox/Hermes-Command-Queue.jsonl`
- Audit: `~/dev/obsidian-vault/00-Agent-Reports/hermes-imessage-bridge.jsonl`
- State: `~/.grid/hermes-imessage-state.json`
- Pending approvals: `~/.grid/hermes-imessage-pending.json`

## Hermes consumption

Hermes or a supervising agent should treat the queue as an operator inbox. Each
line is JSON with `source=imessage`, the normalized sender, command, args,
approval state, and original Messages row id. The queue is append-only so
Obsidian sync and crash recovery can reconstruct what happened.
