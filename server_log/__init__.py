"""Server-side logging that pushes sanitized errors to git.

This module provides three capabilities:

1. **Sanitizer** — scrubs secrets, tokens, and credentials from log messages
   before they leave the process.
2. **GitSink** — a loguru sink that buffers ERROR+ messages into a JSONL file,
   commits, and pushes to git on a timer so an absent operator can review.
3. **Inbox** — the server periodically pulls and reads operator commands from
   a git-tracked inbox file, enabling two-way communication with no SSH/VPN.

All files live under ``<repo_root>/.server-logs/``.
"""
