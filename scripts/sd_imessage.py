"""iMessage delivery for stepdad.finance.

Sends real iMessages via the Mac mini (it runs Messages.app); grid-svr's `grid`
user has SSH access to the Mac as `anikdang`. Two Mac-side senders are used:

  - ``~/bin/notify-anik``        → the operator's own configured handle
  - ``~/bin/notify-to <handle>`` → any handle (per-owner, e.g. dad)

Per-owner handles come from the environment so nothing is hard-coded:
  SD_IMESSAGE_DAD, SD_IMESSAGE_OPERATOR  (E.164 phone or iCloud handle)

When an owner has no configured handle we fall back to the operator (tagged),
so an alert always reaches a human rather than being silently dropped.
"""

from __future__ import annotations

import os
import subprocess

from loguru import logger as log

_MAC_SSH = os.environ.get("SD_MAC_SSH", "anikdang@aniks-mac-mini")

_OWNER_HANDLES = {
    "dad": os.environ.get("SD_IMESSAGE_DAD", "").strip(),
    "operator": os.environ.get("SD_IMESSAGE_OPERATOR", "").strip(),
}


def _ssh_send(remote_args: list[str], body: str) -> bool:
    """Run a Mac-side notify command over SSH with the message body on stdin."""
    if not body or not body.strip():
        return False
    try:
        subprocess.run(
            ["ssh", "-o", "ConnectTimeout=8", "-o", "BatchMode=yes",
             _MAC_SSH, *remote_args],
            input=body, text=True, timeout=25, check=True,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        return True
    except Exception as exc:  # never let a notification failure break a flow
        log.debug("iMessage send failed (non-fatal): {e}", e=str(exc))
        return False


def imessage_operator(body: str) -> bool:
    """Text the operator (you) via notify-anik."""
    return _ssh_send(["~/bin/notify-anik"], body)


def imessage_owner(owner: str, body: str) -> bool:
    """Text a specific owner. Falls back to the operator (tagged) if unmapped."""
    handle = _OWNER_HANDLES.get((owner or "").strip(), "")
    if handle:
        return _ssh_send(["~/bin/notify-to", handle], body)
    return imessage_operator(f"[for {owner or 'unknown'}] {body}")
