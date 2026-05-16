# Windows to Mac Remote Access Runbook

This is the repo-owned handoff for getting the Windows ANIK/ANIK-PC box back
onto the home Mac path safely. It covers the Windows-to-Mac path first:
Tailscale reachability, SSH to the Mac mini, and the GUI/RDP decision boundary.

## Current Known State

- Home Mac mini SSH target: `anikdang@100.120.20.120`.
- Windows ANIK historical Tailscale IP: `100.94.80.45`.
- Windows SSH account name: `anikd`.
- Mac-to-Windows SSH aliases that have existed on the Mac side: `anik`,
  `windows-anik`, `anik-windows`.
- Win 11 Home does not include an inbound RDP server. Do not try to turn the
  dirty user Windows profile into a GRID worker just because RDP is absent.
- RustDesk is the chosen full-GUI path when ANIK-PC is reachable again.
- The GRID worker checkout should be dedicated and separate from
  `C:\Users\anikd\dev\GRID`.

## Code and Docs Work

Run the preflight from an ordinary PowerShell session on Windows:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\server_setup\windows-to-mac-remote-access-preflight.ps1 `
  -MacTailscaleIp 100.120.20.120 `
  -MacSshUser anikdang `
  -ExpectedWindowsTailscaleIp 100.94.80.45 `
  -JsonReportPath "$env:TEMP\grid-windows-to-mac-preflight.json"
```

The script is intentionally read-only. It checks:

- `tailscale.exe` exists and `tailscale status --json` is readable.
- The local Windows node has a tailnet IP, preferably the known ANIK IP.
- `ssh.exe` exists.
- The Mac mini Tailscale IP is in `100.64.0.0/10`.
- The Mac mini is reachable by ICMP if ICMP is allowed.
- TCP `:22` to the Mac mini is reachable.
- Non-interactive SSH auth can run with `BatchMode=yes`.
- `mstsc.exe` exists as a client-side RDP tool.
- TCP `:3389` is reported, but closed is expected for the Mac mini flow unless
  a deliberate RDP gateway/server exists.

Warnings are not automatic blockers. Failures are blockers for this access
path. The JSON report is safe to attach to an agent handoff.

## Manual Tailnet/Admin Steps

Keep these outside scripts and commits:

1. Sign into Tailscale on Windows and approve the machine in the Tailscale admin
   console if it is expired, disabled, or reauth required.
2. Confirm the machine identity. If ANIK-PC gets a new tailnet IP, update this
   runbook, the coordinator notes, and any SSH aliases before registering a
   worker. Do not create a duplicate worker row just because the hostname moved.
3. On the Mac mini, enable Remote Login for `anikdang` if TCP `:22` is closed.
4. If the Windows user key is missing, create or reuse a key under
   `C:\Users\anikd\.ssh`, verify its fingerprint out of band, and add the
   public key to the Mac mini `~/.ssh/authorized_keys` manually.
5. Install and authorize RustDesk on both endpoints for full GUI access. Do not
   enable arbitrary RDP exposure as a substitute.
6. If Mac-to-Windows SSH is being repaired, remember that Windows admin-group
   accounts use `C:\ProgramData\ssh\administrators_authorized_keys`; edits
   there require elevated PowerShell.

## Follow-On GRID Worker Bringup

Only after the remote-access preflight is clean enough to operate the Windows
box:

```powershell
# Use a dedicated checkout outside C:\Users\anikd\dev\GRID.
python scripts\worker.py `
  --hostname Anik-PC `
  --coordinator http://100.75.185.36:8100 `
  --heartbeat-only `
  --max-concurrent 1
```

Hardware note for scheduling: ANIK-PC is a CPU-only Snapdragon Windows box with
16 GB RAM. Do not assign CUDA or GPU jobs.

## Troubleshooting

| Symptom | Likely Cause | Action |
| --- | --- | --- |
| `tailscale.exe` missing | Tailscale is not installed or not in PATH | Install Tailscale manually and sign in through the UI. |
| No local tailnet IP | Device is not authorized or not logged in | Approve/re-auth in the Tailscale admin console. |
| Ping fails but TCP `:22` passes | ICMP blocked | Treat as acceptable; keep the TCP/SSH evidence. |
| TCP `:22` fails | Mac Remote Login off, Mac asleep, or Tailscale disconnected | Wake/check the Mac, Tailscale, and Remote Login. |
| SSH key probe warns | Network path works but auth does not | Add the Windows public key to Mac `authorized_keys` manually. |
| RDP `:3389` closed | Expected for the Mac mini path | Use RustDesk or Mac Screen Sharing for GUI access. |
| Windows tailnet IP changed | Reauth/replacement machine | Update docs and coordinator state before starting worker. |

## Reporting

Every substantial repair session still needs an Obsidian-synced report. On the
Mac mini:

```bash
~/scripts/agent_hub/report_to_hub.sh codex grid-windows-to-mac-remote-access <markdown-file>
```

Manual asks must also be reflected in:

```text
/Users/anikdang/dev/obsidian-vault/Inbox/Agent-TODO.md
```
