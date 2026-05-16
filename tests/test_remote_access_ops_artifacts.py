from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "server_setup" / "windows-to-mac-remote-access-preflight.ps1"
RUNBOOK = ROOT / "server_setup" / "windows-to-mac-remote-access.md"
README = ROOT / "server_setup" / "README.md"


def test_remote_access_artifacts_exist_and_are_linked():
    assert SCRIPT.exists()
    assert RUNBOOK.exists()

    readme = README.read_text()
    assert "windows-to-mac-remote-access.md" in readme
    assert "windows-to-mac-remote-access-preflight.ps1" in readme


def test_preflight_script_is_read_only_and_covers_required_checks():
    script = SCRIPT.read_text()

    required_terms = [
        "tailscale.exe",
        "status --json",
        "ssh.exe",
        "BatchMode=yes",
        "mstsc.exe",
        "3389",
        "100.120.20.120",
        "100.94.80.45",
        "Set-StrictMode -Version Latest",
    ]
    for term in required_terms:
        assert term in script

    forbidden_terms = [
        "tailscale up",
        "Enable-PSRemoting",
        "Enable-NetFirewallRule",
        "New-NetFirewallRule",
        "Restart-Service",
        "Set-Service",
        "Set-ItemProperty",
    ]
    lowered = script.lower()
    for term in forbidden_terms:
        assert term.lower() not in lowered


def test_runbook_separates_manual_tailnet_steps_from_repo_work():
    runbook = RUNBOOK.read_text()

    required_terms = [
        "## Code and Docs Work",
        "## Manual Tailnet/Admin Steps",
        "## Follow-On GRID Worker Bringup",
        "Tailscale admin console",
        "Remote Login",
        "RustDesk",
        "Win 11 Home",
        "C:\\Users\\anikd\\dev\\GRID",
        "scripts\\worker.py",
        "--hostname Anik-PC",
        "--heartbeat-only",
        "/Users/anikdang/dev/obsidian-vault/Inbox/Agent-TODO.md",
    ]
    for term in required_terms:
        assert term in runbook
