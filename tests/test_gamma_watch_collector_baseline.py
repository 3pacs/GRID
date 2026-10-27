"""Offline source-import regression checks; never start the collector."""
import ast
import hashlib
import json
import re
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1] / "collectors" / "gamma_watch"


def test_import_matches_captured_manifest():
    manifest = json.loads((ROOT / "IMPORT-MANIFEST.json").read_text())
    assert len(manifest["files"]) == 16
    for entry in manifest["files"]:
        body = (ROOT / entry["path"]).read_bytes()
        if entry["path"] == "index.html":
            # The manifest is an immutable import receipt, not a current-page
            # checksum. Verify the original bytes beneath the additive alert.
            body, additions = re.subn(
                rb'<script id="gamma-regime-alert">.*?</script>\n',
                b"", body, flags=re.S)
            assert additions == 1
        assert hashlib.sha256(body).hexdigest() == entry["repository_lf_sha256"]


def test_python_syntax():
    for path in ROOT.glob("*.py"):
        ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))


def test_offline_collector_fixtures():
    script = """
import socket, unittest
def blocked(*args, **kwargs):
    raise AssertionError('network forbidden in collector baseline tests')
socket.socket.connect = blocked
socket.socket.connect_ex = blocked
socket.create_connection = blocked
suite = unittest.defaultTestLoader.discover('.', pattern='test_*.py')
result = unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(not result.wasSuccessful())
"""
    result = subprocess.run([sys.executable, "-B", "-c", script], cwd=ROOT,
                            capture_output=True, text=True, timeout=90)
    assert result.returncode == 0, result.stdout + result.stderr
