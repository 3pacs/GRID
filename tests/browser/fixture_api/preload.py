"""Explicitly unmodeled preload surfaces; never a fake successful API payload.

The allowlist is source-checked without importing GRID's database-backed app.
HTTP 501 is a fixture coverage marker, not a production route/status claim.
"""
import json
from pathlib import Path

INVENTORY = json.loads(Path(__file__).with_name('preload_coverage.json').read_text(encoding='utf8'))
BY_PATH = {row['path']: row for row in INVENTORY}


def unavailable(path):
    row = BY_PATH.get(path)
    if row is None:
        return None
    return {
        'error': True,
        'code': 'fixture_preload_unavailable',
        'fixture_scope': 'outside-spy-journey',
        'path': path,
        'reason': 'Background preload contract is not modeled by this SPY journey fixture.',
        'source_handler': f"{row['source']}::{row['handler']}",
    }
