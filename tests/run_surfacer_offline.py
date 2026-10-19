"""Run only bounded synthetic Surfacer tests; never import config/auth/db."""
import os
from pathlib import Path
import socket
import sys
from types import ModuleType
root = Path(__file__).resolve().parents[1]
os.chdir(root)
sys.path.insert(0, str(root))
def blocked(*args, **kwargs):
    raise AssertionError('Network/socket connection prohibited in offline harness')
socket.socket.connect = blocked
socket.socket.connect_ex = blocked
socket.create_connection = blocked
# Install fail-closed seams BEFORE test collection, unlike fallback-only test stubs.
auth = ModuleType('api.auth')
auth.require_auth = blocked
sys.modules['api.auth'] = auth
deps = ModuleType('api.dependencies')
deps.get_db_engine = blocked
sys.modules['api.dependencies'] = deps
import pytest
result = pytest.main(['-q', '-p', 'no:cacheprovider', 'tests/test_surfacer_api.py', 'tests/test_surfacer_request_cache.py'])
assert 'config' not in sys.modules and 'db' not in sys.modules, 'Live configuration/database module imported'
raise SystemExit(result)
