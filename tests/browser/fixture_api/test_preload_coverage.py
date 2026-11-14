"""Local-only source/HTTP coverage checks; never import the real GRID app."""
import ast
import json
import re
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, ProxyHandler, build_opener

import preload
from server import FixtureHandler, ROUTES

ROOT = Path(__file__).resolve().parents[3]


class QuietFixture(FixtureHandler):
    def log_message(self, *_):
        pass


class PreloadCoverage(unittest.TestCase):
    def test_spy_contract_selection_preserves_main_legacy_and_godview_fixtures(self):
        class PartialFixture(QuietFixture):
            scenario = 'partial'

        server = ThreadingHTTPServer(('127.0.0.1', 0), PartialFixture)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        opener = build_opener(ProxyHandler({}))
        base = f'http://127.0.0.1:{server.server_port}'
        try:
            with opener.open(base + '/api/v1/gamma-watch/state', timeout=3) as r:
                legacy = json.load(r)['data']
            self.assertNotIn('schema', legacy)
            self.assertTrue(legacy['quote']['is_rtd'])
            request = Request(base + '/api/v1/gamma-watch/state', headers={'X-Grid-Fixture-Journey': 'spy-options'})
            with opener.open(request, timeout=3) as r:
                spy = json.load(r)['data']
            self.assertEqual(spy['schema'], 'gex-screen/2')
            self.assertGreater(spy['served_at'] - spy['public_price']['source_time'], 7000)
            with opener.open(base + '/api/v1/godview/latest', timeout=3) as r:
                godview = json.load(r)
            self.assertEqual(godview['pillars']['fed_liquidity']['status'], 'stale')
            self.assertEqual(sum(method == 'GET' and bool(pattern.fullmatch('/api/v1/godview/latest')) for method, pattern, _ in ROUTES), 1)
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=2)

    def test_inventory_binds_to_real_preload_paths_and_registered_source_handlers(self):
        source = (ROOT / 'pwa/src/app.jsx').read_text(encoding='utf8')
        block = re.search(r'const OPERATOR_PRELOAD_API_PATHS = \[(.*?)\];', source, re.S).group(1)
        paths = {p.split('?')[0] for p in re.findall(r"'([^']+)'", block)}
        self.assertEqual(len(preload.BY_PATH), 14)
        self.assertEqual(len(preload.INVENTORY), len(preload.BY_PATH))
        registry = (ROOT / 'api/main.py').read_text(encoding='utf8')
        for row in preload.INVENTORY:
            with self.subTest(path=row['path']):
                self.assertIn(row['path'], paths)
                self.assertIn(f'"{row["registry_module"]}"', registry)
                tree = ast.parse((ROOT / row['source']).read_text(encoding='utf8'))
                handler = next(n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == row['handler'])
                routes = [d.args[0].value for d in handler.decorator_list if isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute) and d.func.attr == 'get' and d.args and isinstance(d.args[0], ast.Constant)]
                parent = ast.parse((ROOT / row.get('facade', row['source'])).read_text(encoding='utf8'))
                prefixes = [k.value.value for n in ast.walk(parent) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == 'APIRouter' for k in n.keywords if k.arg == 'prefix' and isinstance(k.value, ast.Constant)]
                self.assertTrue(any(prefix + route == row['path'] for prefix in prefixes for route in routes))
                if row.get('facade'):
                    facade = (ROOT / row['facade']).read_text(encoding='utf8')
                    self.assertIn(Path(row['source']).stem, facade)
                    self.assertIn('router.include_router(', facade)
        # Every current app-wide preload must be either modeled or explicitly
        # omitted. New preload paths cannot silently add another mystery 404.
        for path in paths:
            self.assertTrue(path in preload.BY_PATH or any(method == 'GET' and pattern.fullmatch(path) for method, pattern, _ in ROUTES), path)

    def test_http_marks_only_omitted_preloads_without_faking_success(self):
        server = ThreadingHTTPServer(('127.0.0.1', 0), QuietFixture)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        opener = build_opener(ProxyHandler({}))
        base = f'http://127.0.0.1:{server.server_port}'
        try:
            for row in preload.INVENTORY:
                with self.subTest(path=row['path']):
                    with self.assertRaises(HTTPError) as ctx:
                        opener.open(base + row['path'] + '?limit=8', timeout=2)
                    self.assertEqual(ctx.exception.code, 501)
                    body = json.load(ctx.exception)
                    self.assertEqual(body['path'], row['path'])
                    self.assertEqual(body['code'], 'fixture_preload_unavailable')
                    self.assertEqual(body['source_handler'], f"{row['source']}::{row['handler']}")
            # A real fixture still wins; omitted support is not a catch-all.
            with opener.open(base + '/api/v1/options/signals', timeout=2) as r:
                self.assertEqual(r.status, 200)
                self.assertEqual(json.load(r), {'signals': []})
            for request in [Request(base + '/api/v1/not-a-real-preload'), Request(base + '/api/v1/watchlist/prices', method='POST')]:
                with self.assertRaises(HTTPError) as ctx:
                    opener.open(request, timeout=2)
                self.assertEqual(ctx.exception.code, 404)
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=2)


if __name__ == '__main__':
    unittest.main()
