from dataclasses import replace
from datetime import timedelta
import unittest
from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient
from ingestion.realtime.native_minutes import schwab_minutes, select_as_of
from api.research_intervals import build_router, interval_view
from tests.test_research_intervals import T, S, bars


def payload():
    return {
        "symbol": "SPY",
        "candles": [
            dict(
                datetime=int(b.ts.timestamp() * 1000),
                open=b.open,
                high=b.high,
                low=b.low,
                close=b.close,
                volume=b.volume,
            )
            for b in bars(3)
        ],
    }


def parse(p=None, **kw):
    args = dict(
        requested_symbol="SPY",
        frequency_type="minute",
        frequency=1,
        received_at=T + timedelta(minutes=3),
    )
    args.update(kw)
    return schwab_minutes(payload() if p is None else p, **args)


def select(rows, **kw):
    args = dict(
        symbol="SPY",
        source="fixture",
        price_basis="unadjusted",
        as_of=T + timedelta(minutes=3),
        opens=S.opens,
        closes=S.closes,
    )
    args.update(kw)
    return select_as_of(rows, **args)


class TestNativeMinutes(unittest.TestCase):
    def test_parser_keeps_all_ohlcv(self):
        p = parse()
        self.assertEqual(len(p.bars), 3)
        self.assertEqual(p.bars[0].high, 102)
        self.assertEqual(p.bars[-1].available_at, T + timedelta(minutes=3))
        self.assertEqual(p.bars[0].price_basis, "provider_unspecified")

    def test_forming_excluded(self):
        p = parse(received_at=T + timedelta(minutes=2, seconds=30))
        self.assertEqual(len(p.bars), 2)
        self.assertEqual(p.excluded_forming, 1)

    def test_wrong_interval(self):
        with self.assertRaises(ValueError):
            parse(frequency=5)

    def test_wrong_symbol(self):
        with self.assertRaises(ValueError):
            parse(requested_symbol="QQQ")

    def test_bad_ohlcv(self):
        p = payload()
        p["candles"][0]["high"] = 1
        with self.assertRaises(ValueError):
            parse(p)

    def test_duplicate(self):
        p = payload()
        p["candles"].append(p["candles"][0])
        with self.assertRaises(ValueError):
            parse(p)

    def test_future(self):
        with self.assertRaises(ValueError):
            parse(received_at=T - timedelta(minutes=1))

    def test_backfill_unavailable_in_past(self):
        p = parse(received_at=S.closes + timedelta(days=1))
        self.assertEqual(
            select(
                p.bars,
                source="schwab_pricehistory",
                price_basis="provider_unspecified",
                as_of=S.closes,
            ),
            (),
        )

    def test_revision_cutoff(self):
        old = bars(1)[0]
        new = replace(
            old, close=101.5, revision="new", available_at=T + timedelta(minutes=4)
        )
        self.assertEqual(select([old, new])[0], old)
        self.assertEqual(select([old, new], as_of=T + timedelta(minutes=5))[0], new)

    def test_conflicting_simultaneous_revision_refuses(self):
        old = bars(1)[0]
        with self.assertRaises(ValueError):
            select([old, replace(old, revision="different")])

    def test_revision_selection_order_independent(self):
        from itertools import permutations

        old = bars(1)[0]
        conflict = replace(old, revision="ambiguous-old")
        latest = replace(old, revision="new", available_at=T + timedelta(minutes=2))
        for rows in permutations((old, conflict, latest)):
            self.assertEqual(select(rows), (latest,))
            with self.assertRaises(ValueError):
                select(rows, as_of=T + timedelta(minutes=1))

    def test_exact_duplicate_idempotent(self):
        self.assertEqual(len(select(bars(1) * 2)), 1)

    def test_source_isolation(self):
        self.assertEqual(select([replace(bars(1)[0], source="other")]), ())


def auth(x_role: str | None = Header(default=None)):
    if x_role is None:
        raise HTTPException(401)
    if x_role != "admin":
        raise HTTPException(403)


def client(loader=lambda *args: bars(3), calendar=lambda key: S):
    app = FastAPI()
    app.include_router(
        build_router(
            loader=loader, calendar=calendar, authorize=auth, clock=lambda: S.closes
        )
    )
    return TestClient(app)


def params(**kw):
    p = dict(
        symbol="SPY",
        source="fixture",
        session_key=S.key,
        interval="3m",
        as_of=(T + timedelta(minutes=3)).isoformat(),
        price_basis="unadjusted",
    )
    p.update(kw)
    return p


class TestIntervalApi(unittest.TestCase):
    def test_admin_and_private_headers(self):
        r = client().get(
            "/api/v1/research/intervals", params=params(), headers={"x-role": "admin"}
        )
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "complete")
        self.assertFalse(r.json()["live"])
        self.assertEqual(r.json()["bars"][0]["volume"], 30)
        self.assertEqual(r.headers["Cache-Control"], "no-store, private")

    def test_no_auth_never_reads(self):
        def fail(*args):
            raise AssertionError("loader called")

        r = client(loader=fail).get("/api/v1/research/intervals", params=params())
        self.assertEqual(r.status_code, 401)

    def test_nonadmin(self):
        r = client().get(
            "/api/v1/research/intervals", params=params(), headers={"x-role": "viewer"}
        )
        self.assertEqual(r.status_code, 403)

    def test_unconfigured(self):
        r = client(loader=None).get(
            "/api/v1/research/intervals", params=params(), headers={"x-role": "admin"}
        )
        self.assertEqual(r.status_code, 503)

    def test_gap_visible(self):
        r = client(loader=lambda *args: bars(1)).get(
            "/api/v1/research/intervals", params=params(), headers={"x-role": "admin"}
        )
        self.assertEqual(r.json()["status"], "gapped")
        self.assertIsNone(r.json()["bars"][0]["close"])

    def test_empty_coverage(self):
        r = client(loader=lambda *args: []).get(
            "/api/v1/research/intervals", params=params(), headers={"x-role": "admin"}
        )
        self.assertEqual(r.json()["status"], "no_coverage")

    def test_naive_time(self):
        r = client().get(
            "/api/v1/research/intervals",
            params=params(as_of="2026-11-10T14:33:00"),
            headers={"x-role": "admin"},
        )
        self.assertEqual(r.status_code, 422)

    def test_error_sanitized(self):
        def fail(*args):
            raise RuntimeError("secret must not escape")

        r = client(loader=fail).get(
            "/api/v1/research/intervals", params=params(), headers={"x-role": "admin"}
        )
        self.assertEqual(r.status_code, 503)
        self.assertNotIn("secret", r.text)

    def test_unsupported(self):
        r = client().get(
            "/api/v1/research/intervals",
            params=params(interval="1s"),
            headers={"x-role": "admin"},
        )
        self.assertEqual(r.status_code, 422)

    def test_awaiting_window(self):
        result = interval_view(
            bars(1),
            S,
            symbol="SPY",
            source="fixture",
            interval="3m",
            as_of=T + timedelta(minutes=1),
        )
        self.assertEqual(result["status"], "awaiting_closed_window")


if __name__ == "__main__":
    unittest.main()


class TestRetainedExport(unittest.TestCase):
    def test_read_and_query_export(self):
        import tempfile
        import json
        from pathlib import Path
        from dataclasses import asdict
        from ingestion.realtime.native_minutes import retained_jsonl_loader

        with tempfile.TemporaryDirectory() as folder:
            p = Path(folder) / "minutes.jsonl"
            rows = []
            for bar in bars(3):
                row = asdict(bar)
                row["schema"] = "grid.native-minute.v1"
                row["ts"] = row["ts"].isoformat()
                row["available_at"] = row["available_at"].isoformat()
                rows.append(json.dumps(row))
            p.write_text("\n".join(rows))
            r = client(loader=retained_jsonl_loader(p)).get(
                "/api/v1/research/intervals",
                params=params(),
                headers={"x-role": "admin"},
            )
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json()["status"], "complete")

    def test_missing_provenance_refuses(self):
        import tempfile
        from pathlib import Path
        from ingestion.realtime.native_minutes import retained_jsonl_loader

        with tempfile.TemporaryDirectory() as folder:
            p = Path(folder) / "minutes.jsonl"
            p.write_text('{"schema":"grid.native-minute.v1"}')
            with self.assertRaises(ValueError):
                retained_jsonl_loader(p)("SPY", "fixture", S, S.closes)

    def test_bound_refuses(self):
        import tempfile
        from pathlib import Path
        from ingestion.realtime.native_minutes import retained_jsonl_loader

        with tempfile.TemporaryDirectory() as folder:
            p = Path(folder) / "minutes.jsonl"
            p.write_text("x" * 20)
            with self.assertRaises(ValueError):
                retained_jsonl_loader(p, max_bytes=10)("SPY", "fixture", S, S.closes)
