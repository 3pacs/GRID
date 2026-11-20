"""Disposable network-isolated PG15 test; uses existing image, never pulls.

Runs only the exact options-store schema block and new migration SQL, not full
historical migration replay. No host database, mounts or exposed ports.
"""
import ast
from pathlib import Path
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
schema = (ROOT / 'schema.sql').read_text()
block = schema[schema.index('DO $options_store$'):schema.index('$options_store$;') + len('$options_store$;')]
# This exact pre-column schema retains the original constraints and triggers.
legacy = block.replace('provider_regular_market_at TIMESTAMPTZ,\n        clock_metadata JSONB', 'provider_regular_market_at TIMESTAMPTZ').replace('        clock_metadata       JSONB,\n', '')
tree = ast.parse((ROOT / 'migrations/versions/options_contract_clocks_20261114.py').read_text())
upgrade = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'upgrade')
statements = [ast.literal_eval(n.value.args[0]) for n in upgrade.body]
base_insert = """
INSERT INTO options_capture_batches(capture_batch_id,ticker,snap_date,capture_ordinal,capture_started_at,capture_completed_at,row_count,capture_source)
VALUES('legacy','NVDA','2026-11-13',1,'2026-11-13 21:00Z','2026-11-13 21:01Z',1,'cboe');
INSERT INTO options_snapshots_all(ticker,snap_date,expiry,opt_type,strike,capture_batch_id,capture_ordinal,capture_started_at,capture_completed_at,provider_regular_market_at)
VALUES('NVDA','2026-11-13','2026-11-20','call',200,'legacy',1,'2026-11-13 21:00Z','2026-11-13 21:01Z','2026-11-13 20:59Z');
"""
checks = """
SET ROLE clock_test_writer;
INSERT INTO options_capture_batches(capture_batch_id,ticker,snap_date,capture_ordinal,capture_started_at,capture_completed_at,row_count,capture_source,clock_metadata)
VALUES('new','NVDA','2026-11-13',2,'2026-11-13 22:00Z','2026-11-13 22:01Z',1,'cboe',CAST('{"schema_version":2,"provider":"cboe","bid_ask_updated_at":null,"bid_ask_freshness":"unknown","synchronization":"unknown"}' AS JSONB));
INSERT INTO options_snapshots_all(ticker,snap_date,expiry,opt_type,strike,capture_batch_id,capture_ordinal,capture_started_at,capture_completed_at,provider_regular_market_at,clock_metadata)
VALUES('NVDA','2026-11-13','2026-11-20','call',200,'new',2,'2026-11-13 22:00Z','2026-11-13 22:01Z','2026-11-13 20:59Z',CAST('{"contract_last_trade_raw":"2026-11-13T15:55:00"}' AS JSONB));
RESET ROLE;
DO $$ BEGIN
 IF (SELECT COUNT(*) FROM options_snapshots_all WHERE clock_metadata IS NULL) != 1 THEN RAISE EXCEPTION 'legacy row changed'; END IF;
 IF (SELECT clock_metadata->>'contract_last_trade_raw' FROM options_snapshots_all WHERE capture_batch_id='new') != '2026-11-13T15:55:00' THEN RAISE EXCEPTION 'row clock lost'; END IF;
 IF (SELECT clock_metadata->>'bid_ask_freshness' FROM options_capture_batches WHERE capture_batch_id='new') != 'unknown' THEN RAISE EXCEPTION 'unknown lost'; END IF;
 BEGIN
  UPDATE options_snapshots_all SET clock_metadata='{}' WHERE capture_batch_id='new';
  RAISE EXCEPTION 'append only guard did not fire';
 EXCEPTION WHEN restrict_violation THEN NULL;
 END;
END $$;
SELECT 'row_metadata_pg_bytes',pg_column_size(clock_metadata) FROM options_snapshots_all WHERE capture_batch_id='new';
"""
name = 'cboe-clock-check-' + uuid.uuid4().hex[:10]

def run(args, **kw):
    return subprocess.run(args, check=True, text=True, **kw)

run(['docker', 'image', 'inspect', 'postgres:15-alpine'], stdout=subprocess.DEVNULL)
try:
    run(['docker', 'run', '--pull=never', '--rm', '-d', '--name', name, '--network', 'none', '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', 'postgres:15-alpine'])
    for _ in range(60):
        result = subprocess.run(['docker', 'exec', name, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if result.returncode == 0:
            break
        time.sleep(.5)
    else:
        raise RuntimeError('scratch PG not ready')
    sql = "CREATE ROLE clock_test_writer;\n" + legacy + '\n' + base_insert + "\nGRANT SELECT,INSERT ON options_snapshots_all,options_capture_batches TO clock_test_writer; GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO clock_test_writer;\nBEGIN;\n" + ';\n'.join(statements) + ';\nCOMMIT;\n' + checks
    run(['docker', 'exec', '-i', name, 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres'], input=sql)
    # Fresh-install schema column shape, separate DB, not just the migration.
    run(['docker', 'exec', name, 'createdb', '-U', 'postgres', 'fresh'])
    run(['docker', 'exec', '-i', name, 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'fresh'], input=block + "\nDO $$ BEGIN IF (SELECT COUNT(*) FROM information_schema.columns WHERE table_name IN ('options_snapshots_all','options_capture_batches') AND column_name='clock_metadata' AND data_type='jsonb') != 2 THEN RAISE EXCEPTION 'schema mismatch'; END IF; END $$;")
    print('PASS: migration, legacy NULL, table grants, JSONB insert, append-only guard, fresh options-store schema')
finally:
    subprocess.run(['docker', 'stop', name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
