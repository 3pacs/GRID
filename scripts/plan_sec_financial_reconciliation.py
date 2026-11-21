#!/usr/bin/env python3
"""Build a read-only SEC reconciliation proposal from local captured artifacts.

No database connection, network request or repair execution path exists here.
"""
from __future__ import annotations

import argparse
from datetime import datetime
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingestion.altdata.sec_financial_snapshots import (  # noqa: E402
    MAX_SOURCE_BYTES, prepare_snapshot, build_reconciliation_manifest, _json,
)


def main(argv=None) -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture', type=Path, required=True, help='Local successful SEC capture receipt JSON')
    parser.add_argument('--before-images', type=Path, required=True, help='Read-only database row export JSON')
    parser.add_argument('--output', type=Path, required=True, help='New proposal file; never overwritten')
    parser.add_argument('--fx-inputs', type=Path, help='Explicit currency/rate JSON; default USD only')
    args=parser.parse_args(argv)
    receipt=json.loads(args.capture.read_bytes())
    body_path=(args.capture.resolve().parent / receipt['body_file']).resolve()
    if not body_path.is_relative_to(args.capture.resolve().parent):
        raise ValueError('Capture body must be beside or below its receipt')
    if receipt.get('http_status') != 200 or receipt.get('body_storage') != 'gzip':
        raise ValueError('A complete successful gzip-preserved capture is required')
    with gzip.open(body_path,'rb') as source:
        body=source.read(MAX_SOURCE_BYTES+1)
    if len(body)>MAX_SOURCE_BYTES or hashlib.sha256(body).hexdigest()!=receipt['response_sha256']:
        raise ValueError('Source body hash/size mismatch')
    fx=json.loads(args.fx_inputs.read_bytes()) if args.fx_inputs else {'USD':1.0}
    snapshot=prepare_snapshot(source_body=body, actor_id=receipt['actor_id'], cik=receipt['cik'],
        request_started_at=datetime.fromisoformat(receipt['request_started_at']),
        received_at=datetime.fromisoformat(receipt['received_at']), fx_rates=fx)
    if snapshot.source_url != receipt['source_url']:
        raise ValueError('Receipt is not the expected official Company Facts URL')
    before_bytes=args.before_images.read_bytes()
    export=json.loads(before_bytes,parse_float=Decimal)
    rows=export['rows'] if isinstance(export,dict) else export
    if isinstance(export,dict) and export.get('row_count',len(rows))!=len(rows):
        raise ValueError('Before-image row count mismatch')
    result=build_reconciliation_manifest(snapshot,rows)
    result['beforeimage_artifact_sha256']=hashlib.sha256(before_bytes).hexdigest()
    result['capture_receipt_sha256']=hashlib.sha256(args.capture.read_bytes()).hexdigest()
    if isinstance(export,dict):
        result['beforeimage_captured_at']=export.get('captured_at')
    result['source_received_at']=snapshot.received_at.isoformat()
    counts={}
    for entry in result['entries']:
        counts[entry['disposition']]=counts.get(entry['disposition'],0)+1
    result['disposition_counts']=counts
    with args.output.open('x',encoding='utf-8',newline='\n') as output:
        output.write(_json(result)+'\n')
    print(json.dumps(dict(mode=result['mode'],rows=len(rows),disposition_counts=counts),sort_keys=True))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
