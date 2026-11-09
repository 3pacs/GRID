#!/usr/bin/env python3
"""Archive an already-downloaded document without network or provider calls."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingestion.company_documents import MAX_BYTES, ingest_document


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", required=True, type=Path)
    parser.add_argument("--document", required=True, type=Path)
    parser.add_argument("--archive", required=True, type=Path)
    args = parser.parse_args(argv)
    with args.metadata.open("rb") as stream:
        metadata_bytes = stream.read(65537)
    if len(metadata_bytes) > 65536:
        parser.error("metadata exceeds 64 KiB")
    with args.document.open("rb") as stream:
        content = stream.read(MAX_BYTES + 1)
    result = ingest_document(args.archive, json.loads(metadata_bytes), content)
    print(json.dumps({key: result[key] for key in (
        "schema", "snapshot_id", "company_id", "content_sha256", "parse_status",
        "published_at", "retrieved_at", "byte_length")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
