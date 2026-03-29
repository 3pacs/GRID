#!/usr/bin/env python3
"""Import 3.3M ICIJ relationships into actor_connections."""
import csv, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_engine
from sqlalchemy import text

engine = get_engine()
csv_path = "/data/grid/bulk/icij/relationships.csv"

batch_size = 5000
inserted = 0
skipped = 0
total = 0
batch = []

with open(csv_path) as f:
    reader = csv.DictReader(f)
    for row in reader:
        total += 1
        n1 = row.get("node_id_start", "")
        n2 = row.get("node_id_end", "")
        rel = row.get("rel_type", "unknown")

        if not n1 or not n2:
            skipped += 1
            continue

        try:
            n1_int = int(n1)
            n2_int = int(n2)
        except ValueError:
            skipped += 1
            continue

        actor_a = f"icij_entity_{n1}" if n1_int < 12000000 else f"icij_officer_{n1}"
        actor_b = f"icij_entity_{n2}" if n2_int < 12000000 else f"icij_officer_{n2}"
        rel_type = "icij_" + rel.replace(" ", "_")

        batch.append({"a": actor_a, "b": actor_b, "r": rel_type, "s": 0.5})

        if len(batch) >= batch_size:
            try:
                with engine.begin() as conn:
                    for b in batch:
                        conn.execute(text(
                            "INSERT INTO actor_connections (actor_a, actor_b, relationship, strength) "
                            "VALUES (:a, :b, :r, :s) ON CONFLICT DO NOTHING"
                        ), b)
                inserted += len(batch)
            except Exception as e:
                skipped += len(batch)
            batch = []
            if inserted % 100000 == 0:
                print(f"  {inserted}/{total} inserted ({skipped} skipped)")

if batch:
    try:
        with engine.begin() as conn:
            for b in batch:
                conn.execute(text(
                    "INSERT INTO actor_connections (actor_a, actor_b, relationship, strength) "
                    "VALUES (:a, :b, :r, :s) ON CONFLICT DO NOTHING"
                ), b)
        inserted += len(batch)
    except Exception:
        skipped += len(batch)

print(f"DONE: {inserted} inserted, {skipped} skipped, {total} total rows")
