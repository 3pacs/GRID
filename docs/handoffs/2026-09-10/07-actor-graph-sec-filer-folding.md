# 07 — Fold SEC-filer actor nodes into their ticker actors in the curated graph

Branch: `claude/handoff-07-actor-filer-folding`. Lane: code + one detached ops-exec rerun.

## Evidence

Curated Louvain run 2026-09-10 (`actor_analytics_curated`, 13,226 nodes, 980
communities): three of the twelve headline communities are made of SEC-filer
nodes named like `CENOVUS ENERGY INC.  (CVE, C…`, `BlackRock Inc.  (BLK)  (CIK …`,
`SCRIVNER DOUGLAS G  (CIK 000…`, `iSHARES TRUST  (CIK 00011006…` — 255, 245 and
197 nodes, category `corporation`, connected only to each other. The same
companies exist as `CVE Corp`, `BLK Corp`, … ticker nodes in the market
clusters. The filer nodes are created by the 13F / EDGAR ingestion
(`intelligence/actor_discovery.py` titles `13F Filer (CIK …)`,
`ingestion/altdata/sec_13f_live.py`, `ingestion/altdata/sec_edgar_company.py`,
`ingestion/altdata/institutional_flows.py`, `scripts/parse_edgar.py`).

## Design

1. Resolution: build `intelligence/actor_identity.py` only if
   `pre_create_check.py "actor alias"` shows no coverage — `normalization/entity_map.py`
   and `intelligence/entity_aliases*` may already hold an alias table (Sprint 2
   added entity-map aliases for running pullers). Extend what exists.
   Rules: `(TICKER)` in the display name → canonical `<TICKER> Corp` actor id;
   CIK → ticker via the SEC `company_tickers.json` mapping the enrichment
   already downloads (`ingestion/altdata/small_cap_enrichment.py` uses SEC
   submissions; reuse its cache); person filers (`SCRIVNER DOUGLAS G`) → insider
   actor by normalised name if one exists, else leave.
2. Ingestion: the 13F/EDGAR writers should look up the canonical actor before
   inserting a new node; when found, attach the connection to the canonical id
   and record the filer name as an alias (no new node).
3. One-off merge for the existing graph: `scripts/fold_actor_aliases.py` —
   re-point `actor_connections` from alias nodes to canonical ids (dedupe,
   keep max strength), mark alias actors `merged_into=<canonical>` (add the
   column via a migration with the GRANT footer) and exclude merged actors in
   `scripts/graph_analytics.py::_CURATED_EDGE_SQL`. Parameterized SQL, batches,
   `--dry-run` with counts first.
4. Tests: name → ticker parser, CIK map lookup, fold SQL shape with a fake conn,
   curated SQL excludes merged actors (`tests/test_lever_map_fixes.py` has the
   pattern).
5. After merge: run the fold detached on grid-svr, then
   `run_graph_analytics(scope="curated")` and report communities: the filer
   clusters should disappear and the ticker clusters should absorb them.
   Compare top pagerank / betweenness before and after.

## Guardrails

Never delete `actors` rows (other tables reference them); mark and exclude.
The full-scope `actor_analytics` stays untouched. `raw_series` reads bounded.
