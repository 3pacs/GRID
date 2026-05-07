#!/usr/bin/env python3
"""
Obsidian Backlink Generator for GRID

Scans all project markdown files and adds [[wikilinks]] for cross-references
between documents, modules, concepts, and entities. This makes Obsidian's
graph view show the hidden connections across the knowledge base.

Usage:
    python scripts/obsidian_backlinks.py              # Dry run (shows changes)
    python scripts/obsidian_backlinks.py --apply       # Apply changes
    python scripts/obsidian_backlinks.py --report      # Show link report only
"""

import re
import sys
from pathlib import Path
from collections import defaultdict

GRID_ROOT = Path(__file__).resolve().parent.parent

# ─── Directories to process (project docs, not .claude internals) ───
SCAN_DIRS = [
    GRID_ROOT / "docs",
    GRID_ROOT,  # root-level .md files
]

# Files to skip (not useful for wiki linking)
SKIP_FILES = {
    ".coordination.md",
    "LICENSE.md",
}

# Directories to skip entirely
SKIP_DIRS = {
    ".claude",
    "node_modules",
    ".git",
    "pwa",
    "__pycache__",
    ".obsidian",
}


def collect_markdown_files() -> list[Path]:
    """Collect all markdown files in project docs."""
    files = []
    for scan_dir in SCAN_DIRS:
        if scan_dir == GRID_ROOT:
            # Only root-level .md files
            for f in scan_dir.glob("*.md"):
                if f.name not in SKIP_FILES:
                    files.append(f)
        else:
            for f in scan_dir.rglob("*.md"):
                # Skip excluded directories
                parts = f.relative_to(GRID_ROOT).parts
                if any(p in SKIP_DIRS for p in parts):
                    continue
                if f.name not in SKIP_FILES:
                    files.append(f)
    return sorted(set(files))


# ─── Entity Registry ───
# Maps display text → wiki link target (the page name in Obsidian)
# Format: { "plain text reference": "Wiki Page Name" }

# Build from document filenames
def build_doc_registry(files: list[Path]) -> dict[str, str]:
    """Build linkable entity registry from document filenames."""
    registry = {}
    for f in files:
        stem = f.stem  # filename without extension
        # Map the stem itself (e.g., "architecture" → "architecture")
        registry[stem] = stem
    return registry


# ─── Concept entities that should be linked across docs ───
CONCEPT_LINKS = {
    # Core modules → their doc/concept pages
    "PIT store": "PIT Store",
    "PIT query engine": "PIT Store",
    "PIT-correct": "PIT Store",
    "point-in-time": "PIT Store",
    "pit.py": "PIT Store",
    "store/pit.py": "PIT Store",

    "conflict resolution": "Conflict Resolution",
    "resolver.py": "Conflict Resolution",
    "normalization/resolver.py": "Conflict Resolution",

    "entity_map.py": "Entity Map",
    "entity disambiguation": "Entity Map",
    "normalization/entity_map.py": "Entity Map",

    "decision journal": "Decision Journal",
    "journal/log.py": "Decision Journal",
    "immutable journal": "Decision Journal",

    "model governance": "Model Governance",
    "governance/registry.py": "Model Governance",
    "model lifecycle": "Model Governance",
    "model registry": "Model Governance",

    "feature engineering": "Feature Engineering",
    "features/lab.py": "Feature Engineering",
    "feature lab": "Feature Engineering",

    "regime discovery": "Regime Discovery",
    "regime clustering": "Regime Discovery",
    "discovery/clustering.py": "Regime Discovery",

    "orthogonality": "Orthogonality Audit",
    "discovery/orthogonality.py": "Orthogonality Audit",

    "options scanner": "Options Scanner",
    "discovery/options_scanner.py": "Options Scanner",
    "mispricing detector": "Options Scanner",

    "walk-forward": "Walk-Forward Backtesting",
    "walk-forward backtesting": "Walk-Forward Backtesting",
    "validation/gates.py": "Walk-Forward Backtesting",
    "promotion gate": "Walk-Forward Backtesting",

    "live inference": "Live Inference",
    "inference/live.py": "Live Inference",

    "options recommender": "Options Recommender",
    "trading/options_recommender.py": "Options Recommender",

    "options tracker": "Options Tracker",
    "trading/options_tracker.py": "Options Tracker",

    "dealer gamma": "Dealer Gamma",
    "physics/dealer_gamma.py": "Dealer Gamma",
    "GEX": "Dealer Gamma",

    "Oracle engine": "Oracle Engine",
    "oracle/engine.py": "Oracle Engine",

    "oracle calibration": "Oracle Calibration",
    "oracle/calibration.py": "Oracle Calibration",
    "Brier score": "Oracle Calibration",

    "Hermes": "Hermes Scheduler",
    "hermes_operator.py": "Hermes Scheduler",
    "Hermes scheduler": "Hermes Scheduler",
    "hermes operator": "Hermes Scheduler",

    "BasePuller": "Base Puller",
    "ingestion/base.py": "Base Puller",

    # Intelligence layer
    "trust scorer": "Trust Scorer",
    "trust_scorer.py": "Trust Scorer",
    "Bayesian trust": "Trust Scorer",
    "trust scoring": "Trust Scorer",

    "lever pullers": "Lever Pullers",
    "lever_pullers.py": "Lever Pullers",

    "actor network": "Actor Network",
    "actor_network.py": "Actor Network",

    "cross-reference": "Cross Reference",
    "cross_reference.py": "Cross Reference",
    "lie detector": "Cross Reference",

    "source audit": "Source Audit",
    "source_audit.py": "Source Audit",

    "postmortem": "Postmortem",
    "postmortem.py": "Postmortem",
    "post-mortem": "Postmortem",

    "sleuth": "Sleuth",
    "sleuth.py": "Sleuth",

    "thesis tracker": "Thesis Tracker",
    "thesis_tracker.py": "Thesis Tracker",

    "dollar flows": "Dollar Flows",
    "dollar_flows.py": "Dollar Flows",

    "event sequence": "Event Sequence",
    "event_sequence.py": "Event Sequence",

    "forensics": "Forensics",
    "forensics.py": "Forensics",

    "causation": "Causation",
    "causation.py": "Causation",

    "flow thesis": "Flow Thesis",
    "flow_thesis.py": "Flow Thesis",

    "flow aggregator": "Flow Aggregator",
    "flow_aggregator.py": "Flow Aggregator",

    # Data sources
    "FRED": "FRED",
    "BLS": "BLS",
    "ECB": "ECB",
    "EDGAR": "EDGAR",
    "NOAA": "NOAA",
    "USDA": "USDA",
    "EIA": "EIA",
    "GDELT": "GDELT",
    "FARA": "FARA",
    "FOIA": "FOIA",
    "CoinGecko": "CoinGecko",
    "Polymarket": "Polymarket",
    "dark pool": "Dark Pool",
    "FINRA dark pool": "Dark Pool",
    "congressional trading": "Congressional Trading",
    "insider filings": "Insider Filings",
    "SEC Form 4": "Insider Filings",
    "institutional flows": "Institutional Flows",
    "ETF flows": "Institutional Flows",
    "13F": "Institutional Flows",
    "Fed liquidity": "Fed Liquidity",
    "campaign finance": "Campaign Finance",
    "CFTC": "CFTC COT",
    "Commitments of Traders": "CFTC COT",
    "yield curve": "Yield Curve",
    "Baltic Dry": "Baltic Dry Index",
    "supply chain": "Supply Chain",

    # Infrastructure
    "TimescaleDB": "TimescaleDB",
    "PostgreSQL": "PostgreSQL",
    "FastAPI": "FastAPI",
    "Zustand": "Zustand",
    "SQLAlchemy": "SQLAlchemy",
    "Alembic": "Alembic",

    # LLM layer
    "Ollama": "Ollama",
    "Hyperspace": "Hyperspace",
    "llama.cpp": "llama.cpp",
    "TradingAgents": "TradingAgents",

    # Trading concepts
    "Kelly criterion": "Kelly Criterion",
    "gamma walls": "Dealer Gamma",
    "vanna": "Dealer Gamma",

    # Key tables
    "raw_series": "Raw Series Table",
    "resolved_series": "Resolved Series Table",
    "source_catalog": "Source Catalog Table",
    "feature_registry": "Feature Registry Table",
    "decision_journal": "Decision Journal",

    # Frontend views
    "MoneyFlow": "MoneyFlow View",
    "ActorNetwork": "Actor Network View",
    "CrossReference": "Cross Reference View",
    "TrendTracker": "TrendTracker View",
    "IntelDashboard": "Intel Dashboard View",

    # AstroGrid
    "AstroGrid": "AstroGrid",

    # Signals
    "Trial Gem Hunter": "Trial Gem Hunter",
    "trial signal": "Trial Gem Hunter",

    # Concepts
    "lookahead bias": "PIT Store",
    "assert_no_lookahead": "PIT Store",
    "DISTINCT ON": "PIT Store",
    "vintage policy": "PIT Store",
    "FIRST_RELEASE": "PIT Store",
    "LATEST_AS_OF": "PIT Store",
}


def is_inside_link(text: str, match_start: int, match_end: int) -> bool:
    """Check if a match position is already inside a [[wikilink]], URL, or code block."""
    # Inside [[...]]
    before = text[:match_start]
    text[match_end:]

    # Check if inside [[...]]
    last_open = before.rfind("[[")
    last_close = before.rfind("]]")
    if last_open > last_close:
        return True

    # Check if inside backtick code span
    backtick_count = before.count("`")
    if backtick_count % 2 == 1:
        return True

    # Check if inside markdown link [text](url) or ![img](url)
    # Look for unmatched [ before us
    bracket_depth = 0
    for i in range(match_start - 1, max(match_start - 200, -1), -1):
        if text[i] == "]":
            bracket_depth += 1
        elif text[i] == "[":
            if bracket_depth > 0:
                bracket_depth -= 1
            else:
                # We're inside a [link text] — check if followed by (
                return True

    # Check if inside a URL (http:// or https://)
    url_region = before[-200:] if len(before) > 200 else before
    last_http = max(url_region.rfind("http://"), url_region.rfind("https://"))
    if last_http >= 0:
        # Check if URL hasn't ended (no space/newline since http)
        url_tail = url_region[last_http:]
        if " " not in url_tail and "\n" not in url_tail:
            return True

    return False


def is_in_code_block(lines: list[str], line_idx: int) -> bool:
    """Check if a line is inside a fenced code block."""
    fence_count = 0
    for i in range(line_idx):
        stripped = lines[i].strip()
        if stripped.startswith("```"):
            fence_count += 1
    return fence_count % 2 == 1


def is_heading_line(line: str) -> bool:
    """Check if this line is a heading (don't modify headings)."""
    return line.strip().startswith("#")


def is_table_line(line: str) -> bool:
    """Check if this line is a markdown table row."""
    stripped = line.strip()
    return stripped.startswith("|") and stripped.endswith("|")


def _compile_entity_patterns(
    all_entities: dict[str, str],
) -> list[tuple[re.Pattern, str, str]]:
    """Pre-compile regex patterns for all entities. Sorted longest-first."""
    compiled = []
    for plain_text, target in sorted(all_entities.items(), key=lambda x: -len(x[0])):
        if len(plain_text) < 3:
            continue
        escaped = re.escape(plain_text)
        if " " in plain_text or "/" in plain_text or "." in plain_text:
            pattern = escaped
        else:
            pattern = r"\b" + escaped + r"\b"
        flags = 0
        if plain_text.isupper() and len(plain_text) <= 5:
            flags = 0
        elif not plain_text[0].isupper() or " " in plain_text:
            flags = re.IGNORECASE
        compiled.append((re.compile(pattern, flags), plain_text, target))
    return compiled


# Module-level cache so patterns compile once across calls
_PATTERN_CACHE: list[tuple[re.Pattern, str, str]] = []
_PATTERN_CACHE_KEY: int = 0


def add_wikilinks(content: str, file_path: Path, all_entities: dict[str, str]) -> tuple[str, list[str]]:
    """Add [[wikilinks]] to markdown content. Returns (new_content, list_of_changes)."""
    global _PATTERN_CACHE, _PATTERN_CACHE_KEY

    # Compile patterns once and cache
    cache_key = id(all_entities)
    if cache_key != _PATTERN_CACHE_KEY or not _PATTERN_CACHE:
        _PATTERN_CACHE = _compile_entity_patterns(all_entities)
        _PATTERN_CACHE_KEY = cache_key

    changes = []
    lines = content.split("\n")
    new_lines = []
    linked_targets = set()
    file_stem = file_path.stem
    file_stem_lower = file_stem.lower().replace(" ", "-")

    # Pre-compute code block membership once (O(n) instead of O(n²))
    in_code_block = [False] * len(lines)
    fence_open = False
    for i, ln in enumerate(lines):
        if ln.strip().startswith("```"):
            fence_open = not fence_open
        in_code_block[i] = fence_open

    # Prefilter: only check entities that actually appear in this file
    content_lower = content.lower()
    active_patterns = [
        (rx, pt, tgt) for rx, pt, tgt in _PATTERN_CACHE
        if pt.lower() in content_lower
        and tgt.lower().replace(" ", "-") != file_stem_lower
        and tgt != file_stem
    ]

    if not active_patterns:
        return content, []

    for line_idx, line in enumerate(lines):
        if in_code_block[line_idx]:
            new_lines.append(line)
            continue

        if is_heading_line(line):
            new_lines.append(line)
            continue

        if is_table_line(line):
            new_lines.append(line)
            continue

        stripped = line.strip()
        if stripped and all(c in "-=" for c in stripped):
            new_lines.append(line)
            continue

        modified_line = line
        for rx, plain_text, target in active_patterns:
            if target in linked_targets:
                continue

            m = rx.search(modified_line)
            if not m:
                continue

            if is_inside_link(modified_line, m.start(), m.end()):
                continue

            matched_text = m.group()
            if target == matched_text:
                replacement = f"[[{target}]]"
            else:
                replacement = f"[[{target}|{matched_text}]]"

            modified_line = modified_line[:m.start()] + replacement + modified_line[m.end():]
            linked_targets.add(target)
            changes.append(f"  L{line_idx + 1}: '{matched_text}' → [[{target}]]")

        new_lines.append(modified_line)

    return "\n".join(new_lines), changes


def generate_report(all_changes: dict[str, list[str]], files: list[Path]):
    """Print a summary report of all links that would be added."""
    total_links = sum(len(c) for c in all_changes.values())
    linked_files = sum(1 for c in all_changes.values() if c)

    print(f"\n{'='*60}")
    print("OBSIDIAN BACKLINK REPORT")
    print(f"{'='*60}")
    print(f"Files scanned:  {len(files)}")
    print(f"Files modified: {linked_files}")
    print(f"Links added:    {total_links}")
    print(f"{'='*60}\n")

    # Build a reverse index: target → list of files that link to it
    target_sources = defaultdict(list)
    for filepath, changes in all_changes.items():
        for change in changes:
            # Extract target from "→ [[Target]]"
            match = re.search(r"\[\[(.+?)\]\]", change)
            if match:
                target = match.group(1)
                target_sources[target].append(Path(filepath).stem)

    if target_sources:
        print("TOP CONNECTED ENTITIES (by backlink count):")
        print("-" * 40)
        for target, sources in sorted(target_sources.items(), key=lambda x: -len(x[1]))[:30]:
            print(f"  [[{target}]] ← {len(sources)} files: {', '.join(sources[:5])}")
            if len(sources) > 5:
                print(f"    ... and {len(sources) - 5} more")
        print()

    # Show per-file changes
    if any(all_changes.values()):
        print("PER-FILE CHANGES:")
        print("-" * 40)
        for filepath, changes in sorted(all_changes.items()):
            if changes:
                rel = Path(filepath).relative_to(GRID_ROOT)
                print(f"\n{rel} ({len(changes)} links):")
                for c in changes[:10]:
                    print(f"  {c}")
                if len(changes) > 10:
                    print(f"  ... and {len(changes) - 10} more")


def main():
    apply = "--apply" in sys.argv
    report_only = "--report" in sys.argv

    print("Scanning GRID docs for backlink opportunities...\n")

    files = collect_markdown_files()
    print(f"Found {len(files)} markdown files to process")

    # Build entity registry
    doc_registry = build_doc_registry(files)
    all_entities = {**CONCEPT_LINKS}

    # Add document cross-references (link by stem name)
    # Only add docs with distinctive names (skip generic ones)
    skip_stems = {"README", "CLAUDE", "index", "plan", "config"}
    for stem, target in doc_registry.items():
        if stem not in skip_stems and len(stem) > 3:
            # Use the stem as both the search term and target
            all_entities[stem] = target

    print(f"Entity registry: {len(all_entities)} linkable entities")

    all_changes = {}
    modified_files = {}

    for f in files:
        content = f.read_text(encoding="utf-8", errors="replace")
        new_content, changes = add_wikilinks(content, f, all_entities)
        all_changes[str(f)] = changes
        if changes:
            modified_files[f] = new_content

    generate_report(all_changes, files)

    if report_only:
        return

    if not apply:
        print("\nDry run complete. Use --apply to write changes.")
        print("  python scripts/obsidian_backlinks.py --apply")
        return

    # Apply changes
    written = 0
    for f, new_content in modified_files.items():
        f.write_text(new_content, encoding="utf-8")
        written += 1
        rel = f.relative_to(GRID_ROOT)
        print(f"  ✓ {rel}")

    print(f"\nDone! Wrote {written} files with backlinks.")
    print("Open Obsidian and check the graph view to see connections.")


if __name__ == "__main__":
    main()
