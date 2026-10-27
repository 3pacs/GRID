# offshore_leaks match quality: why it is held, and a proposed minimum rule

**Status:** `offshore_leaks` is held in `ingestion/smart_scheduler.PULLER_REGISTRY` (`hold_reason`) until the owner approves a match rule. Every automated path honours the hold: SmartScheduler, the Hermes operator and fixers, and grid-scheduler (`ingestion/scheduler.py`, through `smart_scheduler.hold_reason_for`). The rule below is a proposal, not implemented.

## Facts (2026-10-02)

- **Nothing has ever been stored.** Before #768, every SmartScheduler run failed inside the constructor: `SOURCE_CONFIG.latency_class = "STATIC"` violated the `source_catalog` CHECK. `pull_log` (`smart:offshore_leaks`) shows FAILED / CheckViolation on every run through 2026-10-02 02:19Z. `raw_series` has **0** `OFFSHORE:*` rows. #768 fixed the constructor, and the first run that got further (10:42Z) caused the lock-table outage fixed by #802. That transaction rolled back.
- **What the current matcher produces.** `match_actors()` was run read-only on grid-svr against the deployed code and `/data/grid/bulk/icij` (2026-10-02 ~11:55Z, no database access).

| | Count |
|---|---|
| Matches (officer ↔ actor) | **71,903**, against 204 of ~490 actors |
| Rows it would write (one per connected entity) | **158,298** |
| `exact` (normalised full name equal) | **42** |
| `partial` (a known name key is a *substring* of the officer name) | **71,861** |
| `partial` hits whose only matching key is a single token (a bare surname or one word) | **71,753** (99.8%) |
| Officer name looks like a company (ltd, limited, inc, holdings, management, trust, nominees, …) | **57,102** |
| Matches where all of the actor's tokens (≥ 2) appear as whole words in the officer name | **185** |
| `exact` and not company-like | **40** |
| Actors with ≥ 1,000 matches | 11 (top: 15,270 / 8,451 / 6,977 / 5,878 / 5,822) |

The bulk comes from `_build_known_names_index()`. It adds each actor's **last word** (≥ 6 characters) as a key, and `_match_officer_to_actor` accepts any key that is a substring of the normalised officer name. Institutional actors' last words ("management", "capital", "partners", …) therefore match thousands of unrelated shell companies. One example is "GTC MANAGEMENT LTD." matched to Oaktree Capital Management. The top categories are defense contractors (7,864), corporations (7,574), lobbying firms (5,406) and central banks (3,463).

## Proposed minimum rule (for owner approval)

Accept a match only if one of the following holds:

1. **Exact:** the normalised officer name equals the normalised actor name (or a curated alias).
2. **Token-set (people only):** the actor is a person, its name has at least 2 tokens, every token appears as a whole word in the officer name, and the officer name is not company-like.

Never accept a single-token key, i.e. a bare surname or one word.

For institutional actors (corporations, funds, banks, governments), require (1) only. Alternatively, require a curated ICIJ node id.

On the numbers above, this keeps **about 185–227** matches, about 0.3% of today's output, instead of 71,903. The range is fuzzy because an exact match on a one-word name may fall outside the 185. The exact subset is 42. Each would still be a *candidate* link for review, not a signal.

**Before un-holding:** implement the rule in `_build_known_names_index` and `_match_officer_to_actor`, and add tests with real false-positive shapes. The same loose matcher has a second consumer that needs the same rule: `intelligence/actor_discovery._cross_reference_icij_with_known_actors` (around line 3063), reached through `run_scale_discovery` → `import_icij_offshore`. The daily cycle does not reach it today. Re-run the read-only count. Then decide whether the result belongs in `raw_series` at all, or in an actor-link table with review status. The `signal_sources` emission ("offshore = SELL") also needs its own decision; it is inactive today (wrong column).
