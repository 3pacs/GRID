# GEX dashboard / GRID API parity

Scope: source audit on 2026-11-09. GRID base 2945e95cacb29cd9c9a880c87bc5346fc1b13226; GEX source 2e2db22. These are inspected source identities, not assertions of installed runtime identity.

## Finding

GRID is the intended API base and GEX its dashboard, but today's implementation is partly segregated. GEX gex-watch computes the level book, applies publication guards, and reads Muse tape.db plus gex-signals signals.db. GRID's derivatives API separately runs DealerGammaEngine against options capture tables. The existing GRID gamma-watch bridge only exposes state, journal, contracts and archive. Shared host or input providers do not establish equivalent validation, calculation, storage or deployment.

| Improvement | GEX source | GRID status before this patch |
| --- | --- | --- |
| Unreconciled SPY minute volume | volume_quality withholds VWAP, relative volume, spikes, analogs and dependent panel/composites | No shared import. Raw signals.db/tape.db and other readers are not changed by the HTTP guard. |
| Previous-session close | safe_previous_close requires previousClose inside the prior completed regular-session range; chartPreviousClose is excluded | Separate daily/capture spot basis. The GEX sanity rule is not universal. |
| Quote-size pressure | pressure.quote_imbalance checks side exchange clocks, receipt age and provider-native size units; explicitly not executed flow/depth | Private quote store preserves sizes, but not this exact GEX interpretation. Ordinary gamma-watch state deliberately withholds Schwab values. |
| Anchored price acceptance | confirmations observes completed closes only after the current level/source/session anchor; volume/VWAP/breadth unavailable | No matching bridge route. |
| Levels, context, scoreboard, panel, trail, heatmaps, records | Existing GEX projection endpoints | Not exposed by the existing GRID gamma-watch router. |
| Feed readiness | GEX private operational/market-source state | Deliberately stripped from ordinary loopback state; this patch does not expose private state. |
| Model Greeks / sign / units | GEX level book, Muse interpretation and GRID DealerGammaEngine have different inputs and conventions | Must reconcile on matched inputs. Similar field names are not a parity proof. |
| Oracle #1094 | Not a GEX data-provider repair | Separate actor-vote/provenance correction. No assertion here about merge or deployment. |

GEX FOLLOWUPS reports the 22:04:05Z Records rollout preserving the live volume containment and readiness observer. That is source-held rollout evidence, not an independent live probe. No live deployment, store content or next-session coverage was tested for this patch.

## First additive bridge

`GET /api/v1/gamma-watch/private/{view}` accepts only:

- levels
- context
- confirmations
- scoreboard
- panel
- trail
- heatmap
- heatmap_live
- records

Each maps to the existing fixed loopback `/api/{view}` projection on port 8769. It does not perform provider fetches, write a database, recompute a value or fall back to an unguarded file. The upstream service may perform its existing read-only projection/cache work.

The route requires the existing GRID admin JWT role and `GAMMA_WATCH_PRIVATE_VIEWS_ENABLED=true`. The flag is **false by default** and accepts only the boolean `True` at the serving boundary; unvalidated strings or integers cannot enable it. Adding code does not activate it. Activation requires verification of the actual admin audience and licensed private use. Authentication alone is not a grant of redistribution rights. No new credentials, grants or account permissions are created.

Responses use `Cache-Control: no-store, private` and `X-GRID-Audience: private`. A new envelope separates `transport_status` from the upstream `status`; `data` retains the exact projection including withheld, unavailable, delayed, historical and degraded evidence. `bridge_received_at` is measured after successful reading and is never a source clock. It is null for disabled or failed reads; `bridge_evaluated_at` records the local request evaluation separately. The bridge does not validate freshness or make old inputs current. Known schemas/statuses are checked; an incompatible upstream revision is unavailable rather than silently reinterpreted. HTTP failures fail closed without forwarding exception messages or response bodies. The existing 4 MiB, three-second, no-redirect and no-proxy reader remains in use.

No private state, raw contracts, arbitrary URL/path, scenario execution, bearer query token or public feed redistribution is added. The ordinary GRID gamma-watch routes and their Schwab-withheld frame remain unchanged. The shared reader now additionally rejects JSON numeric overflow (for example, 1e400) before serialization, rather than allowing an uncontrolled 500. A 401/403/422 generated before the handler contains no projection data and uses the framework headers, as the existing private quote API does.

This is **transport parity preparation**, not full architecture consolidation and not a deployed feature. Quotes/readiness need a separately reviewed same-user entitlement boundary. UI clients are not changed in this patch.

## Remaining integration sequence

1. Preserve the current volume containment while the existing GEX owner validates interval volume and baseline. Do not lift it by changing a status string, replacing missing volume with a median, or reading raw stores around the guard.
2. Review this exact patch and all authentication/negative tests. In a separately authorized release, verify the real GRID admin audience and licensing, compare each GRID response with the corresponding guarded GEX response, then activate the flag. Do not point public Muse feeds at private endpoints.
3. Move reusable source-quality contracts, previous-close validation, units/sign declarations, source timestamps, missingness and provenance into GRID-owned versioned modules. GEX should consume those outputs. Make point-in-time fixtures cover both API and dashboard behavior. Preserve source/basis differences rather than forcing two distinct models to appear equal.
4. Reconcile GRID daily DealerGammaEngine, GEX horizon books and Muse on the same immutable chain, spot, OI date, expiries, units and sign assumptions. Report discrepancies before selecting a canonical formula. OI-derived positioning remains a model, not observed dealer inventory.
5. Move canonical computation/storage ownership behind GRID without introducing a second writer. Preserve historical source rows, capture batches, evaluation versions, trials, finality and existing invalidation barriers. Audit all direct consumers of signals.db/tape.db because HTTP containment alone does not protect them.
6. Migrate dashboard reads to GRID-owned contracts one view at a time behind reversible routing, prove parity including unavailable/stale cases, and remove duplicate calculation only after equivalent evidence. Tie future GEX changes to API contract tests so fixes do not become dashboard-only again.

## Validation and limits

All 1,139 relevant GEX, Schwab, authentication, configuration and GEX paper-log tests pass locally on Python 3.11, with no skips. The initially missing historical engine-reference commit was supplied from the existing local repository and its source-pin test then passed. Outbound Python socket connections were blocked for the entire test process; database configuration pointed at a deliberately unused local port, and no live database or service was used. Synthetic inputs and mocks establish source behavior, not current production data quality. The new bridge/auth suite and neighboring regression suites are included in this broader result.

Changed-file E/W/F and repository blocking Ruff checks pass with the explicit Python 3.11 target required by this repository's runtime. The exact CI-default Ruff invocation reports two pre-existing BaseExceptionGroup target-version errors in unchanged tests/test_resolver_transaction_pg.py; both are also present on the base revision. Wikilink integrity and git diff --check pass. Bandit is not installed in this local test environment and its scan was not run.

Direct GitHub main verification returned 2945e95cacb29cd9c9a880c87bc5346fc1b13226, matching this patch's base. A pull request's base metadata is not a substitute for checking the current branch ref. Open GEX/gamma PR searches found no overlapping bridge change; #982 remains a separate browser-acceptance task.

Not run: the complete backend/PostgreSQL suite, frontend/browser acceptance, installed GRID/GEX parity, source-clock market-session observation, entitlement verification, merge, push, deployment or feature activation. Those remain release gates, not implied by the relevant-suite pass.
