# Ordinary health and cached optional LLM state

`GET /api/v1/system/health` observes the configured LOCAL primary's already
cached availability flag. It never imports `llm.router`, selects a provider,
constructs a client, calls a factory or chat/health method, probes a descriptor,
performs provider network I/O, or changes client caches or provider backoff.
An already loaded `config.settings` supplies the primary name using the existing
LOCAL/legacy QUICK/Gemma/default rules. Unsafe or unfamiliar stored state stays
unknown. No router, factory, client, configuration, schema, frontend, smoke
handler, scheduler, or timeout changes are required.

## Response contract

The existing `HealthResponse` schema, HTTP response behavior, status calculation,
and `degraded_reasons` remain unchanged. `checks` accepts arbitrary objects.
`checks.llm_available` remains a boolean: true requires a positive cached
observation for the configured primary. False means no positive observation;
it does **not** prove a provider is currently unreachable.

Additional `checks.llm` metadata is nested so existing top-level false-check
consumers do not interpret `fresh: false` as another failing subsystem:

| Field | Meaning |
| --- | --- |
| `state` | `available`, `unavailable`, `not_initialized`, or `unknown` |
| `provider` | Configured LOCAL primary name, or null when safely resolving it is impossible |
| `scope` | Always `configured_local_primary`; no selected-provider or fallback readiness claim |
| `source` | Always `process_cache`; identifies the observation source, even when absent |
| `fresh` | Always false; no fresh provider probe or observation timestamp is available |

`available` and `unavailable` require an exact stored boolean `is_available`
read statically from a cached client. They describe the stored flag only, which
may be stale and, for some client types, merely describe key configuration.
`not_initialized` means the router is not loaded or the configured primary has
no entry in its cache; it does not establish that no provider was ever created.
`unknown` covers malformed/partial state, non-boolean flags, methods,
descriptors, unsafe instance dictionaries, and read failures.

The primary's cached observation is narrower than `get_llm(Tier.LOCAL)`'s
selection behavior. An unavailable or uncached primary with a healthy cached
fallback yields false for `llm_available`; it does not probe the primary,
initialize the fallback, or claim that either is selected. Unrelated cached
providers cannot establish LOCAL primary availability. Cached flags do not
establish current routing readiness, endpoint health, successful inference,
GPU readiness, paid-provider eligibility, or backoff eligibility.

## Existing consumers and essential readiness

The Operator view still displays its LLM indicator from the legacy boolean.
It displays false for absent/unknown primary evidence, avoiding a green
indicator without a positive stored observation. Its existing label does not
expose the distinction between unknown and unavailable; callers needing that
distinction must inspect `checks.llm.state`.

The existing smoke health handler classifies the top-level `status` and records
false checks in `not_ok`. It can report `status=ok` with `llm_available` in
`not_ok`: optional cached provider evidence never determined essential service
readiness. Database connectivity, data freshness, pool capacity, scheduler
threads, disk checks, connector checks, and their degradation reasons retain
their previous behavior. Exhausted pools and unreachable databases still
produce `degraded`. The original pool test file and shared helper are preserved.

## Verification and limits

`tests/test_system_health_cached_llm.py` compiles named definitions from actual
source without importing the application. It executes the original health
mechanism with actual router selection and a forbidden factory stub, then
checks the repaired route against synthetic unavailable primary/healthy cached
fallback, no cache, unrelated cache, malformed state, descriptors, methods,
exceptions, essential DB/pool/scheduler behavior, and source-bound consumers.
All DB, network, provider, process, and ambient route state are fake. Temporary
module replacements restore prior `sys.modules` and existing package objects
on success and exceptions. No real service calls or sleeps are needed.

For source-only review, the runner may set `GRID_HEALTH_ORIGINAL_SOURCE` to the
pinned original `api/routers/system.py`. Otherwise the portable baseline restores
the exact original LLM block in the route. Run the test directly using unittest,
or collect it with pytest in the normal repository environment.

This change removes a reproduced source mechanism capable of provider-probe
latency. It does not prove that mechanism caused previously observed production
5.24–5.40 second health requests and does not guarantee total health latency
under five seconds. Existing serial DB, filesystem, and scheduler/systemd work
remains. Native Python 3.11 and full application/CI verification remain separate
from synthetic source-only verification; no deployment or runtime verification
is implied.
