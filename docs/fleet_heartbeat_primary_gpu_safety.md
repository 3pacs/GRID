# Fleet heartbeat primary GPU safety

Fleet sweeps now default to monitoring, heartbeat records and debounced alerts.
Python callers must explicitly supply `do_heal=True` to request permitted
auto-heal. The CLI defaults to the same behavior; `--heal` opts in, and the
existing `--no-heal` wins if both flags are supplied. `--dry-run` still suppresses
records, healing and email while retaining probes and schema handling.

Primary `grid-svr` GPU recovery requires operator handling. `_heal` refuses it
before constructing commands, including direct calls, DOWN/DEGRADED states,
two strikes, repeated attempts and truthy heal overrides. Changing registry
`heal` flags cannot authorize a local NVIDIA module unload/reload. GPU registry
aliases targeting a local host, localhost/loopback, or a `grid-svr` hostname
are also refused. Unspecified IPv4 addresses, invalid address/hostname identities,
multicast and broadcast addresses cannot establish a permitted remote GPU target.
These are source identity checks, not runtime routing or hardware claims.
Unknown or malformed destructive target bindings fail closed.
There is no retry or alternate recovery command after refusal.

The guard also protects GPU-owning service families: `ollama`, `grid-llamacpp`,
`grid-worker` and NVIDIA services, including `.service` and instance spellings.
It resolves endpoint ownership before permitting a restart, recognizes the
`oracle-27b` identity independently of mutable registries, and refuses known
local GPU endpoint ports 8081, 8086 and 11434. A registered endpoint alias cannot
bypass its protected ownership by being passed as a unit. These protections
also reverse-resolve declared endpoint owners, so a generic failed-unit probe
cannot bypass the endpoint protection by emitting a fresh owning-unit name.
URL ports are validated before classifying the hostname; malformed remote ports
are refused while valid remote and unrelated endpoint/owner healing is retained.
These protections use source bindings and spelling checks, without DNS or host inspection.
Arbitrary operating-system service aliases and remote names cannot be discovered
from this source; an operator must review new bindings before enabling healing.

The source basis is the retained oracle service's `LLAMACPP_NGL=99`, worker GPU
access, and the repository service documentation describing grid-svr Ollama as
sharing its GPU. Historical documentation disagrees about current topology.
Physical GPU count and the oracle URL remain unchanged: exact current nonsecret
operator inventory/bindings were unavailable. Those metadata questions remain
unresolved; this patch does not establish current hardware or routing facts.

Passive node/GPU/unit/endpoint/freshness diagnostics, SQL schema and records,
two-strike alerts, periodic re-notification, recovery notices, cooldown, daily
cap and futile-heal suppression retain their behavior. Explicit permitted
remote GPU and unrelated unit/endpoint healing retain existing behavior.
Refusal returns `(False, reason)` and, when reached by an explicit sweep, is
recorded as `healed=False` with the refusal in `heal_detail`; alerts continue to
carry the observed diagnostic and full current down roster.

Tests use fake engines/SQL results, command helpers, HTTP and email. The author
packet's runner installs external-effect fences before importing target source,
runs Python 3.11.15 with `-I -B`, and uses only copied local test dependencies.
Original-source negative controls capture unsafe commands in fakes; default
and guard bypass mutants must fail assertions. This is source-only evidence,
not a runtime recovery, deployment, publication or production-health claim.
