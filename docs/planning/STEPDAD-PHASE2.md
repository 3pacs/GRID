# stepdad.finance — Phase 2 design spec

Status: design only. No code in this slice.

Source of the mandate — `AGENTS.md:29` (2026-05-30 entry): *"Voice input (local
Whisper `/transcribe`) = Phase 2. Profiles (named layout+allocation, hundreds,
arena battle via existing `trading.py` wallets) = Phase 2/3."* Phase 1 (the
natural-language composer) is live; see `AGENTS.md:5-34` for what shipped and
what's still broken (`stepdad.png` favicon, `/quote` staleness).

## Cross-cutting constraint: D8 (persona-surface freeze)

`docs/planning/V5-TRANSFORMATION.md:390-397` (decision D8): *"Persona surfaces
are frozen without operator sign-off. `Home.jsx`, `TenYearPortfolio.jsx`,
`DadNav.jsx`, `views/mobile/`, and anything in `DAD_VIEWS` are not touched by
the adoption campaign or decomposition work unless the operator explicitly
includes them."* `docs/planning/V5-TRANSFORMATION.md:459-460` (phase R2)
explicitly excludes `Home.jsx` from the frontend adoption campaign for the
same reason.

Both features in this spec require changes to `pwa/src/views/Home.jsx` (the
composer input row, the save/save-mix affordance, the mic button) — the exact
file D8 freezes. **This spec treats every Home.jsx touchpoint below as
pending operator sign-off**, not as an approved plan. Backend work (new
routers, new tables) does not touch a frozen surface and can proceed
independently; no dad-facing pixel should ship until the operator has
reviewed the mockup-in-words below. This is listed again per-feature and in
the consolidated question list at the end.

`DAD_VIEWS` — `pwa/src/app.jsx:11`: `new Set(['home', 'ten-year',
'ticker-lookup'])`. Persona is read via `isSimpleUser()` /
`pwa/src/authSession.js:59`, driven by the JWT `role` claim
(`pwa/src/authSession.js:2,46`, `api/auth.py:77` `VALID_ROLES = ("admin",
"contributor")`).

---

# Feature 1: Profiles

**Naming note:** `api/routers/dad.py` already has a `_get_finviz_profile`
helper (`api/routers/dad.py:748`) and other `profile`-named internals, but
those are ticker research snapshots pulled from Finviz — unrelated to this
feature's "named layout+allocation" concept. Checked per `CLAUDE.md`'s
grep-before-create rule; no existing module covers named, saveable
composer layouts, so this is genuinely new, not a rebuild of something that
already exists.

## 1. Problem statement

**Dad's story (73, on a phone, plain language):** Right now every time he
opens stepdad.finance he has to type or tap a question from scratch, even
for the same handful of things he checks every week ("how's my retirement
mix doing", "show me the aggressive one"). He wants to save a look he liked
and get straight back to it — tap a name, see the same dashboard, no
re-typing, no re-explaining.

**Operator's story:** The composer already produces a `{spoken_reply,
widgets[], allocation[]}` layout per request (`api/routers/chat.py:305-318`
`ChatComposeResponse`). A "profile" is that same shape, given a name and
persisted. Separately, the operator wants to generate many candidate
allocations, let them run as isolated paper-money cohorts, and only promote
the winner to something dad sees — reusing `trading/wallet_manager.py`'s
existing isolated-capital-pool machinery instead of building a new
simulation engine.

## 2. Acceptance criteria

- Dad can save the dashboard he's currently looking at under a short name
  and return to it later without re-asking the question.
- Loading a saved profile renders the same widgets instantly (no LLM round
  trip) — it replays stored `widgets[]`/`allocation[]` through the existing
  `WidgetGrid` (`pwa/src/components/home/widgets.jsx`).
- Dad can see and delete his own saved profiles; he never sees another
  user's profiles.
- The operator can launch a batch of candidate allocations ("arena") that
  each run against an isolated paper wallet, see a leaderboard, and promote
  exactly one winner into a profile dad can open.
- No profile or arena card ever shows a number that wasn't actually computed
  by the composer or actually recorded by a paper wallet — no placeholder
  P&L before a wallet has a trade.
- Every profile/arena write is attributable to an owner (audit trail).

## 3. API shape

New router `api/routers/profiles.py`, mounted the same way `chat` and
`price_alerts` are today (`api/main.py:401` `("chat", "api.routers.chat",
False)`, `api/main.py:438` `("price_alerts", "api.routers.price_alerts",
False)` — add `("profiles", "api.routers.profiles", False)` to that same
table-driven list).

**Auth/roles.** `require_auth` (`api/auth.py:222`) for anything dad himself
manages (create/list/load/delete his own profiles) — contributor is
sufficient, following the pattern every other dad-facing endpoint in
`chat.py` and `price_alerts.py` uses (`Depends(require_auth)`, never
`require_role`). Arena launch and promote are **admin-only**
(`Depends(require_role("admin"))`, the pattern at `api/auth.py:243,580,622`)
because a single call can create hundreds of DB rows and paper wallets —
this is an operator tool, not something dad triggers.

```
POST   /api/v1/profiles                 contributor+   create from a layout dad is looking at
GET    /api/v1/profiles                 contributor+   list owner's active profiles (paginated)
GET    /api/v1/profiles/{id}            contributor+   fetch one (ChatComposeResponse-shaped)
DELETE /api/v1/profiles/{id}            contributor+   soft-delete (active=false)
POST   /api/v1/profiles/{id}/activate   contributor+   bump last_used_at (touch-on-load)

POST   /api/v1/profiles/arena           admin only     launch a batch of candidate allocations
GET    /api/v1/profiles/arena/{id}      contributor+   leaderboard (read-only for either role)
POST   /api/v1/profiles/arena/{id}/promote/{wallet_id}  admin only   promote a winner to a profile
```

`POST /api/v1/profiles` request/response (mirrors `ComposeWidget`/
`ComposeAllocationItem`, `api/routers/chat.py:273-284`):

```json
// request
{
  "name": "My retirement mix",
  "question": "how's my retirement doing",
  "spoken_reply": "...",
  "widgets": [{"type": "ticker_pulse", "title": "...", "props": {"ticker": "AAPL"}}],
  "allocation": [{"ticker": "AAPL", "weight": 0.4}]
}
// response
{"id": 17, "name": "My retirement mix", "created_at": "2026-09-10T..."}
```

`widgets[].type` is validated against the same fixed `_WIDGET_CATALOG`
(`api/routers/chat.py:1866-1893`) used by the composer — a saved profile
can't reference a widget type that doesn't exist, same guard as
`_validate_compose_layout` (`api/routers/chat.py:1970-2016`).

`GET /api/v1/profiles` list response follows `security.md`'s pagination
rule and the pattern at `api/routers/journal.py::get_all`: `{entries: [...],
total, limit, offset, has_more}`.

`POST /api/v1/profiles/arena` request:

```json
{
  "base_profile_id": 17,
  "variant_count": 200,
  "variant_strategy": "weight_perturbation",
  "initial_capital": 10000.0
}
```

Response: `{"arena_id": "...", "wallet_ids": [...], "variant_count": 200}`.
`variant_count` is capped (proposed 500, `Field(..., le=500)`) and validated
at the boundary per `security.md`'s "validate all user-supplied parameters"
rule — not trusted to whatever the LLM or client sends.

**Error cases:** blank/duplicate name for the same owner → 409 with a plain
message (dad-facing errors match Home.jsx's tone, e.g. "You already have a
mix called that — try a different name."); unknown widget type in a save
request → 422 (silently dropped like the composer does, or rejected —
open question below); `variant_count` over the cap → 422; promote on a
`wallet_id` not in that arena → 404; arena launch while another arena from
the same `base_profile_id` is still running → 409 (prevents runaway
duplicate batches).

**Rate limits.** Reuse `grid_rate_limits` (`api/auth.py:60-71`, the
persisted-not-in-memory table `security.md` requires): `profiles_create`
keyed per IP, generous (e.g. 20/hour) since dad may save several mixes in a
session; `profiles_arena` keyed per IP, tight (e.g. 2/day) since each call
can create hundreds of wallet rows.

## 4. DB shape

New table `sd_profiles` (matching the `sd_` prefix convention of
`sd_price_alerts` — `api/routers/price_alerts.py:39` — and
`sd_capability_requests` — `api/routers/chat.py:2148`):

```sql
CREATE TABLE IF NOT EXISTS sd_profiles (
    id                BIGSERIAL PRIMARY KEY,
    owner             TEXT NOT NULL DEFAULT 'dad',
    name              TEXT NOT NULL,
    question          TEXT,
    spoken_reply      TEXT,
    widgets           JSONB NOT NULL DEFAULT '[]',
    allocation        JSONB NOT NULL DEFAULT '[]',
    source_wallet_id  TEXT REFERENCES trading_wallets(id),
    active            BOOLEAN NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at      TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sd_profiles_owner_name
    ON sd_profiles(owner, name) WHERE active;
```

Arena candidates **reuse** `trading_wallets` rather than inventing a
parallel wallet concept — `trading/wallet_manager.py:36-57` already has
everything an arena candidate needs: `wallet_type` (set to `"paper"`),
`status` (`ACTIVE`/`PAUSED`/`KILLED`, with automatic drawdown-based killing
at `trading/wallet_manager.py:181-203`), `total_pnl`, and a `metadata JSONB`
column (`trading/wallet_manager.py:53`) free for arena tagging —
`{"arena_id": "...", "candidate_allocation": [...]}`. This follows
`CLAUDE.md`'s pre-build rule ("extend and wire, not build new") instead of
duplicating wallet bookkeeping.

One new lightweight table is still needed because `WalletManager` has no
concept of a *batch*:

```sql
CREATE TABLE IF NOT EXISTS sd_profile_arenas (
    id                TEXT PRIMARY KEY,
    requested_by      TEXT NOT NULL,
    base_profile_id   BIGINT REFERENCES sd_profiles(id),
    variant_count     INTEGER NOT NULL,
    status            TEXT NOT NULL DEFAULT 'RUNNING'
                      CHECK (status IN ('RUNNING', 'DONE', 'CANCELLED')),
    promoted_profile_id BIGINT REFERENCES sd_profiles(id),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ
);
```

Migration file: `migrations/0061_stepdad_profiles.sql` (last existing is
`migrations/0060_sponsor_ticker_map.sql`), built from
`migrations/_TEMPLATE.sql` with its required GRANT footer
(`migrations/_TEMPLATE.sql:29-38`):

```sql
GRANT ALL ON sd_profiles TO grid;
GRANT USAGE, SELECT ON SEQUENCE sd_profiles_id_seq TO grid;
GRANT ALL ON sd_profile_arenas TO grid;
```

## 5. Frontend touchpoints

**All of the below are pending D8 sign-off** (see cross-cutting section).

- `pwa/src/views/Home.jsx`: a "Remember this" button next to the existing
  "← Start over" button (`pwa/src/views/Home.jsx:161-163`, `S.startOver`
  style at `:357-361`). In words: same visual weight as Start Over, tapping
  it opens one plain text field for a name (no modal-heavy form) and posts
  the current `layout` state to `POST /api/v1/profiles`.
- A "My Mixes" panel on the opening screen, visually identical in pattern
  to `AlertsPanel` (`pwa/src/views/Home.jsx:245-262`, styles `S.alertsPanel`
  /`S.alertRow` at `:327-343`) — a card, one row per saved profile, a
  "Cancel"-style delete button per row. Tapping a row calls `GET
  /api/v1/profiles/{id}` and sets `layout` directly — no LLM call, so it's
  as fast as the existing alert list load (`loadAlerts`, `Home.jsx:43-49`).
- `pwa/src/components/home/widgets.jsx` needs **no new widget type** — a
  loaded profile replays the same fixed catalog (`verdict`, `ticker_pulse`,
  `watchlist`, `macro_regime`, `news`, `money_flow`) the composer already
  produces, so `WidgetGrid` renders it unchanged.
- Arena results are an **operator-only** surface, not a `DAD_VIEWS` entry,
  so D8 doesn't gate it the same way — it's a new route following the
  `routes.js` pattern (`pwa/src/routes.js:38-49` shows the route-entry
  shape), e.g. a "Profile Arena" drawer item under the `trading` group. In
  words: a leaderboard table — variant #, total P&L, drawdown, status badge
  (ACTIVE/PAUSED/KILLED, same vocabulary `wallet_manager.py` already uses),
  one "Promote to dad" button per row, disabled once any candidate from
  that arena has already been promoted.
- `pwa/src/api.js` gains `saveProfile()`, `listProfiles()`,
  `loadProfile(id)`, `deleteProfile(id)`, `launchArena()`,
  `getArena(id)`, `promoteArenaWinner()` — all following the existing
  `_fetch` wrapper pattern used by `compose()`/`listAlerts()`
  (`pwa/src/api.js:904-938`).
- No fabricated data: a profile card shows exactly what was stored; an
  arena leaderboard shows exactly what `WalletManager.get_dashboard()`
  (`trading/wallet_manager.py:278-335`) returns — never an estimate.

## 6. Safety

- **PIT correctness for arena backtests.** If arena candidates are ever
  scored against historical data (not just forward paper P&L), that
  scoring **must** go through `validation/backtest.py`'s
  `WalkForwardBacktest` (`validation/backtest.py:39`), which is built on
  `store/pit.py`'s `PITStore` (`validation/backtest.py:31`, `store/pit.py:23`)
  and calls `assert_no_lookahead()` (`store/pit.py:210-245`) before any
  result is used. No new code path may query `resolved_series`/
  `feature_registry` directly for arena scoring — this is the CLAUDE.md
  non-negotiable guardrail, and it applies even though this feature is
  "just for dad's UI."
- **Paper-only.** Every arena wallet is created with `wallet_type="paper"`
  via `WalletManager.create_wallet` (`trading/wallet_manager.py:64-93`).
  Nothing in this feature may call the live-money paths in
  `api/routers/trading.py` (Hyperliquid `:381-428`, Robinhood `:430+`) —
  promoting a winning profile changes what dad *sees*, never what trades
  for real.
- **Journal immutability.** If a promoted profile's allocation later drives
  real paper trades, they log through `journal/log.py`'s `DecisionJournal
  .log_decision` (`journal/log.py:25,44-56`) exactly as today — this spec
  introduces no new mutate-in-place write path, consistent with the
  class's own docstring guarantee (`journal/log.py:4-7`).
- **What's logged.** Every profile create/delete/activate and every arena
  launch/promote is attributed to `owner` (JWT username via
  `_user_id_from_token`, the same helper `price_alerts.py:27` imports from
  `chat.py`), for audit — mirroring `sd_price_alerts.owner`
  (`price_alerts.py:41`).

## 7. Risks, effort, operator decisions

**Risks.**
- "Hundreds" of paper wallets per arena run is real DB load with no
  retention story today — `trading/wallet_manager.py` has no archival or
  cleanup path, so `trading_wallets` grows unbounded across repeated arena
  runs unless this feature adds one.
- D8 blocks all dad-facing UI until sign-off; backend (profiles CRUD,
  arena engine) can be built and tested standalone in the meantime.
- The mandate text itself ("hundreds", "arena battle") is under-specified —
  batch size, candidate-generation strategy, and promotion criteria are all
  undefined; see decisions below.

**Effort (rough, agent-days):**
- Phase 2 — profiles CRUD + save/load UI (pending D8 sign-off): 2–3 days.
- Phase 3 — arena (batch wallet generation, leaderboard, promote flow,
  retention job): 3–5 days, builds on Phase 2 tables.

**Decisions the operator must make before code starts:**
1. Sign off on the Home.jsx "Remember this" / "My Mixes" UI (D8), or
   explicitly defer Profiles' dad-facing half until reviewed separately
   from the backend.
2. Define "hundreds": target variant count, how candidates are generated
   (random weight perturbation around a base allocation? sector-rotation
   presets? LLM-authored variants?), and run cadence (on-demand only, or
   scheduled via Hermes).
3. Promotion criteria: manual operator pick from the leaderboard, or an
   auto-promote rule (e.g., top Sharpe after N trades — the same
   survive/die shape `trading/paper_engine.py:1-10`'s docstring already
   describes for strategies: *"win_rate > 40% AND drawdown < 5% AND
   Sharpe > 0.5"*)?
4. Retention: how long do non-winning arena wallets live before cleanup,
   and does that cleanup delete rows or just mark them inactive (the
   journal-immutability-adjacent question of whether losing arena data is
   ever discarded)?

---

# Feature 2: Voice

## 1. Problem statement

**Dad's story (73, on a phone, plain language):** Typing on a phone is slow
and error-prone for him, especially longer questions. He wants to tap a
button, say what he wants out loud ("how are my stocks doing"), and see the
same dashboard he'd get from typing it.

**Operator's story:** The fleet already runs a dedicated transcription
service — whisper.cpp (whisper-large-v3-turbo) on koala card 1
(`config.py:244-246`: `WHISPER_BASE_URL: str = "http://koala:8092"`,
`WHISPER_ENABLED: bool = True`), stood up specifically for this
(`AGENTS.md:71`, 2026-05-09 entry: *"Card 1 = ... Whisper service
(whisper.cpp, :8092) ... Round-trip voice test passed"*). The composer
code already anticipates this — `api/routers/chat.py:270`'s comment: *"The
user describes what they want to see (typed or, later, spoken via
Whisper)"*. Voice should be a thin proxy to that existing service, not a
bundled model, matching the operator's explicit instruction in this task's
mandate.

## 2. Acceptance criteria

- Tapping the mic button on `Home.jsx` starts recording with a visible,
  unambiguous recording indicator (not a subtle color change — a
  73-year-old needs to clearly tell "recording" from "not recording").
- Stopping recording sends the audio to the transcribe endpoint; the
  transcribed text fills the same `input` state `compose()` already reads
  (`pwa/src/views/Home.jsx:56-94`) — voice is purely an alternate way to
  populate the existing ask box, no parallel submission path.
- Empty or failed transcription shows a plain retry prompt, never a silent
  no-op or a garbage `compose()` call with empty text.
- If koala:8092 is unreachable or `WHISPER_ENABLED` is false, the mic
  button degrades gracefully (hidden, or a calm "voice isn't available
  right now — type instead" message) rather than erroring — the same
  graceful-degradation contract CLAUDE.md already states for
  Hyperspace/Ollama: *"calls return `None` if offline; system operates
  without them."*
- No audio is retained in the database beyond what's needed to transcribe
  it — dad's voice isn't a stored artifact.

## 3. API shape

New router `api/routers/voice.py`, mounted via the same table-driven list
in `api/main.py` used for `chat`/`price_alerts` (`api/main.py:401,438`).

```
POST /api/v1/voice/transcribe    contributor+   audio in, transcript out
```

- **Auth:** `require_auth` (`api/auth.py:222`) — same level as `compose`
  (`api/routers/chat.py:2022`); no admin gating needed, this is a dad-facing
  input method.
- **Request:** `multipart/form-data`, one audio file field (webm/opus or
  wav from the browser's `MediaRecorder`). Size capped (proposed 10 MB /
  ~90s) and validated at the boundary — `security.md`: *"Validate all
  user-supplied parameters at the boundary ... Don't trust that downstream
  code will clamp."*
- **Behavior:** thin proxy — stream the uploaded file to
  `WHISPER_BASE_URL` (`config.py:246`) and return the transcript. **Open
  question:** no code in this repo currently calls `WHISPER_BASE_URL` —
  grepping the codebase (`api/`, `intelligence/`, `llm/`, `scripts/`) shows
  only the `config.py` declaration and comments; the koala whisper.cpp
  server's actual request/response contract (field name, JSON shape,
  `/transcribe` vs `/inference` route) needs to be confirmed against the
  live service before this endpoint is built, not assumed from the config
  variable name.
- **Response:** `{"text": "...", "duration_s": 4.2}` on success;
  `{"text": "", "error": "voice_unavailable"}` with **HTTP 200** (not 500)
  when Whisper is disabled or koala is unreachable, so the frontend
  degrades on the same code path as "feature not configured" rather than
  treating a fleet hiccup as a hard error — mirrors how `compose()` returns
  `CARD_BUSY_MESSAGE` (`api/routers/chat.py:1091,2081-2086`) instead of a
  500 when the LLM is unreachable.
- **Rate limit:** reuse `grid_rate_limits` (`api/auth.py:60-71`) keyed
  `voice_transcribe`, tighter than text endpoints since audio uploads cost
  more (proposed 30/hour/ip).
- **Error cases:** no audio in the request → 422; audio over the size cap
  → 413; koala unreachable/timeout → 200 with `error: "voice_unavailable"`
  (graceful, per above); transcription returns empty text → `{"text": ""}`,
  frontend shows "I didn't catch that — try again," not a raw error.

## 4. DB shape

No new table for the core flow. Audio is proxied through and discarded —
nothing is stored, by design (privacy: dad's voice recordings and even
transcripts shouldn't accumulate in the database as a side effect of a
convenience feature). If the operator later wants usage telemetry (voice
vs. typed request ratio), that's a single counter/event row, not audio
storage, and is called out as an open question below rather than assumed
into this spec's baseline design. No migration is required for Phase 2
voice unless that telemetry is explicitly requested.

## 5. Frontend touchpoints

**Pending D8 sign-off**, same as Profiles — the mic button lives on
`Home.jsx`'s input row, both on the opening screen (`pwa/src/views/
Home.jsx:137-143`, `S.box`/`S.btn` styles at `:286-294`) and the bottom ask
bar on the answer screen (`Home.jsx:186-190`, `S.boxSmall`/`S.btnSmall` at
`:377-387`).

In words: a round mic icon button to the left of the existing text input,
matching its height. Tap once to start — the icon fills solid red with a
soft pulsing ring and a text label "Listening…" next to it (a legible cue,
not just a color swap, per the acceptance criteria). Tap again to stop.
While the audio uploads and transcribes, the *same* `Working`/`Dots`
loading pattern already used for `compose()` (`Home.jsx:198-213`,
`workingInline` style at `:347`) reappears, so dad sees one consistent
"thinking" animation regardless of whether he typed or spoke. On success
the transcript fills the input field; whether it also auto-submits via the
existing `compose()` call, or waits for one more tap, is an explicit
operator decision below (accuracy vs. friction trade-off, not a default
this spec presumes).

Uses the browser's native `MediaRecorder` API — no new frontend dependency,
consistent with `frontend.md`'s "don't introduce new frameworks" rule.
`pwa/src/api.js` gains one method, `transcribeVoice(blob)`, POSTing
`FormData` through the existing `_fetch` wrapper (pattern at
`pwa/src/api.js:904-938`).

## 6. Safety

- **PIT correctness:** not applicable. Voice only fills a text box before
  the existing `compose()` call runs; it introduces no new read of
  analytical data and no new inference path, so there is no PIT surface to
  violate.
- **Paper-only trading:** not applicable. Voice cannot trigger a trade any
  more than typing can — `compose()` already only ever creates a price
  alert or a dashboard layout (`api/routers/chat.py:2034-2138`), never a
  trade.
- **Journal immutability:** not applicable to transcription itself. If
  usage telemetry is added later (open question), it must be append-only
  events, never an editable row — the same spirit `journal/log.py:4-7`
  states for decisions, applied by analogy even though this feature
  wouldn't literally use `DecisionJournal`.
- **What's logged:** request metadata only — owner, timestamp, audio
  duration, success/failure — at `log.warning`/`log.debug` per CLAUDE.md's
  log-level rule (*"Transient network errors ... use `log.warning`"*).
  Audio content is never logged; transcript text is never written to
  `errors.jsonl` or any log beyond what's strictly needed to debug one
  specific reported failure, and even then only at debug level and only
  transiently.
- **Secrets:** `WHISPER_BASE_URL` is an internal-fleet hostname already in
  `config.py`, not a secret — same category as `OLLAMA_KOALA_BASE_URL`
  (`config.py:218`) and `KOKORO_TTS_BASE_URL` (`config.py:240`). No new
  secret-handling is introduced by proxying to it.

## 7. Risks, effort, operator decisions

**Risks.**
- The whisper.cpp server's real HTTP contract is unverified from this
  checkout (see API shape above) — building against an assumed contract
  risks a broken integration discovered only at deploy time.
- Browser mic permissions and `MediaRecorder` codec support vary by
  device/OS; iOS Safari (likely what dad's phone runs, since
  stepdad.finance is a PWA) has known `MediaRecorder` quirks that need a
  real-device pass, not just a desktop-browser check — per CLAUDE.md's UI
  rule to test the actual feature in a browser before calling it done,
  applied here specifically to mobile Safari/Chrome, not just `npm run
  dev` on a laptop.
- D8 sign-off blocks all frontend work, same as Profiles.

**Effort (rough, agent-days):**
- Confirm koala's whisper.cpp contract + backend proxy endpoint: 0.5–1 day
  (contract confirmation first, endpoint is small once confirmed).
- Frontend mic button + `MediaRecorder` wiring + loading states (pending
  D8 sign-off): 1–2 days.
- Real-device testing pass on dad's actual phone/OS: 0.5 day.

**Decisions the operator must make before code starts:**
1. Confirm koala:8092's actual whisper.cpp HTTP contract (this spec cannot
   verify a live service from a static checkout).
2. Sign off on the mic button touching `Home.jsx` (D8).
3. Auto-submit after transcription, or require a confirming tap before
   `compose()` fires?
4. Any voice usage telemetry desired, or fully stateless (no persistence
   of transcript text at all) by default?
5. What should the mic button do on a device with no microphone permission
   granted — hide entirely, or show a one-time "allow microphone" prompt
   path?

---

# Open questions for the operator (consolidated)

1. **D8 sign-off** — both features need `Home.jsx` changes (a "Remember
   this" button + "My Mixes" panel for Profiles; a mic button for Voice).
   Per `docs/planning/V5-TRANSFORMATION.md:390-397`, nothing touches that
   file without explicit operator approval. Backend work for both features
   can proceed without this; no pixel ships to dad until it's granted.
2. **"Hundreds"** — target arena variant count, candidate-generation
   strategy, and run cadence are undefined in the mandate and need an
   operator call before the arena engine is built.
3. **Promotion criteria** — manual pick from the leaderboard vs. an
   auto-promote rule, and if automatic, what thresholds.
4. **Arena retention** — how long losing candidate wallets live, and
   whether cleanup deletes or just deactivates them.
5. **Whisper contract** — koala:8092's actual request/response shape needs
   confirming against the live service; nothing in this repo calls it yet.
6. **Voice auto-submit** — fill-and-wait vs. fill-and-go after
   transcription.
7. **Voice telemetry** — none by default (stateless), or a lightweight
   usage counter.
