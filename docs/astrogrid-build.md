# AstroGrid Build

## Core Model
- `sky`
- `bodies`
- `positions`
- `motions`
- `aspects`
- `cycles`
- `events`
- `signals`
- `engines`
- `seer`
- `personas`
- `logs`

Everything hangs from computed sky state.

## Rooms
- Sky
- Ephemeris
- Lenses
- Engines
- Seer
- Events
- Readings
- Signals
- Vault
- Logs
- Settings

## Primary Entities

### `sky_snapshot`
- timestamp
- location
- source
- precision
- bodies payload
- aspects payload
- cycles payload
- events payload

### `engine_run`
- engine key
- lens mode
- sky snapshot id
- input feature set
- reading
- omen
- prediction claims
- confidence
- horizon
- citations to features used

### `seer_run`
- sky snapshot id
- active engines
- merge mode
- convergence map
- contradiction map
- world-state overlays used
- final reading
- final prediction set
- confidence band

### `persona_run`
- persona key
- source mode
- allowed lenses
- question
- answer
- cited engine runs
- cited seer run

### `outcome_log`
- target run id
- prediction claim
- target horizon
- observed outcome
- score
- notes

## Source Order
1. compute sky
2. derive signals
3. run engines separately
4. log engine outputs
5. merge through Seer
6. log Seer output
7. answer through persona
8. score later against reality

## Hard Laws
- no invented numbers
- no silent mixing of traditions
- no Seer without source engine traces
- no persona answer without declared lens basis
- no prediction without later scoring path

## Financial Layer
AstroGrid may emit:
- timing bias
- regime omen
- volatility omen
- directional caution
- resonance windows

But financial output is a layer.

It does not replace the celestial spine.

## Product Lanes

### Lane 1: Observatory
- computed sky
- orrery
- aspect field
- event pulses
- cycle bands

### Lane 2: Engines
- tradition-specific readings
- separate ledgers
- toggleable lenses

### Lane 3: Seer
- merged reading
- merged forecast
- contradiction display

### Lane 4: Commerce
- horoscopes
- paid readings
- compute access
- NFT or artifact drops
- agent access

## Build Sequence

### Phase 1
- standalone branch
- standalone shell
- runtime separation
- authoritative sky contract
- local fallback math contract

### Phase 2
- Sky room
- Ephemeris room
- Events room
- Signals room

### Phase 3
- Lenses room
- engine registry
- engine run logging
- separate readings UI

### Phase 4
- Seer room
- merge logic
- contradiction view
- confidence view

### Phase 5
- persona layer
- Qwen masks
- question interface
- answer logging

### Phase 6
- horoscopes
- financial analysis products
- compute products
- Vault surfaces

## Immediate Tasks
- define authoritative astro source contract
- define fallback local math contract
- define celestial object registry
- define object payload contract
- define visualization payload contract
- define engine registry schema
- define lens state schema
- define Seer merge schema
- define persona schema
- define log tables and namespaces
- define room-by-room UI ownership
- define which GRID signals AstroGrid may legally consume

## Branch Discipline
- branch: `astrogrid` or `codex/astrogrid-*`
- keep AstroGrid docs and code out of generic GRID lanes
- treat shared DB and API work as explicit integration gates

## Frontend Split
- `astrogrid/` ships the product shell
- `astrogrid_web/` is the fast observatory lab
- `astrogrid_shared/` holds shared frontend logic that both surfaces may reuse

Do not fork payload normalization or celestial math twice.

Prototype in `astrogrid_web/`.

Promote stable pieces into `astrogrid/`.

## Frontend Split
- `astrogrid/` ships the product shell
- `astrogrid_web/` remains the fast visual lab
- `astrogrid_shared/` holds reusable frontend AstroGrid logic
- duplicate math or payload shaping moves into `astrogrid_shared/`
- snapshot normalization belongs in `astrogrid_shared/snapshot.js`
- prototype visuals graduate from `astrogrid_web/` into `astrogrid/`, then stop diverging

## Watchwords
Compute first.

Split cleanly.

Log everything.

Merge late.

Speak once.
