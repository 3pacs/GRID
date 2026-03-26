# AstroGrid Celestial Registry

## Law
No fake sky.

If it can be computed reliably, track it.

If it cannot be computed reliably, do not paint it as truth.

## Registry Purpose
The registry defines what AstroGrid is allowed to render as celestial state.

Each object must declare:
- `id`
- `name`
- `class`
- `source`
- `precision`
- `enabled`
- `visual_priority`
- `track_mode`

## Core Set
Track from the start:
- Sun
- Moon
- Mercury
- Venus
- Mars
- Jupiter
- Saturn
- Uranus
- Neptune
- Pluto
- Rahu
- Ketu

These form the first lawful sky.

## Second Ring
Track when source quality is confirmed:
- Chiron
- Ceres
- Pallas
- Juno
- Vesta
- major fixed stars
- eclipse points

## Third Ring
Track only with strong source support:
- additional asteroids
- comets
- selected deep sky anchors

No ornamental clutter.

## Object Classes
- `luminary`
- `planet`
- `dwarf_planet`
- `node`
- `asteroid`
- `fixed_star`
- `event_point`
- `deep_sky`

## Source Classes
- `authoritative`
- `computed`
- `derived`

`authoritative`
: highest-trust astro source

`computed`
: local or internal deterministic math

`derived`
: not directly observed; inferred from other state

## Precision Classes
- `exact`
- `high`
- `medium`
- `approx`

The UI should know the difference.

## Track Modes
- `always`
- `optional`
- `research`
- `hidden`

`always`
: part of default sky

`optional`
: user can enable

`research`
: visible only in advanced or experimental mode

`hidden`
: kept in system, not rendered by default

## Required Object Payload
Every tracked object should expose:
- `id`
- `timestamp`
- `longitude`
- `latitude`
- `right_ascension`
- `declination`
- `distance`
- `speed`
- `is_retrograde`
- `sign`
- `degree_in_sign`
- `house` later
- `visibility_flags`
- `precision`
- `source`

## Derived Object Payload
For certain classes, also expose:
- aspect relations
- cycle state
- event thresholds
- ingress flags
- station flags
- eclipse relation

## Rendering Law
No object without payload.

No payload without source.

No source without precision label.

## Visual Priority
Suggested tiers:

### Tier 1
- Sun
- Moon
- Mercury
- Venus
- Mars
- Jupiter
- Saturn

### Tier 2
- Uranus
- Neptune
- Pluto
- Rahu
- Ketu

### Tier 3
- asteroids
- fixed stars
- event points

Priority controls:
- label persistence
- line weight
- glyph size
- inspector order

## Registry Example

```json
{
  "id": "venus",
  "name": "Venus",
  "class": "planet",
  "source": "authoritative",
  "precision": "high",
  "enabled": true,
  "visual_priority": 1,
  "track_mode": "always"
}
```

## Immediate Tasks
1. define registry schema
2. define core object set
3. define second-ring object list
4. define precision labels
5. define source labels
6. define payload contract
7. make all AstroGrid views read from registry-backed object data

## Watchword
If the sky is not computed, it is not in the chamber.
