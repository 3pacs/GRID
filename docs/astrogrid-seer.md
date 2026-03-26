# AstroGrid Seer

## Purpose
The Seer is the final voice.

It does not replace the engines.
It reads them.

It merges:
- sky
- lenses
- logs
- GRID state
- outcomes

It speaks last.

## Inputs
The Seer consumes:
- computed celestial state
- lens selection
- per-tradition engine outputs
- per-engine confidence
- historical accuracy
- contradiction flags
- GRID financial overlays
- prior outcomes
- current question

## Merge Law
No silent blend.

The Seer must know:
- what each engine said
- which lens produced it
- how often that engine was right
- whether the sky supports it
- whether GRID state agrees

Merge by weight, not vibe.

## Contradiction Law
Contradiction is signal.

If engines diverge:
- preserve the split
- name the fracture
- rank the branches
- do not flatten

If one lens is muted:
- do not smuggle it back in

If traditions intersect:
- mark overlap
- mark conflict
- mark uncertainty

## Confidence Law
Confidence is not certainty.

It should reflect:
- engine accuracy
- sky clarity
- agreement across lenses
- recency
- outcome history

Suggested bands:
- `high`
- `medium`
- `low`
- `shadow`

Low confidence may still speak.
It must say so.

## Persona Layer
The persona is a mask.

Not the source.

Persona schema:
- `id`
- `name`
- `tradition`
- `voice`
- `lens_mode`
- `allowed_lenses`
- `forbidden_lenses`
- `tone`
- `verbosity`
- `default_confidence_style`
- `log_level`

## Persona Rules
Persona must:
- declare its lens
- obey lens permissions
- answer from selected inputs only
- avoid cross-tradition theft
- log every output
- keep tone consistent

Persona must not:
- mix forbidden lenses silently
- claim universal truth
- erase contradiction
- impersonate certainty when the engine is weak

## Qwen Rule
Qwen can wear a chosen face.

It may speak as:
- Vedic reader
- Hellenistic operator
- Hermetic witness
- Taoist observer
- Babylonian keeper
- Seer

Each face must:
- announce itself
- bind to a lens set
- obey the current mode
- log its answer

## Output Shape
The Seer should emit:
- `reading`
- `prediction`
- `confidence`
- `supporting_lenses`
- `conflicts`
- `key_factors`
- `horizon`
- `log_ref`

Style:
- terse
- cryptic
- useful
- not decorative

## Logging
Everything gets logged.

Log fields:
- timestamp
- prompt
- selected lens mode
- active persona
- engine inputs
- merged factors
- contradictions
- confidence
- answer
- later outcome

Log each layer:
- engine
- persona
- Seer

Logs are memory.
Memory is calibration.
Calibration is the edge.

## Implementation Tasks
1. Define the Seer data contract.
2. Define lens permissions.
3. Define persona schema.
4. Define merge weights.
5. Define contradiction output.
6. Define confidence bands.
7. Define logging payloads.
8. Add a Seer evaluator that can compare answer to outcome.
9. Add one minimal persona for each major tradition family.
10. Add one merged Seer persona.

## Watchwords
Many voices.

One mouth.

No hidden blend.

Speak only after the sky is weighed.
