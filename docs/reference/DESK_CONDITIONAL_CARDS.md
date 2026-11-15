# Conditional outcomes, not just a pass/fail badge

Stacked on #1220. Pure `conditional_factory_cards(raw, **adapter_args)` and
`render_conditional_cards(raw, **adapter_args)` preserve #1217 report cuts.
No producer change, file read/write, scheduler, model call or research execution.

## Preserved
Overall and every supplied year/market-phase/Fed-balance-sheet/VIX-third/spike/
sector/group/sector-phase/sector-vs-market stratum, including losers. Nights and
gross_bp/net_bp/sd_bp/t_gross stay numerically unchanged. Under 30 nights the
producer supplies counts only; no means/statistics are invented. Null t stays
unknown. Metadata keeps the cost assumptions and sector-assignment caveats
without calling them measured or point-in-time verified. Original report bytes
are never changed and the independently pinned digest remains in output.

## Interpretation
These are retrospective development descriptions, not independent confirmations.
t_gross is the gross-mean statistic, not a net-mean t-test. Strata overlap and
cannot be pooled by adding their nights. Favorable strata are not reranked,
selected or promoted. Negative overall outcomes remain visible.
Confirmation and forward status say not_supplied, never passed. A separate
failed-confirmation record must still be joined by its actual owner; this
report schema does not contain it. Do not replace or erase it with this card.

## Validation and display
Uses the existing report hash/clock/selection validation. Cards must belong to
ranked trials; overall nights equal the ranked count. Cuts cannot exceed overall.
Unknown dimensions and incomplete metric shapes reject rather than vanish.
Source metadata is depth/size/numeric bounded and marked unverified. Renderer
operates directly on verified bytes, not a mutable pre-built card, and escapes
source labels. Withheld/stale evidence remains visible as withheld, not current.

## Checks
```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_conditional_cards.py' -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_factory_report.py' -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_desk_evidence.py' -v
```
Expected 19 + 26 + 17 synthetic tests. No actual research artifacts or holdouts.
Claude verifies exact kit; Anik applies. Full #1164 still requires real caller
wiring, separate confirmation/forward outcomes, hypothesis/invalidation context
and the other desk inputs. This is not an automatically validated strategy.

## Missing regime coverage
The producer's year/market-phase/Fed/VIX/spike groups partition traded nights.
For these dimensions only, show supplied classified nights and the residual
unclassified nights against the overall count; missing dimension says
not_supplied, not zero coverage. Reject sums exceeding the overall count.
This checks reported counts, not actual timestamp uniqueness or point-in-time
label quality. Sector/group/sector-phase/sector-vs-market memberships can overlap
within a night and are never added into a unique-night denominator.
