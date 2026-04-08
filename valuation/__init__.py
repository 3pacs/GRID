"""
GRID Valuation & Derivatives Support Module.

Combines balance-sheet intrinsic values, company milestones/guidance,
and derivatives positioning into a unified valuation timeline.

Modules:
  - intrinsic: Balance sheet + earnings-based intrinsic value calculations
  - milestones: Company plans, guidance, rumors tracker with probability weights
  - derivatives_support: Short float + GEX + options positioning composite
  - composite: Ties everything together into timeline + Claude Max prompt
"""
