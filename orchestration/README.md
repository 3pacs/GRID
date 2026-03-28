# GRID Multi-Model Orchestration

This directory enables multi-model collaboration on GRID without API access.
Each model works through its chat UI. Git is the coordination bus.

## Structure

```
orchestration/
├── briefs/          # Task briefs to paste into other model UIs
│   ├── TEMPLATE.md  # Base template for all briefs
│   ├── astrogrid.md # AstroGrid standalone planning brief
│   ├── ux.md        # UX/frontend brief (Gemini)
│   ├── algo.md      # Algorithm/logic brief (Codex/ChatGPT)
│   └── research.md  # Research brief (any model)
├── inbox/           # Paste model outputs here (code, designs, configs)
│   └── .gitkeep
├── integrate.py     # Validates and slots inbox contributions into codebase
└── reconcile.py     # Reviews external contributions for style/safety
```

## Workflow

1. **Generate brief:** `python orchestration/integrate.py brief ux`
   → prints a ready-to-paste prompt for the target model

2. **Paste into model UI** (Gemini, ChatGPT, Codex, etc.)
   → model produces code/design

3. **Save output:** paste result into `orchestration/inbox/<name>.jsx` (or `.py`, `.sql`, etc.)

4. **Integrate:** `python orchestration/integrate.py slot inbox/MyComponent.jsx`
   → validates, moves to correct location, runs tests

5. **Reconcile:** `python orchestration/reconcile.py`
   → reviews all recent external contributions for style drift, security, PIT correctness
