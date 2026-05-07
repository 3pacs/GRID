### 1. Pre-flight coverage check (mandatory for new files)

Before creating any new module, script, ingestor, or intelligence component:

1. The dispatcher has already run `scripts/pre_create_check.py "<concept>"` for your primary concept and embedded the output below. READ IT. If it shows existing coverage, the default is to EXTEND the canonical module, not create a new one.
2. [[Cross Reference|Cross-reference]] `docs/MODULE_INVENTORY.md` for every module that touches the same table or signal.
3. Only create new files when `pre_create_check` exits 1 AND inventory shows no coverage. Document your decision in the return JSON.
