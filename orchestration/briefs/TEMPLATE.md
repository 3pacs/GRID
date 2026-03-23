# GRID Task Brief

## Context
GRID is a multi-agent trading intelligence platform.
- **Backend:** Python 3.11, FastAPI, SQLAlchemy 2.0, PostgreSQL 15 + TimescaleDB
- **Frontend:** React 18, Vite, Zustand, inline styles (no CSS framework), dark theme (#080C10 bg)
- **Icons:** Lucide React
- **LLM:** llama.cpp local inference (Hermes 8B)

## Your Task
<!-- FILL: specific task description -->

## Constraints
<!-- FILL: what the model must NOT do -->
- Do not introduce new dependencies unless absolutely necessary
- Follow existing patterns in the codebase (examples below)
- All data queries must be PIT-correct (no future data leakage)
- SQL must use parameterized queries (never f-strings or .format())
- Python functions need type hints
- React components use functional style + hooks, Zustand for state

## Existing Patterns
<!-- FILL: relevant code snippets showing how things are currently done -->

## Expected Output
<!-- FILL: what you want back — a component, a function, a design, etc. -->

## File Placement
<!-- FILL: where the output goes in the repo -->
