# Hand-offs — 2026-09-10 evening

Nine independent work packages, written so separate agents (Opus sessions or
Gemini via `gemini-task.yml`) can run them in parallel. Every agent reads
`00-COMMON.md` first. Branches are per hand-off so nothing collides; the only
shared resource is the grid-svr runner.

| # | Hand-off | Needs the operator first? | Server lane | Code |
|---|---|---|---|---|
| 01 | Land the merged fixes, restart Hermes daemons, prove the error log went quiet | yes — restart the stalled runner | heavy | none unless regression |
| 02 | Replace dead FRED series (buybacks, credit proxies) | no | one lookup + one pull | small |
| 03 | Options tapes score zero hits — fix outcome scoring | no | one rescoring run | medium |
| 04 | No CPU-only Qwen: retire :8080, embeddings to a tailnet GPU node, Gemini provider | confirm the OCMRI reading | unit changes (sudo) | medium |
| 05 | Insider Form 4 transaction code → motivation not "Unknown" | no | one backfill | medium |
| 06 | Robinhood: activate account, wallet, rotation/executor wiring | yes — create the API credential | status checks | medium |
| 07 | Fold SEC-filer actor nodes into ticker actors, rerun curated graph | no | one detached rerun | medium |
| 08 | Long Plays entry gate reachable for trial gems; fix empty sweep verdicts | no | one board rebuild | medium |
| 09 | Move test CI to the Dell runner "alien" | yes — install the runner | none on grid-svr | small |

## Launching

- **Opus session**: paste the hand-off file contents (plus 00-COMMON) as the
  opening message, or point the session at `docs/handoffs/2026-09-10/NN-*.md`
  on `main`.
- **Gemini on grid-svr**: dispatch `gemini-task.yml` with `prompt` =
  "Read docs/handoffs/2026-09-10/00-COMMON.md and NN-<slug>.md in the current
  directory and carry out the hand-off. Report what changed, what was verified,
  what is blocked." Best for 01 and the server-side halves of 02, 03, 05, 07, 08.
  Gemini has the box; code PRs still need a reviewer before merge.

## Ordering hints

01 first (it unblocks every server-side verification). 02–09 are independent
of each other. 04's server step (disabling the CPU unit) and 01's restarts
should not run in the same minute — coordinate through the operator or run 04
after 01 reports.

## What was finished today (context)

- Lever map: options/congressional feeds in puller categories, curated Louvain
  communities, motivation models (#408–#410).
- Hermes error-log repairs (#411): yfinance flood, extractor timeouts, worker
  400s, ECB dupes, source-audit scan, BLS key, IMF host, Qwen 3.8 config alignment.
- Robinhood crypto connector (#412), dry-run default.
- Verified fleet: Qwen 3.8 27B on every GPU tier; only the CPU :8080 unit is 3.6.
