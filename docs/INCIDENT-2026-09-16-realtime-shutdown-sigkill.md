# Investigation: grid-realtime's first restart required a forced SIGKILL

**Status:** OPEN -- investigation only, no code or production changes in this doc/PR
**Date of event:** 2026-09-16, during grid-realtime's first-ever restart (PR #514
activation)
**Owner:** Anik (decisions) + agent (further investigation, if pursued)

This document strictly separates what was directly observed (journal log lines, a
read-only database query) from what remains unconfirmed, and from hypotheses about
root cause. Hypotheses are grounded in code inspection and historical data, but are
**not proven** -- the processes involved were already terminated by the time this
investigation began, so they could not be inspected live.

## 1. Observed facts (journal log, verbatim, `journalctl -u grid-realtime`, `short-iso`)

```
2026-09-16T19:17:33+0000  Flushed 80 candles to realtime_candles (79 symbols)   [periodic flusher, routine]
2026-09-16T19:18:03+0000  systemd: Stopping GRID Realtime Market Data Listener...
2026-09-16T19:18:03+0000  Received signal 15 — initiating graceful shutdown
2026-09-16T19:18:03+0000  Cancelling feed tasks...
2026-09-16T19:18:03+0000  Yahoo feed cancelled — shutting down
2026-09-16T19:18:03+0000  DEX scanner cancelled — shutting down
2026-09-16T19:18:11+0000  Binance WS feed cancelled — shutting down
2026-09-16T19:18:12+0000  Flushing 723 remaining candles...
                          (( 81 seconds of total silence from PID 16196 -- no further
                             log line of any kind, at any level, appears before the
                             kill below ))
2026-09-16T19:19:33+0000  systemd: grid-realtime.service: State 'stop-sigterm' timed out. Killing.
2026-09-16T19:19:33+0000  Killing process 16196 (python3) with signal SIGKILL     [main process]
2026-09-16T19:19:33+0000  Killing process 17962 (python3) with signal SIGKILL
2026-09-16T19:19:33+0000  Killing process 51883 (python3) with signal SIGKILL
                          ... (23 more python3-named PIDs in the 51883-51945 range) ...
2026-09-16T19:19:33+0000  Killing process 55004 (jemalloc_bg_thd) with signal SIGKILL
2026-09-16T19:19:33+0000  Killing process 58366 (python3) with signal SIGKILL
                          ... (10 more python3-named PIDs in the 58366-58395 range,
                             two labeled "n/a") ...
2026-09-16T19:19:33+0000  Killing process 381321 (python3) with signal SIGKILL
2026-09-16T19:19:33+0000  Killing process 2459161 (n/a) with signal SIGKILL
2026-09-16T19:19:33+0000  Killing process 2968064 (python3) with signal SIGKILL
2026-09-16T19:19:34+0000  Main process exited, code=killed, status=9/KILL
2026-09-16T19:19:34+0000  grid-realtime.service: Failed with result 'timeout'.
2026-09-16T19:19:34+0000  Consumed 3d 8h 36min 7.473s CPU time.        [cumulative over the 49-day run]
2026-09-16T19:19:34+0000  Started GRID Realtime Market Data Listener.  [new process, PID 2999240]
```

Full untruncated log for this window is quoted above in its entirety (no lines
omitted between 19:17:33 and 19:19:34) -- this is the complete record for the
shutdown attempt.

**Total: 42 distinct PIDs killed** alongside the main process (16196), spanning
label `python3` (37), `n/a` (3, meaning the process had already exited/exec'd by
the time systemd read `/proc/<pid>/comm`, or the name field was otherwise
unavailable), and `jemalloc_bg_thd` (1). **Confirmed** (2026-09-16 follow-up,
see §4c): these are genuine separate forked processes, not thread IDs.
systemd's `Killing process` log line for a stop-timeout SIGKILL fallback
iterates only `cgroup.procs`, which lists thread-group leaders (i.e. distinct
processes) -- it never enumerates individual thread IDs. (This corrects the
original version of this document, which read the same ambiguity in the log
line alone as leaving the thread-vs-process question open, and leaned toward
"mostly threads" as the more likely read at the time -- superseded by the
follow-up's direct check of the actual systemd/kernel semantics, not a new
guess.)

**Elapsed time**: `systemctl restart` was issued at approximately 19:18:01 (the
GitHub Actions step group start for `Restart grid-realtime`); SIGKILL fired at
19:19:33 -- **~92 seconds**, closely matching systemd's default `TimeoutStopUSec`
(90s; this unit does not override it). This is the stop-timeout elapsing, not a
faster or slower kill.

**PID number pattern**: the 42 killed PIDs cluster into two tight numeric ranges
(51883-51945, ~30 PIDs; 58366-58395, ~12 PIDs) plus four isolated outliers
(17962, 381321, 2459161, 2968064) whose PID numbers are far apart from each other
and from the two clusters -- consistent with (not proof of) accumulation at
different, widely-spaced points over the 49-day uptime, rather than all being
created in one recent burst.

## 2. What a direct, read-only query establishes -- and what it does NOT

**Exactly what was checked:**

```sql
SELECT source, count(*), min(created_at), max(created_at)
FROM realtime_candles
WHERE ts > TIMESTAMP '2026-09-16 18:00:00+00'
  AND created_at BETWEEN '2026-09-16T19:18:00Z' AND '2026-09-16T19:19:34Z'
GROUP BY source
```

Result: **zero rows**, for any source, with `created_at` in that window.

**What this establishes, precisely**: no row was newly INSERTED with a
`created_at` timestamp between the "Flushing 723 remaining candles..." log
line and the kill. That is all a `created_at` timestamp can prove, because
`created_at` is set only by an actual `INSERT` (`DEFAULT now()`, never
touched again).

**What this does NOT establish**: that the flush never completed.
`INSERT_SQL` is `ON CONFLICT (symbol, interval, ts) DO NOTHING` --a
completed flush attempt that tries to write N rows where some or all of
those exact `(symbol, interval, ts)` keys **already existed** in the table
would insert **fewer than N rows, or zero**, and produce no `created_at`
change for any of them, indistinguishable from "never attempted" by this
query alone. Whether any of the 723 keys in this specific batch already
existed was not independently checked -- the exact list of 723
`(symbol, ts)` pairs was never logged and cannot be reconstructed now that
the process is gone, so this cannot be resolved after the fact. A
supporting (not conclusive) observation: the periodic flusher's own last
successful cycle, 30 seconds earlier ("Flushed 80 candles... 79 symbols" at
19:17:33), would have just started a fresh, not-yet-flushed bucket for
each of those ~79 continuously-active symbols -- for those specific keys
specifically, a first-ever write for that timestamp is the more likely
case, which would make a DO-NOTHING-masked completion less likely for
*them*. This reasoning does not extend to the remaining ~640+ entries in
the 723 (the long-accumulated, rarely-active `dex_scanner` symbols -- see
hypothesis 4a), whose prior-existence status is simply unknown.

**Conclusion for this section: zero rows were newly inserted in that
window. Whether the final flush completed is UNCONFIRMED**, not
established either way by this query -- see Section 3.

## 3. Unconfirmed

- **Whether the final shutdown flush completed.** Not established by the
  database query above (see Section 2 for exactly why -- `DO NOTHING` can
  mask a completed attempt) and not established by any surviving log line
  either (neither "Final flush: N candles written" nor "Final flush timed
  out after 30s" -- the two outcomes the code itself would log -- appears
  anywhere before the kill). No independent evidence (a stack trace, a
  core dump, an APM trace) exists to resolve this either way. Absent such
  evidence, this remains open in both directions: a completed flush that
  happened to write zero *new* rows is not ruled out, and neither is a
  flush that never ran to completion at all.
- **Where exactly execution was stuck** between "Flushing 723 remaining
  candles..." (19:18:12) and the kill (19:19:33), if it was in fact stuck
  rather than completed-with-nothing-to-insert. The code path is:
  `log.info(...)` → `builder.flush_all()` (synchronous, pure in-memory
  dict iteration, should be fast even for 723 entries) → `builder.drain()`
  (same) → `build_insert_values()` (same) → `await asyncio.wait_for(
  bounded_write(_write_final_flush_sync, rows), timeout=30)`. The
  30-second bound wraps only the last step. 81 seconds of total silence,
  with zero log output of any kind, means execution did not reach the end
  of that `await` in the normal way (or, per the point above, executed it
  in a way that produced no distinguishing log or row) -- but *why* is not
  established. No stack trace, core dump, or other diagnostic was
  captured before the SIGKILL destroyed the process; none of these can be
  reconstructed after the fact.
- Whether the 723-candle count itself (vs. flusher.py's routine ~80)
  directly caused a hang, versus being coincidental to some other stuck
  condition (or to no hang at all, per the point above), is not
  established -- see hypothesis below.
- Whether the killed PIDs were genuinely leaked/orphaned resources
  accumulated over time, or were created in the final moments of the
  shutdown attempt itself, is not established from the log alone (the
  numeric-range clustering is suggestive, not conclusive).

## 4. Hypotheses (not confirmed -- grounded in code/data, not proof)

### 4a. Why 723 "remaining candles" (vs. the routine ~80)

**Grounded in code inspection, high confidence this is at least part of the
explanation**: `CandleBuilder.candles` (`ingestion/realtime/candle_builder.py`) is
a plain dict keyed by `(symbol, interval)`. An entry is only ever removed by being
moved into the flush queue when a **new tick for that exact key** arrives in a
later bucket (`ingest()`), or swept by `flush_all()` at shutdown. There is no
time-based or count-based eviction for a symbol that simply stops receiving
ticks -- so a symbol seen exactly once, then never again, leaves a permanent entry
in `builder.candles` until the next shutdown.

`ingestion/realtime/feeds/dex_scanner.py` discovers liquidity-spike tokens
dynamically -- its symbol set is not a fixed list like Binance's 31 or Yahoo's ~31,
it grows with every distinct token that has ever spiked. Queried directly (also
read-only):

```sql
SELECT source, count(DISTINCT symbol) FROM realtime_candles
WHERE ts > now() - interval '60 days' GROUP BY source
```
→ `binance: 28`, `dex: 794`, `yahoo: 34`

**794 distinct `dex` symbols over 60 days** closely matches the 723 figure logged
at shutdown (the two numbers are not expected to match exactly -- 723 was a
point-in-time snapshot after 49 days, 794 is a 60-day distinct count -- but the
order of magnitude and which source dominates both agree). This is the most
likely explanation for why `active_symbols` reached 723 rather than the routine
~80: `dex_scanner`'s ever-growing, never-evicted symbol set, not a general leak in
Binance or Yahoo handling.

### 4b. Why the flush of that many candles might stall

**Not confirmed.** 723 rows in a single `execute_batch` call is not, on its own,
an unusually large batch for Postgres -- the routine periodic flush already
handles ~80 without issue, and `page_size=500` batching in
`execute_batch(cur, INSERT_SQL, rows, page_size=500)` means 723 rows would be sent
as two sub-batches, not one giant statement. Nothing in the code or logs directly
shows the DB write itself was the bottleneck as opposed to something earlier in
the synchronous `flush_all()`/`drain()`/`build_insert_values()` chain, or a
resource-starvation issue unrelated to the row count (see 4c).

### 4c. Why 42 PIDs, including a `jemalloc_bg_thd`-named one, were killed

**Updated 2026-09-16, incorporating a follow-up investigation session's
direct evidence** (credited throughout this section; not this document's
own original work). Two things are now on materially firmer footing than
the original version of this section, and one remains genuinely open.

**Confirmed, not circumstantial: the 42 PIDs are real forked processes.**
systemd's cgroup-kill fallback (`Killing process NNNN`) enumerates
`cgroup.procs` specifically -- thread-group leaders only. It never lists
individual thread IDs. So whatever spawned these 42 processes used a real
`fork()`/`clone()` without `CLONE_THREAD`, not Python's `threading` module.
This replaces the original version's weaker, log-line-only reasoning
("balance of evidence favors mostly-threads") with a direct check of the
actual kernel/systemd semantics involved.

**The originally-proposed yfinance/curl_cffi thread-leak hypothesis is
not supported by direct investigation, but is not conclusively ruled
out either.** The follow-up session: grepped the installed `yfinance`
1.7.0 and `curl_cffi` 0.16.3 source trees on grid-svr for
`subprocess`/`multiprocessing`/`os.fork` -- zero matches in either. Traced
`yf.download(..., threads=True)` (confirmed in `yahoo.py` source, line 87)
to the `multitasking` package it depends on, which defaults to
`ENGINE="thread"` -- confirmed live by importing it in the actual
production venv and running a real `yf.download()` call, observing zero
threads left behind afterward. Live-`strace`'d the actual running
production process (PID 2999240) for 90 seconds spanning two real 60-second
Yahoo poll cycles: every `clone3()` call carried `CLONE_THREAD`, and thread
creation/exit counts balanced -- ordinary, short-lived thread churn (~31
threads per poll), not a leak, in the window observed. Given the leak's own
estimated rate (next paragraph) is roughly one event per ~1,400 poll
cycles, a 2-cycle trace was never likely to catch it regardless of whether
the hypothesis is true -- **this evidence weighs against yfinance/curl_cffi
as the source, it does not conclusively rule out an intermittent cause
there or elsewhere.** `jemalloc_bg_thd`'s presence as one of the 42 *is*
still consistent with some native/compiled dependency being involved
somewhere in the process tree -- that observation stands; only the specific
yfinance/curl_cffi explanation for it has been investigated and weakened,
not the underlying "something native leaked a process" finding.

**The real leak is confirmed to exist and remains genuinely unexplained.**
Not found in any code path inspected so far: GRID application code,
`yfinance`, `curl_cffi`, or the `multitasking` package. `dex_scanner.py`
(the other realtime feed with any child-process surface area) was also
checked -- it uses only `aiohttp`, no subprocess path.

**The "~1 per 1,400 poll cycles" figure is an estimate, not a directly
measured rate.** It is derived from dividing an approximate poll count
(60s Yahoo polls over the 49-day uptime, roughly 70,000) by the observed
process count (roughly 50, from this incident's own PID list plus the
prior process's cumulative history) -- a single order-of-magnitude
estimate from one incident's aftermath, not a rate independently confirmed
by repeated measurement. Treat it as "rare enough that a short trace
wouldn't be expected to catch it," not as a precise, reproducible figure.

### 4d. Whether the leak (if real) is the actual cause of the shutdown stall

**Not established, and not necessarily the same thing.** A resource leak
accumulating over 49 days and a single shutdown-sequence stall on one particular
restart are two different claims; this document does not claim the former proves
the latter. It is plausible that a large number of already-stuck native threads
could starve or block the executor thread pool that `bounded_write()` and
`yf.download(threads=True)` both rely on, which would be consistent with the
`await asyncio.wait_for(bounded_write(...), timeout=30)` never resolving within
its own bound (starvation before the task is even scheduled can, in principle,
also be bounded by `wait_for`'s timer -- so this alone does not fully explain 81
seconds of *total* silence either). This is presented as a plausible connecting
hypothesis, not a conclusion.

## 5. What is NOT claimed

- This is not claimed to be caused by anything in PR #514 -- the leaked/orphaned
  resources (if that is what they are) predate this PR by weeks, and the
  WorkingDirectory/backup/semaphore/gate changes in #514 do not touch
  `feeds/yahoo.py`, `feeds/dex_scanner.py`, or `CandleBuilder`.
- This is not claimed to indicate the new process (PID 2999240) is currently
  unhealthy -- it started cleanly, both feeds were confirmed delivering real data
  within 10 minutes of restart (see the PR #514 activation comment thread), and
  it shows zero child processes after its first ~11 minutes of runtime.
- The `dex_scanner` symbol-accumulation hypothesis (4a) and the
  thread-leak-during-yahoo-polling hypothesis (4c) are independent claims with
  different confidence levels -- 4a is grounded directly in code + a matching
  data point; 4c's specific yfinance/curl_cffi explanation is now
  investigated and weighed against (source grep, live import test, and a
  90s/2-cycle strace of the real process, all showing no leak in that
  window) -- but a short trace not reproducing a rare, estimated-not-measured
  event is not the same as ruling the hypothesis out conclusively, and the
  underlying "something native leaked a real process" finding stands either
  way.

## 6. Recommended next steps

1. ✅ **Done (2026-09-16 follow-up): an unconditional completion marker for
   the final-flush phase.** `ingestion/realtime/ws_listener.py::
   _run_final_flush` now logs `"Final flush phase reached its end
   (outcome=...)"` regardless of outcome (`written`/`timed_out`/`failed`/
   `nothing_to_flush`), explicitly worded so it cannot be misread as a
   persistence guarantee for the non-`written` cases (their own
   `log.error` lines, immediately above, already state the candles were
   NOT written). Extracted into a standalone async function and covered by
   `tests/test_realtime_final_flush_marker.py` (all four outcomes exercised
   directly, plus a test pinning down the marker's own wording contract).
   This directly answers the original ask this document was written to
   satisfy: a future SIGKILL with no marker line before it proves the kill
   landed mid-flush; one with the marker line proves the phase completed
   (by whichever outcome, not necessarily a successful write).
2. **Watch `Threads:` in `/proc/<realtime-pid>/status` over time** on the current
   process (baseline: 52 threads at ~11 minutes post-restart) -- if it climbs
   materially over days/weeks without bound, that would be direct, current
   evidence for a live leak (rather than inferring from the now-unavailable old
   process). **Not done as part of this follow-up** -- no additional production
   instrumentation beyond item 1 above was added this round.
3. **Add symbol-count / active_symbols as a monitored metric** (e.g. logged
   periodically, not just at shutdown) so a future restart's "Flushing N remaining
   candles" isn't the first time anyone learns the count has grown to hundreds.
   Not done.
4. **Consider an eviction policy for `CandleBuilder.candles`** (e.g. drop an
   entry if it hasn't received a tick in N intervals) if the dex_scanner
   accumulation (4a) is judged worth addressing -- a design/product decision, not
   made here.
5. **A true controlled reproduction of hypothesis 4c remains not performed.**
   The 2026-09-16 follow-up's live strace (§4c) observed the real process
   under real load for 90 seconds/2 poll cycles and found no leak in that
   window -- useful, real evidence, but not a substitute for a dedicated
   reproduction (e.g. a staging instance running `yf.download(threads=True)`
   in a tight loop for a duration long enough to plausibly catch a
   ~1-per-1,400-cycle event, watching `/proc/<pid>/status` `Threads:` and
   `lsof`/`ls /proc/<pid>/fd` for handle growth throughout). Still not
   attempted, consistent with making no additional production changes.

This is tracked separately from `docs/TODO-REALTIME-CANDLE-CORRECTNESS.md` (which
covers candle *merge* correctness across a restart, a different concern from
shutdown reliability) and from any oracle-scoring/prediction-dedup work (see
`docs/TODO-DUP-WRITES.md`, unrelated).

## 7. 2026-09-16 follow-up -- what changed and why

This document was revised the same day it was merged, incorporating direct
evidence from a separate investigation session (`local_d17dbc48`,
"Investigate grid-realtime orphaned-process shutdown failure") that ran
concurrently and produced findings not available when this document was
first written. Specifically corrected: the thread-vs-process framing in §1
and §4c (now confirmed as processes, not "balance of evidence favors
threads"), and the yfinance/curl_cffi hypothesis in §4c (investigated via
source grep, a live import test, and a real strace -- weakened, not
confirmed as ruled out). Added: the `_run_final_flush` marker (§6, item 1),
implemented and tested in this same follow-up, based on that other
session's original (uncommitted) diff -- reproduced faithfully with one
wording refinement (the marker's own log message now states explicitly, in
the message itself rather than only in a source comment, that reaching it
does not prove persistence for the non-`written` outcomes).
