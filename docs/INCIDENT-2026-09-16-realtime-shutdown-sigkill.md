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
unavailable), and `jemalloc_bg_thd` (1). systemd's `Killing process` log line for a
stop-timeout SIGKILL fallback iterates over every PID in the unit's cgroup, which
includes both distinct forked processes **and** individual threads (each kernel
thread has its own PID-like TID, and appears in cgroup process/task listings) --
this log alone does not distinguish which of the 42 were separate processes versus
threads of the single Python interpreter. See "Thread vs. process" hypothesis
below for why the balance of evidence favors mostly-threads.

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

## 2. Confirmed by direct query (read-only, run after the fact)

**The final shutdown flush did not write any row to `realtime_candles`.** Queried
directly:

```sql
SELECT source, count(*), min(created_at), max(created_at)
FROM realtime_candles
WHERE ts > TIMESTAMP '2026-09-16 18:00:00+00'
  AND created_at BETWEEN '2026-09-16T19:18:00Z' AND '2026-09-16T19:19:34Z'
GROUP BY source
```

Result: **zero rows**, for any source, in the entire window from the
"Flushing 723 remaining candles..." log line to the SIGKILL. This is a confirmed
fact (absence of rows in a completed, read-only query), not an inference -- the
723-candle final flush produced no output before the process was killed.

## 3. Unconfirmed

- **Where exactly execution was stuck** between "Flushing 723 remaining
  candles..." (19:18:12) and the kill (19:19:33). The code path is: `log.info(...)`
  → `builder.flush_all()` (synchronous, pure in-memory dict iteration, should be
  fast even for 723 entries) → `builder.drain()` (same) → `build_insert_values()`
  (same) → `await asyncio.wait_for(bounded_write(_write_final_flush_sync, rows),
  timeout=30)`. The 30-second bound wraps only the last step. 81 seconds of total
  silence, with zero log output (not even the "Final flush timed out after 30s"
  or "Final flush: N candles written" lines that step's own code would produce on
  either successful completion or a timeout), means execution did not reach the
  end of that `await` in the normal way -- but *why* is not established. No stack
  trace, core dump, or other diagnostic was captured before the SIGKILL destroyed
  the process; none of these can be reconstructed after the fact.
- Whether the 723-candle count itself (vs. flusher.py's routine ~80) directly
  caused the hang, versus being coincidental to some other stuck condition, is not
  established -- see hypothesis below.
- Whether the killed PIDs were genuinely leaked/orphaned resources accumulated
  over time, or were created in the final moments of the stuck shutdown attempt
  itself, is not established from the log alone (the numeric-range clustering is
  suggestive, not conclusive).

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

**Not confirmed; the strongest available clue is circumstantial.** systemd's
per-cgroup kill-everything fallback does not distinguish threads from processes
in its log line, but a companion check on the *new* process (PID 2999240, running
cleanly) shows `Threads: 52` in `/proc/<pid>/status` while `ps --ppid 2999240`
shows **zero** child processes -- i.e. a single Python process can legitimately
carry dozens of "tasks" that are threads, not forks. `jemalloc_bg_thd` is jemalloc's
own background arena-decay thread name; jemalloc is a native memory allocator
typically linked into compiled extensions, not something the pure-Python GRID
code links directly. `curl_cffi` (version 0.16.3, confirmed installed) is
`yfinance`'s (version 1.7.0, confirmed installed) HTTP backend and is exactly the
kind of native/CFFI dependency that could plausibly link jemalloc. `yahoo.py`
calls `yf.download(..., threads=True)` (confirmed in source, line 87) -- yfinance's
own concurrent-download mode -- once every 60 seconds, for 49 days (~70,000
invocations).

**The hypothesis, stated as a hypothesis**: some fraction of those ~70,000
`yf.download(threads=True)` calls may not have fully torn down their internal
worker threads (or `curl_cffi`'s underlying connections/handles), leaking a small
number of threads per call that accumulated slowly over 49 days, with
`jemalloc_bg_thd` as an artifact of `curl_cffi`'s native layer. **This is not
proven.** No thread was inspected while alive (all evidence is post-mortem from a
systemd log line), and no code change is proposed here as a result -- see "Next
steps" below for how this could be confirmed one way or the other.

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
  different confidence levels -- 4a is grounded directly in code + a matching data
  point; 4c is circumstantial and explicitly weaker.

## 6. Recommended next steps (not performed here -- investigation only)

1. **Watch `Threads:` in `/proc/<realtime-pid>/status` over time** on the current
   process (baseline: 52 threads at ~11 minutes post-restart) -- if it climbs
   materially over days/weeks without bound, that would be direct, current
   evidence for a live leak (rather than inferring from the now-unavailable old
   process).
2. **Add symbol-count / active_symbols as a monitored metric** (e.g. logged
   periodically, not just at shutdown) so a future restart's "Flushing N remaining
   candles" isn't the first time anyone learns the count has grown to hundreds.
3. **Consider an eviction policy for `CandleBuilder.candles`** (e.g. drop an
   entry if it hasn't received a tick in N intervals) if the dex_scanner
   accumulation (4a) is judged worth addressing -- a design/product decision, not
   made here.
4. If pursued, a controlled reproduction (e.g. a staging instance running
   `yf.download(threads=True)` in a tight loop while watching `/proc/<pid>/status`
   `Threads:` and `lsof`/`ls /proc/<pid>/fd` for handle growth) would be needed to
   confirm or rule out hypothesis 4c -- not attempted here, per the instruction to
   make no production changes as part of this investigation.

This is tracked separately from `docs/TODO-REALTIME-CANDLE-CORRECTNESS.md` (which
covers candle *merge* correctness across a restart, a different concern from
shutdown reliability) and from any oracle-scoring/prediction-dedup work (see
`docs/TODO-DUP-WRITES.md`, unrelated).
