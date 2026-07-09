# ThroughputMeter — pipeline data-rate visibility

**Date:** 2026-07-09
**Status:** approved for implementation
**Scope:** additive only — no change to existing pipeline behavior.

## Goal

See how fast data moves through the pipeline: report the sustained
throughput of emitted IQ data at shutdown, next to the existing
`pipeline stopped: ...` counter line.

## Decision

Measure at the **output** of the pipeline (the emit callback), not at
ingest. Rationale:

- The emitted unit is the useful product — samples that made it all the
  way through interpret → buffer → emit. Its rate (in **MS/s**) is the
  metric already validated in the field ("2 MS/s clean").
- Measuring here is pure composition — wrap the `Emit` callback — so it
  touches **zero core files** (`runtime.py`, `buffer.py`,
  `interpreter.py`, `sources.py`, `queues.py` are all unchanged).
- The "are we keeping up / dropping?" question is already answered by the
  existing drop counters (`queue_dropped`, `router_dropped`, …). An
  input-side meter would be largely redundant with those, and ingest
  counts raw UDP bytes (headers + context packets), which are not
  directly comparable to emitted-sample counts anyway. The source is
  deliberately "dumb and fast; never parses," so meaningful IQ bytes are
  not available at ingest without crossing that design boundary.

## Component: `ThroughputMeter`

New file `src/sceptre_pipeline/metrics.py`. A wrapper implementing the
`Emit` interface (`Callable[[dict], None]`):

```
ThroughputMeter(wrapped_emit)
  .start()            # stamp time.monotonic() just before pipeline.run()
  .__call__(unit)     # tally, then delegate to wrapped_emit(unit)
  .summary() -> str   # one-line report
```

**State:** `total_samples`, `total_bytes`, `unit_count`, `_start` (monotonic).

**Tally (per unit):**
- `total_samples += unit["num_samples"]`
- `total_bytes += num_samples * unit["bytes_per_sample"]`
- then call `wrapped_emit(unit)` (counting must never swallow data).

Missing/None `num_samples` or `bytes_per_sample` are treated as 0 for the
tally (defensive; the meter never raises into the Thread B loop).

**Timer:** whole-run wall clock. `start()` stamps `time.monotonic()`;
`summary()` computes `elapsed = time.monotonic() - _start`. Matches the
"starts and stops at the beginning and end" mental model.

**Rates (decimal, SDR convention):**
- `MS/s = total_samples / elapsed / 1e6`
- `MB/s = total_bytes / elapsed / 1e6`

**Output line:**
```
pipeline throughput: 12,500,000 samples in 6.25 s = 2.00 MS/s (16.0 MB/s)
```

**Guards:**
- zero samples → `pipeline throughput: no samples emitted`
- `elapsed <= 0` (or `start()` never called) → report the sample count
  without a rate, e.g. `... (elapsed too short to compute rate)`.

**Threading:** Thread B runs on the calling (main) thread, so `__call__`
and `summary()` are only ever touched by the main thread. No lock needed.

## Wiring — `__main__.py` only (~4 lines)

```python
meter = ThroughputMeter(demo_emit)
runtime = _build_runtime(args, emit=meter)   # emit param already exists
...
meter.start()
pipeline.run()                                # inside existing try
...
logger.info(meter.summary())                  # after the try/except
```

Placed after the `try/except KeyboardInterrupt` block so it logs on every
exit path (Ctrl+C live, and replay-EOF SHUTDOWN) right after the
`pipeline stopped: ...` line.

## Tests — `tests/test_metrics.py` (additive)

- Feed fake units → assert `total_samples` / `total_bytes` and that
  `summary()` reports the expected MS/s and MB/s.
- Assert every unit is delegated to the wrapped emit.
- Assert the zero-samples and short/absent-elapsed guards.
- Assert a missing/None `bytes_per_sample` does not raise.

## Non-goals (YAGNI)

- No per-second / live-updating readout — one summary at shutdown only.
- No CLI flag — always on (one extra log line).
- No ingest-side or per-stream breakdown — aggregate across all streams.
