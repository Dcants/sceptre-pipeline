# Capture-efficiency metric + bandwidth display — design

**Date:** 2026-07-09 (revised 2026-07-11, twice, after two code-review passes)
**Status:** Implemented on `feature/capture-efficiency-metrics`
**Scope:** `src/sceptre_pipeline/metrics.py`, `src/sceptre_pipeline/__main__.py`, tests in `tests/test_metrics.py` + `tests/test_cli.py`

## Motivation

Live testing on the real Sceptre over a 1 GbE link exposed a blind spot: when the
streamed sample rate exceeds what the cord can carry, packets are dropped in the
kernel / on the wire — *upstream* of every counter the pipeline owns. In one run
the four drop counters all read clean (`queue_dropped=0 … router_dropped=0`)
while **~93% of the stream was silently lost**. The pipeline cannot *prevent* that
loss (network physics, not a code bug), but it should **make it visible** — plus
surface the `bandwidth_hz` the operator controls.

See memory `sceptre-live-network-ceiling` for the field findings.

## Revision history

- **v1** — whole-run average delivered rate vs a snapshot sum of declared rates.
  Review found it only sound for a single steady full-duration live stream.
- **v2** — active-window model integrating over EMIT wall-clock. A second review
  found three real defects: pooled multi-stream verdict masks a lost stream;
  wall-clock integration breaks when a buffered burst drains fast; a display/
  percentage contradiction under partial-context windows.
- **v3** — integrate over **VITA sample time** and verdict **per stream**. A third
  review found the per-rate `[min,max]` envelope mis-handles a **recurring** rate
  (A→B→A false loss), masks loss at a **retune boundary**, and lets duplicate/
  overlapping windows read >100%.
- **v4 (current)** — integrate **incrementally in sample-time order** through a
  bounded per-stream **jitter buffer**, charging each sample-time gap as loss at
  the rate then in effect. Correct across retune recurrence and retune-boundary
  loss; `expected ≥ delivered` by construction so efficiency ≤ 100%; memory is
  bounded (O(jitter-buffer × streams), independent of run length).

## What is built

1. **Bandwidth on the live per-window output.** `demo_emit` prints `bandwidth_hz`
   after `sample_rate_hz` (already in `unit["context"]`; no interpreter change).

2. **Capture-efficiency line at shutdown**, LIVE runs only.

## Design — bounded sample-time integration (`metrics.py`)

For each stream, `_StreamStat` integrates incrementally over VITA sample time. A
window at `start_timestamp = ts` carrying `n` samples at `rate` occupies
`[ts, ts + n/rate]`. Windows enter a fixed-size min-heap **jitter buffer**
(`_REORDER_WINDOW = 64`) keyed on `ts`; once full, the oldest is *committed*, and
`finalize()` drains the rest in `ts` order at shutdown. Each commit:

- charges the sample-time GAP since the previous window's end as loss at the rate then in effect: `expected += cur_rate × max(0, ts − last_end)`;
- adds the window's own samples to both sides: `expected += n`, `delivered += n`;
- advances `last_end = max(last_end, ts + n/rate)` and `cur_rate = rate`.

So `expected = delivered + Σ gap-loss ≥ delivered` → **efficiency ≤ 100% always**
(a duplicate/overlapping window adds `n` to both with a non-positive gap). Because
each interval is charged in time order at the active rate, retune is correct even
when a **rate recurs** (A→B→A), and loss *at* a retune boundary is counted (not
masked). This holds on the SDR sample clock, so it is immune to emit pacing (a
buffered burst), packet reordering (within the buffer), cold-start (first
delivered window), and an idle tail (last delivered window).

**Memory is bounded:** O(`_REORDER_WINDOW` × streams), independent of run length —
nothing is stored per-window unboundedly (verified by feeding 200k windows: the
buffer peaks at 64 and stays flat).

**Verdict is the WORST declared stream** (`min` per-stream %), rounded before the
verdict test so the printed % and verdict can't disagree. Undeclared streams are
excluded and flagged; a declared stream that is all-untimed is flagged "not
assessable" (not silently dropped into a lone-clean-stream line).

**Known limits (docstring):** loss *after* a stream's last window (or a
single-window stream) has no later timestamp to measure against; reordering
deeper than the jitter buffer commits a very late window without its gap; a
retune-boundary gap is attributed to the pre-retune rate.

**Guards:** `total_samples == 0` → `None`; no declared rate → "declared rate
unknown"; declared but nothing measurable → "window too short". No wall-clock is
read by `capture_summary()`.

**Output form:**
- Clean single steady stream: `capture: delivered {D} MS/s of declared {R} MS/s = {pct}% (bandwidth {B} MHz) - {verdict}`, where `D = pct/100 × R` (so it matches `pct` by construction).
- Multi-stream / retune / undeclared: `capture: worst stream {pct}% of {N} declared streams (<extras>) - {verdict}` (or `captured {pct}%` for one measurable stream), where `<extras>` names `rate changed mid-run` and/or `+{K} undeclared stream(s), {U} samples excluded`.

Separators are ASCII hyphens (Windows cp1252 console safety). `summary()` is unchanged.

### CLI (`__main__.py`)

- `demo_emit`: `bandwidth_hz={context.get('bandwidth_hz')}` after `sample_rate_hz`.
- `main()`: log the capture line only when `args.live`. Any replay's wall-clock is
  decoupled from real capture, and the line's premise is live network loss; the
  throughput line still prints on every path.

### Sample output

```
capture: delivered 10.00 MS/s of declared 10.00 MS/s = 100.0% (bandwidth 5.0 MHz) - OK      # clean
capture: delivered 1.35 MS/s of declared 20.00 MS/s = 6.8% (bandwidth 10.0 MHz) - SEVERE UPSTREAM LOSS (check link rate / MTU)
capture: worst stream 20.0% of 2 declared streams - SEVERE UPSTREAM LOSS (check link rate / MTU)   # multi-stream, one lossy
capture: captured 100.0% (rate changed mid-run) - OK                                         # retune
```

## Non-goals (YAGNI)

- No packet-counter-gap tally, no `--rcvbuf` flag.
- No reorder *reassembly* (the metric tolerates reordering; the buffer still emits
  variable-size windows — Part II's consumer must tolerate that).
- No change to interpreter / buffer / queue / sources.

## Testing (`tests/test_metrics.py`, `tests/test_cli.py`)

Units carry `start_timestamp`; no clock needed for capture tests:

- Single steady stream → 100% OK, rate-vs-rate form + bandwidth.
- Severe loss (sample-time gap) → low %, SEVERE.
- Rounding boundary: true 89.96% → prints `90.0%` and reads OK.
- Retune → 100% (not false SEVERE), flags "rate changed mid-run".
- Multi-stream, one lossy → **worst-stream** verdict surfaces the loss (not masked).
- Undeclared stream → excluded + flagged; can't mask a lossy declared stream.
- **Burst-drain immunity**: frozen wall-clock, correct result from sample time.
- **Reordering immunity**: out-of-order windows give the same result as in-order.
- Edge: no samples → `None`; no declared rate → "declared rate unknown"; untimed → "window too short".
- CLI: unpaced replay omits the capture line but keeps throughput; `demo_emit` prints `bandwidth_hz`.
