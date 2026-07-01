# sceptre-pipeline

Live ingestion pipeline for a Sceptre SDR that streams IQ data over UDP using a
VITA-49.2 subset (VRL disabled, no VRT trailer on context packets). The runtime goal:

> **UDP packets → interpret → accumulate → emit `{numpy array + typed context dict}`**
> to downstream consumers (FFT / recording / audio).

## Architecture

Two threads, one bounded queue between them:

```
                 ┌──────────────────────────────┐
 UDP socket ──►  │ Thread A — SOURCE            │
 or .pkl replay  │ socket / pickle-file reader  │   dumb and fast; never parses
                 └──────────────┬───────────────┘
                                │ raw packet bytes
                        bounded raw_queue        (lossy: drop-oldest + count)
                                │
                 ┌──────────────▼───────────────┐
                 │ Thread B — INTERPRETER +     │   parses headers, decodes context,
                 │ BUFFER                       │   owns current_context, accumulates
                 │                              │   IQ payloads, flushes on triggers
                 └──────────────┬───────────────┘
                                │ on_emit(unit)
                                ▼
                 {"samples": np.ndarray (complex64),
                  "context": {rf_hz, sample_rate_hz, ...},
                  "start_timestamp", "num_samples"}
```

- **Thread A (SOURCE):** socket *or* pickle-file → pushes raw packet bytes onto a
  bounded `raw_queue`.
- **Thread B (INTERPRETER + BUFFER):** pulls raw bytes, parses the header, decodes
  context, accumulates data payloads, flushes on triggers (context change, counter
  gap, max samples, max age), and emits units to a downstream callback. The buffer
  lives inside Thread B — it is not its own thread.

Emitted units go to a pluggable `on_emit(unit)` callback, so a queue-backed sink
can drop in later.

## Quickstart

```sh
pip install -e '.[dev]'
```

> **Note:** the `python -m sceptre_pipeline` CLI below lands in Stage 4 of the
> build plan; until then only the test suite and the raw capture script run.

Replay a recorded capture offline (no SDR needed):

```sh
python -m sceptre_pipeline --replay recordings/single_frequency.pkl
```

Ingest live from the SDR, optionally recording the raw packets:

```sh
python -m sceptre_pipeline --live --host 0.0.0.0 --port 5000 --record
```

Capture raw UDP to a pickle without running the pipeline:

```sh
python receiver/recieve_udp.py --port 5000 --duration 5
```

## Development

- **Runtime dependencies are stdlib + numpy only.** `pytest` is dev-only; the
  shipped library imports nothing but the standard library and numpy (enforced
  by a test).
- Target Python ≥ 3.10 (developed on 3.13 / numpy 2.3).
- Run the test suite with `pytest`. The two captures in `recordings/`
  (`single_frequency.pkl`, `change_frequency.pkl`) are the empirical ground
  truth for the wire format — the interpreter and buffer are developed and
  tested entirely offline against them via `ReplaySource`.
- Wire-format details (header layout, CIF walk, payload trimming, endianness)
  live in `docs/implementation-plan.md`, Appendix A — it overrides the PDF
  where they conflict.
