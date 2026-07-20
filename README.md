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

## Quickstart (Docker — recommended)

Runs on any machine with [Docker](https://docs.docker.com/get-docker/)
installed — no Python setup needed.

```sh
# Replay the bundled capture (no SDR needed) — see the pipeline work end-to-end:
docker compose run --rm replay

# Ingest live from a Sceptre SDR streaming to this machine on UDP port 5000:
docker compose up live

# Stop live ingest — from a second terminal, or just Ctrl-C (both shut down cleanly):
docker compose stop live
```

- Point the SDR at the Docker host's IP, UDP port **5000**. Use a different
  port with `SCEPTRE_PORT=6000 docker compose up live`.
- Host `./recordings/` is mounted into both services at `/data/recordings`:
  replay reads captures from it, and live `--record` output is saved to it.
- The container takes the exact same flags as the local CLI
  (`docker compose run --rm replay --help` shows them all).
- **Linux hosts:** the container runs as uid 1000 (`appuser`); if your user
  has a different uid, give the live service write access to `./recordings`
  before using `--record` — e.g. `chmod a+w recordings`, or add
  `user: "$(id -u):$(id -g)"` to the `live` service.

Replay a different capture, paced at its recorded packet timing:

```sh
docker compose run --rm replay --replay /data/recordings/change_frequency.pkl --pace
```

> **Git Bash on Windows:** MSYS rewrites container paths like
> `/data/recordings/…` into Windows paths before Docker sees them. Prefix such
> commands with `MSYS_NO_PATHCONV=1`. PowerShell and cmd are unaffected. In
> PowerShell, set the port with `$env:SCEPTRE_PORT="6000"` before running
> compose.

Ingest live while recording the raw packets (interactive; Ctrl-C stops it;
`--service-ports` is required because `docker compose run` does not publish
ports by default; keep the two port numbers matched to `SCEPTRE_PORT` if you
changed it):

```sh
docker compose run --rm --service-ports live --live --host 0.0.0.0 --port 5000 --record
```

Verify the full test suite passes on this machine (build fails if any test
fails):

```sh
docker build --target test .
```

## Local development (without Docker)

Python ≥ 3.10 (developed on 3.13 / numpy 2.3):

```sh
pip install -e '.[dev]'

# Replay a recorded capture offline (no SDR needed):
python -m sceptre_pipeline --replay recordings/single_frequency.pkl

# Ingest live from the SDR, optionally recording the raw packets:
python -m sceptre_pipeline --live --host 0.0.0.0 --port 5000 --record

# Capture raw UDP to a pickle without running the pipeline:
python receiver/recieve_udp.py --port 5000 --duration 5
```

- **Runtime dependencies are stdlib + numpy only.** `pytest` is dev-only; the
  shipped library imports nothing but the standard library and numpy (enforced
  by a test).
- Run the test suite with `pytest`. The two captures in `recordings/`
  (`single_frequency.pkl`, `change_frequency.pkl`) are the empirical ground
  truth for the wire format — the interpreter and buffer are developed and
  tested entirely offline against them via `ReplaySource`.
- Wire-format details (header layout, CIF walk, payload trimming, endianness)
  live in `docs/implementation-plan.md`, Appendix A — it overrides the PDF
  where they conflict.
