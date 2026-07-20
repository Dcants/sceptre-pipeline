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

## Run it

1. Install [Docker](https://docs.docker.com/get-docker/) if you don't have it.
2. Clone this repo, and from its folder run:

```sh
SCEPTRE_PORT=XXXX docker compose up live
#Listens on every interface at a defined port#

SCEPTRE_HOST=X.X.X.X SCEPTRE_PORT=XXXX docker compose up live
#Listens on a defined interface at a defined port#
```


```powershell
$env:SCEPTRE_PORT="XXXX"; docker compose up live
#Listens on every interface at a defined port#

$env:SCEPTRE_HOST="X.X.X.X"; $env:SCEPTRE_PORT="XXXX"; docker compose up live
#Listens on a defined interface at a defined port#
```

Swap in your own IP and port. Both are optional — plain
`docker compose up live` listens on every interface at port 5000.

You'll see one line per emitted unit as data flows. **Ctrl-C stops it cleanly**
(final flush + throughput summary), as does `docker compose stop live`.

To record the raw packets while listening (the capture saves into
`./recordings/` when you stop):

```sh & pwsh
docker compose run --rm --service-ports live --live --host 0.0.0.0 --port 5000 --record
```

(`--service-ports` is required — `docker compose run` doesn't publish ports on
its own. Match `--port` to `SCEPTRE_PORT` if you changed it.)

## Replay a capture (no SDR needed)

Run the pipeline against a recorded capture — useful for testing without
hardware; two captures ship with the repo:

```sh & pwsh
docker compose run --rm replay        # replays recordings/single_frequency.pkl
docker compose run --rm replay --replay /data/recordings/change_frequency.pkl --pace
```

Your `./recordings/` folder is mounted at `/data/recordings` inside the
container; `--pace` plays back at the recorded packet timing.

## Check it works on a new machine

```sh
docker build --target test .
```

Builds the image and runs the full test suite inside it — the build fails if
anything is broken.

## Configuration

| Variable | Default | What it does |
|---|---|---|
| `SCEPTRE_HOST` | all interfaces | Which of your machine's IPs to listen on |
| `SCEPTRE_PORT` | `5000` | UDP port to listen on |

The container takes the exact same flags as the local CLI —
`docker compose run --rm replay --help` lists them all.


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

## Platform notes

- **Git Bash on Windows:** MSYS rewrites container paths like
  `/data/recordings/…` into Windows paths before Docker sees them. Prefix such
  commands with `MSYS_NO_PATHCONV=1`. PowerShell and cmd are unaffected.
- **Linux:** the container writes recordings as uid 1000 (`appuser`); if your
  user has a different uid, run `chmod a+w recordings` once, or add
  `--user "$(id -u):$(id -g)"` to `docker compose run`.
