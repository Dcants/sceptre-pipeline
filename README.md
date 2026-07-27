# sceptre-pipeline

Live ingestion pipeline for a Sceptre SDR that streams IQ data over UDP using a
VITA-49.2 subset (VRL disabled, no VRT trailer on context packets). The runtime goal:

> **UDP packets → interpret → accumulate → emit `{numpy array + typed context dict}`**
> to downstream consumers (FFT / recording / audio).

## Choose your deployment

| Your situation | Prerequisites | The one command |
|---|---|---|
| Linux server or VM | Docker Engine or Podman | `SCEPTRE_PORT=5000 docker compose up live` |
| Any OS, internet available | Python ≥ 3.10 | in a venv: `pip install .` then `python -m sceptre_pipeline --live --host 0.0.0.0 --port 5000` |
| Air-gapped, Python installed | Python ≥ 3.10 + a prepared `wheelhouse/` | `pip install --no-index --find-links wheelhouse .` |
| No Python on the target | A build machine running the same OS | `python scripts/build_binary.py`, copy `dist/sceptre-pipeline` |

> **Docker Desktop (Windows/macOS) cannot run live capture.** Its user-space
> network forwarder does not deliver externally-arriving UDP to published
> container ports — packets from the SDR silently vanish. This is
> architectural, not a firewall or configuration issue. Containers are for
> **Linux hosts only**; the same image and compose file run under Docker
> Engine or Podman (e.g. on RHEL). On Windows and macOS, run live capture
> natively (rows 2–4 above).

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
can drop in later — see [Extending the pipeline](#extending-the-pipeline).

## Multiple streams

Multistream support is automatic — you don't need multiple listeners or any
extra configuration. The Sceptre VITA-49 format multiplexes streams on a single
UDP port: every packet carries a `stream_id` in its header, and the pipeline
demuxes on that field — per-stream context, a separate accumulation buffer per
stream, and each emitted unit tagged with its `stream_id`. The only related
knob is `--max-streams` (default 64), a safety cap on how many distinct stream
IDs are tracked so a flood of bogus IDs can't grow memory without limit;
packets on streams beyond the cap are dropped and counted.

## Linux server: run it in a container

The `live` service uses **host networking** — the container binds the host's
network stack directly, so no ports are published and nothing sits between the
NIC and the socket. From the repo folder:

```sh
docker compose up live                                   # every interface, port 5000
SCEPTRE_PORT=6000 docker compose up live                 # every interface, custom port
SCEPTRE_HOST=192.0.2.10 SCEPTRE_PORT=6000 docker compose up live   # one interface
```

With host networking, `SCEPTRE_HOST` is the IP the app itself binds (host
networking and port publishing are mutually exclusive — there is no publish
address anymore).

You'll see one line per emitted unit as data flows. **Ctrl-C stops it cleanly**
(final flush + throughput summary), as does `docker compose stop live`.

To record the raw packets while listening (the capture saves into
`./recordings/` — mounted at `/data/recordings` in the container — when you
stop):

```sh
docker compose run --rm live --live --host 0.0.0.0 --port 5000 --record
```

If your engine needs root, prefix commands with `sudo` — or add yourself to the
docker group once (`sudo usermod -aG docker $USER`, then log out and back in).
Podman runs the same compose file rootless via `podman compose`.

### Validate the image on a new machine

```sh
docker build --target test .
```

Builds the image and runs the full test suite inside it — the build fails if
anything is broken.

### Replay a capture in the container (no SDR needed)

```sh
docker compose run --rm replay        # replays recordings/single_frequency.pkl
docker compose run --rm replay --replay /data/recordings/change_frequency.pkl --pace
```

Your `./recordings/` folder is mounted at `/data/recordings` inside the
container; `--pace` plays back at the recorded packet timing.

## Any OS: install with pip (internet available)

`pyproject.toml` is the dependency manifest — there is no `requirements.txt`.
Runtime dependencies are stdlib + numpy only.

```sh
# bash
python -m venv .venv && . .venv/bin/activate && pip install .
```

```powershell
# PowerShell
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install .
```

Then go live (Ctrl-C stops it cleanly; add `--record` to save the raw packets
into `./recordings/`):

```sh
python -m sceptre_pipeline --live --host 0.0.0.0 --port 5000
```

Or replay a shipped capture first — no SDR needed:

```sh
python -m sceptre_pipeline --replay recordings/single_frequency.pkl
```

## Air-gapped: offline wheelhouse install

`wheelhouse/` is a per-deployment artifact: it is gitignored, never committed,
and **must be regenerated for each deployment bundle you prepare** — don't
reuse one from a previous bundle.

1. On an internet-connected machine, from the repo folder:

   ```sh
   python scripts/build_wheelhouse.py
   ```

   downloads numpy wheels for Windows x86_64, Linux x86_64/aarch64, and macOS
   arm64/x86_64 across CPython 3.10–3.13, plus the hatchling build toolchain,
   into `wheelhouse/`, and prints a per-platform report.

2. Copy the repo **including `wheelhouse/`** to the target machine (USB drive,
   media transfer, etc.).

3. On the target:

   ```sh
   pip install --no-index --find-links wheelhouse .
   ```

`--no-index` guarantees nothing touches the network — the build backend and
numpy both resolve from the wheelhouse.

## No Python on the target: standalone binary

```sh
pip install -e '.[dev]'          # brings in PyInstaller
python scripts/build_binary.py
```

produces a self-contained `dist/sceptre-pipeline` (`dist\sceptre-pipeline.exe`
on Windows) that needs no Python on the machine that runs it. PyInstaller
cannot cross-compile — **build on the same OS you will deploy to.** Binaries
are per-machine deployment artifacts and are never committed.

The binary is unsigned, so the first run may be blocked:

- **Windows SmartScreen:** click "More info" → "Run anyway" (or right-click
  the `.exe` → Properties → tick "Unblock").
- **macOS Gatekeeper:** right-click the binary → Open → Open (or clear the
  quarantine flag: `xattr -d com.apple.quarantine sceptre-pipeline`).

## Configuration

| Variable | Default | What it does |
|---|---|---|
| `SCEPTRE_HOST` | all interfaces | Compose `live` service: the IP the app binds on the host's network stack |
| `SCEPTRE_PORT` | `5000` | Compose `live` service: UDP port to listen on |

These drive the compose file; the CLI itself takes `--host`/`--port` directly.
The container accepts the exact same flags as the local CLI —
`docker compose run --rm replay --help` lists them all.

## Extending the pipeline

Every emitted window goes to a plain callable — the `emit` seam. The CLI's
one-line-per-unit printer is just one such callback; pass your own consumer to
`BufferRouter(emit=...)` using the same object graph the CLI wires. Save this
as `power_meter.py` and run
`python power_meter.py recordings/single_frequency.pkl`:

```python
"""Custom consumer at the emit seam: mean power per emitted window."""

import sys
import threading

import numpy as np

from sceptre_pipeline.buffer import BufferRouter
from sceptre_pipeline.interpreter import Interpreter
from sceptre_pipeline.queues import BoundedRawQueue
from sceptre_pipeline.runtime import Pipeline
from sceptre_pipeline.sources import ReplaySource


def power_meter(unit):
    """Called once per emitted window; unit["samples"] is a complex64 array."""
    samples = unit["samples"]
    context = unit.get("context") or {}
    power_db = 10 * np.log10(np.mean(np.abs(samples) ** 2))
    print(
        f"stream {unit['stream_id']}: {unit['num_samples']} samples "
        f"@ rf={context.get('rf_hz')} Hz -> mean power {power_db:.1f} dB"
    )


def main(capture_path):
    stop = threading.Event()
    raw_queue = BoundedRawQueue(maxsize=32768)
    router = BufferRouter(emit=power_meter, max_samples=625_000, max_age_s=1.0)
    source = ReplaySource(capture_path, raw_queue, stop)
    Pipeline(source, raw_queue, Interpreter(), router, stop).run()


if __name__ == "__main__":
    main(sys.argv[1])
```

Each `unit` dict carries `samples` (a complex64 `np.ndarray`), `context`
(`rf_hz`, `sample_rate_hz`, `bandwidth_hz`, …), `stream_id`, `num_samples`,
and `start_timestamp`. The callback runs on the pipeline's interpreter thread:
return quickly (hand heavy work to your own queue/thread) and don't raise — an
exception would propagate into the pipeline loop. For live ingest, swap
`ReplaySource(...)` for `LiveSource(host, port, raw_queue, stop)`.

## Local development

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

- **Runtime dependencies are stdlib + numpy only.** `pytest` and PyInstaller
  are dev-only; the shipped library imports nothing but the standard library
  and numpy (enforced by a test).
- Run the test suite with `pytest`. The two captures in `recordings/`
  (`single_frequency.pkl`, `change_frequency.pkl`) are the empirical ground
  truth for the wire format — the interpreter and buffer are developed and
  tested entirely offline against them via `ReplaySource`.
- Wire-format details (header layout, CIF walk, payload trimming, endianness)
  live in `docs/implementation-plan.md`, Appendix A — it overrides the PDF
  where they conflict.

## Platform & tuning notes

- **Linux UDP buffer:** the live source requests an 8 MB `SO_RCVBUF`, but the
  kernel clamps the grant to `net.core.rmem_max` (~208 KiB by default) and the
  pipeline logs a "SO_RCVBUF clamped" warning when that happens. Raise it:

  ```sh
  sudo sysctl -w net.core.rmem_max=16777216
  echo 'net.core.rmem_max=16777216' | sudo tee /etc/sysctl.d/99-sceptre.conf  # persist across reboots
  ```

  The user-space raw queue is bounded separately — `--queue-size` (default
  32768 packets) sets how much Thread-B stall it absorbs before dropping
  oldest.
- **Linux containers:** the container writes recordings as uid 1000
  (`appuser`); if your user has a different uid, run `chmod a+w recordings`
  once, or add `--user "$(id -u):$(id -g)"` to `docker compose run`.
- **Docker Desktop (Windows/macOS):** cannot deliver externally-arriving UDP
  to containers, so live capture on these platforms must run natively — see
  [Choose your deployment](#choose-your-deployment).
