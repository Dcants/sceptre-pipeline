# Docker Containerization — Design

**Date:** 2026-07-20
**Status:** Approved
**Goal:** Wrap sceptre-pipeline in Docker so it runs on any machine with Docker installed, in both replay and live-UDP modes, distributed by building from the cloned repo.

## Context

The pipeline is pure Python (stdlib + numpy, Python ≥ 3.10, developed on 3.13 /
numpy 2.3) with a `python -m sceptre_pipeline` CLI offering two modes:

- `--replay PATH` — replay a recorded `.pkl` capture (no hardware needed).
- `--live --port N` — ingest live VITA-49 UDP from the Sceptre SDR over the
  network; optional `--record` writes a bounded `.pkl` capture.

The SDR streams over the network, so no USB passthrough is involved — the
container needs only a published UDP port and volume mounts for recordings.

## Decisions (from brainstorming)

1. **Run modes:** support both live and replay in one image.
2. **Distribution:** build from the repo — `Dockerfile` + `docker-compose.yml`
   checked in; each machine clones and builds. No registry, no image tar.
3. **Approach:** compose + a `test` build target (chosen over Dockerfile-only
   and over baking recordings into the image — the repo is cloned anyway, so
   recordings are mounted, not copied).
4. **README** gets rewritten afterward with Docker as the front-door quickstart.

## New files

| File | Purpose |
|---|---|
| `Dockerfile` | Two targets: `runtime` (default) and `test`. |
| `docker-compose.yml` | `replay` and `live` services. |
| `.dockerignore` | Exclude `.venv/`, `.git/`, `__pycache__/`, `docs/`, scratch files. |

No changes to pipeline source code — it is container-ready as-is.

## Dockerfile

- Base image: `python:3.13-slim` (matches the dev environment).
- **`runtime` target (default):**
  - `WORKDIR /app`; copy `pyproject.toml`, `README.md`, `src/`.
  - `pip install --no-cache-dir .`
  - Create and switch to a non-root user.
  - `ENTRYPOINT ["python", "-m", "sceptre_pipeline"]` — container args are
    exactly the CLI's args; no `CMD` default (argparse prints usage and exits 2
    when misused, same as local).
- **`test` target:** `FROM runtime`, runs as root throughout (it is a
  build-only stage, never shipped or run as a container),
  `pip install --no-cache-dir .[dev]`, copy `tests/` (includes
  `tests/conftest.py`), `recordings/` (tests use the two captures as
  wire-format ground truth), and `receiver/` (one test imports
  `receiver/recieve_udp.py`), then `RUN pytest tests/` — so
  `docker build --target test .` fails the build if the suite fails on that
  machine. This is the portability check.

## docker-compose.yml

- **`replay` service:**
  - `build: { context: ., target: runtime }`
  - Volume: `./recordings:/data/recordings:ro`
  - Command: `--replay /data/recordings/single_frequency.pkl`
  - Usage: `docker compose run --rm replay` (override the command to replay a
    different capture or pass `--pace`).
- **`live` service:**
  - Same build.
  - Ports: `"${SCEPTRE_PORT:-5000}:${SCEPTRE_PORT:-5000}/udp"`
  - Volume: `./recordings:/data/recordings` (read-write) with
    `working_dir: /data` — `default_recording_path(cwd)` appends its own
    `recordings/` subdirectory, so cwd `/data` puts auto-named `--record`
    output at `/data/recordings/…`, which is the host's `./recordings/`.
  - Command: `--live --host 0.0.0.0 --port ${SCEPTRE_PORT:-5000}` — `--record`
    stays opt-in, appended by the user, matching CLI behavior.
  - `stop_signal: SIGINT` on both services. `stop_grace_period: 10s` on
    `replay`; `stop_grace_period: 60s` on `live` (see Shutdown semantics).

## Shutdown semantics (load-bearing)

`docker stop` sends SIGTERM by default; Python does not turn SIGTERM into
`KeyboardInterrupt`, so the pipeline's clean-shutdown path (final flush,
throughput summary, capture-efficiency line, recording save) would be skipped
and the process killed. Setting `stop_signal: SIGINT` on both services makes
`docker stop` / `docker compose down` equivalent to Ctrl-C, which the CLI
already handles cleanly. Interactive `docker compose run` forwards Ctrl-C as
SIGINT natively. Replay runs end on their own (SHUTDOWN sentinel), so the
signal choice matters only for `live`, but is set on both services for
consistency.

`stop_grace_period` differs by service: `replay` keeps the default `10s` —
replay has no `--record` path in its committed command, so there is nothing
large to flush. `live` uses `60s` because a `--record` shutdown pickles the
in-memory capture (bounded up to 512 MB) to a bind-mounted host file; a
SIGKILL that lands mid-write (once the grace period expires) truncates that
file instead of leaving a valid `.pkl`. 60s is sized for that worst-case write
on the bind mount, not for normal shutdown, which is typically much faster.

## Error handling

- CLI exit codes pass through unchanged (0 success, 1 source failure/bind
  failure, 2 bad args) — compose surfaces them as the container exit code.
- A live bind failure inside the container (port already claimed in-container)
  exits 1 via the existing bind watchdog; host-side port conflicts fail at
  `docker compose up` with Docker's own error.
- Missing volume mount for replay → the CLI's existing bad-replay-path error,
  exit 1.

## README rewrite (follow-up task in same effort)

- Docker quickstart first: clone → `docker compose run --rm replay` (works
  immediately), live ingest command, `docker build --target test .` as the
  "verify on this machine" one-liner.
- Local `pip install -e '.[dev]'` workflow kept as the development path.
- Remove the stale "CLI lands in Stage 4" note (Stage 4 shipped).
- Document `SCEPTRE_PORT`, the recordings volume, and `--record` output
  location under Docker.

## Verification plan

1. `docker build .` and `docker build --target test .` both succeed locally.
2. `docker compose run --rm replay` emits units and the shutdown throughput
   summary against `single_frequency.pkl`.
3. Live smoke test: start the `live` service, send packets from the host (e.g.
   replaying a capture's raw packets over UDP), confirm ingest lines appear,
   then `docker compose down` and confirm clean shutdown; with `--record`,
   confirm the `.pkl` lands in `./recordings/` on the host.

## Out of scope

- Registry publishing / CI pipelines (can be added later).
- Baking captures into the image.
- Any change to pipeline source code.
