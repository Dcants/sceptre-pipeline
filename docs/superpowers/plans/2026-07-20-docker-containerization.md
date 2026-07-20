# Docker Containerization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wrap sceptre-pipeline in Docker (runtime image + test build target + compose services) so it runs and verifies on any machine with Docker, in both replay and live-UDP modes.

**Architecture:** One `Dockerfile` with two targets — `runtime` (slim non-root image whose ENTRYPOINT is the existing `python -m sceptre_pipeline` CLI) and `test` (adds pytest + fixtures and runs the suite during the build). A `docker-compose.yml` defines `replay` and `live` services that mount `./recordings` and publish the UDP port. No pipeline source changes.

**Tech Stack:** Docker (BuildKit), docker compose v2, `python:3.13-slim` base, pip/hatchling (existing build backend).

**Spec:** `docs/superpowers/specs/2026-07-20-docker-containerization-design.md`

## Global Constraints

- Base image `python:3.13-slim` (project targets Python ≥ 3.10, developed on 3.13 / numpy 2.3).
- Runtime dependencies are stdlib + numpy only — the runtime image installs nothing else.
- No changes to pipeline source code (`src/`), tests, or recordings.
- Recordings are mounted at run time, never baked into the `runtime` target (only the `test` target copies them, as test fixtures).
- CLI exit codes pass through unchanged: 0 success, 1 source/bind failure, 2 bad args.
- `stop_signal: SIGINT` + `stop_grace_period: 10s` on both compose services (Python doesn't convert SIGTERM to `KeyboardInterrupt`; the CLI's clean-shutdown path needs SIGINT).
- `ENV PYTHONUNBUFFERED=1` in the image (the demo consumer `print()`s one line per unit; without it, `docker logs` shows nothing until buffers flush).
- Commits: Conventional Commits, no AI attribution.
- All commands below run from the repo root: `C:\Users\djmcc\Desktop\Ocean\projects\sceptre-pipeline` (Git Bash path `/c/Users/djmcc/Desktop/Ocean/projects/sceptre-pipeline`).

## File Structure

| File | Task | Responsibility |
|---|---|---|
| `.dockerignore` | 1 | Keep `.venv` (~large), `.git`, caches, and `docs/` out of the build context. |
| `Dockerfile` | 1, 2 | `runtime` target (Task 1) + `test` target (Task 2). |
| `docker-compose.yml` | 3 | `replay` and `live` services: volumes, UDP port, shutdown signal. |
| `README.md` | 4 | Rewritten quickstart: Docker front door, local dev path kept. |

---

### Task 1: `.dockerignore` + `Dockerfile` runtime target

**Files:**
- Create: `.dockerignore`
- Create: `Dockerfile`

**Interfaces:**
- Consumes: existing `pyproject.toml` (hatchling build, `packages = ["src/sceptre_pipeline"]`), existing CLI `python -m sceptre_pipeline`.
- Produces: image tag `sceptre-pipeline` with `ENTRYPOINT ["python", "-m", "sceptre_pipeline"]` and a `runtime` build target that Task 2 extends (`FROM runtime AS test`) and Task 3's compose services build (`target: runtime`). Non-root user is named `appuser`.

- [ ] **Step 1: Write `.dockerignore`**

```gitignore
.venv/
.git/
__pycache__/
**/__pycache__/
*.pyc
.pytest_cache/
docs/
```

Note: `recordings/`, `tests/`, and `receiver/` must NOT be ignored — the `test` target (Task 2) copies all three (`tests/conftest.py` resolves fixtures at `PROJECT_ROOT / "recordings"`, and `test_receive_udp_defaults_into_project_recordings_dir` imports `receiver/recieve_udp.py`).

- [ ] **Step 2: Write `Dockerfile` (runtime target only for now)**

```dockerfile
# syntax=docker/dockerfile:1

FROM python:3.13-slim AS runtime

# Unbuffered stdout so the one-line-per-unit demo output appears live in
# `docker logs`; without it Python block-buffers when stdout is not a tty.
ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Package metadata + source only. Recordings are mounted at run time.
COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir .

RUN useradd --create-home appuser
USER appuser

# Container args are exactly the CLI's args: `docker run sceptre-pipeline
# --replay /data/recordings/x.pkl` == `python -m sceptre_pipeline --replay ...`
ENTRYPOINT ["python", "-m", "sceptre_pipeline"]
```

- [ ] **Step 3: Build the runtime image**

Run: `docker build -t sceptre-pipeline .`
Expected: build succeeds; final lines name the `runtime` stage and export the image.

- [ ] **Step 4: Verify argparse passthrough (exit 2, usage on stderr)**

Run: `docker run --rm sceptre-pipeline; echo "exit=$?"`
Expected: usage text `usage: sceptre_pipeline [-h] ...` with `one of the arguments --replay --live is required`, then `exit=2`.

- [ ] **Step 5: Verify replay works with a mounted capture**

Run (Git Bash — `MSYS_NO_PATHCONV` stops MSYS mangling the `/data` path):

```bash
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "/c/Users/djmcc/Desktop/Ocean/projects/sceptre-pipeline/recordings:/data/recordings:ro" \
  sceptre-pipeline --replay /data/recordings/single_frequency.pkl; echo "exit=$?"
```

Expected: many `stream_id=... num_samples=... rf_hz=...` lines, then `pipeline stopped:` and throughput-summary log lines, then `exit=0`.

- [ ] **Step 6: Verify non-root**

Run: `docker run --rm --entrypoint whoami sceptre-pipeline`
Expected: `appuser`

- [ ] **Step 7: Commit**

```bash
git add .dockerignore Dockerfile
git commit -m "feat: add Dockerfile runtime target + .dockerignore"
```

---

### Task 2: `Dockerfile` test target

**Files:**
- Modify: `Dockerfile` (append the `test` stage after the `runtime` stage)

**Interfaces:**
- Consumes: `runtime` stage from Task 1 (`FROM runtime AS test`); repo dirs `tests/`, `recordings/`, `receiver/`.
- Produces: `docker build --target test .` = one-command "does the full suite pass on this machine" check. Build fails if pytest fails. Nothing ships from this stage.

- [ ] **Step 1: Verify the target doesn't exist yet**

Run: `docker build --target test . 2>&1 | tail -1`
Expected: error containing `target stage "test" could not be found` (BuildKit wording may vary slightly).

- [ ] **Step 2: Append the test stage to `Dockerfile`**

```dockerfile

# ---- test target -----------------------------------------------------------
# Build-only stage (never shipped, runs as root): `docker build --target test .`
# fails the build if the suite fails on this machine — the portability check.
FROM runtime AS test
USER root

RUN pip install --no-cache-dir .[dev]

# tests/conftest.py resolves PROJECT_ROOT / "recordings"; one test imports
# receiver/recieve_udp.py — all three trees must sit beside src/ under /app.
COPY tests/ tests/
COPY recordings/ recordings/
COPY receiver/ receiver/

RUN pytest tests/

# Docker builds the LAST stage when no --target is given; re-export runtime
# so an unqualified `docker build .` ships the runtime image, not test.
FROM runtime
```

- [ ] **Step 3: Run the test build**

Run: `docker build --target test .`
Expected: build succeeds; the `RUN pytest tests/` step output shows the full suite passing (all tests pass, exit 0). If any test fails the build fails — that is the intended behavior.

- [ ] **Step 4: Verify the default build still targets runtime**

Run: `docker build -t sceptre-pipeline . && docker run --rm --entrypoint python sceptre-pipeline -c "import importlib.util; print(importlib.util.find_spec('pytest'))"`
Expected: `None` (pytest is not in the runtime image).

- [ ] **Step 5: Commit**

```bash
git add Dockerfile
git commit -m "feat: add Dockerfile test target running pytest at build time"
```

---

### Task 3: `docker-compose.yml` with replay + live services

**Files:**
- Create: `docker-compose.yml`

**Interfaces:**
- Consumes: `runtime` build target and image tag `sceptre-pipeline` from Task 1.
- Produces: services `replay` and `live`; env var `SCEPTRE_PORT` (default 5000); container path `/data/recordings` ↔ host `./recordings` — the exact commands Task 4's README documents.

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  replay:
    build:
      context: .
      target: runtime
    image: sceptre-pipeline
    volumes:
      - ./recordings:/data/recordings:ro
    command: ["--replay", "/data/recordings/single_frequency.pkl"]
    # SIGINT (not the SIGTERM default) so `docker stop` takes the CLI's
    # Ctrl-C clean-shutdown path: final flush + summaries + recording save.
    stop_signal: SIGINT
    stop_grace_period: 10s

  live:
    build:
      context: .
      target: runtime
    image: sceptre-pipeline
    ports:
      - "${SCEPTRE_PORT:-5000}:${SCEPTRE_PORT:-5000}/udp"
    volumes:
      - ./recordings:/data/recordings
    # default_recording_path(cwd) appends its own recordings/ subdir, so cwd
    # /data lands bare `--record` output in /data/recordings == host ./recordings.
    working_dir: /data
    command: ["--live", "--host", "0.0.0.0", "--port", "${SCEPTRE_PORT:-5000}"]
    stop_signal: SIGINT
    stop_grace_period: 10s
```

- [ ] **Step 2: Validate compose file**

Run: `docker compose config --quiet; echo "exit=$?"`
Expected: `exit=0`, no warnings.

- [ ] **Step 3: Verify replay service end-to-end**

Run: `docker compose run --rm replay; echo "exit=$?"`
Expected: same output as Task 1 Step 5 (unit lines + `pipeline stopped:` + throughput summary), `exit=0`.

- [ ] **Step 4: Verify replay command override**

Run: `docker compose run --rm replay --replay /data/recordings/change_frequency.pkl; echo "exit=$?"`
Expected: unit lines showing `rf_hz` changing mid-run, `exit=0`.

- [ ] **Step 5: Write the live smoke-test sender (scratch file, NOT committed)**

Save as `send_capture.py` in the session scratchpad (any temp dir works; stdlib-only, run with any Python 3):

```python
"""Replay a capture's raw UDP packets at localhost:PORT — live smoke test."""
import pickle, socket, sys, time

path, port = sys.argv[1], int(sys.argv[2])
with open(path, "rb") as f:
    cap = pickle.load(f)
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
for _ts, _ip, _port, payload in cap["packets"]:
    sock.sendto(payload, ("127.0.0.1", port))
    time.sleep(0.0005)  # gentle pacing so loopback doesn't drop
print(f"sent {len(cap['packets'])} packets")
```

- [ ] **Step 6: Live smoke test — ingest, clean stop**

```bash
docker compose up -d live
python <scratchpad>/send_capture.py recordings/single_frequency.pkl 5000
docker compose logs live | head -30
docker compose stop live
docker compose logs live | tail -15
docker inspect --format '{{.State.ExitCode}}' "$(docker compose ps -a -q live)"
docker compose down
```

Expected: first `logs` shows `stream_id=...` unit lines (ingest worked through the published UDP port); after `stop`, logs end with `interrupted; shutting down`, `pipeline stopped:`, throughput summary, and capture-efficiency line (live mode); exit code `0` (SIGINT path, not 137/SIGKILL).

- [ ] **Step 7: Live smoke test — `--record` writes to host `./recordings/`**

```bash
CID=$(docker compose run --rm --service-ports -d live --live --host 0.0.0.0 --port 5000 --record)
python <scratchpad>/send_capture.py recordings/single_frequency.pkl 5000
docker stop --signal SIGINT "$CID"
ls recordings/
```

Expected: a new `udp_capture_*.pkl` in host `./recordings/` (this is the `working_dir: /data` behavior); the two fixture captures untouched. Delete the new capture afterward: `rm recordings/udp_capture_*.pkl` (keep the fixture `.pkl`s!).

- [ ] **Step 8: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add docker-compose replay + live services"
```

---

### Task 4: README rewrite

**Files:**
- Modify: `README.md` (replace the Quickstart section; keep the Architecture section verbatim; replace the Development section)

**Interfaces:**
- Consumes: service names `replay`/`live`, env var `SCEPTRE_PORT`, container path `/data/recordings`, `--target test` — all exactly as created in Tasks 1–3.
- Produces: the user-facing contract. Every command in the README must have been run successfully during Tasks 1–3 (or be re-run now) before commit.

- [ ] **Step 1: Replace README content**

Keep lines 1–41 (title, intro, `## Architecture` section incl. diagram) exactly as they are. Replace everything from `## Quickstart` to end of file with:

````markdown
## Quickstart (Docker — recommended)

Runs on any machine with [Docker](https://docs.docker.com/get-docker/)
installed — no Python setup needed.

```sh
# Replay the bundled capture (no SDR needed) — see the pipeline work end-to-end:
docker compose run --rm replay

# Ingest live from a Sceptre SDR streaming to this machine on UDP port 5000:
docker compose up live

# Stop live ingest (clean shutdown: final flush + throughput summary):
docker compose stop live
```

- Point the SDR at the Docker host's IP, UDP port **5000**. Use a different
  port with `SCEPTRE_PORT=6000 docker compose up live`.
- Host `./recordings/` is mounted into both services at `/data/recordings`:
  replay reads captures from it, and live `--record` output is saved to it.
- The container takes the exact same flags as the local CLI
  (`docker compose run --rm replay --help` shows them all).

Replay a different capture, paced at its recorded packet timing:

```sh
docker compose run --rm replay --replay /data/recordings/change_frequency.pkl --pace
```

Ingest live while recording the raw packets (interactive; Ctrl-C stops it;
`--service-ports` is required because `docker compose run` does not publish
ports by default):

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
````

Also delete the now-stale note between the old Quickstart heading and the first
command ("the `python -m sceptre_pipeline` CLI below lands in Stage 4…") — it
does not survive the replacement above, but double-check it is gone.

- [ ] **Step 2: Verify every README command**

Run each Docker command from the new README top section once (replay run, `--pace` variant, `--target test` build; the live ones were exercised in Task 3 — re-run `docker compose up -d live` + `docker compose stop live` if quick). All must succeed exactly as written.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: rewrite README with Docker quickstart"
```
