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

# Re-export runtime as the default (unqualified) build output
FROM runtime
