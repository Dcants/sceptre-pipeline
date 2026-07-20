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
