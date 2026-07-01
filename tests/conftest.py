"""Shared fixtures and synthetic-packet builders for the sceptre-pipeline tests.

The two captures in ``recordings/`` are the empirical ground truth for the wire
format (see docs/implementation-plan.md, Appendix A). Synthetic packet builders
cover the paths absent from the real fixtures (gain, GPS, ephemeris, sub-32-bit
formats) and are filled in from Stage 2 onward.
"""

from __future__ import annotations

import pickle
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RECORDINGS_DIR = PROJECT_ROOT / "recordings"


def _load_capture(path: Path | str) -> dict:
    """Load a UDP capture pickle.

    Schema: ``{"start_time_ns": int, "duration_seconds": float,
    "packets": [(ts_ns, ip, port, payload_bytes), ...]}``

    Pickle is safe here: only project-tracked fixture captures and captures
    produced locally by ``receiver/recieve_udp.py`` are loaded, and the pickle
    schema is fixed by the existing capture format.
    """
    with Path(path).open("rb") as file:
        return pickle.load(file)


@pytest.fixture
def recordings_dir() -> Path:
    return RECORDINGS_DIR


@pytest.fixture
def single_frequency_path(recordings_dir: Path) -> Path:
    return recordings_dir / "single_frequency.pkl"


@pytest.fixture
def change_frequency_path(recordings_dir: Path) -> Path:
    return recordings_dir / "change_frequency.pkl"


@pytest.fixture
def load_capture():
    """The capture-loading helper, as a fixture so tests don't import conftest."""
    return _load_capture


def build_data_packet(**fields) -> bytes:
    """Build a synthetic VITA-49 IF data packet (stub — filled in from Stage 2).

    Target layout per Appendix A:
    ``[20B header][8B id-word][N x 8B IQ samples][4B trailer]``
    """
    raise NotImplementedError(
        "Synthetic data-packet builder is implemented in Stage 2."
    )


def build_context_packet(**fields) -> bytes:
    """Build a synthetic VITA-49 IF context packet (stub — filled in from Stage 2).

    Target layout per Appendix A:
    ``[20B header][8B id-word][4B CIF][context fields...]``
    """
    raise NotImplementedError(
        "Synthetic context-packet builder is implemented in Stage 2."
    )


@pytest.fixture
def data_packet_builder():
    return build_data_packet


@pytest.fixture
def context_packet_builder():
    return build_context_packet
