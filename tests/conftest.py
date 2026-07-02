"""Shared fixtures and synthetic-packet builders for the sceptre-pipeline tests.

The two captures in ``recordings/`` are the empirical ground truth for the wire
format (see docs/implementation-plan.md, Appendix A). Synthetic packet builders
cover the paths absent from the real fixtures (gain, GPS, ephemeris, sub-32-bit
formats) and are filled in from Stage 2 onward.
"""

from __future__ import annotations

import pickle
import struct
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


def encode_payload_format_u64(
    *,
    real_complex_code: int = 0b01,  # 01 = complex cartesian
    data_item_format: int = 0b01110,  # IEEE-754 single float
    packing_bits: int = 32,
    item_bits: int = 32,
) -> int:
    """Encode a Data Packet Payload Format word (Appendix A exact windows)."""
    u = (real_complex_code & 0b11) << 61
    u |= (data_item_format & 0x1F) << 56
    u |= ((packing_bits - 1) & 0x3F) << 38
    u |= ((item_bits - 1) & 0x3F) << 32
    return u


def standard_context_fields(
    *,
    bandwidth_hz: float = 500_000.0,
    rf_hz: float = 97_300_000.0,
    sample_rate_hz: float = 625_000.0,
    payload_format_u64: int | None = None,
) -> dict[int, bytes]:
    """CIF-bit -> raw field bytes matching the real recordings' context."""
    if payload_format_u64 is None:
        payload_format_u64 = encode_payload_format_u64()
    return {
        29: struct.pack(">q", int(bandwidth_hz * 2**20)),
        27: struct.pack(">q", int(rf_hz * 2**20)),
        21: struct.pack(">q", int(sample_rate_hz * 2**20)),
        15: struct.pack(">Q", payload_format_u64),
    }


def build_context_packet(
    *,
    fields: dict[int, bytes],
    stream_id: int = 123,
    counter: int = 0,
    int_ts: int = 1_750_000_000,
    frac_ts: int = 0,
    class_id: bool = True,
) -> bytes:
    """Synthetic VITA-49 IF context packet: [20B header][8B id-word][4B CIF][fields].

    ``fields`` maps CIF bit -> raw field bytes; encoded in descending bit order.
    """
    cif = 0
    for bit in fields:
        cif |= 1 << bit
    payload = b"".join(fields[bit] for bit in sorted(fields, reverse=True))
    body = (b"\x11" * 8 if class_id else b"") + struct.pack(">I", cif) + payload
    total = 20 + len(body)
    assert total % 4 == 0
    word0 = (
        (0x4 << 28)
        | (int(class_id) << 27)
        | (1 << 22)  # TSI = UTC
        | (2 << 20)  # TSF = real-time ps
        | ((counter & 0xF) << 16)
        | (total // 4)
    )
    return struct.pack(">IIIQ", word0, stream_id, int_ts, frac_ts) + body


def build_data_packet(
    *,
    payload: bytes,
    stream_id: int = 123,
    counter: int = 0,
    int_ts: int = 1_750_000_000,
    frac_ts: int = 0,
    class_id: bool = True,
    trailer: bool = True,
    id_word: bytes = b"\x11" * 8,  # per-packet ps counter on the wire (C3): opaque
    pad: bytes = b"",
) -> bytes:
    """Synthetic VITA-49 IF data packet: [20B header][8B id-word][payload][4B trailer].

    ``pad`` fills the payload to the next 32-bit word for sub-32-bit formats.
    The trailer bytes are deliberately garbage to prove they get trimmed.
    """
    body = (id_word if class_id else b"") + payload + pad + (b"\xAA" * 4 if trailer else b"")
    total = 20 + len(body)
    assert total % 4 == 0, "payload+pad must end on a 32-bit word boundary"
    word0 = (
        (0x1 << 28)
        | (int(class_id) << 27)
        | (int(trailer) << 26)
        | (1 << 22)
        | (2 << 20)
        | ((counter & 0xF) << 16)
        | (total // 4)
    )
    return struct.pack(">IIIQ", word0, stream_id, int_ts, frac_ts) + body


def encode_iq_float32(samples) -> bytes:
    """Encode complex samples as big-endian interleaved float32 I,Q pairs."""
    return b"".join(struct.pack(">ff", c.real, c.imag) for c in samples)
