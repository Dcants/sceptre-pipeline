"""Shared fixtures and synthetic-packet builders for the sceptre-pipeline tests.

The two captures in ``recordings/`` are the empirical ground truth for the wire
format (see docs/implementation-plan.md; Stage 2.5 supersedes Appendix A
SC1-C3 with the flag-driven header model). Synthetic packet builders cover the
paths absent from the real fixtures: gain, GPS, ephemeris, sub-32-bit formats,
and the default Sceptre config (class_id=0, trailer=0 -> 20-byte header).

Wire layout emitted by the builders (Stage 2.5, flag-driven):

    [word0][stream_id][class_id? 8B][int_ts? 4B][frac_ts? 8B][body...]

The Class ID sits BEFORE the timestamps; tsi/tsf of 0 omit the corresponding
timestamp field entirely, so header length is a pure function of the flags.
"""

from __future__ import annotations

import pickle
import struct
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RECORDINGS_DIR = PROJECT_ROOT / "recordings"

# Real Class IDs observed on the wire (OUI 0xFFFFFA + class codes). Context
# and data packets carry different class codes; both are constant per capture.
DATA_CLASS_ID = bytes.fromhex("00fffffa00160000")
CONTEXT_CLASS_ID = bytes.fromhex("00fffffa20110003")


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


@pytest.fixture(autouse=True)
def _fresh_module_rate_limits(monkeypatch):
    """Reset interpreter-module log-rate-limiter state before every test.

    The limiter state is process-global by design (live runtime); without a
    reset, caplog assertions would depend on how many earlier tests already
    consumed a reason's first-N budget — i.e. on test file ordering.
    """
    from sceptre_pipeline import interpreter

    monkeypatch.setattr(interpreter, "_module_log", interpreter._RateLimitedLog())
    monkeypatch.setattr(interpreter, "_UNSUPPORTED_WORDS_LOGGED", set())
    monkeypatch.setattr(interpreter, "_unsupported_cap_announced", False)


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
    packing_method: int = 0,  # 1 = link-efficient (unsupported)
    event_tag: int = 0,  # bits 54:52 (unsupported when nonzero)
    channel_tag: int = 0,  # bits 51:48 (unsupported when nonzero)
) -> int:
    """Encode a Data Packet Payload Format word (Appendix A exact windows)."""
    u = (packing_method & 0b1) << 63
    u |= (real_complex_code & 0b11) << 61
    u |= (data_item_format & 0x1F) << 56
    u |= (event_tag & 0x7) << 52
    u |= (channel_tag & 0xF) << 48
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


def build_header(
    *,
    packet_type: int,
    total_words: int,
    stream_id: int,
    counter: int,
    int_ts: int,
    frac_ts: int,
    class_id: bytes | None,
    trailer: bool,
    tsi: int = 1,  # UTC
    tsf: int = 2,  # real-time picoseconds
) -> bytes:
    """Flag-driven header: [word0][stream_id][class_id?][int_ts?][frac_ts?]."""
    word0 = (
        ((packet_type & 0xF) << 28)
        | (int(class_id is not None) << 27)
        | (int(trailer) << 26)
        | ((tsi & 0x3) << 22)
        | ((tsf & 0x3) << 20)
        | ((counter & 0xF) << 16)
        | (total_words & 0xFFFF)
    )
    hdr = struct.pack(">II", word0, stream_id)
    if class_id is not None:
        assert len(class_id) == 8, "Class ID is always 8 bytes on the wire"
        hdr += class_id
    if tsi:
        hdr += struct.pack(">I", int_ts)
    if tsf:
        hdr += struct.pack(">Q", frac_ts)
    return hdr


def header_len_for(*, class_id: bool, tsi: int = 1, tsf: int = 2) -> int:
    """Expected header length for a Sceptre packet (word0 + stream_id always)."""
    return 8 + (8 if class_id else 0) + (4 if tsi else 0) + (8 if tsf else 0)


def build_context_packet(
    *,
    fields: dict[int, bytes],
    stream_id: int = 123,
    counter: int = 0,
    int_ts: int = 1_750_000_000,
    frac_ts: int = 0,
    class_id: bool = True,
    class_id_bytes: bytes = CONTEXT_CLASS_ID,
    tsi: int = 1,
    tsf: int = 2,
) -> bytes:
    """Synthetic VITA-49 IF context packet: [header][4B CIF][fields].

    ``fields`` maps CIF bit -> raw field bytes; encoded in descending bit order.
    """
    cif = 0
    for bit in fields:
        cif |= 1 << bit
    body = struct.pack(">I", cif) + b"".join(
        fields[bit] for bit in sorted(fields, reverse=True)
    )
    hdr_len = header_len_for(class_id=class_id, tsi=tsi, tsf=tsf)
    total = hdr_len + len(body)
    assert total % 4 == 0
    return (
        build_header(
            packet_type=0x4,
            total_words=total // 4,
            stream_id=stream_id,
            counter=counter,
            int_ts=int_ts,
            frac_ts=frac_ts,
            class_id=class_id_bytes if class_id else None,
            trailer=False,
            tsi=tsi,
            tsf=tsf,
        )
        + body
    )


def build_data_packet(
    *,
    payload: bytes,
    stream_id: int = 123,
    counter: int = 0,
    int_ts: int = 1_750_000_000,
    frac_ts: int = 0,
    class_id: bool = True,
    class_id_bytes: bytes = DATA_CLASS_ID,
    trailer: bool = True,
    tsi: int = 1,
    tsf: int = 2,
    pad: bytes = b"",
) -> bytes:
    """Synthetic VITA-49 IF data packet: [header][payload][pad][4B trailer?].

    ``pad`` fills the payload to the next 32-bit word for sub-32-bit formats.
    The trailer bytes are deliberately garbage to prove they get trimmed.
    """
    body = payload + pad + (b"\xAA" * 4 if trailer else b"")
    hdr_len = header_len_for(class_id=class_id, tsi=tsi, tsf=tsf)
    total = hdr_len + len(body)
    assert total % 4 == 0, "payload+pad must end on a 32-bit word boundary"
    return (
        build_header(
            packet_type=0x1,
            total_words=total // 4,
            stream_id=stream_id,
            counter=counter,
            int_ts=int_ts,
            frac_ts=frac_ts,
            class_id=class_id_bytes if class_id else None,
            trailer=trailer,
            tsi=tsi,
            tsf=tsf,
        )
        + body
    )


def encode_iq_float32(samples) -> bytes:
    """Encode complex samples as big-endian interleaved float32 I,Q pairs."""
    return b"".join(struct.pack(">ff", c.real, c.imag) for c in samples)
