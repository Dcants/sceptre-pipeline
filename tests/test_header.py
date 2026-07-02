"""Stage 2: 20-byte header parse — verified word0 bit extraction (Appendix A)."""

from __future__ import annotations

import struct

import pytest

from sceptre_pipeline.interpreter import Header, parse_header


def _header_bytes(word0: int, stream_id: int, int_ts: int, frac_ts: int) -> bytes:
    return struct.pack(">IIIQ", word0, stream_id, int_ts, frac_ts)


def test_data_packet_word0_vector() -> None:
    """Acceptance vector: 1e600800 -> type 1, cid 1, trl 1, tsi 1, tsf 2, cnt 0, size 2048."""
    hdr = parse_header(
        _header_bytes(0x1E600800, 123, 1_750_000_000, 500_000_000_000)
    )
    assert hdr.packet_type == 1
    assert hdr.class_id is True
    assert hdr.trailer is True
    assert hdr.tsi == 1
    assert hdr.tsf == 2
    assert hdr.packet_counter == 0
    assert hdr.packet_size == 2048  # words = 8192 bytes
    assert hdr.stream_id == 123
    assert hdr.int_ts == 1_750_000_000
    assert hdr.frac_ts == 500_000_000_000
    assert hdr.timestamp == 1_750_000_000.5  # int_ts + frac_ts / 1e12


def test_context_packet_word0_vector() -> None:
    """Acceptance vector: 4b600010 -> type 4, cid 1, trl 0, size 16."""
    hdr = parse_header(_header_bytes(0x4B600010, 123, 0, 0))
    assert hdr.packet_type == 4
    assert hdr.class_id is True
    assert hdr.trailer is False
    assert hdr.tsi == 1
    assert hdr.tsf == 2
    assert hdr.packet_counter == 0
    assert hdr.packet_size == 16  # words = 64 bytes


def test_counter_extraction_bits_12_15() -> None:
    hdr = parse_header(_header_bytes(0x1E6D0800, 1, 0, 0))
    assert hdr.packet_counter == 0xD


def test_short_packet_raises() -> None:
    with pytest.raises(ValueError):
        parse_header(b"\x00" * 19)


def test_header_is_frozen_dataclass() -> None:
    hdr = parse_header(_header_bytes(0x1E600800, 1, 0, 0))
    assert isinstance(hdr, Header)
    with pytest.raises(AttributeError):
        hdr.packet_type = 2  # type: ignore[misc]


def test_real_fixture_headers(single_frequency_path, load_capture) -> None:
    """First packet is context (16 words / 64 B); data packets are 2048 words / 8192 B."""
    packets = load_capture(single_frequency_path)["packets"]

    ctx_hdr = parse_header(packets[0][3])
    assert ctx_hdr.packet_type == 4
    assert ctx_hdr.class_id is True
    assert ctx_hdr.trailer is False
    assert ctx_hdr.packet_size == 16
    assert len(packets[0][3]) == 16 * 4
    assert ctx_hdr.stream_id == 123

    data_raw = next(p[3] for p in packets if parse_header(p[3]).packet_type == 1)
    data_hdr = parse_header(data_raw)
    assert data_hdr.class_id is True  # PDF says "always 0" — empirically FALSE (C1)
    assert data_hdr.trailer is True
    assert data_hdr.packet_size == 2048
    assert len(data_raw) == 2048 * 4
    assert data_hdr.tsi == 1 and data_hdr.tsf == 2
    # empirical fixture values: the SDR clock is NOT wall-synced (int_ts is
    # ~2**24, and frac_ts exceeds one second on this hardware); the plan's
    # timestamp formula is applied regardless
    assert data_hdr.int_ts == 16_777_210
    assert data_hdr.frac_ts > 0
    assert data_hdr.timestamp == data_hdr.int_ts + data_hdr.frac_ts / 1e12
