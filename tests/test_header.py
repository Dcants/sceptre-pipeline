"""Stage 2/2.5: flag-driven header parse — word0 bit extraction + dynamic offsets."""

from __future__ import annotations

import struct

import pytest
from conftest import CONTEXT_CLASS_ID, DATA_CLASS_ID

from sceptre_pipeline.interpreter import Header, parse_header


def _packet(
    word0: int, stream_id: int, class_id: bytes | None, int_ts: int, frac_ts: int
) -> bytes:
    """Lay out header bytes exactly as the wire does for a given word0.

    Built by hand (not via conftest.build_header) so the real observed word0
    values — which carry reserved bits 24/25 the parser must ignore — can be
    used verbatim as acceptance vectors.
    """
    raw = struct.pack(">II", word0, stream_id)
    if class_id is not None:
        raw += class_id
    return raw + struct.pack(">IQ", int_ts, frac_ts)


def test_data_packet_word0_vector() -> None:
    """Acceptance vector: 1e600800 -> type 1, cid 1, trl 1, tsi 1, tsf 2, cnt 0, size 2048."""
    hdr = parse_header(
        _packet(0x1E600800, 123, DATA_CLASS_ID, 1_750_000_000, 500_000_000_000)
    )
    assert hdr.packet_type == 1
    assert hdr.class_id == DATA_CLASS_ID
    assert hdr.has_trailer is True
    assert hdr.tsi == 1
    assert hdr.tsf == 2
    assert hdr.packet_counter == 0
    assert hdr.packet_size == 2048  # words = 8192 bytes
    assert hdr.stream_id == 123
    assert hdr.header_len == 28  # class_id present -> timestamps shift by 8
    assert hdr.int_ts == 1_750_000_000
    assert hdr.frac_ps == 500_000_000_000
    assert hdr.timestamp == 1_750_000_000.5  # int_ts + frac_ps / 1e12


def test_context_packet_word0_vector() -> None:
    """Acceptance vector: 4b600010 -> type 4, cid 1, trl 0, size 16."""
    hdr = parse_header(_packet(0x4B600010, 123, CONTEXT_CLASS_ID, 0, 0))
    assert hdr.packet_type == 4
    assert hdr.class_id == CONTEXT_CLASS_ID
    assert hdr.has_trailer is False
    assert hdr.tsi == 1
    assert hdr.tsf == 2
    assert hdr.packet_counter == 0
    assert hdr.packet_size == 16  # words = 64 bytes
    assert hdr.header_len == 28


def test_counter_extraction_bits_12_15() -> None:
    hdr = parse_header(_packet(0x1E6D0800, 1, DATA_CLASS_ID, 0, 0))
    assert hdr.packet_counter == 0xD


def test_packet_shorter_than_word0_raises() -> None:
    with pytest.raises(ValueError):
        parse_header(b"\x00" * 3)


def test_packet_truncated_before_flagged_fields_raises() -> None:
    """word0 promises class_id + both timestamps (28 bytes); give it only 20."""
    raw = _packet(0x1E600800, 1, DATA_CLASS_ID, 0, 0)
    with pytest.raises(ValueError):
        parse_header(raw[:20])


def test_header_is_frozen_dataclass() -> None:
    hdr = parse_header(_packet(0x1E600800, 1, DATA_CLASS_ID, 0, 0))
    assert isinstance(hdr, Header)
    with pytest.raises(AttributeError):
        hdr.packet_type = 2  # type: ignore[misc]


def test_real_fixture_headers(single_frequency_path, load_capture) -> None:
    """First packet is context (16 words / 64 B); data packets are 2048 words / 8192 B."""
    packets = load_capture(single_frequency_path)["packets"]

    ctx_hdr = parse_header(packets[0][3])
    assert ctx_hdr.packet_type == 4
    assert ctx_hdr.class_id == CONTEXT_CLASS_ID
    assert ctx_hdr.has_trailer is False
    assert ctx_hdr.packet_size == 16
    assert len(packets[0][3]) == 16 * 4
    assert ctx_hdr.stream_id == 123

    data_raw = next(p[3] for p in packets if parse_header(p[3]).packet_type == 1)
    data_hdr = parse_header(data_raw)
    assert data_hdr.class_id == DATA_CLASS_ID  # PDF says "always 0" — empirically set
    assert data_hdr.has_trailer is True
    assert data_hdr.packet_size == 2048
    assert len(data_raw) == 2048 * 4
    assert data_hdr.tsi == 1 and data_hdr.tsf == 2
    # the flag-driven parse reads the timestamps at their true offsets (16/20
    # here): a real 2026 wall-clock instant, not the Class ID OUI (Stage 2.5)
    assert data_hdr.int_ts == 1_782_443_209
    assert 0 < data_hdr.frac_ps < 10**12
    assert data_hdr.timestamp == data_hdr.int_ts + data_hdr.frac_ps / 1e12
