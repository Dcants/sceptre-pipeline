"""Stage 2: context decode — real recordings are ground truth; gain/GPS/ECEF
are absent from both fixtures (C4) so those paths use synthetic bytes only."""

from __future__ import annotations

import struct

import pytest
from conftest import (
    build_context_packet,
    encode_payload_format_u64,
    standard_context_fields,
)

from sceptre_pipeline.interpreter import (
    Interpreter,
    decode_context,
    decode_payload_format,
    parse_header,
)


def _decode(raw: bytes) -> dict:
    return decode_context(raw, parse_header(raw))


# --- real recordings (authoritative) ---------------------------------------


def test_real_context_values(single_frequency_path, load_capture) -> None:
    raw = load_capture(single_frequency_path)["packets"][0][3]
    ctx = _decode(raw)
    assert ctx["bandwidth_hz"] == 500_000.0
    assert ctx["rf_hz"] == 97_300_000.0
    assert ctx["sample_rate_hz"] == 625_000.0
    assert ctx["is_complex"] is True
    assert ctx["bytes_per_sample"] == 8
    assert ctx["data_item_format"] == 0b01110
    assert ctx["component_dtype"] == ">f4"
    assert "gain_db" not in ctx  # bit 23 absent from both fixtures (C4)


def test_real_cif_word_and_context_count(single_frequency_path, load_capture) -> None:
    packets = load_capture(single_frequency_path)["packets"]
    contexts = [
        p[3] for p in packets if parse_header(p[3]).packet_type == 4
    ]
    assert len(contexts) == 6  # the periodic heartbeat in single_frequency
    decoded = []
    for raw in contexts:
        hdr = parse_header(raw)
        cif = struct.unpack_from(">I", raw, hdr.header_len)[0]
        assert cif == 0x28208000  # present bits [29, 27, 21, 15]
        decoded.append(decode_context(raw, hdr))
    # periodic context is identical — Stage 3 must NOT flush on it
    assert all(d == decoded[0] for d in decoded)


def test_change_frequency_rf_sequence(change_frequency_path, load_capture) -> None:
    packets = load_capture(change_frequency_path)["packets"]
    rf_sequence: list[float] = []
    for _, _, _, raw in packets:
        hdr = parse_header(raw)
        if hdr.packet_type != 4:
            continue
        rf = decode_context(raw, hdr)["rf_hz"]
        if not rf_sequence or rf_sequence[-1] != rf:
            rf_sequence.append(rf)
    assert rf_sequence == [97_300_000.0, 103_700_000.0]


# --- payload format: exact 64-bit windows ----------------------------------


def test_payload_format_windows_on_real_word() -> None:
    fmt = decode_payload_format(0x2E0007DF00000000)  # verified real value
    assert fmt["is_complex"] is True
    assert fmt["data_item_format"] == 0b01110
    assert fmt["component_bits"] == 32
    assert fmt["bytes_per_sample"] == 8


def test_payload_format_encoder_roundtrips_real_word() -> None:
    assert encode_payload_format_u64() == 0x2E0007DF00000000


def test_format_assertion_rejects_non_float() -> None:
    bad = encode_payload_format_u64(data_item_format=0b00000)  # signed fixed-point
    with pytest.raises(ValueError, match="format"):
        decode_payload_format(bad)


def test_format_assertion_rejects_odd_size() -> None:
    # real int16: float assertion passes only for 0b01110, so force format
    # float but 16-bit components -> bytes_per_sample 4, not 8
    bad = encode_payload_format_u64(
        real_complex_code=0b01, packing_bits=16, item_bits=16
    )
    with pytest.raises(ValueError, match="bytes_per_sample"):
        decode_payload_format(bad)


def test_format_assertion_via_context_packet() -> None:
    """The interpreter rejects a non-float context wholesale (fail loudly)."""
    fields = standard_context_fields(
        payload_format_u64=encode_payload_format_u64(data_item_format=0b00111)
    )
    raw = build_context_packet(fields=fields)
    with pytest.raises(ValueError):
        _decode(raw)


# --- synthetic-only decode paths (C4) ---------------------------------------


def test_synthetic_gain_two_stages_summed() -> None:
    fields = standard_context_fields()
    # stage2 = bits 31:16, stage1 = bits 15:0; each int16 / 128 dB, summed
    fields[23] = struct.pack(">hh", 256, -64)  # 2.0 dB + (-0.5 dB)
    ctx = _decode(build_context_packet(fields=fields))
    assert ctx["gain_db"] == pytest.approx(1.5)
    # the rest of the walk stayed in sync past the inserted field
    assert ctx["sample_rate_hz"] == 625_000.0
    assert ctx["bytes_per_sample"] == 8


def test_walk_advances_past_gps_and_ephemeris_without_desync() -> None:
    fields = standard_context_fields()
    fields[23] = struct.pack(">hh", 128, 0)  # 1.0 dB
    fields[14] = b"\x5A" * 44  # Formatted GPS — advanced past, not decoded
    fields[12] = b"\xA5" * 52  # ECEF Ephemeris — advanced past, not decoded
    ctx = _decode(build_context_packet(fields=fields))
    assert ctx["bandwidth_hz"] == 500_000.0
    assert ctx["rf_hz"] == 97_300_000.0
    assert ctx["gain_db"] == pytest.approx(1.0)
    assert ctx["sample_rate_hz"] == 625_000.0
    assert ctx["is_complex"] is True


# --- Interpreter context handling -------------------------------------------


def test_interpreter_adopts_context_and_returns_record(
    single_frequency_path, load_capture
) -> None:
    raw = load_capture(single_frequency_path)["packets"][0][3]
    interp = Interpreter()
    assert interp.current_context is None
    record = interp.process(raw)
    assert record["type"] == "context"
    assert record["data"] is None
    assert record["context"]["rf_hz"] == 97_300_000.0
    assert interp.current_context == record["context"]
    md = record["metadata"]
    assert md["stream_id"] == 123
    assert md["is_complex"] is True
    assert md["gap_before"] is False
