"""Stage 2: context decode — real recordings are ground truth; gain/GPS/ECEF
are absent from both fixtures (C4) so those paths use synthetic bytes only."""

from __future__ import annotations

import logging
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
    assert ctx["supported"] is True  # Stage 2.75 classification
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


# Stage 2.75 inverts the old fail-loud contract: decode_payload_format is a
# CLASSIFIER now — it never raises; unsupportable layouts get supported=False.


def test_classifier_supports_signed_fixed_point_as_int32() -> None:
    # under the old contract this word raised; 32-bit signed fixed-point is
    # now a supported dynamic format (complex int32)
    word = encode_payload_format_u64(data_item_format=0b00000)
    fmt = decode_payload_format(word)
    assert fmt["supported"] is True
    assert fmt["component_dtype"] == ">i4"
    assert fmt["bytes_per_sample"] == 8  # complex: 2 x 4-byte components


def test_classifier_flags_float_code_with_odd_size_unsupported() -> None:
    # float32 code with 16-bit components has no dtype: classified, not raised
    bad = encode_payload_format_u64(
        real_complex_code=0b01, packing_bits=16, item_bits=16
    )
    fmt = decode_payload_format(bad)
    assert fmt["supported"] is False
    assert fmt["component_dtype"] is None
    assert fmt["bytes_per_sample"] is None


def test_unsupported_context_packet_decodes_without_raising() -> None:
    """An unsupported-format context still decodes into a labeled record."""
    fields = standard_context_fields(
        payload_format_u64=encode_payload_format_u64(data_item_format=0b00111)
    )
    ctx = _decode(build_context_packet(fields=fields))  # must not raise
    assert ctx["supported"] is False
    assert ctx["component_dtype"] is None
    assert ctx["bytes_per_sample"] is None
    # the rest of the walk still decoded normally
    assert ctx["bandwidth_hz"] == 500_000.0
    assert ctx["sample_rate_hz"] == 625_000.0


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


# --- Stage 2.75: complete, stop-safe CIF0 walk --------------------------------


def test_bit31_context_field_change_is_zero_width() -> None:
    """Bit 31 is an indicator only: adds 0 bytes, never aborts the walk."""
    fields = {31: b"", **standard_context_fields()}
    ctx = _decode(build_context_packet(fields=fields))
    assert ctx["context_field_change"] is True
    assert ctx["bandwidth_hz"] == 500_000.0
    assert ctx["rf_hz"] == 97_300_000.0
    assert ctx["sample_rate_hz"] == 625_000.0
    assert ctx["bytes_per_sample"] == 8
    assert ctx["supported"] is True


def test_full_cif0_fixed_size_set_keeps_walk_in_sync() -> None:
    """Every CIF0 fixed-size field (bits 30-10) advances at its exact size —
    the decoded fields interleaved below stay in sync only if all sizes are
    right."""
    fields = standard_context_fields()
    fields[30] = b"\x11" * 4  # Reference Point Id
    fields[28] = b"\x22" * 8  # IF Ref Freq
    fields[26] = b"\x33" * 8  # RF Ref Freq Offset
    fields[25] = b"\x44" * 8  # IF Band Offset
    fields[24] = b"\x55" * 4  # Reference Level
    fields[22] = b"\x66" * 4  # Over-Range Count
    fields[20] = b"\x77" * 8  # Timestamp Adjustment
    fields[19] = b"\x88" * 4  # Timestamp Calibration
    fields[18] = b"\x99" * 4  # Temperature
    fields[17] = b"\xaa" * 8  # Device Id
    fields[16] = b"\xbb" * 4  # State/Event
    fields[13] = b"\xcc" * 44  # Formatted INS
    fields[11] = b"\xdd" * 52  # Relative Ephemeris
    fields[10] = b"\xee" * 4  # Ephemeris Ref Id
    ctx = _decode(build_context_packet(fields=fields))
    assert ctx["bandwidth_hz"] == 500_000.0
    assert ctx["rf_hz"] == 97_300_000.0
    assert ctx["sample_rate_hz"] == 625_000.0
    assert ctx["bytes_per_sample"] == 8
    assert ctx["supported"] is True


def test_variable_length_bit_stops_walk_preserving_decoded_fields(caplog) -> None:
    """GPS-ASCII (bit 9) is variable-length: the walk stops there but every
    already-decoded higher-bit field survives."""
    fields = standard_context_fields()
    fields[9] = b"\x00" * 8  # bytes the walk cannot size
    with caplog.at_level(logging.WARNING, logger="sceptre_pipeline.interpreter"):
        ctx = _decode(build_context_packet(fields=fields))
    assert ctx["bandwidth_hz"] == 500_000.0
    assert ctx["rf_hz"] == 97_300_000.0
    assert ctx["sample_rate_hz"] == 625_000.0
    assert ctx["bytes_per_sample"] == 8
    assert any("stopping walk" in r.getMessage() for r in caplog.records)


def test_cif_enable_bit_stops_walk_preserving_decoded_fields() -> None:
    """A CIF1-enable bit (1) would require parsing a second CIF word: stop
    safely, keep everything already decoded."""
    fields = standard_context_fields()
    fields[1] = b""  # enable bits carry no CIF0 payload of their own
    ctx = _decode(build_context_packet(fields=fields))
    assert ctx["bandwidth_hz"] == 500_000.0
    assert ctx["bytes_per_sample"] == 8


# --- Interpreter context handling -------------------------------------------


def test_interpreter_adopts_context_and_returns_record(
    single_frequency_path, load_capture
) -> None:
    raw = load_capture(single_frequency_path)["packets"][0][3]
    interp = Interpreter()
    assert interp.context_for(123) is None
    record = interp.process(raw)
    assert record["type"] == "context"
    assert record["data"] is None
    assert record["context"]["rf_hz"] == 97_300_000.0
    assert interp.context_for(123) == record["context"]
    md = record["metadata"]
    assert md["stream_id"] == 123
    assert md["is_complex"] is True
    assert md["gap_before"] is False
