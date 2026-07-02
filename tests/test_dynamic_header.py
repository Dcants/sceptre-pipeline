"""Stage 2.5: flag-driven dynamic header parsing — one code path, both configs.

The two recordings were captured with class_id AND trailer enabled: 28-byte
header, 8-byte Class ID BEFORE the timestamps, 4-byte trailer on data packets.
Default-config Sceptre (class_id=0, trailer=0) uses a 20-byte header with the
timestamps at offsets 8/12. Every offset must derive from the word0 flags —
the earlier fixed-offset model silently read the Class ID as the integer
timestamp and produced a 1970-garbage start_timestamp.
"""

from __future__ import annotations

import struct
from datetime import datetime, timezone

import pytest
from conftest import (
    CONTEXT_CLASS_ID,
    DATA_CLASS_ID,
    build_context_packet,
    build_data_packet,
    encode_iq_float32,
    standard_context_fields,
)

from sceptre_pipeline.interpreter import decode_context, parse_header, trim_data

# 1020 samples @ 625 kHz = 1.632 ms between data packets
PACKET_CADENCE_PS = 1_632_000_000
PS_PER_SECOND = 10**12


# --- recording config: class_id=1, trailer=1 -> 28-byte header ---------------


def test_recording_data_header_len_28_with_trailer(
    single_frequency_path, load_capture
) -> None:
    packets = load_capture(single_frequency_path)["packets"]
    raw = next(p[3] for p in packets if parse_header(p[3]).packet_type == 1)
    hdr = parse_header(raw)
    assert hdr.header_len == 28
    assert hdr.has_trailer is True
    assert hdr.class_id == DATA_CLASS_ID  # 00fffffa00160000
    assert hdr.tsi == 1 and hdr.tsf == 2


def test_recording_context_header_len_28_no_trailer(
    single_frequency_path, load_capture
) -> None:
    hdr = parse_header(load_capture(single_frequency_path)["packets"][0][3])
    assert hdr.packet_type == 4
    assert hdr.header_len == 28
    assert hdr.has_trailer is False
    assert hdr.class_id == CONTEXT_CLASS_ID  # context class codes differ from data


def test_recording_timestamp_is_a_real_2026_instant_not_1970(
    single_frequency_path, load_capture
) -> None:
    """The regression this stage fixes: int_ts must be the timestamp, not the OUI."""
    packets = load_capture(single_frequency_path)["packets"]
    raw = next(p[3] for p in packets if parse_header(p[3]).packet_type == 1)
    hdr = parse_header(raw)
    assert hdr.int_ts == 1_782_443_209  # 2026-06-26T03:06:49Z
    assert hdr.int_ts != 0x00FFFFFA  # the old parser read the Class ID OUI here
    assert hdr.frac_ps == 697_277_396_042
    assert hdr.timestamp == hdr.int_ts + hdr.frac_ps / 1e12
    assert datetime.fromtimestamp(hdr.timestamp, timezone.utc).year == 2026


def test_recording_frac_ps_advances_by_packet_cadence(
    single_frequency_path, load_capture
) -> None:
    """frac_ps is a real-time picosecond counter: +1_632_000_000 per packet,
    always < 1e12, rolling into int_ts at each second boundary."""
    packets = load_capture(single_frequency_path)["packets"]
    headers = [
        parse_header(p[3]) for p in packets if parse_header(p[3]).packet_type == 1
    ]

    first = [h.frac_ps for h in headers[:5]]
    assert [b - a for a, b in zip(first, first[1:])] == [PACKET_CADENCE_PS] * 4

    assert all(h.frac_ps < PS_PER_SECOND for h in headers)

    # across the whole capture (including second rollovers) the combined
    # picosecond timeline advances by the cadence, +/- 1 ps of clock jitter
    timeline = [h.int_ts * PS_PER_SECOND + h.frac_ps for h in headers]
    deltas = {b - a for a, b in zip(timeline, timeline[1:])}
    assert deltas <= {PACKET_CADENCE_PS - 1, PACKET_CADENCE_PS, PACKET_CADENCE_PS + 1}


# --- default config: class_id=0, trailer=0 -> 20-byte header ------------------


def test_default_config_data_header_len_20_timestamps_at_8_and_12() -> None:
    payload = encode_iq_float32([complex(1, 2), complex(3, 4)])
    raw = build_data_packet(
        payload=payload,
        class_id=False,
        trailer=False,
        int_ts=1_750_000_000,
        frac_ts=250_000_000_000,
    )
    # the wire really does carry the timestamps at offsets 8/12
    assert struct.unpack_from(">I", raw, 8)[0] == 1_750_000_000
    assert struct.unpack_from(">Q", raw, 12)[0] == 250_000_000_000

    hdr = parse_header(raw)
    assert hdr.header_len == 20
    assert hdr.has_trailer is False
    assert hdr.class_id is None
    assert hdr.int_ts == 1_750_000_000
    assert hdr.frac_ps == 250_000_000_000
    assert hdr.timestamp == 1_750_000_000.25

    body, num_samples = trim_data(raw, hdr, bytes_per_sample=8)
    assert num_samples == 2
    assert body == payload


def test_default_config_context_decodes_via_same_code_path() -> None:
    raw = build_context_packet(fields=standard_context_fields(), class_id=False)
    hdr = parse_header(raw)
    assert hdr.header_len == 20
    ctx = decode_context(raw, hdr)
    assert ctx["rf_hz"] == 97_300_000.0
    assert ctx["sample_rate_hz"] == 625_000.0
    assert ctx["bytes_per_sample"] == 8


# --- offsets are flag-driven, never hardcoded --------------------------------


@pytest.mark.parametrize("class_id", [False, True])
@pytest.mark.parametrize("trailer", [False, True])
def test_flipping_flags_shifts_header_len_and_payload_end(
    class_id: bool, trailer: bool
) -> None:
    payload = encode_iq_float32([complex(1, -1), complex(0.5, 0.25)])
    raw = build_data_packet(
        payload=payload,
        class_id=class_id,
        trailer=trailer,
        int_ts=1_700_000_000,
        frac_ts=500_000_000_000,
    )
    hdr = parse_header(raw)
    assert hdr.header_len == 20 + (8 if class_id else 0)
    assert (hdr.class_id is not None) is class_id
    assert hdr.has_trailer is trailer

    # the timestamps track the Class ID shift in every combination
    assert hdr.int_ts == 1_700_000_000
    assert hdr.frac_ps == 500_000_000_000

    body, num_samples = trim_data(raw, hdr, bytes_per_sample=8)
    assert num_samples == 2
    assert body == payload
    payload_end = len(raw) - (4 if trailer else 0)
    assert raw[hdr.header_len : payload_end] == payload


@pytest.mark.parametrize(
    ("tsi", "tsf", "expected_len"),
    [(0, 0, 8), (1, 0, 12), (0, 2, 16), (1, 2, 20)],
)
def test_tsi_tsf_flags_gate_timestamp_presence(
    tsi: int, tsf: int, expected_len: int
) -> None:
    payload = encode_iq_float32([complex(1, 1)])
    raw = build_data_packet(
        payload=payload, class_id=False, trailer=False, tsi=tsi, tsf=tsf
    )
    hdr = parse_header(raw)
    assert hdr.header_len == expected_len
    assert (hdr.int_ts is not None) is bool(tsi)
    assert (hdr.frac_ps is not None) is bool(tsf)
    if not tsi and not tsf:
        assert hdr.timestamp == 0.0

    body, num_samples = trim_data(raw, hdr, bytes_per_sample=8)
    assert num_samples == 1
    assert body == payload
