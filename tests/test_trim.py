"""Stage 2/2.5: data trim (flag-driven bounds), counter-gap detection,
data-before-context.

Trim math must come from the trimmed body length (1020 samples), never the
PDF formula floor((packet_size*4 - 20)/8) = 1021, which injects the tail of
the header as a garbage first sample.
"""

from __future__ import annotations

import logging

import numpy as np
import pytest
from conftest import (
    build_context_packet,
    build_data_packet,
    encode_iq_float32,
    standard_context_fields,
)

from sceptre_pipeline.interpreter import Interpreter, parse_header, trim_data


def _primed_interpreter() -> Interpreter:
    interp = Interpreter()
    record = interp.process(build_context_packet(fields=standard_context_fields()))
    assert record["type"] == "context"
    return interp


# --- real-recording trim (authoritative) ------------------------------------


def test_real_data_packet_trims_to_1020_samples(
    single_frequency_path, load_capture
) -> None:
    packets = load_capture(single_frequency_path)["packets"]
    interp = Interpreter()
    record = None
    for _, _, _, raw in packets:
        record = interp.process(raw)
        if record and record["type"] == "data":
            break

    body = record["data"]
    assert len(body) == 8160
    assert len(body) % 8 == 0
    assert len(body) // 8 == 1020  # NOT the PDF's 1021

    # no garbage first sample: header bytes excluded, values are sane IQ
    components = np.frombuffer(body, dtype=">f4")
    assert components.size == 2040
    assert not np.isnan(components).any()
    assert np.abs(components).max() < 32.0


def test_all_real_data_packets_trim_cleanly(single_frequency_path, load_capture) -> None:
    packets = load_capture(single_frequency_path)["packets"]
    interp = Interpreter()
    total_samples = 0
    data_records = 0
    for _, _, _, raw in packets:
        record = interp.process(raw)
        if record and record["type"] == "data":
            assert len(record["data"]) == 8160
            assert record["metadata"]["gap_before"] is False  # contiguous capture
            total_samples += len(record["data"]) // 8
            data_records += 1
    assert data_records == 3066
    assert total_samples == 3_127_320  # plan's end-to-end ground truth


# --- synthetic trim arithmetic ----------------------------------------------


def test_trim_synthetic_complex_float32_drops_header_and_trailer() -> None:
    samples = [complex(1.0, -1.0), complex(0.5, 0.25), complex(-2.0, 3.0)]
    raw = build_data_packet(payload=encode_iq_float32(samples))
    hdr = parse_header(raw)
    body, num_samples = trim_data(raw, hdr, bytes_per_sample=8)
    assert num_samples == 3
    assert body == encode_iq_float32(samples)  # 28B header and \xAA trailer gone


def test_trim_sub_32bit_format_pads_are_floored_away() -> None:
    """Sub-32-bit synthetic format — cannot use the fixtures (C4).

    Note: the plan's example (int16, 2 B/sample) cannot work with the
    length-floor rule — its 2 B word-alignment pad is indistinguishable from
    one more sample. We use 24-bit real samples (3 B/sample), whose 2 B pad
    (< bytes_per_sample) is genuinely floored away.
    """
    payload = bytes(range(30))  # 10 samples x 3 B (24-bit real)
    raw = build_data_packet(payload=payload, pad=b"\x00\x00")  # to 32-bit word
    hdr = parse_header(raw)
    body, num_samples = trim_data(raw, hdr, bytes_per_sample=3)
    assert num_samples == 10
    assert body == payload  # the 2 pad bytes are trimmed


def test_trim_is_pure_and_independent_of_format_assertion() -> None:
    """trim_data works on 2 B/sample even though decode_context would reject it."""
    payload = bytes(range(12))  # 6 samples x 2 B, lands on a word boundary
    raw = build_data_packet(payload=payload)
    hdr = parse_header(raw)
    body, num_samples = trim_data(raw, hdr, bytes_per_sample=2)
    assert num_samples == 6
    assert body == payload


@pytest.mark.parametrize("class_id", [False, True])
@pytest.mark.parametrize("trailer", [False, True])
def test_trim_handles_every_flag_combination(class_id: bool, trailer: bool) -> None:
    """start/end derive from the flags INDEPENDENTLY (C1) — all 4 combos."""
    payload = encode_iq_float32([complex(1, 2), complex(-3, 4)])
    raw = build_data_packet(payload=payload, class_id=class_id, trailer=trailer)
    hdr = parse_header(raw)
    assert (hdr.class_id is not None) is class_id and hdr.has_trailer is trailer
    body, num_samples = trim_data(raw, hdr, bytes_per_sample=8)
    assert num_samples == 2
    assert body == payload


# Stage 2.75 datagram-length hardening: an oversized datagram trims to
# exactly packet_size*4 (no trailing bytes ingested as samples); an
# under-length one raises (caught by the process() guard as truncation)
# instead of silently short-reading.


def test_trim_oversized_datagram_never_ingests_trailing_bytes(caplog) -> None:
    payload = encode_iq_float32([complex(1, 2), complex(3, 4)])
    first = build_data_packet(payload=payload)
    second = build_data_packet(payload=encode_iq_float32([complex(9, 9)]))
    glued = first + second  # two concatenated packets in one datagram
    hdr = parse_header(glued)  # word0 (and packet_size) of the FIRST packet
    with caplog.at_level(logging.WARNING, logger="sceptre_pipeline.interpreter"):
        body, num_samples = trim_data(glued, hdr, bytes_per_sample=8)
    assert num_samples == 2
    assert body == payload  # nothing of the second packet leaked in
    assert any("packet_size" in r.getMessage() for r in caplog.records)


def test_trim_undersized_datagram_raises_instead_of_short_reading() -> None:
    raw = build_data_packet(
        payload=encode_iq_float32([complex(1, 2), complex(3, 4)])
    )
    truncated = raw[:-3]  # header intact, payload cut short
    with pytest.raises(ValueError, match="truncated"):
        trim_data(truncated, parse_header(truncated), bytes_per_sample=8)


# --- Interpreter data path ----------------------------------------------------


def test_counter_gap_sets_gap_before(caplog) -> None:
    """Acceptance: counters ...3,5... -> gap_before=True."""
    interp = _primed_interpreter()
    payload = encode_iq_float32([complex(1, 1)])

    r3 = interp.process(build_data_packet(payload=payload, counter=3))
    assert r3["metadata"]["gap_before"] is False  # first data packet: no prev

    r5 = interp.process(build_data_packet(payload=payload, counter=5))
    assert r5["metadata"]["gap_before"] is True  # 4 went missing

    r6 = interp.process(build_data_packet(payload=payload, counter=6))
    assert r6["metadata"]["gap_before"] is False


def test_counter_wrap_15_to_0_is_not_a_gap() -> None:
    interp = _primed_interpreter()
    payload = encode_iq_float32([complex(1, 1)])
    interp.process(build_data_packet(payload=payload, counter=15))
    r0 = interp.process(build_data_packet(payload=payload, counter=0))
    assert r0["metadata"]["gap_before"] is False


def test_counter_tracking_is_per_stream() -> None:
    # Stage 2.75: context is per-stream too, so prime both streams
    interp = Interpreter()
    for sid in (1, 2):
        interp.process(
            build_context_packet(fields=standard_context_fields(), stream_id=sid)
        )
    payload = encode_iq_float32([complex(1, 1)])
    interp.process(build_data_packet(payload=payload, counter=3, stream_id=1))
    r = interp.process(build_data_packet(payload=payload, counter=9, stream_id=2))
    assert r["metadata"]["gap_before"] is False  # different stream, no prev


def test_context_without_payload_format_drops_data_with_warning(caplog) -> None:
    """A context missing CIF bit 15 must not crash Thread B on the next data."""
    interp = Interpreter()
    fields = standard_context_fields()
    del fields[15]  # no Data Packet Payload Format
    record = interp.process(build_context_packet(fields=fields))
    assert record["type"] == "context"
    assert "bytes_per_sample" not in record["context"]

    raw = build_data_packet(payload=encode_iq_float32([complex(1, 1)]))
    with caplog.at_level(logging.WARNING, logger="sceptre_pipeline.interpreter"):
        assert interp.process(raw) is None
    assert any("payload format" in r.getMessage() for r in caplog.records)


def test_data_before_context_returns_none_and_warns(caplog) -> None:
    interp = Interpreter()
    raw = build_data_packet(payload=encode_iq_float32([complex(1, 1)]))
    with caplog.at_level(logging.WARNING, logger="sceptre_pipeline.interpreter"):
        assert interp.process(raw) is None
    assert any("context" in r.getMessage().lower() for r in caplog.records)


def test_unknown_packet_type_returns_none_and_warns(caplog) -> None:
    interp = _primed_interpreter()
    raw = bytearray(build_data_packet(payload=encode_iq_float32([complex(1, 1)])))
    raw[0] = (0x2 << 4) | (raw[0] & 0x0F)  # packet type 2
    with caplog.at_level(logging.WARNING, logger="sceptre_pipeline.interpreter"):
        assert interp.process(bytes(raw)) is None


def test_data_record_shape() -> None:
    interp = _primed_interpreter()
    record = interp.process(
        build_data_packet(payload=encode_iq_float32([complex(1, 1)]), counter=7)
    )
    assert set(record) == {"type", "metadata", "context", "data"}
    assert record["type"] == "data"
    assert record["context"] is None
    assert isinstance(record["data"], bytes)
    md = record["metadata"]
    assert set(md) == {
        "stream_id",
        "counter",
        "timestamp",
        "packet_size",
        "dtype",
        "bytes_per_sample",
        "is_complex",
        "gap_before",
    }
    assert md["counter"] == 7
    assert md["dtype"] == ">f4"
    assert md["bytes_per_sample"] == 8
    assert md["is_complex"] is True
