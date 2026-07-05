"""Stage 2.75: per-stream context routing.

The Sceptre format multiplexes multiple streams on one socket, so the
interpreter keeps ``_contexts[stream_id]`` — interleaved traffic must never
fold into one accumulator, a single-stream capture is simply the N=1 case,
and a spoofed-stream flood is bounded by ``max_streams``.
"""

from __future__ import annotations

import logging

from conftest import (
    build_context_packet,
    build_data_packet,
    encode_iq_float32,
    encode_payload_format_u64,
    standard_context_fields,
)

from sceptre_pipeline.interpreter import Interpreter

COMPLEX_FLOAT32 = encode_payload_format_u64()  # the recorded format
REAL_INT16 = encode_payload_format_u64(
    real_complex_code=0b00, data_item_format=0b00000, packing_bits=16, item_bits=16
)

FLOAT_PAYLOAD = encode_iq_float32([complex(1.0, -1.0)])  # 8 B = 1 cf32 sample
INT16_PAYLOAD = bytes(range(8))  # 8 B = 4 real-int16 samples


def _dual_stream_interpreter() -> Interpreter:
    """Stream 1: complex float32 @ 97.3 MHz. Stream 2: real int16 @ 103.7 MHz."""
    interp = Interpreter()
    interp.process(
        build_context_packet(
            fields=standard_context_fields(payload_format_u64=COMPLEX_FLOAT32),
            stream_id=1,
        )
    )
    interp.process(
        build_context_packet(
            fields=standard_context_fields(
                rf_hz=103_700_000.0, payload_format_u64=REAL_INT16
            ),
            stream_id=2,
        )
    )
    return interp


# --- interleaved streams stay separate ----------------------------------------


def test_interleaved_streams_keep_their_own_context() -> None:
    interp = _dual_stream_interpreter()

    ctx1 = interp.context_for(1)
    ctx2 = interp.context_for(2)
    assert ctx1["rf_hz"] == 97_300_000.0 and ctx1["component_dtype"] == ">f4"
    assert ctx2["rf_hz"] == 103_700_000.0 and ctx2["component_dtype"] == ">i2"

    # interleave data; each record must be decoded against ITS stream's context
    r1a = interp.process(build_data_packet(payload=FLOAT_PAYLOAD, stream_id=1, counter=0))
    r2a = interp.process(build_data_packet(payload=INT16_PAYLOAD, stream_id=2, counter=8))
    r1b = interp.process(build_data_packet(payload=FLOAT_PAYLOAD, stream_id=1, counter=1))
    r2b = interp.process(build_data_packet(payload=INT16_PAYLOAD, stream_id=2, counter=9))

    for r in (r1a, r2a, r1b, r2b):
        assert r is not None and r["type"] == "data"
    assert r1a["metadata"]["dtype"] == ">f4"
    assert r1a["metadata"]["bytes_per_sample"] == 8
    assert r1a["metadata"]["is_complex"] is True
    assert r2a["metadata"]["dtype"] == ">i2"
    assert r2a["metadata"]["bytes_per_sample"] == 2
    assert r2a["metadata"]["is_complex"] is False
    assert interp.errors == 0 and interp.dropped == 0


def test_interleaved_counters_never_cross_streams() -> None:
    interp = _dual_stream_interpreter()
    # stream 1 counts 0,1,2 while stream 2 counts 7,8 — interleaved, no gaps
    seq = [(1, 0), (2, 7), (1, 1), (2, 8), (1, 2)]
    for sid, counter in seq:
        payload = FLOAT_PAYLOAD if sid == 1 else INT16_PAYLOAD
        r = interp.process(
            build_data_packet(payload=payload, stream_id=sid, counter=counter)
        )
        assert r["metadata"]["gap_before"] is False


def test_context_change_on_one_stream_leaves_the_other_alone() -> None:
    interp = _dual_stream_interpreter()
    interp.process(
        build_context_packet(
            fields=standard_context_fields(
                rf_hz=200_000_000.0, payload_format_u64=COMPLEX_FLOAT32
            ),
            stream_id=1,
            counter=1,
        )
    )
    assert interp.context_for(1)["rf_hz"] == 200_000_000.0
    assert interp.context_for(2)["rf_hz"] == 103_700_000.0  # untouched


def test_second_valid_stream_is_routed_not_dropped() -> None:
    """Foreign-but-valid traffic is a stream, not garbage (multi-stream is
    native; nothing is dropped for being 'the wrong stream')."""
    interp = Interpreter()
    interp.process(
        build_context_packet(fields=standard_context_fields(), stream_id=7)
    )
    r = interp.process(build_data_packet(payload=FLOAT_PAYLOAD, stream_id=7))
    assert r["type"] == "data"
    assert interp.dropped == 0


# --- single stream is the N=1 case ---------------------------------------------


def test_single_stream_recording_is_the_n1_case(
    single_frequency_path, load_capture
) -> None:
    interp = Interpreter()
    data_records = 0
    for _, _, _, raw in load_capture(single_frequency_path)["packets"]:
        record = interp.process(raw)
        if record and record["type"] == "data":
            data_records += 1
    assert data_records == 3066  # nothing dropped for being "the wrong stream"
    assert interp.errors == 0
    assert interp.dropped == 0


# --- spoofed-stream flood is bounded by max_streams ------------------------------


def test_max_streams_defaults_to_64() -> None:
    from sceptre_pipeline.interpreter import DEFAULT_MAX_STREAMS

    assert DEFAULT_MAX_STREAMS == 64
    assert Interpreter().max_streams == 64


def test_context_flood_bounded_by_max_streams(caplog) -> None:
    """Spoofed CONTEXT packets are the memory-growth threat: each decoded
    context claims a tracked-stream slot, so slots are capped."""
    interp = Interpreter(max_streams=4)
    for sid in range(4):
        interp.process(
            build_context_packet(fields=standard_context_fields(), stream_id=sid)
        )

    with caplog.at_level(logging.WARNING, logger="sceptre_pipeline.interpreter"):
        for sid in range(1000, 1200):  # 200 spoofed stream ids
            assert (
                interp.process(
                    build_context_packet(
                        fields=standard_context_fields(), stream_id=sid
                    )
                )
                is None
            )

    assert interp.dropped == 200  # dropped-and-counted, never folded
    assert len(interp._contexts) == 4
    assert len(interp._last_counter) <= 4
    flood_logs = [r for r in caplog.records if "max_streams" in r.getMessage()]
    assert 1 <= len(flood_logs) <= 5  # rate-limited, not one line per packet

    # the tracked streams still flow normally after the flood
    r = interp.process(build_data_packet(payload=FLOAT_PAYLOAD, stream_id=2))
    assert r["type"] == "data"


def test_data_flood_on_unknown_streams_creates_no_state() -> None:
    """DATA packets on never-contexted streams are dropped without creating
    ANY per-stream state — garbage headers wearing random stream_ids cannot
    consume max_streams slots and lock out later legitimate streams."""
    interp = Interpreter(max_streams=4)
    for sid in range(2000, 2200):  # 200 unknown stream ids
        assert (
            interp.process(build_data_packet(payload=FLOAT_PAYLOAD, stream_id=sid))
            is None
        )
    assert interp.dropped == 200
    assert interp._contexts == {}
    assert interp._last_counter == {}

    # a legitimate NEW stream arriving after the flood is admitted normally
    interp.process(build_context_packet(fields=standard_context_fields(), stream_id=1))
    r = interp.process(build_data_packet(payload=FLOAT_PAYLOAD, stream_id=1))
    assert r["type"] == "data"


def test_failed_context_decode_never_consumes_a_slot() -> None:
    """A datagram must actually DECODE as a context before its stream_id can
    claim one of the max_streams slots."""
    interp = Interpreter(max_streams=2)
    interp.process(build_context_packet(fields=standard_context_fields(), stream_id=1))

    for sid in range(3000, 3050):  # 50 truncated contexts on distinct streams
        bad = build_context_packet(fields=standard_context_fields(), stream_id=sid)
        assert interp.process(bad[:30]) is None  # header ok, CIF missing
    assert interp.errors == 50
    assert len(interp._contexts) == 1  # no slot consumed by garbage

    # the second real stream still gets the remaining slot
    interp.process(build_context_packet(fields=standard_context_fields(), stream_id=2))
    assert interp.context_for(2) is not None


def test_flooded_new_context_is_also_bounded() -> None:
    interp = Interpreter(max_streams=2)
    interp.process(build_context_packet(fields=standard_context_fields(), stream_id=1))
    interp.process(build_context_packet(fields=standard_context_fields(), stream_id=2))
    # a context packet for a THIRD stream is dropped once the cap is hit
    assert (
        interp.process(
            build_context_packet(fields=standard_context_fields(), stream_id=3)
        )
        is None
    )
    assert interp.dropped == 1
    assert interp.context_for(3) is None
    assert len(interp._contexts) == 2
