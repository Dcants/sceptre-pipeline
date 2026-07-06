"""Stage 3: BufferRouter — one IngestBuffer per stream_id, bounded.

The Sceptre format multiplexes streams on one socket (Stage 2.75 made the
interpreter's context per-stream); the router keeps ACCUMULATION per-stream
too, so interleaved records on two stream_ids never share a window. New
streams beyond ``max_streams`` are dropped-and-counted (spoofed-stream flood
protection, mirroring the interpreter's bound).
"""

from __future__ import annotations

import numpy as np

from conftest import (
    build_context_packet,
    build_data_packet,
    encode_iq_float32,
    standard_context_fields,
)

from sceptre_pipeline.buffer import BufferRouter
from sceptre_pipeline.interpreter import Interpreter

STREAM_A, STREAM_B, STREAM_C = 0xA1, 0xB2, 0xC3

A_SAMPLES = encode_iq_float32([complex(1.0, 1.0)])
B_SAMPLES = encode_iq_float32([complex(2.0, -2.0)])


def _ctx(interp: Interpreter, stream_id: int, counter: int = 0, **fields) -> dict:
    return interp.process(
        build_context_packet(
            fields=standard_context_fields(**fields),
            stream_id=stream_id,
            counter=counter,
        )
    )


def _data(
    interp: Interpreter, stream_id: int, payload: bytes, counter: int, int_ts: int
) -> dict:
    return interp.process(
        build_data_packet(
            payload=payload, stream_id=stream_id, counter=counter, int_ts=int_ts
        )
    )


def _router(emitted: list, **kwargs) -> BufferRouter:
    kwargs.setdefault("max_samples", 1_000_000)
    kwargs.setdefault("max_age_s", 60.0)
    return BufferRouter(emit=emitted.append, **kwargs)


def test_interleaved_streams_never_mix() -> None:
    emitted: list = []
    router = _router(emitted)
    interp = Interpreter()

    router.push(_ctx(interp, STREAM_A, rf_hz=97_300_000.0))
    router.push(_ctx(interp, STREAM_B, rf_hz=103_700_000.0))
    # interleave data with independent per-stream counters
    router.push(_data(interp, STREAM_A, A_SAMPLES, counter=0, int_ts=100))
    router.push(_data(interp, STREAM_B, B_SAMPLES, counter=0, int_ts=105))
    router.push(_data(interp, STREAM_A, A_SAMPLES, counter=1, int_ts=110))
    router.push(_data(interp, STREAM_B, B_SAMPLES, counter=1, int_ts=115))
    assert emitted == []

    router.flush_all()
    assert len(emitted) == 2
    by_stream = {unit["stream_id"]: unit for unit in emitted}
    assert set(by_stream) == {STREAM_A, STREAM_B}

    a, b = by_stream[STREAM_A], by_stream[STREAM_B]
    # each stream's window holds ONLY its own samples, under its own context
    np.testing.assert_array_equal(
        a["samples"], np.array([1 + 1j, 1 + 1j], dtype=np.complex64)
    )
    np.testing.assert_array_equal(
        b["samples"], np.array([2 - 2j, 2 - 2j], dtype=np.complex64)
    )
    assert a["context"]["rf_hz"] == 97_300_000.0
    assert b["context"]["rf_hz"] == 103_700_000.0
    assert a["start_timestamp"] == 100.0
    assert b["start_timestamp"] == 105.0


def test_every_emitted_unit_carries_stream_id_and_format_metadata() -> None:
    emitted: list = []
    router = _router(emitted)
    interp = Interpreter()

    router.push(_ctx(interp, STREAM_A))
    router.push(_data(interp, STREAM_A, A_SAMPLES, counter=0, int_ts=100))
    router.flush_all()

    unit = emitted[0]
    assert unit["stream_id"] == STREAM_A
    assert unit["component_dtype"] == ">f4"
    assert unit["is_complex"] is True
    assert unit["bytes_per_sample"] == 8
    md = unit["metadata"]
    assert md["component_dtype"] == ">f4"
    assert md["is_complex"] is True
    assert md["bytes_per_sample"] == 8


def test_new_streams_past_max_streams_are_dropped_and_counted() -> None:
    emitted: list = []
    router = _router(emitted, max_streams=2)
    interp = Interpreter()

    router.push(_ctx(interp, STREAM_A))
    router.push(_ctx(interp, STREAM_B))
    assert router.dropped == 0

    # a third stream cannot claim a buffer: records dropped-and-counted
    router.push(_ctx(interp, STREAM_C))
    router.push(_data(interp, STREAM_C, A_SAMPLES, counter=0, int_ts=100))
    assert router.dropped == 2
    assert len(router._buffers) == 2

    # tracked streams are unaffected
    router.push(_data(interp, STREAM_A, A_SAMPLES, counter=0, int_ts=100))
    router.flush_all()
    assert len(emitted) == 1
    assert emitted[0]["stream_id"] == STREAM_A


def test_maybe_flush_on_age_polls_every_buffer() -> None:
    emitted: list = []
    router = _router(emitted, max_age_s=5.0)
    interp = Interpreter()

    router.push(_ctx(interp, STREAM_A))
    router.push(_ctx(interp, STREAM_B))
    router.push(_data(interp, STREAM_A, A_SAMPLES, counter=0, int_ts=100))
    router.push(_data(interp, STREAM_B, B_SAMPLES, counter=0, int_ts=105))

    router.maybe_flush_on_age()
    assert emitted == []  # both windows younger than max_age_s

    for buf in router._buffers.values():  # age BOTH open windows
        buf._first_arrival -= 3600.0
    router.maybe_flush_on_age()
    assert {unit["stream_id"] for unit in emitted} == {STREAM_A, STREAM_B}


def test_flush_all_drains_every_buffer_once() -> None:
    emitted: list = []
    router = _router(emitted)
    interp = Interpreter()

    router.push(_ctx(interp, STREAM_A))
    router.push(_ctx(interp, STREAM_B))
    router.push(_data(interp, STREAM_A, A_SAMPLES, counter=0, int_ts=100))
    router.push(_data(interp, STREAM_B, B_SAMPLES, counter=0, int_ts=105))

    router.flush_all()
    assert len(emitted) == 2
    router.flush_all()  # drained buffers have nothing more to emit
    assert len(emitted) == 2


def test_context_change_on_one_stream_does_not_flush_the_other() -> None:
    emitted: list = []
    router = _router(emitted)
    interp = Interpreter()

    router.push(_ctx(interp, STREAM_A, rf_hz=97_300_000.0))
    router.push(_ctx(interp, STREAM_B, rf_hz=97_300_000.0))
    router.push(_data(interp, STREAM_A, A_SAMPLES, counter=0, int_ts=100))
    router.push(_data(interp, STREAM_B, B_SAMPLES, counter=0, int_ts=105))

    # rf change on stream A only
    router.push(_ctx(interp, STREAM_A, counter=1, rf_hz=103_700_000.0))
    assert len(emitted) == 1
    assert emitted[0]["stream_id"] == STREAM_A
    assert emitted[0]["context"]["rf_hz"] == 97_300_000.0

    # stream B's window is still open, still under its original context
    router.flush_all()
    by_stream = {unit["stream_id"]: unit for unit in emitted[1:]}
    assert by_stream[STREAM_B]["context"]["rf_hz"] == 97_300_000.0


def test_size_trigger_fires_independently_per_stream() -> None:
    emitted: list = []
    router = _router(emitted, max_samples=2)
    interp = Interpreter()

    router.push(_ctx(interp, STREAM_A))
    router.push(_ctx(interp, STREAM_B))
    router.push(_data(interp, STREAM_A, A_SAMPLES, counter=0, int_ts=100))
    router.push(_data(interp, STREAM_B, B_SAMPLES, counter=0, int_ts=105))
    assert emitted == []  # one sample each: neither at max_samples yet

    router.push(_data(interp, STREAM_A, A_SAMPLES, counter=1, int_ts=110))
    assert len(emitted) == 1  # only stream A reached max_samples
    assert emitted[0]["stream_id"] == STREAM_A
    assert emitted[0]["num_samples"] == 2


def test_data_for_unseen_stream_creates_buffer_but_cold_start_drops() -> None:
    """The router creates buffers lazily on ANY record type; a data-first
    stream hits the buffer's own cold-start drop (no context adopted yet)."""
    emitted: list = []
    router = _router(emitted)
    # hand-fed: the interpreter would drop data-before-context itself
    rogue = {
        "type": "data",
        "metadata": {
            "stream_id": STREAM_A,
            "counter": 0,
            "timestamp": 100.0,
            "packet_size": 10,
            "dtype": ">f4",
            "bytes_per_sample": 8,
            "is_complex": True,
            "gap_before": False,
        },
        "context": None,
        "data": A_SAMPLES,
    }
    router.push(rogue)
    assert STREAM_A in router._buffers
    router.flush_all()
    assert emitted == []


def test_push_none_is_a_noop() -> None:
    emitted: list = []
    router = _router(emitted)
    router.push(None)  # Interpreter.process returns None on drops
    router.flush_all()
    assert emitted == []
    assert router.dropped == 0
