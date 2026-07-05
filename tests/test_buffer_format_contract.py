"""Stage 2.75 Part D guard for the Stage 3 buffer format contract.

Dynamic payload formats open two silent-corruption paths in the buffer unless
Stage 3 amends its contract in lockstep:

1. The flush identity is ``(rf_hz, sample_rate_hz, component_dtype,
   is_complex)`` — NOT just frequency/rate, and not ``data_item_format``
   alone: real-int16 and complex-int16 share ``component_dtype`` (">i2") and
   ``data_item_format``, so ``is_complex`` is required or the buffer would
   concatenate bytes with two different sample interpretations into one array.
2. An unsupported context (``supported is False`` / ``component_dtype is
   None``) is adopted for labeling but leaves the buffer INERT: it accepts no
   data and never calls ``np.dtype(None)`` (which silently means float64) —
   until a supported context arrives.

Every emitted unit's metadata carries ``component_dtype``/``is_complex``/
``bytes_per_sample`` so downstream can normalize/upcast (integer formats
emerge un-normalized; float64 is downcast — documented policy).

Self-activates once ``sceptre_pipeline.buffer`` lands (Stage 3); skips
cleanly until then.
"""

from __future__ import annotations

import pytest

buffer = pytest.importorskip("sceptre_pipeline.buffer")

from conftest import (  # noqa: E402
    build_context_packet,
    build_data_packet,
    encode_iq_float32,
    encode_payload_format_u64,
    standard_context_fields,
)

from sceptre_pipeline.interpreter import Interpreter  # noqa: E402

REAL_INT16 = encode_payload_format_u64(
    real_complex_code=0b00, data_item_format=0b00000, packing_bits=16, item_bits=16
)
COMPLEX_INT16 = encode_payload_format_u64(
    real_complex_code=0b01, data_item_format=0b00000, packing_bits=16, item_bits=16
)
UNSUPPORTED = encode_payload_format_u64(real_complex_code=0b10)  # complex polar

INT16_PAYLOAD = bytes(range(8))  # 4 real-int16 samples / 2 complex-int16 samples


def _make(emitted: list):
    buf = buffer.IngestBuffer(
        emit=emitted.append, max_samples=1_000_000, max_age_s=60.0
    )
    return buf, Interpreter()


def _push_context(buf, interp, payload_format_u64: int, counter: int = 0) -> None:
    buf.push(
        interp.process(
            build_context_packet(
                fields=standard_context_fields(payload_format_u64=payload_format_u64),
                counter=counter,
            )
        )
    )


def test_is_complex_change_with_identical_item_format_flushes() -> None:
    """real-int16 -> complex-int16: same data_item_format, same
    component_dtype — the flush identity MUST still see the change."""
    emitted: list = []
    buf, interp = _make(emitted)

    _push_context(buf, interp, REAL_INT16, counter=0)
    buf.push(
        interp.process(build_data_packet(payload=INT16_PAYLOAD, counter=1, int_ts=100))
    )
    assert emitted == []  # window open under real-int16

    _push_context(buf, interp, COMPLEX_INT16, counter=2)
    assert len(emitted) == 1  # the interpretation changed -> flush, no mixing
    unit = emitted[0]
    assert unit["context"]["is_complex"] is False
    assert unit["metadata"]["is_complex"] is False
    assert unit["metadata"]["component_dtype"] == ">i2"
    assert unit["metadata"]["bytes_per_sample"] == 2

    # the new window accumulates under the complex-int16 interpretation
    buf.push(
        interp.process(build_data_packet(payload=INT16_PAYLOAD, counter=3, int_ts=200))
    )
    buf.flush()
    assert len(emitted) == 2
    assert emitted[1]["context"]["is_complex"] is True
    assert emitted[1]["metadata"]["bytes_per_sample"] == 4


def test_identical_heartbeat_context_does_not_flush() -> None:
    emitted: list = []
    buf, interp = _make(emitted)
    _push_context(buf, interp, REAL_INT16, counter=0)
    buf.push(
        interp.process(build_data_packet(payload=INT16_PAYLOAD, counter=1, int_ts=100))
    )
    _push_context(buf, interp, REAL_INT16, counter=2)  # heartbeat, no change
    assert emitted == []  # identical interpretation -> window stays open


def test_unsupported_context_leaves_buffer_inert_until_supported() -> None:
    emitted: list = []
    buf, interp = _make(emitted)

    _push_context(buf, interp, REAL_INT16, counter=0)
    buf.push(
        interp.process(build_data_packet(payload=INT16_PAYLOAD, counter=1, int_ts=100))
    )

    # the unsupported context flushes the open window (interpretation change)
    # and is adopted for labeling only
    _push_context(buf, interp, UNSUPPORTED, counter=2)
    assert len(emitted) == 1
    assert emitted[0]["context"]["component_dtype"] == ">i2"

    # inert: data pushed under an unsupported context is NOT accumulated.
    # The interpreter already drops these; the buffer must be safe in depth
    # against a hand-fed record too.
    rogue = {
        "type": "data",
        "metadata": {
            "stream_id": 123,
            "counter": 3,
            "timestamp": 300.0,
            "packet_size": 10,
            "dtype": None,
            "bytes_per_sample": None,
            "is_complex": True,
            "gap_before": False,
        },
        "context": None,
        "data": INT16_PAYLOAD,
    }
    buf.push(rogue)
    buf.flush()  # must not emit and must not crash on np.dtype(None)
    assert len(emitted) == 1

    # a supported context re-enables flow — and the re-enabled window holds
    # ONLY the post-re-enable samples: had the buffer silently accumulated
    # the rogue bytes instead of staying inert, they would surface here
    _push_context(buf, interp, REAL_INT16, counter=4)
    buf.push(
        interp.process(build_data_packet(payload=INT16_PAYLOAD, counter=5, int_ts=400))
    )
    buf.flush()
    assert len(emitted) == 2
    assert emitted[1]["metadata"]["component_dtype"] == ">i2"
    assert emitted[1]["num_samples"] == 4  # 8 bytes / 2 B — no rogue leak
    assert emitted[1]["start_timestamp"] == 400.0


def test_every_emitted_unit_carries_format_metadata() -> None:
    emitted: list = []
    buf, interp = _make(emitted)
    _push_context(buf, interp, encode_payload_format_u64(), counter=0)  # cf32
    buf.push(
        interp.process(
            build_data_packet(
                payload=encode_iq_float32([complex(1, -1)]), counter=1, int_ts=100
            )
        )
    )
    buf.flush()
    assert len(emitted) == 1
    md = emitted[0]["metadata"]
    assert md["component_dtype"] == ">f4"
    assert md["is_complex"] is True
    assert md["bytes_per_sample"] == 8
