"""Stage 2.5 guard for the Stage 3 buffer flush contract.

``IngestBuffer.flush()`` resets ``_chunks``/``_num_samples``/
``_start_timestamp``/``_first_arrival`` and MUST keep ``_current_context``.
Data is dropped ONLY at cold start (``_current_context is None``, before the
first context packet). No flush trigger — size, real-context-change, age, or
gap — may null the context, so after any flush the very next data packet is
accepted immediately under the retained context and its window is contiguous
in sample time (no "waiting for a context packet" gap).

Self-activates once ``sceptre_pipeline.buffer`` lands (Stage 3); skips cleanly
until then.
"""

from __future__ import annotations

import pytest

buffer = pytest.importorskip("sceptre_pipeline.buffer")

from conftest import (  # noqa: E402
    build_context_packet,
    build_data_packet,
    encode_iq_float32,
    standard_context_fields,
)

from sceptre_pipeline.interpreter import Interpreter  # noqa: E402

TWO_SAMPLES = encode_iq_float32([complex(1.0, -1.0), complex(0.5, 0.25)])


def _primed(emitted: list, max_samples: int) -> tuple:
    """An IngestBuffer + Interpreter with one context packet already pushed."""
    buf = buffer.IngestBuffer(
        emit=emitted.append, max_samples=max_samples, max_age_s=60.0
    )
    interp = Interpreter()
    buf.push(interp.process(build_context_packet(fields=standard_context_fields())))
    assert buf._current_context is not None
    return buf, interp


def test_size_flush_retains_context_and_next_data_is_contiguous() -> None:
    emitted: list = []
    buf, interp = _primed(emitted, max_samples=4)
    ctx = buf._current_context

    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=0, int_ts=100)))
    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=1, int_ts=101)))
    assert len(emitted) == 1  # size trigger fired at exactly max_samples
    assert emitted[0]["num_samples"] == 4
    assert emitted[0]["start_timestamp"] == 100.0
    assert buf._current_context is ctx  # the flush kept the context latched

    # (a) the very next data packet is accepted, not dropped
    rec = interp.process(
        build_data_packet(
            payload=TWO_SAMPLES, counter=2, int_ts=102, frac_ts=250_000_000_000
        )
    )
    t_next = rec["metadata"]["timestamp"]
    assert t_next == 102.25
    buf.push(rec)

    # (b) a subsequent flush emits it under the still-latched context
    buf.flush()
    assert len(emitted) == 2
    window = emitted[1]
    assert window["context"] == emitted[0]["context"] == ctx
    assert window["num_samples"] == 2
    # (c) contiguity: the window starts at that data packet's own timestamp
    assert window["start_timestamp"] == t_next


def test_gap_flush_retains_context() -> None:
    emitted: list = []
    buf, interp = _primed(emitted, max_samples=1_000_000)
    ctx = buf._current_context

    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=0, int_ts=100)))
    # counter 0 -> 5: mod-16 gap flushes the open window before appending
    gap_rec = interp.process(
        build_data_packet(payload=TWO_SAMPLES, counter=5, int_ts=200)
    )
    assert gap_rec["metadata"]["gap_before"] is True
    buf.push(gap_rec)

    assert len(emitted) == 1
    assert emitted[0]["start_timestamp"] == 100.0
    assert buf._current_context is ctx  # gap flush kept the context

    buf.flush()
    assert len(emitted) == 2
    assert emitted[1]["context"] == emitted[0]["context"] == ctx
    assert emitted[1]["start_timestamp"] == 200.0


def test_age_flush_retains_context_and_accepts_next_data() -> None:
    emitted: list = []
    buf, interp = _primed(emitted, max_samples=1_000_000)
    ctx = buf._current_context

    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=0, int_ts=100)))
    # age the open window past max_age_s without new packets, then poll —
    # the age trigger is polled (Stage 3 gotcha 3), so drive it directly
    buf._first_arrival -= 3600.0
    buf.maybe_flush_on_age()

    assert len(emitted) == 1
    assert emitted[0]["start_timestamp"] == 100.0
    assert buf._current_context is ctx  # the age flush kept the context

    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=1, int_ts=200)))
    buf.flush()
    assert len(emitted) == 2
    assert emitted[1]["context"] == ctx
    assert emitted[1]["start_timestamp"] == 200.0


def test_context_change_flush_latches_new_context_and_accepts_next_data() -> None:
    emitted: list = []
    buf, interp = _primed(emitted, max_samples=1_000_000)

    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=0, int_ts=100)))
    # a REAL flush_fields diff (rf change) flushes the open window first,
    # labeled with the OLD context, then adopts the new one
    buf.push(
        interp.process(
            build_context_packet(
                fields=standard_context_fields(rf_hz=103_700_000.0), counter=1
            )
        )
    )
    assert len(emitted) == 1
    assert emitted[0]["context"]["rf_hz"] == 97_300_000.0
    assert buf._current_context is not None
    assert buf._current_context["rf_hz"] == 103_700_000.0

    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=1, int_ts=200)))
    buf.flush()
    assert len(emitted) == 2
    assert emitted[1]["context"]["rf_hz"] == 103_700_000.0
    assert emitted[1]["start_timestamp"] == 200.0


def test_no_flush_call_nulls_the_context() -> None:
    emitted: list = []
    buf, interp = _primed(emitted, max_samples=4)
    ctx = buf._current_context

    buf.flush()  # empty flush: no-op, context untouched
    assert emitted == []
    assert buf._current_context is ctx

    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=0, int_ts=100)))
    buf.flush()  # manual flush of a partial window
    assert len(emitted) == 1
    assert buf._current_context is ctx
    # accumulation state was reset, context was not
    assert buf._num_samples == 0
    assert not buf._chunks


def test_data_dropped_only_at_cold_start() -> None:
    emitted: list = []
    buf = buffer.IngestBuffer(emit=emitted.append, max_samples=4, max_age_s=60.0)

    # a primed interpreter can produce data records, but THIS buffer has no
    # context yet -> cold-start drop is the one legitimate drop
    interp = Interpreter()
    interp.process(build_context_packet(fields=standard_context_fields()))
    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=0, int_ts=100)))
    # dropped means DROPPED at push — not held for later
    assert not buf._chunks
    assert buf._num_samples == 0

    buf.flush()
    assert emitted == []

    # once a context arrives, the first emitted window contains ONLY
    # post-context samples, timed by the post-context packet — the cold-start
    # bytes must not leak in under a context never in effect for them
    buf.push(interp.process(build_context_packet(fields=standard_context_fields(), counter=1)))
    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=1, int_ts=500)))
    buf.flush()
    assert len(emitted) == 1
    assert emitted[0]["num_samples"] == 2
    assert emitted[0]["start_timestamp"] == 500.0
