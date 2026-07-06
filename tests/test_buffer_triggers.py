"""Stage 3: per-buffer flush triggers, single conversion, and recording runs.

The four correctness gotchas (docs/implementation-plan.md Stage 3), each pinned
by a test here:

1. Context-change ordering: flush (labeled OLD context) -> adopt new -> new
   data accrues under the new context.
2. Periodic context is not a change: an identical heartbeat over
   ``flush_fields`` must NOT flush (counter/timestamp always change; only a
   real diff over the flush identity counts).
3. The age trigger is POLLED, not reactive: nothing flushes until
   ``maybe_flush_on_age()`` is called from the dequeue loop.
4. A mod-16 counter gap breaks contiguity: flush BEFORE appending, so every
   emitted array is gapless.

Plus the single-conversion contract (Appendix A endianness): the wire is
big-endian, so ``np.frombuffer(raw, ">f4").astype(np.float32)`` must byteswap
BEFORE ``view(np.complex64)`` pairs I,Q. ``view`` never swaps bytes — reading
big-endian bytes as native float32 and then viewing yields NaN/garbage with no
exception (demonstrated below). Amplitude/precision policy (Stage 2.75 Part
D): integer formats emerge as un-normalized raw counts and float64 is downcast
to float32; the stamped ``component_dtype`` lets downstream normalize/upcast.
"""

from __future__ import annotations

import struct

import numpy as np
import pytest

from conftest import (
    build_context_packet,
    build_data_packet,
    encode_iq_float32,
    encode_payload_format_u64,
    standard_context_fields,
)

from sceptre_pipeline import buffer as buffer_mod
from sceptre_pipeline.buffer import IngestBuffer
from sceptre_pipeline.interpreter import Interpreter

TWO_SAMPLES = encode_iq_float32([complex(1.0, -1.0), complex(0.5, 0.25)])

REAL_INT16 = encode_payload_format_u64(
    real_complex_code=0b00, data_item_format=0b00000, packing_bits=16, item_bits=16
)
COMPLEX_FLOAT64 = encode_payload_format_u64(
    real_complex_code=0b01, data_item_format=0b01111, packing_bits=64, item_bits=64
)


def _primed(emitted: list, **kwargs) -> tuple[IngestBuffer, Interpreter]:
    kwargs.setdefault("max_samples", 1_000_000)
    kwargs.setdefault("max_age_s", 60.0)
    buf = IngestBuffer(emit=emitted.append, **kwargs)
    interp = Interpreter()
    buf.push(interp.process(build_context_packet(fields=standard_context_fields())))
    return buf, interp


# --- gotcha 1: context-change ordering ------------------------------------


def test_context_change_flushes_old_window_then_adopts_new() -> None:
    emitted: list = []
    buf, interp = _primed(emitted)

    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=0, int_ts=100)))
    buf.push(
        interp.process(
            build_context_packet(
                fields=standard_context_fields(rf_hz=103_700_000.0), counter=1
            )
        )
    )

    # the boundary-spanning window is labeled with the OLD context
    assert len(emitted) == 1
    assert emitted[0]["context"]["rf_hz"] == 97_300_000.0
    # the new context is adopted AFTER the flush
    assert buf._current_context["rf_hz"] == 103_700_000.0

    # new data accrues under the new context
    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=1, int_ts=200)))
    buf.flush()
    assert len(emitted) == 2
    assert emitted[1]["context"]["rf_hz"] == 103_700_000.0


def test_sample_rate_only_change_flushes_window_labeled_old_rate() -> None:
    """sample_rate_hz is part of the flush identity: a rate-only change (rf,
    dtype, is_complex all unchanged) must flush, or the open window's samples
    would be emitted under a rate never in effect for them."""
    emitted: list = []
    buf, interp = _primed(emitted)

    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=0, int_ts=100)))
    buf.push(
        interp.process(
            build_context_packet(
                fields=standard_context_fields(sample_rate_hz=1_250_000.0), counter=1
            )
        )
    )

    assert len(emitted) == 1
    assert emitted[0]["context"]["sample_rate_hz"] == 625_000.0  # OLD rate
    assert buf._current_context["sample_rate_hz"] == 1_250_000.0

    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=1, int_ts=200)))
    buf.flush()
    assert emitted[1]["context"]["sample_rate_hz"] == 1_250_000.0


# --- gotcha 2: periodic heartbeat is not a change --------------------------


def test_identical_heartbeat_context_never_flushes() -> None:
    emitted: list = []
    buf, interp = _primed(emitted)

    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=0, int_ts=100)))
    # heartbeats differ in counter AND timestamp — never in flush_fields
    for n in range(1, 7):
        buf.push(
            interp.process(
                build_context_packet(
                    fields=standard_context_fields(), counter=n, int_ts=100 + n
                )
            )
        )
    assert emitted == []  # zero context-triggered flushes
    buf.flush()
    assert len(emitted) == 1
    assert emitted[0]["num_samples"] == 2  # the window stayed open throughout


def test_change_confined_to_non_flush_field_does_not_flush() -> None:
    """The diff runs over flush_fields ONLY: a context differing solely in a
    non-identity field (bandwidth) is adopted without flushing — a whole-dict
    comparison would wrongly fragment windows on live AGC/bandwidth wiggle."""
    emitted: list = []
    buf, interp = _primed(emitted)

    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=0, int_ts=100)))
    buf.push(
        interp.process(
            build_context_packet(
                fields=standard_context_fields(bandwidth_hz=750_000.0), counter=1
            )
        )
    )
    assert emitted == []  # window stays open across the non-identity change
    assert buf._current_context["bandwidth_hz"] == 750_000.0  # still adopted

    buf.flush()
    assert emitted[0]["num_samples"] == 2


# --- gotcha 3: age trigger is polled, not reactive --------------------------


class _FakeClock:
    """Stand-in for the ``time`` module: only ``monotonic`` is consumed."""

    def __init__(self, start: float) -> None:
        self.now = start

    def monotonic(self) -> float:
        return self.now


def test_age_trigger_is_polled_not_reactive(monkeypatch) -> None:
    clock = _FakeClock(1000.0)
    monkeypatch.setattr(buffer_mod, "time", clock)

    emitted: list = []
    buf, interp = _primed(emitted, max_age_s=5.0)
    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=0, int_ts=100)))

    # a young window does not flush when polled
    clock.now = 1004.9
    buf.maybe_flush_on_age()
    assert emitted == []

    # a late append does NOT restart the age clock (window age counts from
    # the FIRST chunk's arrival, not the last — no idle-timeout semantics)
    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=1, int_ts=101)))
    clock.now = 1005.5  # window is 5.5 s old; last append only 0.6 s ago

    # pushing onto an already-aged window still does not flush: the age
    # trigger fires ONLY from the poll, never reactively inside push()
    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=2, int_ts=102)))
    assert emitted == []

    buf.maybe_flush_on_age()
    assert len(emitted) == 1
    assert emitted[0]["num_samples"] == 6  # all three chunks, one window
    assert emitted[0]["start_timestamp"] == 100.0

    # the empty buffer never age-flushes again
    clock.now = 2000.0
    buf.maybe_flush_on_age()
    assert len(emitted) == 1


# --- gotcha 4: counter gap breaks contiguity --------------------------------


def test_counter_gap_flushes_before_append_windows_stay_gapless() -> None:
    emitted: list = []
    buf, interp = _primed(emitted)

    first = encode_iq_float32([complex(1.0, 2.0)])
    after_gap = encode_iq_float32([complex(3.0, 4.0)])
    buf.push(interp.process(build_data_packet(payload=first, counter=0, int_ts=100)))
    rec = interp.process(build_data_packet(payload=after_gap, counter=7, int_ts=200))
    assert rec["metadata"]["gap_before"] is True
    buf.push(rec)

    # pre-gap window flushed BEFORE the post-gap chunk was appended
    assert len(emitted) == 1
    np.testing.assert_array_equal(
        emitted[0]["samples"], np.array([1.0 + 2.0j], dtype=np.complex64)
    )
    buf.flush()
    # the post-gap window holds ONLY post-gap samples: no mixing across the gap
    np.testing.assert_array_equal(
        emitted[1]["samples"], np.array([3.0 + 4.0j], dtype=np.complex64)
    )
    assert emitted[1]["start_timestamp"] == 200.0


# --- size trigger ------------------------------------------------------------


def test_size_trigger_flushes_at_exactly_max_samples() -> None:
    emitted: list = []
    buf, interp = _primed(emitted, max_samples=4)

    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=0, int_ts=100)))
    assert emitted == []  # 2 < 4: stays open
    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=1, int_ts=101)))
    assert len(emitted) == 1  # fired at exactly max_samples
    assert emitted[0]["num_samples"] == 4
    # accumulation reset: the next packet opens a fresh window
    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=2, int_ts=102)))
    assert len(emitted) == 1
    assert buf._num_samples == 2


# --- single conversion: astype-before-view ----------------------------------

# A big-endian float32 whose byte-reversal has an all-ones exponent: read the
# same bytes as NATIVE float32 (i.e. skip the byteswap) and you get NaN.
NAN_TRAP_BYTES = b"\x3f\xc0\xc0\x7f"
NAN_TRAP_VALUE = struct.unpack(">f", NAN_TRAP_BYTES)[0]  # ~1.506, |v| < 16


def test_view_before_astype_produces_nan_astype_first_does_not() -> None:
    """Documents the Appendix A endianness trap that flush() must avoid."""
    raw = encode_iq_float32([complex(NAN_TRAP_VALUE, NAN_TRAP_VALUE)])

    # WRONG order: interpret big-endian bytes as native float32, then view —
    # .view() never swaps bytes, so the pairs decode as NaN with no exception
    bad = np.frombuffer(raw, dtype=np.float32).view(np.complex64)
    assert np.isnan(bad).any()

    # RIGHT order: byteswap via astype FIRST, then pair I,Q via view
    good = np.frombuffer(raw, dtype=">f4").astype(np.float32).view(np.complex64)
    assert not np.isnan(good).any()
    assert np.all(np.abs(good) < 16)


def test_emitted_array_is_native_complex64_with_exact_values() -> None:
    emitted: list = []
    buf, interp = _primed(emitted)
    buf.push(
        interp.process(
            build_data_packet(
                payload=encode_iq_float32([complex(NAN_TRAP_VALUE, NAN_TRAP_VALUE)]),
                counter=0,
                int_ts=100,
            )
        )
    )
    buf.flush()

    arr = emitted[0]["samples"]
    assert arr.dtype == np.complex64
    assert arr.dtype.isnative
    assert not np.isnan(arr).any()
    expected = np.complex64(complex(NAN_TRAP_VALUE, NAN_TRAP_VALUE))
    np.testing.assert_array_equal(arr, np.array([expected], dtype=np.complex64))


def test_real_integer_format_emerges_unnormalized_float32() -> None:
    emitted: list = []
    buf = IngestBuffer(emit=emitted.append, max_samples=1_000_000, max_age_s=60.0)
    interp = Interpreter()
    buf.push(
        interp.process(
            build_context_packet(
                fields=standard_context_fields(payload_format_u64=REAL_INT16)
            )
        )
    )
    buf.push(
        interp.process(
            build_data_packet(
                payload=struct.pack(">4h", 1000, -2000, 3000, -4000),
                counter=0,
                int_ts=100,
            )
        )
    )
    buf.flush()

    arr = emitted[0]["samples"]
    assert arr.dtype == np.float32  # real format: no complex view
    # raw counts, NOT normalized by full scale (downstream's job, via metadata)
    np.testing.assert_array_equal(
        arr, np.array([1000.0, -2000.0, 3000.0, -4000.0], dtype=np.float32)
    )
    assert emitted[0]["num_samples"] == 4
    assert emitted[0]["metadata"]["component_dtype"] == ">i2"
    assert emitted[0]["metadata"]["is_complex"] is False
    assert emitted[0]["metadata"]["bytes_per_sample"] == 2


def test_complex_float64_is_downcast_to_complex64() -> None:
    emitted: list = []
    buf = IngestBuffer(emit=emitted.append, max_samples=1_000_000, max_age_s=60.0)
    interp = Interpreter()
    buf.push(
        interp.process(
            build_context_packet(
                fields=standard_context_fields(payload_format_u64=COMPLEX_FLOAT64)
            )
        )
    )
    buf.push(
        interp.process(
            build_data_packet(
                payload=struct.pack(">dd", 0.123456789, -0.987654321),
                counter=0,
                int_ts=100,
            )
        )
    )
    buf.flush()

    arr = emitted[0]["samples"]
    assert arr.dtype == np.complex64  # lossy downcast is the documented policy
    np.testing.assert_allclose(
        arr, np.array([0.123456789 - 0.987654321j]), rtol=1e-6
    )
    assert emitted[0]["num_samples"] == 1
    assert emitted[0]["metadata"]["component_dtype"] == ">f8"
    assert emitted[0]["metadata"]["bytes_per_sample"] == 16


# --- defensive inputs --------------------------------------------------------


def test_push_none_is_a_noop() -> None:
    emitted: list = []
    buf, _ = _primed(emitted)
    buf.push(None)  # Interpreter.process returns None on drops
    buf.flush()
    assert emitted == []


def test_rogue_data_without_dtype_is_dropped_even_under_supported_context() -> None:
    """Defense in depth: a hand-fed record with no usable interpretation must
    never be latched — flush() may never see np.dtype(None)."""
    emitted: list = []
    buf, interp = _primed(emitted)
    rogue = {
        "type": "data",
        "metadata": {
            "stream_id": 123,
            "counter": 0,
            "timestamp": 100.0,
            "packet_size": 10,
            "dtype": None,
            "bytes_per_sample": None,
            "is_complex": True,
            "gap_before": False,
        },
        "context": None,
        "data": b"\x00" * 8,
    }
    buf.push(rogue)
    assert not buf._chunks
    buf.flush()
    assert emitted == []

    # real data still flows afterwards
    buf.push(interp.process(build_data_packet(payload=TWO_SAMPLES, counter=0, int_ts=100)))
    buf.flush()
    assert len(emitted) == 1
    assert emitted[0]["num_samples"] == 2


def test_latchable_but_unconvertible_dtype_is_dropped_not_deferred() -> None:
    """A hand-fed dtype that np.dtype() accepts with a matching itemsize but
    that astype(float32) cannot convert (bytes/str/void) must be rejected at
    push — otherwise the ValueError detonates later, inside flush()/age-poll
    on Thread B, and the poisoned chunks would wedge the buffer forever."""
    emitted: list = []
    buf, _ = _primed(emitted)
    rogue = {
        "type": "data",
        "metadata": {
            "stream_id": 123,
            "counter": 0,
            "timestamp": 100.0,
            "packet_size": 10,
            "dtype": ">S8",  # itemsize 8: passes a size-only consistency check
            "bytes_per_sample": 8,
            "is_complex": False,
            "gap_before": False,
        },
        "context": None,
        "data": b"\x00" * 8,
    }
    buf.push(rogue)
    assert not buf._chunks
    assert buf.dropped == 1
    buf.flush()  # must not raise and must not emit
    assert emitted == []


# --- real recordings ---------------------------------------------------------


def _run_capture_through_buffer(capture: dict, emitted: list) -> IngestBuffer:
    buf = IngestBuffer(
        emit=emitted.append,
        max_samples=10**9,
        max_age_s=10**9,
        stream_id=123,
    )
    interp = Interpreter()
    for _ts_ns, _ip, _port, payload in capture["packets"]:
        record = interp.process(payload)
        if record is not None:
            buf.push(record)
    assert interp.errors == 0 and interp.dropped == 0
    return buf


def test_single_frequency_recording_zero_context_flushes(
    single_frequency_path, load_capture
) -> None:
    emitted: list = []
    buf = _run_capture_through_buffer(load_capture(single_frequency_path), emitted)

    # 6 identical heartbeat contexts -> ZERO context-triggered flushes: with
    # size/age limits out of reach, the whole capture is one open window
    assert emitted == []

    buf.flush()
    assert len(emitted) == 1
    unit = emitted[0]
    assert unit["num_samples"] == 3_127_320  # 3066 data packets x 1020 samples
    assert unit["context"]["rf_hz"] == 97_300_000.0
    assert unit["context"]["sample_rate_hz"] == 625_000.0
    assert unit["stream_id"] == 123

    arr = unit["samples"]
    assert arr.shape == (3_127_320,)
    assert arr.dtype == np.complex64
    assert arr.dtype.isnative
    # astype-before-view proof on real data: finite, plausible amplitudes
    assert not np.isnan(arr).any()
    assert float(np.abs(arr).max()) < 16.0


def test_change_frequency_recording_boundary_window_labeled_old_rf(
    change_frequency_path, load_capture
) -> None:
    emitted: list = []
    buf = _run_capture_through_buffer(load_capture(change_frequency_path), emitted)

    # exactly ONE real context change (97.3 -> 103.7 MHz): the boundary-
    # spanning window is flushed at the change, labeled with the OLD context
    assert len(emitted) == 1
    assert emitted[0]["context"]["rf_hz"] == 97_300_000.0

    buf.flush()  # the final window carries the NEW context
    assert len(emitted) == 2
    assert emitted[1]["context"]["rf_hz"] == 103_700_000.0

    assert emitted[0]["num_samples"] > 0 and emitted[1]["num_samples"] > 0
    assert emitted[0]["num_samples"] + emitted[1]["num_samples"] == 3_126_300
    for unit in emitted:
        assert not np.isnan(unit["samples"]).any()
