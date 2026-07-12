"""Tests for ``ThroughputMeter`` — the output-side pipeline data-rate meter.

The meter wraps the emit callback: on every emitted ``{num_samples,
bytes_per_sample, ...}`` unit it tallies samples/bytes, delegates to the real
consumer, and reports one throughput line at shutdown. A fake clock is injected
so ``elapsed`` — and therefore the reported rate — is deterministic. Capture
efficiency runs on the units' VITA sample timestamps, not the clock.
"""

from __future__ import annotations

from sceptre_pipeline.metrics import ThroughputMeter


def _unit(num_samples, bytes_per_sample=8, **extra):
    """A minimal emitted-unit stand-in (only the fields the meter reads)."""
    unit = {"num_samples": num_samples, "bytes_per_sample": bytes_per_sample}
    unit.update(extra)
    return unit


class _FakeClock:
    """Returns preset values in order; the last value repeats forever.

    ``start()`` reads the clock once and ``summary()`` reads it once, so a
    two-value clock pins ``elapsed`` exactly.
    """

    def __init__(self, *values):
        self._values = list(values)

    def __call__(self):
        if len(self._values) > 1:
            return self._values.pop(0)
        return self._values[0]


def test_tallies_samples_and_bytes():
    meter = ThroughputMeter(lambda unit: None)
    meter(_unit(1000, 8))
    meter(_unit(500, 8))
    assert meter.total_samples == 1500
    assert meter.total_bytes == 1500 * 8
    assert meter.unit_count == 2


def test_delegates_every_unit_to_wrapped_emit():
    received = []
    meter = ThroughputMeter(received.append)
    first, second = _unit(10), _unit(20)
    meter(first)
    meter(second)
    assert received == [first, second]


def test_summary_reports_expected_rates():
    meter = ThroughputMeter(lambda unit: None, clock=_FakeClock(0.0, 6.25))
    meter.start()
    meter(_unit(12_500_000, 8))
    line = meter.summary()
    assert "12,500,000 samples" in line
    assert "6.25 s" in line
    assert "2.00 MS/s" in line
    assert "16.0 MB/s" in line


def test_summary_zero_samples():
    meter = ThroughputMeter(lambda unit: None)
    assert meter.summary() == "pipeline throughput: no samples emitted"


def test_summary_without_start_reports_no_rate():
    meter = ThroughputMeter(lambda unit: None)
    meter(_unit(100, 8))
    line = meter.summary()
    assert "100 samples" in line
    assert "too short" in line
    assert "MS/s" not in line


def test_summary_zero_elapsed_reports_no_rate():
    meter = ThroughputMeter(lambda unit: None, clock=_FakeClock(5.0, 5.0))
    meter.start()
    meter(_unit(100, 8))
    assert "too short" in meter.summary()


def test_missing_bytes_per_sample_is_counted_as_zero():
    meter = ThroughputMeter(lambda unit: None, clock=_FakeClock(0.0, 1.0))
    meter.start()
    meter({"num_samples": 1000})  # no bytes_per_sample key
    meter({"num_samples": 500, "bytes_per_sample": None})
    assert meter.total_samples == 1500
    assert meter.total_bytes == 0
    line = meter.summary()
    assert "1,500 samples" in line
    assert "0.0 MB/s" in line


# --- capture efficiency (sample-time, per-stream; silent-loss detector) ---


def _ctx_unit(num_samples, stream_id, ts=None, sample_rate_hz=None, bandwidth_hz=None):
    """An emitted unit for the capture meter: samples + optional context + SDR ts."""
    context = {}
    if sample_rate_hz is not None:
        context["sample_rate_hz"] = sample_rate_hz
    if bandwidth_hz is not None:
        context["bandwidth_hz"] = bandwidth_hz
    unit = _unit(num_samples, 8, stream_id=stream_id, context=context)
    if ts is not None:
        unit["start_timestamp"] = ts
    return unit


def test_capture_single_steady_stream_full_and_bandwidth():
    # three contiguous 1 s windows at 10 MS/s -> expected == delivered -> 100%.
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    for ts in (0.0, 1.0, 2.0):
        meter(_ctx_unit(10_000_000, 5, ts=ts, sample_rate_hz=10_000_000.0, bandwidth_hz=5_000_000.0))
    line = meter.capture_summary()
    assert "delivered 10.00 MS/s of declared 10.00 MS/s = 100.0%" in line
    assert "bandwidth 5.0 MHz" in line
    assert "OK" in line


def test_capture_detects_severe_loss():
    # two 1M-sample windows @20MHz whose sample time spans [0, 2.0]s -> expected
    # 40M, delivered 2M -> 5%. The 1.95 s gap between them is lost samples.
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(1_000_000, 5, ts=0.0, sample_rate_hz=20_000_000.0))
    meter(_ctx_unit(1_000_000, 5, ts=1.95, sample_rate_hz=20_000_000.0))  # ends at 2.0
    line = meter.capture_summary()
    assert "5.0%" in line
    assert "SEVERE UPSTREAM LOSS" in line


def test_capture_verdict_rounded_at_boundary():
    # true 89.96% must round to 90.0% AND read OK (verdict from the rounded value).
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(4_996_000, 5, ts=0.0, sample_rate_hz=10_000_000.0))  # ends 0.4996
    meter(_ctx_unit(4_000_000, 5, ts=0.6, sample_rate_hz=10_000_000.0))  # ends 1.0; gap loses 1.004M
    line = meter.capture_summary()
    assert "90.0%" in line
    assert "OK" in line
    assert "PARTIAL" not in line


def test_capture_retune_not_false_loss():
    # 2 MS/s for 9 s then 20 MS/s for 1 s, all delivered -> 100%, flagged.
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(18_000_000, 5, ts=0.0, sample_rate_hz=2_000_000.0))   # ends 9.0
    meter(_ctx_unit(20_000_000, 5, ts=9.0, sample_rate_hz=20_000_000.0))  # ends 10.0
    line = meter.capture_summary()
    assert "100.0%" in line
    assert "rate changed mid-run" in line
    assert "OK" in line


def test_capture_worst_stream_not_masked_by_healthy_one():
    # stream 5 healthy (100%), stream 6 loses half. The verdict must reflect the
    # WORST stream, not a pooled average that would hide stream 6's loss.
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(10_000_000, 5, ts=0.0, sample_rate_hz=10_000_000.0))  # A ends 1.0
    meter(_ctx_unit(10_000_000, 5, ts=1.0, sample_rate_hz=10_000_000.0))  # A ends 2.0 -> 100%
    meter(_ctx_unit(1_000_000, 6, ts=0.0, sample_rate_hz=2_000_000.0))    # B ends 0.5
    meter(_ctx_unit(1_000_000, 6, ts=1.5, sample_rate_hz=2_000_000.0))    # B ends 2.0 -> 50%
    line = meter.capture_summary()
    assert "50.0%" in line
    assert "2 declared streams" in line
    assert "PARTIAL LOSS" in line


def test_capture_undeclared_excluded_and_flagged():
    # declared stream 5 lossy (50%); undeclared stream 6 heavy -> must not mask.
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(1_000_000, 5, ts=0.0, sample_rate_hz=2_000_000.0))  # ends 0.5
    meter(_ctx_unit(1_000_000, 5, ts=1.5, sample_rate_hz=2_000_000.0))  # ends 2.0 -> 50%
    meter(_ctx_unit(50_000_000, 6, ts=0.0))                             # undeclared
    line = meter.capture_summary()
    assert "50.0%" in line
    assert "PARTIAL LOSS" in line
    assert "undeclared" in line
    assert "bandwidth" not in line


def test_capture_ignores_wall_clock_burst_drain():
    # efficiency is sample-time based, so a frozen/garbage wall-clock (as during
    # a buffered burst that drains in microseconds) must not affect the result.
    meter = ThroughputMeter(lambda u: None, clock=lambda: 999.0)
    meter.start()
    meter(_ctx_unit(10_000_000, 5, ts=0.0, sample_rate_hz=10_000_000.0, bandwidth_hz=5_000_000.0))
    meter(_ctx_unit(10_000_000, 5, ts=1.0, sample_rate_hz=10_000_000.0, bandwidth_hz=5_000_000.0))
    assert "100.0%" in meter.capture_summary()


def test_capture_robust_to_reordering():
    # windows arriving out of sample-time order (observed on the real link) give
    # the same efficiency as in-order: the envelope is min/max, order-free.
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    for ts in (0.0, 2.0, 1.0):  # deliberately out of order
        meter(_ctx_unit(10_000_000, 5, ts=ts, sample_rate_hz=10_000_000.0))
    assert "100.0%" in meter.capture_summary()  # envelope [0, 3.0], 30M/30M


def test_capture_none_when_no_samples():
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    assert meter.capture_summary() is None


def test_capture_declared_unknown_when_no_rate():
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(1000, 5, ts=0.0))  # samples, no declared rate
    assert "declared rate unknown" in meter.capture_summary()


def test_capture_window_too_short_when_untimed():
    # a declared stream whose windows carry no sample timestamp cannot be placed
    # in time -> no measurable envelope -> "too short".
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(10_000_000, 5, sample_rate_hz=10_000_000.0))  # no ts
    assert "too short" in meter.capture_summary()


def test_capture_retune_recurrence_not_false_loss():
    # A->B->A: the rate RETURNS to a prior value (e.g. DDC ladder 10->20->10).
    # Fully delivered contiguous windows must read 100%, not fabricated loss.
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(10_000_000, 5, ts=0.0, sample_rate_hz=10_000_000.0))  # A [0,1]
    meter(_ctx_unit(20_000_000, 5, ts=1.0, sample_rate_hz=20_000_000.0))  # B [1,2]
    meter(_ctx_unit(10_000_000, 5, ts=2.0, sample_rate_hz=10_000_000.0))  # A [2,3]
    line = meter.capture_summary()
    assert "100.0%" in line
    assert "OK" in line


def test_capture_loss_at_retune_boundary_is_flagged():
    # total loss in the gap BETWEEN the old-rate and new-rate windows must be
    # counted (at the pre-retune rate), not masked.
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(10_000_000, 5, ts=0.0, sample_rate_hz=10_000_000.0))  # ends 1.0
    meter(_ctx_unit(20_000_000, 5, ts=5.0, sample_rate_hz=20_000_000.0))  # 4 s loss gap
    line = meter.capture_summary()
    # expected 10M + 10MHz*4s (40M) + 20M = 70M; delivered 30M -> 42.9%
    assert "42.9%" in line
    assert "SEVERE UPSTREAM LOSS" in line


def test_capture_duplicate_windows_never_exceed_100():
    # duplicated delivery (same start_timestamp) must never report >100% or a
    # delivered rate above the declared rate.
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(10_000_000, 5, ts=0.0, sample_rate_hz=10_000_000.0, bandwidth_hz=5_000_000.0))
    meter(_ctx_unit(10_000_000, 5, ts=0.0, sample_rate_hz=10_000_000.0, bandwidth_hz=5_000_000.0))
    line = meter.capture_summary()
    assert "100.0%" in line
    assert "200" not in line
    assert "20.00 MS/s of declared 10.00 MS/s" not in line


def test_capture_nested_window_does_not_corrupt_gap_rate():
    # a nested/overlapping window at a different rate must NOT become the rate
    # charged for the following gap (the frontier rate is kept).
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(2_000_000, 5, ts=0.0, sample_rate_hz=1_000_000.0))    # [0, 2.0] @1MHz
    meter(_ctx_unit(1_000, 5, ts=0.5, sample_rate_hz=100_000_000.0))      # nested, high rate
    meter(_ctx_unit(1_000_000, 5, ts=3.0, sample_rate_hz=1_000_000.0))    # gap [2,3] @1MHz
    line = meter.capture_summary()
    # the [2,3] gap is 1 s @1MHz (1M lost), NOT @100MHz; -> ~75%, not the ~3% the bug gave
    assert "75.0%" in line
    assert "SEVERE" not in line


def test_capture_bandwidth_is_sticky():
    # a later same-rate window whose context omits bandwidth keeps the known one.
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(10_000_000, 5, ts=0.0, sample_rate_hz=10_000_000.0, bandwidth_hz=5_000_000.0))
    meter(_ctx_unit(10_000_000, 5, ts=1.0, sample_rate_hz=10_000_000.0))  # no bandwidth
    assert "bandwidth 5.0 MHz" in meter.capture_summary()


def test_capture_guard_lines_share_capture_prefix():
    # degraded-state lines must share the 'capture:' prefix so a log filter catches them.
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(1000, 5, ts=0.0))  # samples, no declared rate
    assert meter.capture_summary().startswith("capture:")


def test_capture_declared_unmeasurable_stream_is_flagged_not_hidden():
    # a 2nd declared stream whose windows are all untimed (unmeasurable) must not
    # be silently dropped into a lone clean-stream report.
    meter = ThroughputMeter(lambda u: None)
    meter.start()
    meter(_ctx_unit(10_000_000, 5, ts=0.0, sample_rate_hz=10_000_000.0, bandwidth_hz=5_000_000.0))
    meter(_ctx_unit(10_000_000, 5, ts=1.0, sample_rate_hz=10_000_000.0, bandwidth_hz=5_000_000.0))
    meter(_ctx_unit(10_000_000, 6, sample_rate_hz=10_000_000.0))  # declared but untimed
    line = meter.capture_summary()
    assert "bandwidth" not in line  # forced out of the lone-clean-stream branch
    assert "not assessable" in line
