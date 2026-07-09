"""Tests for ``ThroughputMeter`` — the output-side pipeline data-rate meter.

The meter wraps the emit callback: on every emitted ``{num_samples,
bytes_per_sample, ...}`` unit it tallies samples/bytes, delegates to the real
consumer, and reports one throughput line at shutdown. A fake clock is injected
so ``elapsed`` — and therefore the reported rate — is deterministic.
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
