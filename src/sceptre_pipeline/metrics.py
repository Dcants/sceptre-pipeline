"""Output-side pipeline data-rate measurement.

``ThroughputMeter`` wraps the emit callback so every emitted
``{samples, num_samples, bytes_per_sample, ...}`` unit is tallied on its way to
the real downstream consumer, then reports one throughput line at shutdown. It
measures at the pipeline OUTPUT by design (see
``docs/superpowers/specs/2026-07-09-throughput-meter-design.md``): the emitted
sample count is the useful IQ that made it all the way through interpret →
buffer → emit, reported in the same MS/s units validated on the real Sceptre
stream. "Are we keeping up?" is already answered by the drop counters in the
existing ``pipeline stopped: ...`` line, so no input-side meter is needed.

Threading: the emit callback runs on Thread B, which ``Pipeline.run`` executes
on the calling (main) thread; ``summary()`` is called on that same thread after
``run()`` returns. All access is single-threaded, so no lock is needed.
"""

from __future__ import annotations

import time
from typing import Any, Callable

Emit = Callable[[dict[str, Any]], None]


class ThroughputMeter:
    """Wrap an emit callback to measure the pipeline's output data rate.

    ``clock`` is injectable so tests can pin ``elapsed`` deterministically; in
    production it is ``time.monotonic`` (steady, never jumps backwards).
    """

    def __init__(self, wrapped_emit: Emit, clock: Callable[[], float] = time.monotonic) -> None:
        self._wrapped_emit = wrapped_emit
        self._clock = clock
        self.total_samples = 0
        self.total_bytes = 0
        self.unit_count = 0
        self._start: float | None = None

    def start(self) -> None:
        """Stamp the start of the measurement window (call before ``run()``)."""
        self._start = self._clock()

    def __call__(self, unit: dict[str, Any]) -> None:
        """Tally one emitted unit, then hand it to the wrapped consumer.

        Counts before delegating so a downstream that raises still leaves the
        produced-throughput tally correct. Missing/None sizes count as 0 — the
        meter must never raise into the Thread B loop.
        """
        num_samples = unit.get("num_samples") or 0
        bytes_per_sample = unit.get("bytes_per_sample") or 0
        self.total_samples += num_samples
        self.total_bytes += num_samples * bytes_per_sample
        self.unit_count += 1
        self._wrapped_emit(unit)

    def summary(self) -> str:
        """One-line throughput report for the shutdown log."""
        if self.total_samples == 0:
            return "pipeline throughput: no samples emitted"
        elapsed = self._clock() - self._start if self._start is not None else 0.0
        if elapsed <= 0:
            return (
                f"pipeline throughput: {self.total_samples:,} samples "
                "(elapsed too short to compute rate)"
            )
        ms_per_s = self.total_samples / elapsed / 1e6
        mb_per_s = self.total_bytes / elapsed / 1e6
        return (
            f"pipeline throughput: {self.total_samples:,} samples in "
            f"{elapsed:.2f} s = {ms_per_s:.2f} MS/s ({mb_per_s:.1f} MB/s)"
        )
