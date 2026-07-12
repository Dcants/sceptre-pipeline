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

``capture_summary()`` additionally reports capture EFFICIENCY — delivered vs the
samples the stream declared it should produce — to surface silent upstream loss
(kernel/link/fragment drops) that the drop counters cannot see. It integrates
against each stream's own VITA sample timestamps (the SDR clock), in sample-time
order, with a small fixed-size jitter buffer that undoes the link's out-of-order
delivery. Memory is therefore BOUNDED (O(jitter-buffer × streams), independent of
run length) while remaining correct across mid-run retune (including a rate that
recurs), loss at retune boundaries, packet reordering, cold-start, idle tails,
and duplicate/overlapping windows. The verdict is per stream, so no stream masks
another's loss.

Threading: the emit callback runs on Thread B, which ``Pipeline.run`` executes
on the calling (main) thread; ``summary()``/``capture_summary()`` are called on
that same thread after ``run()`` returns. All access is single-threaded, so no
lock is needed.
"""

from __future__ import annotations

import heapq
import time
from typing import Any, Callable

Emit = Callable[[dict[str, Any]], None]

_REORDER_WINDOW = 64
"""Per-stream jitter-buffer depth (in windows). Emitted windows are committed to
the integration in sample-time order once this many are held, so bounded
reordering is undone with O(_REORDER_WINDOW) memory per stream. Reordering
deeper than this degrades gracefully — a very late window commits with no gap
rather than growing memory — it is never stored unbounded."""


class _StreamStat:
    """Per-stream capture bookkeeping, integrated incrementally over VITA sample
    time with a bounded jitter buffer (so memory is fixed regardless of run
    length).

    A window at ``start_timestamp = ts`` carrying ``n`` samples at ``rate``
    occupies ``[ts, ts + n/rate]`` of sample time. Windows are committed in
    sample-time order; for each, the sample-time GAP since the previous window's
    end is charged as loss at the rate then in effect (``expected +=
    rate * gap``), and the window's own ``n`` counts toward both ``expected`` and
    ``delivered``. Thus ``expected = delivered + Σ gap-loss ≥ delivered``, so the
    ratio can never exceed 100% (a duplicate/overlapping window adds ``n`` to
    both and a non-positive gap, so it stays ≤ 100%). Retune is handled correctly
    even when a rate recurs, because each interval is charged in time order at
    the rate actually active — no per-rate envelope to merge disjoint intervals.
    """

    __slots__ = (
        "total_samples", "delivered", "expected", "_last_end", "_cur_rate",
        "_pending", "rate", "bandwidth", "rate_changed",
    )

    def __init__(self) -> None:
        self.total_samples = 0  # every sample this stream emitted
        self.delivered = 0  # samples committed toward efficiency (== Σn after finalize)
        self.expected = 0.0  # delivered + integrated gap-loss (sample-time)
        self._last_end: float | None = None  # sample-time end of last committed window
        self._cur_rate: float | None = None  # rate in effect at the committed frontier
        self._pending: list[tuple[float, float, int]] = []  # min-heap on ts (jitter buffer)
        self.rate: float | None = None  # latest declared sample rate (Hz)
        self.bandwidth: float | None = None
        self.rate_changed = False

    def add(self, ts: float, rate: float, n: int) -> None:
        """Buffer one rated, timestamped window; commit the oldest if full."""
        heapq.heappush(self._pending, (ts, rate, n))
        if len(self._pending) > _REORDER_WINDOW:
            self._commit(*heapq.heappop(self._pending))

    def _commit(self, ts: float, rate: float, n: int) -> None:
        if self._last_end is not None:
            gap = ts - self._last_end
            if gap > 0:  # sample time the SDR produced but we never received
                self.expected += self._cur_rate * gap
        self.expected += n
        self.delivered += n
        end = ts + n / rate
        # advance the frontier — and the rate charged to the NEXT gap — only when
        # this window actually extends it; a nested/overlapping window must not
        # overwrite the frontier's rate
        if self._last_end is None or end > self._last_end:
            self._last_end = end
            self._cur_rate = rate

    def finalize(self) -> None:
        """Drain the jitter buffer in sample-time order (call before reading)."""
        while self._pending:
            self._commit(*heapq.heappop(self._pending))


class ThroughputMeter:
    """Wrap an emit callback to measure the pipeline's output data rate.

    ``clock`` is injectable so tests can pin ``elapsed`` deterministically; in
    production it is ``time.monotonic`` (steady, never jumps backwards). Only
    ``summary()`` uses the clock — ``capture_summary()`` runs on sample time.
    """

    def __init__(self, wrapped_emit: Emit, clock: Callable[[], float] = time.monotonic) -> None:
        self._wrapped_emit = wrapped_emit
        self._clock = clock
        self.total_samples = 0
        self.total_bytes = 0
        self.unit_count = 0
        self._start: float | None = None
        # per-stream capture bookkeeping (bounded, sample-time); see _StreamStat
        self._streams: dict[Any, _StreamStat] = {}

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
        # Per-stream capture bookkeeping for capture_summary(), on SAMPLE time
        # (VITA start_timestamp), never wall-clock. Guarded with .get()/None so a
        # stray or context-less unit never raises into the Thread B loop.
        context = unit.get("context") or {}
        rate = context.get("sample_rate_hz")
        ts = unit.get("start_timestamp")
        stat = self._streams.get(unit.get("stream_id"))
        if stat is None:
            stat = _StreamStat()
            self._streams[unit.get("stream_id")] = stat
        stat.total_samples += num_samples
        if rate is not None:
            if stat.rate is not None and rate != stat.rate:
                stat.rate_changed = True
            stat.rate = rate
            bandwidth = context.get("bandwidth_hz")
            if bandwidth is not None:  # sticky: a later rate-only context keeps it
                stat.bandwidth = bandwidth
            if ts is not None and rate > 0 and num_samples > 0:
                stat.add(ts, rate, num_samples)
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

    @staticmethod
    def _verdict(pct: float) -> str:
        if pct >= 90.0:
            return "OK"
        if pct >= 50.0:
            return "PARTIAL LOSS - investigate"
        return "SEVERE UPSTREAM LOSS (check link rate / MTU)"

    def capture_summary(self) -> str | None:
        """One-line capture-efficiency report: delivered vs declared samples.

        The pipeline's drop counters cannot see loss upstream of the socket
        (kernel/link/fragment drops when the stream outruns the cord). This
        compares samples actually delivered against samples the context packets
        DECLARED should arrive, PER STREAM, integrated over each stream's VITA
        sample time — so silent loss is visible and no stream masks another.

        Because it runs on sample time (not wall-clock) and integrates in
        sample-time order, it is correct across mid-run retune (including a rate
        that recurs), counts loss at retune boundaries, and is immune to emit
        pacing (a buffered burst), packet reordering (within the jitter buffer),
        the cold-start pre-roll (excluded — pre-sync drops are in
        ``interpreter_dropped``), and an idle tail. Efficiency can never exceed
        100% (duplicates/overlaps stay bounded). The verdict is the WORST
        declared stream's. Streams that never declare a rate — or that deliver
        only untimed windows — are excluded and flagged, so they can neither
        fabricate nor mask loss.

        Known limits: loss AFTER a stream's last delivered window (or a
        single-window stream) has no later timestamp to measure against;
        reordering deeper than the jitter buffer commits a very late window
        without its gap. Returns ``None`` if no samples were emitted. Callers
        should log this only for LIVE runs — a replay's wall-clock is decoupled
        from real capture and the line's premise is live network loss.
        """
        if self.total_samples == 0:
            return None
        for stat in self._streams.values():
            stat.finalize()
        declared = {sid: s for sid, s in self._streams.items() if s.rate is not None}
        if not declared:
            return "capture: declared rate unknown - cannot assess loss"
        measurable = {sid: s for sid, s in declared.items() if s.expected > 0}
        if not measurable:
            return "capture: window too short to assess loss"
        unmeasurable_declared = len(declared) - len(measurable)

        per_stream = {sid: 100.0 * s.delivered / s.expected for sid, s in measurable.items()}
        worst_sid = min(per_stream, key=per_stream.get)  # verdict driven by worst stream
        pct = round(per_stream[worst_sid], 1)
        verdict = self._verdict(pct)

        undeclared = [s for s in self._streams.values() if s.rate is None]
        retuned = any(s.rate_changed for s in declared.values())

        # Clean single steady stream: the familiar rate-vs-rate form with
        # bandwidth. Requires exactly ONE declared stream, fully measurable, no
        # retune, nothing undeclared — else a second stream would be misrepresented.
        if len(declared) == 1 and unmeasurable_declared == 0 and not undeclared and not retuned:
            s = measurable[worst_sid]
            delivered_rate = pct / 100.0 * s.rate
            body = (
                f"delivered {delivered_rate / 1e6:.2f} MS/s of declared "
                f"{s.rate / 1e6:.2f} MS/s = {pct:.1f}%"
            )
            if s.bandwidth is not None:
                body += f" (bandwidth {s.bandwidth / 1e6:.1f} MHz)"
            return f"capture: {body} - {verdict}"

        # Otherwise: worst-stream headline with honest flags for everything else.
        if len(measurable) == 1:
            note = f"captured {pct:.1f}%"
        else:
            note = f"worst stream {pct:.1f}% of {len(declared)} declared streams"
        extras: list[str] = []
        if retuned:
            extras.append("rate changed mid-run")
        if unmeasurable_declared:
            extras.append(f"+{unmeasurable_declared} declared stream(s) not assessable")
        if undeclared:
            u_samples = sum(s.total_samples for s in undeclared)
            extras.append(
                f"+{len(undeclared)} undeclared stream(s), {u_samples:,} samples excluded"
            )
        if extras:
            note += " (" + "; ".join(extras) + ")"
        return f"capture: {note} - {verdict}"
