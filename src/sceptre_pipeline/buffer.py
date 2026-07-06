"""Stage 3: per-stream ingest buffering — accumulate, flush on four triggers,
convert once.

``IngestBuffer`` turns the interpreter's per-packet records into emitted
``{samples, context, ...}`` units for ONE stream; ``BufferRouter`` owns one
buffer per ``stream_id`` (bounded by ``max_streams``, mirroring the
interpreter's spoofed-stream flood bound) so multiplexed streams never share
an accumulator.

A window flushes on exactly four triggers:

1. **Real context change** — a diff over ``flush_fields`` only (counter and
   timestamp always change; an identical periodic heartbeat must NOT flush).
   The open window flushes BEFORE the new context is adopted, so the
   boundary-spanning array is labeled with the context actually in effect
   while its samples arrived. The flush identity is
   ``(rf_hz, sample_rate_hz, component_dtype, is_complex)`` (Stage 2.75
   Part D): byte interpretation varies independently of frequency, and
   ``component_dtype`` alone is ">i2" for BOTH real and complex int16, so
   ``is_complex`` is required.
2. **Size** — ``num_samples >= max_samples`` after an append.
3. **Age** — POLLED via ``maybe_flush_on_age()`` from the dequeue loop; the
   buffer never flushes on its own between pushes.
4. **Counter gap** — ``gap_before`` flushes the open window before appending,
   so every emitted array is contiguous in sample time.

Flushing resets accumulation but KEEPS the current context: after any flush
the very next data packet is accepted immediately (data is dropped only at
cold start, before the first context, or while the context is unsupported).

An unsupported context (``supported is False`` / ``component_dtype is None``)
is adopted for labeling but leaves the buffer INERT — it accepts no data and
never builds ``np.dtype(None)`` (which would silently mean float64) — until a
supported context re-enables flow.

Conversion happens ONCE per emitted window, in the mandatory order
``np.frombuffer(raw, component_dtype).astype(np.float32)`` (byteswap first)
then ``.view(np.complex64)`` (pair I,Q) — ``view`` never swaps bytes, so
viewing big-endian components directly yields NaN/garbage with no exception.
Amplitude/precision policy (Part D): emitted arrays are funneled to
float32/complex64, so integer formats emerge UN-NORMALIZED (raw counts) and
float64 is DOWNCAST (lossy); every emitted unit stamps ``component_dtype``,
``is_complex``, and ``bytes_per_sample`` so downstream can normalize/upcast.

Threading: all methods run on Thread B (the dequeue loop); ``dropped``
counters are single-writer there, and GIL-atomic int reads make them safe to
observe from other threads without a lock.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Iterable

import numpy as np

from .interpreter import DEFAULT_MAX_STREAMS, _RateLimitedLog

logger = logging.getLogger(__name__)

# The flush identity (Stage 2.75 Part D): a change in ANY of these fields
# between the adopted context and a newly arrived one flushes the open window.
DEFAULT_FLUSH_FIELDS = ("rf_hz", "sample_rate_hz", "component_dtype", "is_complex")

Emit = Callable[[dict[str, Any]], None]


class IngestBuffer:
    """Accumulates one stream's data records; emits converted windows."""

    def __init__(
        self,
        emit: Emit,
        max_samples: int,
        max_age_s: float,
        stream_id: int | None = None,
        flush_fields: Iterable[str] = DEFAULT_FLUSH_FIELDS,
    ) -> None:
        self._emit = emit
        self._max_samples = max_samples
        self._max_age_s = max_age_s
        self._stream_id = stream_id
        self._flush_fields = tuple(flush_fields)
        # accumulation state — reset by flush()
        self._chunks: list[bytes] = []
        self._num_samples = 0
        self._component_dtype: str | None = None  # latched from a window's first chunk
        self._bytes_per_sample: int | None = None
        self._is_complex: bool | None = None
        self._start_timestamp: float | None = None  # packet time of first chunk
        self._first_arrival: float | None = None  # time.monotonic() at first chunk
        # labeling state — NEVER reset by flush()
        self._current_context: dict[str, Any] | None = None
        self._inert = False  # True while the adopted context is unsupported
        self.dropped = 0  # data records dropped (cold start / inert / unusable)
        self._log = _RateLimitedLog()

    def push(self, record: dict[str, Any] | None) -> None:
        """Route one interpreter record (None, from a dropped packet, is a no-op)."""
        if record is None:
            return
        if record["type"] == "context":
            self._push_context(record["context"])
        elif record["type"] == "data":
            self._push_data(record)

    def _push_context(self, ctx: dict[str, Any] | None) -> None:
        if ctx is None:
            return
        if self._chunks and self._differs(ctx):
            self.flush()  # BEFORE adopting: the window keeps its old label
        self._current_context = ctx
        self._inert = (
            ctx.get("supported") is False or ctx.get("component_dtype") is None
        )
        if self._inert and self._log.should_log("inert-context"):
            logger.warning(
                "stream %s adopted an unsupported context; buffer inert until "
                "a supported context arrives",
                self._stream_id,
            )

    def _differs(self, new_ctx: dict[str, Any]) -> bool:
        old = self._current_context
        if old is None:
            return True
        return any(old.get(f) != new_ctx.get(f) for f in self._flush_fields)

    def _push_data(self, record: dict[str, Any]) -> None:
        if self._current_context is None or self._inert:
            # cold start (no context yet) or present-but-unsupported stream
            self.dropped += 1
            if self._log.should_log("no-usable-context"):
                logger.warning(
                    "dropping data on stream %s: no supported context adopted",
                    self._stream_id,
                )
            return
        md = record["metadata"]
        dtype = md.get("dtype")
        bps = md.get("bytes_per_sample")
        data = record["data"]
        # defense in depth against hand-fed records: never latch an
        # interpretation that flush() could not safely convert
        if dtype is None or not bps or len(data) % bps:
            self.dropped += 1
            if self._log.should_log("unusable-data"):
                logger.warning(
                    "dropping data on stream %s: unusable interpretation "
                    "(dtype=%r bytes_per_sample=%r len=%d)",
                    self._stream_id,
                    dtype,
                    bps,
                    len(data),
                )
            return
        if self._chunks and md.get("gap_before"):
            self.flush()  # the gap breaks contiguity: close the window first
        if self._chunks and (
            dtype != self._component_dtype
            or bool(md.get("is_complex")) != self._is_complex
            or bps != self._bytes_per_sample
        ):
            # interpretation changed without a context record (hand-fed /
            # defensive): never mix two sample interpretations in one window
            self.flush()
        if not data:
            return
        if not self._chunks:  # first chunk of a window: latch and validate
            if not self._latch(dtype, bool(md.get("is_complex")), bps):
                self.dropped += 1
                return
            self._start_timestamp = md.get("timestamp")
            self._first_arrival = time.monotonic()
        self._chunks.append(data)
        self._num_samples += len(data) // bps
        if self._num_samples >= self._max_samples:
            self.flush()

    def _latch(self, dtype: str, is_complex: bool, bps: int) -> bool:
        """Latch a window's sample interpretation; False if it can't convert."""
        try:
            dt = np.dtype(dtype)
        except TypeError:
            dt = None
        # only numeric component kinds (int/uint/float) convert to float32;
        # a bytes/str/void dtype would pass a size-only check here and defer
        # its ValueError into flush() on Thread B, wedging the buffer
        if dt is None or dt.kind not in "iuf" or dt.itemsize * (
            2 if is_complex else 1
        ) != bps:
            if self._log.should_log("latch-invalid"):
                logger.warning(
                    "dropping data on stream %s: dtype %r inconsistent with "
                    "bytes_per_sample %r",
                    self._stream_id,
                    dtype,
                    bps,
                )
            return False
        self._component_dtype = dtype
        self._is_complex = is_complex
        self._bytes_per_sample = bps
        return True

    def maybe_flush_on_age(self) -> None:
        """Age trigger — polled from the dequeue loop, never self-firing."""
        if (
            self._chunks
            and time.monotonic() - self._first_arrival >= self._max_age_s
        ):
            self.flush()

    def flush(self) -> None:
        """Convert the open window once and emit it; keep the context."""
        if not self._chunks:
            return
        raw = b"".join(self._chunks)
        # mandatory order: byteswap to native via astype FIRST, then pair I,Q
        arr = np.frombuffer(raw, dtype=self._component_dtype).astype(np.float32)
        if self._is_complex:
            arr = arr.view(np.complex64)
        unit = {
            "samples": arr,
            "context": self._current_context,
            "start_timestamp": self._start_timestamp,
            "num_samples": self._num_samples,
            "stream_id": self._stream_id,
            "component_dtype": self._component_dtype,
            "is_complex": self._is_complex,
            "bytes_per_sample": self._bytes_per_sample,
            "metadata": {
                "stream_id": self._stream_id,
                "component_dtype": self._component_dtype,
                "is_complex": self._is_complex,
                "bytes_per_sample": self._bytes_per_sample,
            },
        }
        # reset accumulation BEFORE emitting (an emit failure must not leave
        # the window re-emittable); the context is KEPT so the next data
        # packet is accepted immediately
        self._chunks = []
        self._num_samples = 0
        self._component_dtype = None
        self._bytes_per_sample = None
        self._is_complex = None
        self._start_timestamp = None
        self._first_arrival = None
        self._emit(unit)


class BufferRouter:
    """One ``IngestBuffer`` per ``stream_id``, created lazily and bounded.

    New streams beyond ``max_streams`` are dropped-and-counted (rate-limited
    log) so a spoofed-stream flood cannot grow memory without limit; existing
    streams are unaffected. All methods run on Thread B.
    """

    def __init__(
        self,
        emit: Emit,
        max_samples: int,
        max_age_s: float,
        flush_fields: Iterable[str] = DEFAULT_FLUSH_FIELDS,
        max_streams: int = DEFAULT_MAX_STREAMS,
    ) -> None:
        self._emit = emit
        self._max_samples = max_samples
        self._max_age_s = max_age_s
        self._flush_fields = tuple(flush_fields)
        self._max_streams = max_streams
        self._buffers: dict[int | None, IngestBuffer] = {}
        self.dropped = 0  # records dropped: new streams beyond max_streams
        self._log = _RateLimitedLog()

    def push(self, record: dict[str, Any] | None) -> None:
        if record is None:
            return
        sid = record["metadata"]["stream_id"]
        buf = self._buffers.get(sid)
        if buf is None:
            if len(self._buffers) >= self._max_streams:
                self.dropped += 1
                if self._log.should_log("stream-flood"):
                    logger.warning(
                        "record on new stream %s beyond max_streams=%d; "
                        "dropping (spoofed-stream flood protection)",
                        sid,
                        self._max_streams,
                    )
                return
            buf = IngestBuffer(
                emit=self._emit,
                max_samples=self._max_samples,
                max_age_s=self._max_age_s,
                stream_id=sid,
                flush_fields=self._flush_fields,
            )
            self._buffers[sid] = buf
        buf.push(record)

    def maybe_flush_on_age(self) -> None:
        """Poll the age trigger on EVERY stream's buffer (dequeue-loop tick)."""
        for buf in self._buffers.values():
            buf.maybe_flush_on_age()

    def flush_all(self) -> None:
        """Drain every stream's open window (EOF / shutdown)."""
        for buf in self._buffers.values():
            buf.flush()
