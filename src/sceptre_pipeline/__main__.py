""" ``python -m sceptre_pipeline`` — replay a capture or go live.

Wires the full pipeline (``BoundedRawQueue`` + ``Interpreter`` + per-stream
``BufferRouter`` + a source), runs the Thread B loop on the main thread, and
prints one demo line per emitted unit. Replay ends when the capture's SHUTDOWN
sentinel drains through; live runs until Ctrl-C, which the main thread turns
into a clean ``stop()`` + final flush inside ``Pipeline.run``'s ``finally``.

``--replay`` XOR ``--live`` is a required mutually exclusive group, so argparse
exits 2 on neither/both. The live ``--record`` path builds a BOUNDED Recorder
(default cap ``recorder.DEFAULT_LIVE_RECORD_MAX_BYTES``) so an open-ended live
session cannot grow memory without limit; the save path comes from
``default_recording_path`` unless the user gives one.
"""

from __future__ import annotations

import argparse
import logging
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any, NamedTuple

from .buffer import DEFAULT_FLUSH_FIELDS, BufferRouter, Emit
from .interpreter import DEFAULT_MAX_STREAMS, Interpreter
from .metrics import ThroughputMeter
from .queues import BoundedRawQueue
from .recorder import (
    DEFAULT_LIVE_RECORD_MAX_BYTES,
    Recorder,
    default_recording_path,
)
from .runtime import DEFAULT_POLL_INTERVAL_S, Pipeline
from .sources import LiveSource, ReplaySource

logger = logging.getLogger(__name__)

# One second of the known 625 kHz Sceptre stream is a sensible default window.
DEFAULT_MAX_SAMPLES = 625_000
DEFAULT_MAX_AGE_S = 1.0
# Raw-queue depth: worst-case memory when full is ~270 MB (8 KiB datagrams);
# the stall time it absorbs scales inversely with the incoming stream rate.
DEFAULT_QUEUE_SIZE = 32768
# DEFAULT_POLL_INTERVAL_S is imported from runtime so the CLI and library
# defaults cannot drift.


def demo_emit(unit: dict[str, Any]) -> None:
    """Print one line per emitted unit — the demo downstream consumer.

    Reads context keys with ``.get()`` so a missing field prints ``None``
    rather than raising: an exception here would propagate straight into the
    Thread B loop.
    """
    context = unit.get("context") or {}
    samples = unit["samples"]
    print(
        f"stream_id={unit['stream_id']} "
        f"num_samples={unit['num_samples']} "
        f"shape={tuple(samples.shape)} "
        f"dtype={samples.dtype} "
        f"rf_hz={context.get('rf_hz')} "
        f"sample_rate_hz={context.get('sample_rate_hz')} "
        f"bandwidth_hz={context.get('bandwidth_hz')} "
        f"start_timestamp={unit['start_timestamp']}"
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser (module-level so tests can exercise it directly)."""
    parser = argparse.ArgumentParser(
        prog="sceptre_pipeline",
        description="Replay a Sceptre VITA-49 capture or ingest a live UDP stream.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--replay", metavar="PATH", help="replay a recorded .pkl capture"
    )
    mode.add_argument(
        "--live", action="store_true", help="ingest a live UDP stream"
    )

    parser.add_argument(
        "--pace",
        action="store_true",
        help="replay: pace playback by the recorded packet timing",
    )
    parser.add_argument(
        "--host", default="0.0.0.0", help="live: bind host (default: %(default)s)"
    )
    parser.add_argument(
        "--port", type=int, default=None, help="live: bind UDP port"
    )
    parser.add_argument(
        "--record",
        nargs="?",
        const=True,
        default=False,
        metavar="PATH",
        help="live: record the raw stream (optional output PATH; default: auto)",
    )
    parser.add_argument(
        "--record-max-bytes",
        type=int,
        default=DEFAULT_LIVE_RECORD_MAX_BYTES,
        help="live --record: hard byte cap (default: %(default)s)",
    )
    parser.add_argument(
        "--record-max-packets",
        type=int,
        default=None,
        help="live --record: hard packet cap (default: none)",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=DEFAULT_MAX_SAMPLES,
        help="window size in samples (default: %(default)s = 1 s @ 625 kHz)",
    )
    parser.add_argument(
        "--max-age-s",
        type=float,
        default=DEFAULT_MAX_AGE_S,
        help="flush a partial window after this many seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--max-streams",
        type=int,
        default=DEFAULT_MAX_STREAMS,
        help="max concurrent stream_ids tracked (default: %(default)s)",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=DEFAULT_QUEUE_SIZE,
        help="bounded raw-queue capacity (default: %(default)s)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL_S,
        help="Thread B dequeue poll interval, seconds (default: %(default)s)",
    )
    return parser


class _Runtime(NamedTuple):
    """The wired object graph a CLI run drives (factored out for testability)."""

    pipeline: Pipeline
    source: LiveSource | ReplaySource
    recorder: Recorder | None
    record_path: Path | None


def _build_runtime(args: argparse.Namespace, emit: Emit = demo_emit) -> _Runtime:
    """Wire queue + interpreter + router + source + Pipeline from parsed args.

    Pure construction: ``main`` validates cross-flag preconditions (a live
    ``--port``, a positive ``--poll-interval``) BEFORE calling this. Kept
    module-level so a test can pin the real object graph — the tri-state
    ``--record`` parse, the bounded ``Recorder``, and its attachment to the
    ``LiveSource`` — without a subprocess.
    """
    stop = threading.Event()
    raw_queue = BoundedRawQueue(maxsize=args.queue_size)
    interpreter = Interpreter(max_streams=args.max_streams)
    router = BufferRouter(
        emit=emit,
        max_samples=args.max_samples,
        max_age_s=args.max_age_s,
        flush_fields=DEFAULT_FLUSH_FIELDS,  # never invent flush-field strings
        max_streams=args.max_streams,
    )

    recorder: Recorder | None = None
    record_path: Path | None = None
    source: LiveSource | ReplaySource
    if args.live:
        if args.record is not False:
            # bounded by default: an open-ended live session must stay bounded
            recorder = Recorder(
                max_bytes=args.record_max_bytes,
                max_packets=args.record_max_packets,
            )
            record_path = (
                Path(args.record)
                if isinstance(args.record, str)
                else default_recording_path(Path.cwd())
            )
        source = LiveSource(
            args.host, args.port, raw_queue, stop, recorder=recorder
        )
    else:
        source = ReplaySource(args.replay, raw_queue, stop, pace=args.pace)

    pipeline = Pipeline(
        source,
        raw_queue,
        interpreter,
        router,
        stop,
        poll_interval=args.poll_interval,
    )
    return _Runtime(pipeline, source, recorder, record_path)


def main(argv: Sequence[str] | None = None) -> int:
    """Parse args, wire the pipeline, and run it. Returns 0 on success, 1 on
    a source failure (bad replay / live bind failure), 2 on bad CLI args."""
    parser = build_parser()
    args = parser.parse_args(argv)
    # validate before constructing anything (parser.error exits 2)
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be > 0")
    if args.live and args.port is None:
        parser.error("--live requires --port")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Wrap the demo consumer so every emitted unit is tallied on its way out;
    # the meter's summary logs at shutdown next to the "pipeline stopped" line.
    meter = ThroughputMeter(demo_emit)
    runtime = _build_runtime(args, emit=meter)
    pipeline = runtime.pipeline
    source = runtime.source

    # A live bind failure enqueues no SHUTDOWN and no Event, so run() would poll
    # an empty queue forever. Watch source.ready off-thread; if the bind failed,
    # stop the pipeline so run() unwinds. Pipeline stays source-agnostic.
    bind_failed = threading.Event()
    if args.live:

        def _watch_bind() -> None:
            source.ready.wait()  # type: ignore[union-attr]
            if source.bound_address is None:  # type: ignore[union-attr]
                bind_failed.set()
                pipeline.stop()

        threading.Thread(
            target=_watch_bind, name="live-bind-watchdog", daemon=True
        ).start()

    start_time_ns = time.time_ns()
    meter.start()
    try:
        pipeline.run()
    except KeyboardInterrupt:
        # Ctrl-C on the main thread while run() blocks. run()'s finally has
        # already unwound (flush + join + log); stop() just re-sets the Event.
        logger.info("interrupted; shutting down")
        pipeline.stop()

    # Logs on every exit path (replay EOF and live Ctrl-C alike), right after
    # the "pipeline stopped: ..." counter line from run()'s finally.
    logger.info(meter.summary())
    # Capture efficiency: delivered vs the rate the stream declared. Surfaces
    # silent upstream loss (kernel/link drops) the drop counters cannot see.
    # LIVE ONLY: the ratio is only meaningful when wall-clock is real time. An
    # unpaced replay measures CPU speed; even a PACED replay only matches when
    # the recording's pacing matches its declared rate (not guaranteed — a gappy
    # recording replays as false loss), so replay is excluded entirely.
    capture = meter.capture_summary()
    if capture is not None and args.live:
        logger.info(capture)

    exit_code = 0
    if bind_failed.is_set():
        logger.error(
            "live bind failed on %s:%s; exiting nonzero", args.host, args.port
        )
        exit_code = 1
    elif source.failed:
        logger.error("source failed; exiting nonzero")
        exit_code = 1

    recorder = runtime.recorder
    record_path = runtime.record_path
    if recorder is not None and record_path is not None:
        duration_s = (time.time_ns() - start_time_ns) / 1e9
        saved = recorder.save(record_path, start_time_ns, duration_s)
        logger.info(
            "saved live recording to %s (%d packets, %d bytes, capped=%s)",
            saved,
            len(recorder),
            recorder.total_bytes,
            recorder.capped,
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
