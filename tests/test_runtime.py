"""Stage 4: runtime wiring (Pipeline) + the CLI demo consumer.

The Pipeline starts Thread A (the source) and runs the Thread B loop on the
CALLING thread: dequeue, discriminate SHUTDOWN from a None timeout, poll the age
trigger, interpret, and push into the per-stream BufferRouter (which owns the
emit callback). The final flush + Thread-A join + counter log live in run()'s
finally so they happen on EVERY exit path — EOF (SHUTDOWN) and Event-stop alike.

All threading tests are hang-proof: run() is joined with a timeout and the
thread is asserted finished; no test can block forever on a bug.
"""

from __future__ import annotations

import logging
import socket
import threading
import time

import numpy as np
import pytest

from conftest import (
    build_context_packet,
    build_data_packet,
    encode_iq_float32,
    standard_context_fields,
)

from sceptre_pipeline.buffer import BufferRouter
from sceptre_pipeline.interpreter import Interpreter
from sceptre_pipeline.queues import SHUTDOWN, BoundedRawQueue
from sceptre_pipeline.runtime import Pipeline
from sceptre_pipeline.sources import LiveSource, PacketSource, ReplaySource

# Pinned empirically (see docs/implementation-plan.md): single_frequency has
# 3066 data packets x 1020 samples; change_frequency has 3065 x 1020.
SINGLE_FREQ_TOTAL_SAMPLES = 3_127_320
CHANGE_FREQ_TOTAL_SAMPLES = 3_126_300

TWO_SAMPLES = encode_iq_float32([complex(1.0, -1.0), complex(0.5, 0.25)])

JOIN_TIMEOUT_S = 30.0


class _NoopSource(PacketSource):
    """A source that produces nothing: the test hand-feeds the raw queue."""

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


class _StopTrackingSource(PacketSource):
    """A no-producing source that records whether ``stop()`` was called.

    Stands in for a real source (whose ``stop()`` sets the Event and joins
    Thread A) so a test can prove ``run()``'s finally joined the source even
    when the final flush raised.
    """

    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True


def _run_in_thread(pipeline: Pipeline) -> threading.Thread:
    # daemon=True so a run()-hang regression can never wedge pytest at
    # interpreter exit (matches sources.py's own thread convention)
    thread = threading.Thread(
        target=pipeline.run, name="pipeline-thread-B", daemon=True
    )
    thread.start()
    return thread


# --- 1 & 2: full-stack replay integration ---------------------------------


def test_replay_single_frequency_full_stack(single_frequency_path) -> None:
    emitted: list = []
    lock = threading.Lock()

    def emit(unit: dict) -> None:
        with lock:
            emitted.append(unit)

    stop = threading.Event()
    raw_queue = BoundedRawQueue(maxsize=8192)  # > 3073 items: never drops
    interpreter = Interpreter()
    router = BufferRouter(emit=emit, max_samples=100_000, max_age_s=1e9)
    source = ReplaySource(single_frequency_path, raw_queue, stop)
    pipeline = Pipeline(source, raw_queue, interpreter, router, stop)

    thread = _run_in_thread(pipeline)
    thread.join(timeout=JOIN_TIMEOUT_S)
    assert not thread.is_alive(), "pipeline.run() did not finish (hang)"

    assert emitted, "no units emitted"
    assert sum(u["num_samples"] for u in emitted) == SINGLE_FREQ_TOTAL_SAMPLES
    for unit in emitted:
        assert unit["context"]["rf_hz"] == 97.3e6
        assert unit["context"]["sample_rate_hz"] == 625e3
        assert unit["samples"].dtype == np.complex64
        assert len(unit["samples"]) == unit["num_samples"]
    assert raw_queue.dropped == 0
    assert interpreter.errors == 0


def test_replay_change_frequency_rf_transition(change_frequency_path) -> None:
    emitted: list = []
    lock = threading.Lock()

    def emit(unit: dict) -> None:
        with lock:
            emitted.append(unit)

    stop = threading.Event()
    raw_queue = BoundedRawQueue(maxsize=8192)
    interpreter = Interpreter()
    router = BufferRouter(emit=emit, max_samples=100_000, max_age_s=1e9)
    source = ReplaySource(change_frequency_path, raw_queue, stop)
    pipeline = Pipeline(source, raw_queue, interpreter, router, stop)

    thread = _run_in_thread(pipeline)
    thread.join(timeout=JOIN_TIMEOUT_S)
    assert not thread.is_alive(), "pipeline.run() did not finish (hang)"

    # RF labels in order of first appearance
    rf_order: list = []
    for unit in emitted:
        rf = unit["context"]["rf_hz"]
        if rf not in rf_order:
            rf_order.append(rf)
    assert rf_order == [97.3e6, 103.7e6]

    seg_low = [u for u in emitted if u["context"]["rf_hz"] == 97.3e6]
    seg_high = [u for u in emitted if u["context"]["rf_hz"] == 103.7e6]
    assert seg_low and seg_high, "one of the two RF segments was empty"
    assert sum(u["num_samples"] for u in emitted) == CHANGE_FREQ_TOTAL_SAMPLES


# --- 3: SHUTDOWN vs None-timeout discrimination ---------------------------


def test_shutdown_and_timeout_are_not_conflated() -> None:
    emitted: list = []
    stop = threading.Event()
    raw_queue = BoundedRawQueue(maxsize=64)
    interpreter = Interpreter()
    router = BufferRouter(emit=emitted.append, max_samples=1_000_000, max_age_s=1e9)

    age_polls = []
    original_age = router.maybe_flush_on_age

    def counting_age() -> None:
        age_polls.append(1)
        original_age()

    router.maybe_flush_on_age = counting_age  # type: ignore[method-assign]

    pipeline = Pipeline(
        _NoopSource(), raw_queue, interpreter, router, stop, poll_interval=0.02
    )

    # hand-feed a context and one partial-window data packet, then nothing
    raw_queue.put(build_context_packet(fields=standard_context_fields()))
    raw_queue.put(build_data_packet(payload=TWO_SAMPLES, counter=0))

    thread = _run_in_thread(pipeline)
    # let several get() timeouts elapse: None means "poll age + keep going"
    time.sleep(0.25)
    assert thread.is_alive(), "loop exited on a None timeout (treated it as SHUTDOWN)"
    assert age_polls, "the age trigger was never polled on a timeout"
    assert emitted == [], "partial window flushed before SHUTDOWN"

    # now the distinct SHUTDOWN sentinel: drain, flush_all, exit
    raw_queue.put(SHUTDOWN)
    thread.join(timeout=5.0)
    assert not thread.is_alive(), "SHUTDOWN did not end the loop"
    assert len(emitted) == 1, "flush_all did not emit the partial window exactly once"
    assert emitted[0]["num_samples"] == 2


# --- 4: stop() path flushes the final window (no SHUTDOWN ever arrives) ----


def test_stop_event_path_flushes_final_window() -> None:
    emitted: list = []
    stop = threading.Event()
    raw_queue = BoundedRawQueue(maxsize=64)
    interpreter = Interpreter()
    router = BufferRouter(emit=emitted.append, max_samples=1_000_000, max_age_s=1e9)
    pipeline = Pipeline(
        _NoopSource(), raw_queue, interpreter, router, stop, poll_interval=0.02
    )

    raw_queue.put(build_context_packet(fields=standard_context_fields()))
    raw_queue.put(build_data_packet(payload=TWO_SAMPLES, counter=0))

    thread = _run_in_thread(pipeline)
    time.sleep(0.2)  # process the partial window; no SHUTDOWN is enqueued
    assert emitted == [], "partial window flushed early"

    pipeline.stop()
    pipeline.stop()  # idempotent: a second call must be harmless
    thread.join(timeout=5.0)
    assert not thread.is_alive(), "run() did not return after stop()"
    assert len(emitted) == 1, "final flush on the Event path did not emit the window"
    assert emitted[0]["num_samples"] == 2


# --- 5: defense-in-depth — a raising process() must not kill Thread B ------


def test_process_exception_does_not_kill_thread_b(caplog) -> None:
    emitted: list = []
    stop = threading.Event()
    raw_queue = BoundedRawQueue(maxsize=64)
    interpreter = Interpreter()

    def boom(_raw: bytes) -> dict:
        raise RuntimeError("simulated bug inside the interpreter guard")

    interpreter.process = boom  # type: ignore[method-assign]
    router = BufferRouter(emit=emitted.append, max_samples=100, max_age_s=1e9)
    pipeline = Pipeline(
        _NoopSource(), raw_queue, interpreter, router, stop, poll_interval=0.02
    )

    raw_queue.put(build_data_packet(payload=TWO_SAMPLES, counter=0))
    raw_queue.put(SHUTDOWN)

    with caplog.at_level(logging.WARNING, logger="sceptre_pipeline.runtime"):
        thread = _run_in_thread(pipeline)
        thread.join(timeout=5.0)
    assert not thread.is_alive(), "a raising process() hung / killed Thread B"
    assert interpreter.errors == 1, "the loop did not count the guarded failure"
    assert emitted == []
    # H: the guarded drop must not be fully silent — it logs rate-limited,
    # matching every other drop path's convention.
    guard_warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "process" in r.getMessage()
    ]
    assert guard_warnings, "the guarded process() failure was dropped silently"


# --- 6: CLI (build_parser + main) -----------------------------------------


def test_build_parser_accepts_replay() -> None:
    from sceptre_pipeline.__main__ import build_parser

    args = build_parser().parse_args(["--replay", "foo.pkl"])
    assert args.replay == "foo.pkl"
    assert args.live is False


def test_build_parser_accepts_live() -> None:
    from sceptre_pipeline.__main__ import build_parser

    args = build_parser().parse_args(["--live", "--host", "10.0.0.1", "--port", "5000"])
    assert args.live is True
    assert args.host == "10.0.0.1"
    assert args.port == 5000


def test_build_parser_rejects_both_modes() -> None:
    from sceptre_pipeline.__main__ import build_parser

    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["--replay", "foo.pkl", "--live"])
    assert exc.value.code == 2


def test_build_parser_rejects_neither_mode() -> None:
    from sceptre_pipeline.__main__ import build_parser

    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args([])
    assert exc.value.code == 2


def test_main_replay_end_to_end(single_frequency_path, capsys) -> None:
    from sceptre_pipeline.__main__ import main

    result: dict = {}

    def run() -> None:
        result["code"] = main(
            ["--replay", str(single_frequency_path), "--max-samples", "500000"]
        )

    thread = threading.Thread(target=run, name="cli-main", daemon=True)
    thread.start()
    thread.join(timeout=JOIN_TIMEOUT_S)
    assert not thread.is_alive(), "main() hung on replay"
    assert result["code"] == 0

    out = capsys.readouterr().out
    assert "97300000" in out, "demo consumer never printed the 97.3 MHz rf_hz"
    assert "num_samples" in out, "demo consumer produced no unit lines"


# --- 7: live loopback smoke -----------------------------------------------


def test_live_loopback_smoke() -> None:
    emitted: list = []
    lock = threading.Lock()

    def emit(unit: dict) -> None:
        with lock:
            emitted.append(unit)

    stop = threading.Event()
    raw_queue = BoundedRawQueue(maxsize=256)
    interpreter = Interpreter()
    # flush after 2 samples so a single 4-sample data packet emits immediately
    router = BufferRouter(emit=emit, max_samples=2, max_age_s=1e9)
    source = LiveSource("127.0.0.1", 0, raw_queue, stop)
    pipeline = Pipeline(
        source, raw_queue, interpreter, router, stop, poll_interval=0.02
    )

    thread = _run_in_thread(pipeline)
    try:
        assert source.ready.wait(2.0), "LiveSource never bound its socket"
        host, port = source.bound_address
        ctx_pkt = build_context_packet(fields=standard_context_fields())
        data_pkt = build_data_packet(
            payload=encode_iq_float32([1 + 1j, 2 - 2j, 3 + 0j, 0 + 4j])
        )
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            deadline = time.monotonic() + 3.0
            while not emitted and time.monotonic() < deadline:
                sender.sendto(ctx_pkt, (host, port))
                sender.sendto(data_pkt, (host, port))
                time.sleep(0.05)
        assert emitted, "pipeline emitted no unit from loopback traffic"
        assert emitted[0]["samples"].dtype == np.complex64
    finally:
        pipeline.stop()
        thread.join(timeout=5.0)
    assert not thread.is_alive(), "run() did not join cleanly after stop()"


# --- A: a raising final flush must still join Thread A ---------------------


def test_flush_error_in_finally_still_stops_source() -> None:
    """If an emit raises during the final flush_all, run() must still call
    source.stop() (join Thread A) and re-raise — not leak the source thread."""
    stop = threading.Event()
    raw_queue = BoundedRawQueue(maxsize=64)
    interpreter = Interpreter()

    def boom_emit(_unit: dict) -> None:
        raise RuntimeError("emit failed during the final flush")

    # huge window + huge age => no flush during the loop; both streams' windows
    # stay open until flush_all in run()'s finally, where boom_emit raises.
    router = BufferRouter(emit=boom_emit, max_samples=1_000_000, max_age_s=1e9)
    source = _StopTrackingSource()
    pipeline = Pipeline(
        source, raw_queue, interpreter, router, stop, poll_interval=0.02
    )

    for sid in (1, 2):
        raw_queue.put(
            build_context_packet(fields=standard_context_fields(), stream_id=sid)
        )
        raw_queue.put(build_data_packet(payload=TWO_SAMPLES, counter=0, stream_id=sid))
    raw_queue.put(SHUTDOWN)

    error: dict = {}

    def run() -> None:
        try:
            pipeline.run()
        except BaseException as exc:  # noqa: BLE001 - capture for assertion
            error["exc"] = exc

    thread = threading.Thread(
        target=run, name="pipeline-thread-B", daemon=True
    )
    thread.start()
    thread.join(timeout=5.0)
    assert not thread.is_alive(), "run() hung when the final flush raised"
    assert isinstance(error.get("exc"), RuntimeError), "flush error was not propagated"
    assert source.stopped, "source.stop() was skipped when flush_all raised (leak)"


# --- C: the age trigger under sustained traffic (kills the deleted-poll mutant)


def test_age_flush_under_sustained_traffic() -> None:
    """Under traffic the queue is never empty, so the None-timeout branch never
    runs; the trailing maybe_flush_on_age() after each item is the ONLY live age
    path. Deleting it drops this from >=2 emits to exactly 1 (final flush)."""
    emitted: list = []
    lock = threading.Lock()

    def emit(unit: dict) -> None:
        with lock:
            emitted.append(unit)

    stop = threading.Event()
    raw_queue = BoundedRawQueue(maxsize=4096)
    interpreter = Interpreter()
    # size never triggers; age (0.05 s) is the only mid-stream flush
    router = BufferRouter(emit=emit, max_samples=10_000_000, max_age_s=0.05)
    # poll_interval huge so the None-timeout branch can never fire under traffic
    pipeline = Pipeline(
        _NoopSource(), raw_queue, interpreter, router, stop, poll_interval=5.0
    )

    ctx_pkt = build_context_packet(fields=standard_context_fields())

    def producer() -> None:
        raw_queue.put(ctx_pkt)
        deadline = time.monotonic() + 0.3
        counter = 1
        while time.monotonic() < deadline:
            raw_queue.put(
                build_data_packet(payload=TWO_SAMPLES, counter=counter & 0xF)
            )
            counter += 1
            time.sleep(0.005)  # ~5 ms: queue never times out (< 5 s poll)
        raw_queue.put(SHUTDOWN)

    prod = threading.Thread(target=producer, name="producer", daemon=True)
    thread = _run_in_thread(pipeline)
    prod.start()
    thread.join(timeout=JOIN_TIMEOUT_S)
    assert not thread.is_alive(), "pipeline did not finish under sustained traffic"
    prod.join(timeout=5.0)

    with lock:
        count = len(emitted)
        first_num_samples = emitted[0]["num_samples"] if emitted else None
    assert count >= 2, (
        f"expected >=2 emits (mid-stream age flushes + final), got {count}; "
        "the trailing maybe_flush_on_age() is the only live age path"
    )
    # a mid-stream age flush emits a small window, far below max_samples
    assert first_num_samples is not None and first_num_samples < 10_000_000


# --- G (library): Pipeline rejects a non-positive poll interval -----------


def test_pipeline_rejects_nonpositive_poll_interval() -> None:
    kwargs = dict(
        source=_NoopSource(),
        raw_queue=BoundedRawQueue(maxsize=8),
        interpreter=Interpreter(),
        router=BufferRouter(emit=lambda _u: None, max_samples=10, max_age_s=1e9),
        stop=threading.Event(),
    )
    with pytest.raises(ValueError):
        Pipeline(**kwargs, poll_interval=0)
    with pytest.raises(ValueError):
        Pipeline(**kwargs, poll_interval=-1.0)


# --- B/D/E/G(CLI): main() and the _build_runtime builder ------------------


def test_main_live_bind_failure_exits_nonzero_without_hanging() -> None:
    """B: a live bind onto an occupied UDP port must exit nonzero, not hang."""
    from sceptre_pipeline.__main__ import main

    blocker = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    blocker.bind(("127.0.0.1", 0))
    port = blocker.getsockname()[1]

    result: dict = {}

    def run() -> None:
        result["code"] = main(
            ["--live", "--host", "127.0.0.1", "--port", str(port)]
        )

    thread = threading.Thread(target=run, name="cli-live-bindfail", daemon=True)
    try:
        thread.start()
        thread.join(timeout=JOIN_TIMEOUT_S)
        assert not thread.is_alive(), "main() hung on a live bind failure"
        assert result["code"] != 0, "a live bind failure exited 0"
    finally:
        blocker.close()


def test_main_replay_missing_file_exits_nonzero(tmp_path) -> None:
    """E: a failed replay (missing file) exits nonzero and does not hang."""
    from sceptre_pipeline.__main__ import main

    missing = tmp_path / "nope.pkl"
    result: dict = {}

    def run() -> None:
        result["code"] = main(["--replay", str(missing)])

    thread = threading.Thread(target=run, name="cli-replay-missing", daemon=True)
    thread.start()
    thread.join(timeout=JOIN_TIMEOUT_S)
    assert not thread.is_alive(), "main() hung on a missing replay file"
    assert result["code"] != 0, "a failed replay exited 0"


def test_cli_rejects_nonpositive_poll_interval() -> None:
    """G (CLI): --poll-interval 0 is a usage error (exit 2), before construction."""
    from sceptre_pipeline.__main__ import main

    with pytest.raises(SystemExit) as exc:
        main(["--replay", "foo.pkl", "--poll-interval", "0"])
    assert exc.value.code == 2


def test_build_runtime_live_record_default_is_bounded_recorder() -> None:
    """D(1): --live --record (no path) => a bounded Recorder attached to the
    LiveSource, capped at DEFAULT_LIVE_RECORD_MAX_BYTES."""
    from sceptre_pipeline.__main__ import _build_runtime, build_parser
    from sceptre_pipeline.recorder import DEFAULT_LIVE_RECORD_MAX_BYTES, Recorder

    args = build_parser().parse_args(
        ["--live", "--host", "127.0.0.1", "--port", "5000", "--record"]
    )
    rt = _build_runtime(args)
    assert isinstance(rt.recorder, Recorder)
    assert rt.recorder._max_bytes == DEFAULT_LIVE_RECORD_MAX_BYTES
    assert rt.source._recorder is rt.recorder, "recorder not attached to LiveSource"


def test_build_runtime_record_tristate_paths(tmp_path) -> None:
    """D(2,3): tri-state --record — explicit PATH, bare (default path), absent."""
    from sceptre_pipeline.__main__ import _build_runtime, build_parser
    from sceptre_pipeline.recorder import Recorder

    # explicit path honored verbatim
    explicit = tmp_path / "cap.pkl"
    args = build_parser().parse_args(
        ["--live", "--host", "127.0.0.1", "--port", "5000", "--record", str(explicit)]
    )
    rt = _build_runtime(args)
    assert isinstance(rt.recorder, Recorder)
    assert rt.record_path == explicit

    # bare --record => auto default path under cwd/recordings
    import os

    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        args = build_parser().parse_args(
            ["--live", "--host", "127.0.0.1", "--port", "5000", "--record"]
        )
        rt = _build_runtime(args)
    finally:
        os.chdir(cwd)
    assert rt.record_path is not None
    assert rt.record_path.parent == tmp_path / "recordings"

    # no --record => no recorder, no path
    args = build_parser().parse_args(
        ["--live", "--host", "127.0.0.1", "--port", "5000"]
    )
    rt = _build_runtime(args)
    assert rt.recorder is None
    assert rt.record_path is None
    assert rt.source._recorder is None


def test_build_runtime_replay_has_no_recorder() -> None:
    """D(4): the replay path wires a ReplaySource and no recorder."""
    from sceptre_pipeline.__main__ import _build_runtime, build_parser

    args = build_parser().parse_args(["--replay", "foo.pkl"])
    rt = _build_runtime(args)
    assert isinstance(rt.source, ReplaySource)
    assert rt.recorder is None
    assert rt.record_path is None
