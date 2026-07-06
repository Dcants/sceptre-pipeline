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


def _run_in_thread(pipeline: Pipeline) -> threading.Thread:
    thread = threading.Thread(target=pipeline.run, name="pipeline-thread-B")
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


def test_process_exception_does_not_kill_thread_b() -> None:
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

    thread = _run_in_thread(pipeline)
    thread.join(timeout=5.0)
    assert not thread.is_alive(), "a raising process() hung / killed Thread B"
    assert interpreter.errors == 1, "the loop did not count the guarded failure"
    assert emitted == []


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

    thread = threading.Thread(target=run, name="cli-main")
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
