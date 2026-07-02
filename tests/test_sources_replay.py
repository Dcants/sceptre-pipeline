"""Stage 1: ReplaySource replay, Recorder round-trip/caps, LiveSource fan-out.

Pickle use in this module is safe: it only writes and reloads captures in
pytest tmp dirs plus the two project-tracked fixtures (fixed schema).
"""

from __future__ import annotations

import importlib.util
import logging
import pickle
import socket
import threading
import time
from pathlib import Path

from sceptre_pipeline.queues import SHUTDOWN, BoundedRawQueue
from sceptre_pipeline.recorder import Recorder, default_recording_path
from sceptre_pipeline.sources import LiveSource, PacketSource, ReplaySource


def _drain_until_shutdown(q: BoundedRawQueue, limit: int = 5000) -> list:
    items = []
    for _ in range(limit):
        item = q.get(timeout=2.0)
        assert item is not None, "queue timed out before SHUTDOWN arrived"
        if item is SHUTDOWN:
            return items
        items.append(item)
    raise AssertionError("SHUTDOWN never seen within item limit")


# --- ReplaySource ---------------------------------------------------------


def test_replay_source_is_a_packet_source() -> None:
    assert issubclass(ReplaySource, PacketSource)
    assert issubclass(LiveSource, PacketSource)


def test_replay_full_capture_then_shutdown(single_frequency_path, load_capture) -> None:
    q = BoundedRawQueue(maxsize=4096)
    stop = threading.Event()
    source = ReplaySource(single_frequency_path, q, stop)
    source.start()
    items = _drain_until_shutdown(q)
    source.stop()

    assert len(items) == 3072
    assert all(isinstance(item, bytes) for item in items)
    capture = load_capture(single_frequency_path)
    assert items[0] == capture["packets"][0][3]
    assert items[-1] == capture["packets"][-1][3]
    assert q.dropped == 0


def test_replay_pace_sleeps_are_interrupted_by_stop(tmp_path) -> None:
    capture = {
        "start_time_ns": 0,
        "duration_seconds": 60.0,
        "packets": [
            (0, "127.0.0.1", 5000, b"first"),
            (30_000_000_000, "127.0.0.1", 5000, b"second"),  # 30 s later
        ],
    }
    path = tmp_path / "paced.pkl"
    with path.open("wb") as file:
        pickle.dump(capture, file, protocol=pickle.HIGHEST_PROTOCOL)

    q = BoundedRawQueue(maxsize=8)
    stop = threading.Event()
    source = ReplaySource(path, q, stop, pace=True)
    t0 = time.monotonic()
    source.start()
    assert q.get(timeout=2.0) == b"first"

    stop.set()
    # If pacing used an uninterruptible sleep, no SHUTDOWN would arrive for 30 s.
    assert q.get(timeout=2.0) is SHUTDOWN
    source.stop()
    assert time.monotonic() - t0 < 10.0


def test_replay_missing_or_corrupt_capture_still_signals_shutdown(
    tmp_path, caplog
) -> None:
    """A dead source thread must never leave the consumer polling forever."""
    corrupt = tmp_path / "corrupt.pkl"
    corrupt.write_bytes(b"this is not a pickle")

    for bad_path in (tmp_path / "missing.pkl", corrupt):
        q = BoundedRawQueue(maxsize=8)
        source = ReplaySource(bad_path, q, threading.Event())
        with caplog.at_level(logging.ERROR, logger="sceptre_pipeline.sources"):
            source.start()
            assert q.get(timeout=2.0) is SHUTDOWN, f"no SHUTDOWN for {bad_path.name}"
        source.stop()

    assert sum(
        "failed replaying" in record.getMessage() for record in caplog.records
    ) == 2


# --- Recorder --------------------------------------------------------------


def test_recorder_roundtrip_matches_source_recording_structure(
    tmp_path, single_frequency_path, load_capture
) -> None:
    source_capture = load_capture(single_frequency_path)
    recorder = Recorder()
    for ts_ns, ip, port, data in source_capture["packets"][:10]:
        recorder.append(ts_ns, (ip, port), data)

    out = tmp_path / "roundtrip.pkl"
    recorder.save(
        out,
        source_capture["start_time_ns"],
        source_capture["duration_seconds"],
    )

    reloaded = load_capture(out)
    assert set(reloaded) == set(source_capture)
    assert reloaded["start_time_ns"] == source_capture["start_time_ns"]
    assert reloaded["duration_seconds"] == source_capture["duration_seconds"]
    assert len(reloaded["packets"]) == 10
    # a sample packet tuple is identical to the source recording's
    assert reloaded["packets"][0] == source_capture["packets"][0]


def test_recorder_max_packets_cap_stops_appending_and_logs_once(caplog) -> None:
    recorder = Recorder(max_packets=3)
    with caplog.at_level(logging.WARNING, logger="sceptre_pipeline.recorder"):
        for i in range(10):
            recorder.append(i, ("127.0.0.1", 5000), b"x")

    assert len(recorder) == 3
    # not a ring: the FIRST packets are kept, later ones dropped
    assert [p[0] for p in recorder.packets] == [0, 1, 2]
    cap_warnings = [r for r in caplog.records if "cap" in r.getMessage().lower()]
    assert len(cap_warnings) == 1


def test_recorder_max_bytes_cap() -> None:
    recorder = Recorder(max_bytes=25)
    for i in range(10):
        recorder.append(i, ("127.0.0.1", 5000), b"0123456789")  # 10 B each
    assert len(recorder) == 2  # third packet would exceed 25 bytes


def test_default_recording_path(tmp_path) -> None:
    path = default_recording_path(tmp_path)
    assert path.parent == tmp_path / "recordings"
    assert path.parent.is_dir()  # created on demand
    assert path.name.startswith("udp_capture_")
    assert path.suffix == ".pkl"


# --- LiveSource fan-out decoupling -----------------------------------------


def test_live_source_recorder_unaffected_by_queue_overflow() -> None:
    """Forced raw_queue overflow must not reduce recorder contents."""
    q = BoundedRawQueue(maxsize=2)
    recorder = Recorder()
    stop = threading.Event()
    source = LiveSource("127.0.0.1", 0, q, stop, recorder=recorder)
    source.start()
    try:
        assert source.ready.wait(2.0), "LiveSource never bound its socket"
        host, port = source.bound_address

        payloads = [f"pkt{i:02d}".encode() for i in range(20)]
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            for payload in payloads:
                sender.sendto(payload, (host, port))

        deadline = time.monotonic() + 2.0
        while len(recorder) < len(payloads) and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        source.stop()

    received = len(recorder)
    # loopback should deliver comfortably more than the queue can hold
    assert received >= 10
    # every received packet was recorded: recorder count == queue drops + queue depth
    assert received == q.dropped + len(q)
    assert q.dropped > 0, "test did not force an overflow"
    recorded_payloads = {p[3] for p in recorder.packets}
    assert recorded_payloads <= set(payloads)
    ts_ns, ip, sender_port, data = recorder.packets[0]
    assert ip == "127.0.0.1"
    assert isinstance(ts_ns, int)


# --- receiver/recieve_udp.py refactor --------------------------------------


def test_receive_udp_defaults_into_project_recordings_dir(recordings_dir) -> None:
    script = recordings_dir.parent / "receiver" / "recieve_udp.py"
    spec = importlib.util.spec_from_file_location("recieve_udp", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    path = module.default_output_filename()
    assert path.parent == recordings_dir
    assert path.name.startswith("udp_capture_")
    assert path.suffix == ".pkl"
