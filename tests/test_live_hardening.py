"""Stage 1 addendum: live-path hardening (kernel receive buffer + bounded recording).

Part 1 — SO_RCVBUF: ``LiveSource`` requests a multi-MiB kernel receive buffer
(constructor-configurable), reads back and logs the granted size, and a
refused/clamped set logs a warning without crashing. Rationale: the OS default
(~64 KiB Windows / ~208 KiB Linux) holds only 8-25 packets at ~5 MB/s, so any
Thread-B stall drops packets in the kernel where ``BoundedRawQueue.dropped``
cannot see them.

Part 2 — bounded live recording: ``recorder.DEFAULT_LIVE_RECORD_MAX_BYTES`` is
the single source of truth for the Stage 4 ``--record`` default cap. The CLI
guard tests bind to ``sceptre_pipeline.__main__`` via ``pytest.importorskip``
inside each test, so they self-activate once Stage 4 lands and skip cleanly
until then (same convention as ``test_buffer_flush_contract.py``).
"""

from __future__ import annotations

import logging
import socket
import sys
import threading

import pytest

from sceptre_pipeline import recorder as recorder_mod
from sceptre_pipeline.queues import BoundedRawQueue
from sceptre_pipeline.recorder import Recorder
from sceptre_pipeline.sources import DEFAULT_RCVBUF_BYTES, LiveSource

SOURCES_LOGGER = "sceptre_pipeline.sources"


class FakeSocket:
    """Records setsockopt calls; configurable set/get failure and granted size."""

    def __init__(
        self,
        granted: int | None = None,
        set_raises: bool = False,
        get_raises: bool = False,
    ) -> None:
        self.setsockopt_calls: list[tuple[int, int, int]] = []
        self._granted = granted
        self._set_raises = set_raises
        self._get_raises = get_raises

    def setsockopt(self, level: int, optname: int, value: int) -> None:
        self.setsockopt_calls.append((level, optname, value))
        if self._set_raises:
            raise OSError("permission denied")

    def getsockopt(self, level: int, optname: int) -> int:
        if self._get_raises:
            raise OSError("getsockopt failed")
        if self._granted is not None:
            return self._granted
        # echo the last successfully requested value (Windows-like behavior)
        return self.setsockopt_calls[-1][2] if self.setsockopt_calls else 0


def _live_source(**kwargs) -> LiveSource:
    return LiveSource(
        "127.0.0.1", 0, BoundedRawQueue(maxsize=64), threading.Event(), **kwargs
    )


# --- Part 1: SO_RCVBUF ------------------------------------------------------


def test_default_rcvbuf_is_multi_mib() -> None:
    assert DEFAULT_RCVBUF_BYTES >= 4 * 1024 * 1024


def test_rcvbuf_option_applied_and_granted_size_logged(caplog, monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "win32")  # echo semantics match FakeSocket
    source = _live_source(rcvbuf_bytes=123_456)
    sock = FakeSocket()

    with caplog.at_level(logging.INFO, logger=SOURCES_LOGGER):
        granted = source._configure_rcvbuf(sock)

    assert (socket.SOL_SOCKET, socket.SO_RCVBUF, 123_456) in sock.setsockopt_calls
    assert granted == 123_456
    assert source.granted_rcvbuf == 123_456
    logged = [r for r in caplog.records if "SO_RCVBUF" in r.getMessage()]
    assert logged, "granted SO_RCVBUF size was not logged"
    assert "123456" in logged[0].getMessage().replace(",", "").replace("_", "")


def test_rcvbuf_refused_set_warns_and_continues(caplog) -> None:
    source = _live_source()
    sock = FakeSocket(granted=212_992, set_raises=True)

    with caplog.at_level(logging.WARNING, logger=SOURCES_LOGGER):
        granted = source._configure_rcvbuf(sock)  # must not raise

    assert granted == 212_992
    assert source.granted_rcvbuf == 212_992
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("SO_RCVBUF" in r.getMessage() for r in warnings)


def test_rcvbuf_clamped_below_request_warns(caplog) -> None:
    source = _live_source(rcvbuf_bytes=8 * 1024 * 1024)
    sock = FakeSocket(granted=416_768)  # Linux-style clamp to 2*rmem_max

    with caplog.at_level(logging.WARNING, logger=SOURCES_LOGGER):
        source._configure_rcvbuf(sock)

    warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "SO_RCVBUF" in r.getMessage()
    ]
    assert warnings, "clamped SO_RCVBUF did not log a warning"
    message = warnings[0].getMessage().replace(",", "").replace("_", "")
    assert "8388608" in message and "416768" in message


def test_rcvbuf_huge_value_never_kills_source(caplog) -> None:
    """CPython setsockopt raises TypeError (not OSError) for values >= 2**31;
    the source must warn and keep running, and ready must still fire."""
    stop = threading.Event()
    source = LiveSource(
        "127.0.0.1",
        0,
        BoundedRawQueue(maxsize=64),
        stop,
        rcvbuf_bytes=2 * 1024 * 1024 * 1024,
    )
    with caplog.at_level(logging.WARNING, logger=SOURCES_LOGGER):
        source.start()
        try:
            assert source.ready.wait(2.0), "huge rcvbuf request killed the source"
            assert source.bound_address is not None
        finally:
            source.stop()

    assert any("SO_RCVBUF" in r.getMessage() for r in caplog.records)


def test_rcvbuf_linux_doubled_readback_clamp_detected(caplog, monkeypatch) -> None:
    """Linux getsockopt reports DOUBLE the effective buffer: a clamp at
    rmem_max in [request/2, request) reads back >= request and must still warn."""
    monkeypatch.setattr(sys, "platform", "linux")
    source = _live_source(rcvbuf_bytes=8 * 1024 * 1024)
    sock = FakeSocket(granted=12_582_912)  # 2 * 6 MiB: effective 6 MiB < 8 MiB

    with caplog.at_level(logging.WARNING, logger=SOURCES_LOGGER):
        source._configure_rcvbuf(sock)

    assert any(
        r.levelno == logging.WARNING and "SO_RCVBUF" in r.getMessage()
        for r in caplog.records
    ), "doubled-readback clamp went unwarned"


def test_rcvbuf_linux_honored_doubled_readback_logs_info(caplog, monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    source = _live_source(rcvbuf_bytes=8 * 1024 * 1024)
    sock = FakeSocket(granted=16_777_216)  # 2 * 8 MiB: request fully honored

    with caplog.at_level(logging.INFO, logger=SOURCES_LOGGER):
        source._configure_rcvbuf(sock)

    assert not any(r.levelno == logging.WARNING for r in caplog.records)
    assert any(
        r.levelno == logging.INFO and "SO_RCVBUF" in r.getMessage()
        for r in caplog.records
    )


def test_rcvbuf_getsockopt_failure_survives(caplog) -> None:
    source = _live_source()
    sock = FakeSocket(get_raises=True)

    with caplog.at_level(logging.WARNING, logger=SOURCES_LOGGER):
        granted = source._configure_rcvbuf(sock)  # must not raise

    assert granted is None
    assert source.granted_rcvbuf is None


def test_live_source_real_socket_applies_rcvbuf() -> None:
    stop = threading.Event()
    source = LiveSource("127.0.0.1", 0, BoundedRawQueue(maxsize=64), stop)
    source.start()
    try:
        assert source.ready.wait(2.0), "LiveSource never bound its socket"
        assert source.granted_rcvbuf is not None
        assert source.granted_rcvbuf > 0
        if sys.platform == "win32":
            # Windows honors the request directly
            assert source.granted_rcvbuf >= DEFAULT_RCVBUF_BYTES
    finally:
        source.stop()


def test_live_source_logs_recorder_capped_state_on_stop(caplog) -> None:
    stop = threading.Event()
    recorder = Recorder(max_bytes=recorder_mod.DEFAULT_LIVE_RECORD_MAX_BYTES)
    source = LiveSource(
        "127.0.0.1", 0, BoundedRawQueue(maxsize=64), stop, recorder=recorder
    )
    with caplog.at_level(logging.INFO, logger=SOURCES_LOGGER):
        source.start()
        assert source.ready.wait(2.0)
        source.stop()

    assert any("capped" in r.getMessage() for r in caplog.records)


# --- Part 2: bounded live recording default ---------------------------------


def test_default_live_record_cap_is_bounded_and_sane() -> None:
    cap = recorder_mod.DEFAULT_LIVE_RECORD_MAX_BYTES
    assert isinstance(cap, int)
    # bounded (non-None) but big enough for a useful live capture
    assert 64 * 1024 * 1024 <= cap <= 4 * 1024 * 1024 * 1024


def test_recorder_accepts_default_live_cap() -> None:
    recorder = Recorder(max_bytes=recorder_mod.DEFAULT_LIVE_RECORD_MAX_BYTES)
    assert recorder.append(0, ("127.0.0.1", 5000), b"x" * 8192)
    assert not recorder.capped


# --- Stage 4 CLI guards (self-activate when sceptre_pipeline.__main__ lands) --


def test_cli_record_default_is_bounded() -> None:
    main_mod = pytest.importorskip("sceptre_pipeline.__main__")
    parser = main_mod.build_parser()
    args = parser.parse_args(
        ["--live", "--host", "127.0.0.1", "--port", "5000", "--record"]
    )
    assert args.record_max_bytes == recorder_mod.DEFAULT_LIVE_RECORD_MAX_BYTES
    assert args.record_max_bytes is not None and args.record_max_bytes > 0


def test_cli_record_max_bytes_flag_overrides_default() -> None:
    main_mod = pytest.importorskip("sceptre_pipeline.__main__")
    parser = main_mod.build_parser()
    args = parser.parse_args(
        [
            "--live",
            "--host",
            "127.0.0.1",
            "--port",
            "5000",
            "--record",
            "--record-max-bytes",
            str(1024 * 1024),
        ]
    )
    assert args.record_max_bytes == 1024 * 1024
