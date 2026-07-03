"""Packet sources (Thread A): live UDP socket or offline pickle replay.

A source pushes raw packet bytes onto the bounded ``raw_queue`` — dumb and
fast; it never parses. ``ReplaySource`` additionally enqueues ``SHUTDOWN`` at
end-of-stream so the consumer (Thread B) can flush its final window and exit.
"""

from __future__ import annotations

import logging
import pickle
import socket
import sys
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path

from .queues import SHUTDOWN, BoundedRawQueue
from .recorder import Recorder

logger = logging.getLogger(__name__)

_JOIN_TIMEOUT_S = 2.0

DEFAULT_RCVBUF_BYTES = 8 * 1024 * 1024
"""Default SO_RCVBUF request. The OS default (~64 KiB Windows / ~208 KiB
Linux) holds only 8-25 packets at the stream's ~5 MB/s, so any Thread-B stall
drops packets in the kernel where ``raw_queue.dropped`` cannot count them."""


class PacketSource(ABC):
    """A producer of raw packet bytes running in its own thread."""

    @abstractmethod
    def start(self) -> None:
        """Start producing packets on a background thread."""

    @abstractmethod
    def stop(self) -> None:
        """Signal the source to stop and join its thread."""


class LiveSource(PacketSource):
    """Receive UDP datagrams and fan them out to two independent sinks:

    1. ``raw_queue.put(data)`` — bounded and lossy (the live path), and
    2. ``recorder.append(...)`` — lossless up to its hard cap, if provided.

    A ``raw_queue`` overflow can never cost the recorder a packet.
    """

    def __init__(
        self,
        host: str,
        port: int,
        raw_queue: BoundedRawQueue,
        stop: threading.Event,
        recorder: Recorder | None = None,
        buffer_size: int = 65535,
        rcvbuf_bytes: int = DEFAULT_RCVBUF_BYTES,
    ) -> None:
        self._host = host
        self._port = port
        self._raw_queue = raw_queue
        self._stop = stop
        self._recorder = recorder
        self._buffer_size = buffer_size
        self._rcvbuf_bytes = rcvbuf_bytes
        self._thread: threading.Thread | None = None
        self.ready = threading.Event()
        """Set once the socket is bound (or binding failed); see bound_address."""
        self.bound_address: tuple[str, int] | None = None
        """The actual (host, port) bound — resolves port 0 for tests."""
        self.granted_rcvbuf: int | None = None
        """SO_RCVBUF the kernel actually granted (None until the socket opens)."""

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="sceptre-live-source", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=_JOIN_TIMEOUT_S)

    def _configure_rcvbuf(self, sock: socket.socket) -> int | None:
        """Request a large kernel receive buffer; read back and log the grant.

        Never fails the source: a refused or clamped set logs a warning and
        the loop keeps running with whatever the kernel granted. Linux caps
        the request at ``net.core.rmem_max`` (and reports a doubled value);
        Windows honors it directly.
        """
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self._rcvbuf_bytes)
        except (OSError, OverflowError, TypeError) as exc:
            # CPython raises TypeError/OverflowError (not OSError) for values
            # outside C-int range (>= 2**31)
            logger.warning(
                "could not set SO_RCVBUF to %d: %s", self._rcvbuf_bytes, exc
            )
        granted: int | None = None
        try:
            granted = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        except OSError as exc:
            logger.warning("could not read back SO_RCVBUF: %s", exc)
        # Linux getsockopt reports DOUBLE the effective buffer, so an honored
        # request reads back as 2x; other platforms echo the set value.
        honored_floor = self._rcvbuf_bytes * (
            2 if sys.platform.startswith("linux") else 1
        )
        if granted is not None and granted < honored_floor:
            logger.warning(
                "SO_RCVBUF clamped: requested %d, granted %d "
                "(on Linux, raise net.core.rmem_max to honor the request)",
                self._rcvbuf_bytes,
                granted,
            )
        elif granted is not None:
            logger.info(
                "SO_RCVBUF requested %d, granted %d", self._rcvbuf_bytes, granted
            )
        self.granted_rcvbuf = granted
        return granted

    def _run(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            with sock:
                self._configure_rcvbuf(sock)
                try:
                    sock.bind((self._host, self._port))
                    self.bound_address = sock.getsockname()
                finally:
                    self.ready.set()
                # short timeout so the loop observes the stop Event promptly
                sock.settimeout(0.5)
                logger.info("LiveSource listening on %s:%s", *self.bound_address)
                while not self._stop.is_set():
                    try:
                        data, addr = sock.recvfrom(self._buffer_size)
                    except socket.timeout:
                        continue
                    self._raw_queue.put(data)
                    if self._recorder is not None:
                        self._recorder.append(time.time_ns(), addr, data)
        except OSError:
            logger.exception("LiveSource socket error; source stopped")
        finally:
            if self._recorder is not None:
                logger.info(
                    "LiveSource stopped (queue drops: %d; recorder: %d packets, "
                    "%d bytes, capped=%s)",
                    self._raw_queue.dropped,
                    len(self._recorder),
                    self._recorder.total_bytes,
                    self._recorder.capped,
                )
            else:
                logger.info(
                    "LiveSource stopped (queue drops so far: %d)",
                    self._raw_queue.dropped,
                )


class ReplaySource(PacketSource):
    """Replay a recorded ``.pkl`` capture into the raw queue.

    With ``pace=True``, sleeps by the recorded ``ts_ns`` deltas (interruptible
    via the stop Event). Enqueues ``SHUTDOWN`` when the capture is exhausted.
    """

    def __init__(
        self,
        path: Path | str,
        raw_queue: BoundedRawQueue,
        stop: threading.Event,
        pace: bool = False,
    ) -> None:
        self._path = Path(path)
        self._raw_queue = raw_queue
        self._stop = stop
        self._pace = pace
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="sceptre-replay-source", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=_JOIN_TIMEOUT_S)

    def _run(self) -> None:
        try:
            # pickle is safe here: only locally produced captures with the fixed
            # recorder schema are replayed (see recorder module docstring).
            with self._path.open("rb") as file:
                capture = pickle.load(file)
            packets = capture["packets"]

            prev_ts_ns: int | None = None
            for packet in packets:
                if self._stop.is_set():
                    break
                ts_ns = packet[0]
                if self._pace and prev_ts_ns is not None:
                    delta_s = (ts_ns - prev_ts_ns) / 1e9
                    # Event.wait doubles as an interruptible sleep
                    if delta_s > 0 and self._stop.wait(delta_s):
                        break
                prev_ts_ns = ts_ns
                self._raw_queue.put(packet[3])
        except Exception:
            logger.exception("ReplaySource failed replaying %s", self._path)
        finally:
            # ALWAYS signal end-of-stream — even on a bad/corrupt capture —
            # so Thread B flushes and exits instead of polling forever.
            self._raw_queue.put(SHUTDOWN)
