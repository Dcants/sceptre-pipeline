"""In-memory raw-packet recorder with pickle persistence.

The on-disk schema is byte-compatible with the captures written by
``receiver/recieve_udp.py``::

    {"start_time_ns": int, "duration_seconds": float,
     "packets": [(ts_ns, ip, port, payload_bytes), ...]}

Pickle is the mandated capture format (existing recordings must keep loading);
only locally produced captures are ever written or read.
"""

from __future__ import annotations

import logging
import pickle
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_LIVE_RECORD_MAX_BYTES = 512 * 1024 * 1024
"""Default hard cap for indefinite live ``--record`` sessions (Stage 4 CLI).

An open-ended live session must not grow memory without limit, so the CLI
constructs its Recorder with this cap unless a flag overrides it.
Duration-bounded captures (``receiver/recieve_udp.py``) may keep the
unbounded default."""


def default_recording_path(root: Path | str) -> Path:
    """Return ``root/recordings/udp_capture_<timestamp>.pkl``, creating the dir."""
    recordings = Path(root) / "recordings"
    recordings.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return recordings / f"udp_capture_{timestamp}.pkl"


class Recorder:
    """Accumulate ``(ts_ns, ip, port, payload)`` packets in memory.

    Bounded by a hard cap, NOT a ring: recordings must stay complete from the
    start, so when ``max_packets`` or ``max_bytes`` is reached the recorder
    stops appending (and warns once) rather than evicting old packets.

    Single-writer by design: ``append`` is called only from the source thread;
    ``save``/reads happen after the source has stopped.
    """

    def __init__(
        self, max_packets: int | None = None, max_bytes: int | None = None
    ) -> None:
        self._max_packets = max_packets
        self._max_bytes = max_bytes
        self._packets: list[tuple[int, str, int, bytes]] = []
        self._total_bytes = 0
        self._capped = False

    def append(self, ts_ns: int, addr: tuple[str, int], data: bytes) -> bool:
        """Record one packet; O(1). Returns False once the hard cap is hit."""
        if self._capped:
            return False
        if (
            self._max_packets is not None and len(self._packets) >= self._max_packets
        ) or (
            self._max_bytes is not None
            and self._total_bytes + len(data) > self._max_bytes
        ):
            self._capped = True
            logger.warning(
                "Recorder cap reached (%d packets, %d bytes); "
                "dropping all further packets",
                len(self._packets),
                self._total_bytes,
            )
            return False
        self._packets.append((ts_ns, addr[0], addr[1], data))
        self._total_bytes += len(data)
        return True

    @property
    def packets(self) -> list[tuple[int, str, int, bytes]]:
        """The recorded packets (treat as read-only)."""
        return self._packets

    @property
    def capped(self) -> bool:
        return self._capped

    @property
    def total_bytes(self) -> int:
        return self._total_bytes

    def __len__(self) -> int:
        return len(self._packets)

    def save(
        self, path: Path | str, start_time_ns: int, duration_seconds: float
    ) -> Path:
        """Pickle the capture to ``path`` (byte-compatible with recieve_udp.py)."""
        capture = {
            "start_time_ns": start_time_ns,
            "duration_seconds": duration_seconds,
            "packets": self._packets,
        }
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as file:
            pickle.dump(capture, file, protocol=pickle.HIGHEST_PROTOCOL)
        return path
