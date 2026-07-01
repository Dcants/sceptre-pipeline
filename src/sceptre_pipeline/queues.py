"""Bounded, lossy raw-packet queue shared between the source and interpreter threads."""

from __future__ import annotations

import threading
from collections import deque
from typing import Any

SHUTDOWN: Any = object()
"""End-of-stream sentinel enqueued by a source (e.g. ReplaySource at EOF).

Deliberately distinct from ``None``: ``BoundedRawQueue.get`` returns ``None``
on timeout (the age-poll hook), so the consumer loop can always tell
"nothing arrived, poll the age trigger" apart from "drain, flush and exit".
"""


class BoundedRawQueue:
    """Thread-safe bounded FIFO that drops the OLDEST item when full.

    ``put`` is non-blocking and lossy: on overflow the oldest item is evicted
    and counted in ``dropped``, and the newest is always kept. Deliberately not
    built on ``deque(maxlen=...)``, which drops silently (no count) and gives
    waiting consumers no wake signal.
    """

    def __init__(self, maxsize: int) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be >= 1")
        self._maxsize = maxsize
        self._items: deque[Any] = deque()
        self._cond = threading.Condition()
        self._dropped = 0

    def put(self, item: Any) -> None:
        """Append ``item``; if full, evict the oldest and count the drop."""
        with self._cond:
            if len(self._items) >= self._maxsize:
                self._items.popleft()
                self._dropped += 1
            self._items.append(item)
            self._cond.notify()

    def get(self, timeout: float | None = None) -> Any:
        """Return the next item, or ``None`` if nothing arrives within ``timeout``.

        ``timeout=None`` blocks until an item arrives.
        """
        with self._cond:
            if not self._cond.wait_for(lambda: len(self._items) > 0, timeout):
                return None
            return self._items.popleft()

    @property
    def dropped(self) -> int:
        """Total items evicted by overflow (read under the lock)."""
        with self._cond:
            return self._dropped

    def __len__(self) -> int:
        with self._cond:
            return len(self._items)
