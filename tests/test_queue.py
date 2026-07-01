"""Stage 1: BoundedRawQueue drop-oldest semantics + SHUTDOWN sentinel."""

from __future__ import annotations

import threading
import time

from sceptre_pipeline.queues import SHUTDOWN, BoundedRawQueue


def test_fifo_order() -> None:
    q = BoundedRawQueue(maxsize=8)
    for i in range(5):
        q.put(i)
    assert [q.get(timeout=0.1) for _ in range(5)] == [0, 1, 2, 3, 4]


def test_overflow_evicts_oldest_counts_dropped_keeps_newest() -> None:
    q = BoundedRawQueue(maxsize=3)
    for i in range(5):
        q.put(i)
    assert q.dropped == 2
    assert [q.get(timeout=0.1) for _ in range(3)] == [2, 3, 4]
    assert q.get(timeout=0.05) is None  # queue now empty


def test_get_timeout_returns_none_promptly() -> None:
    q = BoundedRawQueue(maxsize=3)
    t0 = time.monotonic()
    assert q.get(timeout=0.05) is None
    assert time.monotonic() - t0 < 1.0


def test_get_wakes_on_put_from_another_thread() -> None:
    q = BoundedRawQueue(maxsize=3)

    def producer() -> None:
        time.sleep(0.05)
        q.put(b"payload")

    thread = threading.Thread(target=producer)
    thread.start()
    try:
        assert q.get(timeout=2.0) == b"payload"
    finally:
        thread.join()


def test_shutdown_sentinel_is_distinct_from_timeout_none() -> None:
    assert SHUTDOWN is not None
    q = BoundedRawQueue(maxsize=2)
    q.put(SHUTDOWN)
    assert q.get(timeout=0.1) is SHUTDOWN


def test_dropped_starts_at_zero_and_len_tracks_contents() -> None:
    q = BoundedRawQueue(maxsize=4)
    assert q.dropped == 0
    assert len(q) == 0
    q.put(b"a")
    q.put(b"b")
    assert len(q) == 2
