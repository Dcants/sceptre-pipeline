"""Stage 4: runtime wiring — Thread A (source) + the Thread B loop.

``Pipeline`` starts the source on its own thread (Thread A) and runs the
interpret+buffer loop (Thread B) on the CALLING thread. Thread B dequeues raw
datagrams, discriminates the SHUTDOWN sentinel from a None timeout, polls the
age trigger, interprets each datagram, and pushes the resulting record into the
per-stream ``BufferRouter`` — which already owns the emit callback that delivers
finished ``{samples, context, ...}`` units downstream. The Pipeline therefore
takes no emit argument.

Why the loop runs on the caller's thread: a CLI (or a signal handler) can call
``stop()`` from another thread while ``run()`` blocks on the main thread, and
``run()`` unwinds within one ``poll_interval``. The final flush, the Thread-A
join, and the counter log all live in ``run()``'s ``finally`` so they happen on
EVERY exit path — EOF (SHUTDOWN, where ``stop()`` is never called) and
Event-stop (live Ctrl-C) alike. ``IngestBuffer.flush`` no-ops on an empty
window, so ``flush_all`` is safe and idempotent on either path.

Sentinel vs timeout: ``queues.SHUTDOWN`` (a distinct object) means "drain and
exit"; ``BoundedRawQueue.get`` returns ``None`` on timeout, meaning "nothing
arrived — poll the age trigger and keep going". Conflating the two would either
exit early or never flush on age, so they are kept strictly separate here.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from .queues import SHUTDOWN

if TYPE_CHECKING:
    from .buffer import BufferRouter
    from .interpreter import Interpreter
    from .queues import BoundedRawQueue
    from .sources import PacketSource

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL_S = 0.1


class Pipeline:
    """Wire a source (Thread A) to the interpret+buffer loop (Thread B).

    ``stop`` MUST be the same ``threading.Event`` the source was constructed
    with, so a single ``set()`` unwinds both the source's socket/replay loop and
    this loop. The router already holds the emit callback, so no emit is passed.
    """

    def __init__(
        self,
        source: "PacketSource",
        raw_queue: "BoundedRawQueue",
        interpreter: "Interpreter",
        router: "BufferRouter",
        stop: threading.Event,
        poll_interval: float = DEFAULT_POLL_INTERVAL_S,
    ) -> None:
        self._source = source
        self._raw_queue = raw_queue
        self._interpreter = interpreter
        self._router = router
        self._stop = stop
        self._poll_interval = poll_interval

    def run(self) -> None:
        """Run Thread B on the calling thread until SHUTDOWN or ``stop()``.

        Starts Thread A, then loops. On EVERY exit the ``finally`` flushes the
        final partial window of every stream, joins Thread A, and logs the final
        drop/error counters.
        """
        source = self._source
        raw_queue = self._raw_queue
        interpreter = self._interpreter
        router = self._router
        stop = self._stop

        source.start()
        try:
            while not stop.is_set():
                item = raw_queue.get(timeout=self._poll_interval)
                if item is SHUTDOWN:  # end-of-stream: drain and exit
                    break
                if item is None:  # timeout: poll the age trigger only
                    router.maybe_flush_on_age()
                    continue
                try:
                    # the interpreter guards internally and does not raise by
                    # design, but a bug in the guard itself must not kill Thread
                    # B (Stage 2.75 defense-in-depth)
                    record = interpreter.process(item)
                except Exception:
                    interpreter.errors += 1
                    continue
                if record:
                    router.push(record)
                router.maybe_flush_on_age()
        finally:
            router.flush_all()  # final partial window of EVERY stream
            source.stop()  # joins Thread A (the source's own join timeout)
            logger.info(
                "pipeline stopped: queue_dropped=%d interpreter_errors=%d "
                "interpreter_dropped=%d router_dropped=%d",
                raw_queue.dropped,
                interpreter.errors,
                interpreter.dropped,
                router.dropped,
            )

    def stop(self) -> None:
        """Signal Thread B (and the source) to stop; idempotent, any-thread safe.

        Only sets the Event — ``run()``'s ``finally`` performs the flush, the
        Thread-A join, and the counter log. ``Event.set()`` is idempotent, so
        repeated or concurrent calls (e.g. a KeyboardInterrupt handler) are safe.
        """
        self._stop.set()
