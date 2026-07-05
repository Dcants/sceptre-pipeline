"""Stage 2.75: ``Interpreter.process`` never propagates — Thread B must
survive whatever a live socket delivers.

Every malformed/foreign datagram returns ``None`` and lands on exactly one
counter: ``errors`` (parsing raised) or ``dropped`` (policy drop: no context
yet, unsupported format, unknown packet type, stream flood). Logging is
rate-limited so a flood cannot flood the log.
"""

from __future__ import annotations

import logging
import random
import struct

from conftest import (
    build_context_packet,
    build_data_packet,
    encode_iq_float32,
    encode_payload_format_u64,
    standard_context_fields,
)

from sceptre_pipeline.interpreter import Interpreter

ONE_SAMPLE = encode_iq_float32([complex(1.0, -1.0)])


def _primed() -> Interpreter:
    interp = Interpreter()
    assert interp.process(
        build_context_packet(fields=standard_context_fields())
    )["type"] == "context"
    return interp


def _counts(interp: Interpreter) -> tuple[int, int]:
    return interp.errors, interp.dropped


# --- enumerated malformed inputs: return None, count, never raise -------------


def test_short_packet_counts_as_error() -> None:
    interp = _primed()
    assert interp.process(b"\x1c") is None
    assert _counts(interp) == (1, 0)


def test_truncated_header_counts_as_error() -> None:
    interp = _primed()
    raw = build_data_packet(payload=ONE_SAMPLE)
    assert interp.process(raw[:10]) is None  # word0 valid, header cut short
    assert _counts(interp) == (1, 0)


def test_context_truncated_before_cif_counts_as_error() -> None:
    interp = Interpreter()
    raw = build_context_packet(fields=standard_context_fields())
    hdr_only = raw[:28]  # header intact, CIF missing
    assert interp.process(hdr_only) is None
    assert _counts(interp) == (1, 0)


def test_context_truncated_inside_field_counts_as_error() -> None:
    interp = Interpreter()
    raw = build_context_packet(fields=standard_context_fields())
    assert interp.process(raw[:-5]) is None  # last field cut short
    assert _counts(interp) == (1, 0)


def test_unknown_packet_type_counts_as_dropped() -> None:
    interp = _primed()
    raw = bytearray(build_data_packet(payload=ONE_SAMPLE))
    raw[0] = (0x2 << 4) | (raw[0] & 0x0F)  # packet type 2
    assert interp.process(bytes(raw)) is None
    assert _counts(interp) == (0, 1)


def test_data_before_context_counts_as_dropped() -> None:
    interp = Interpreter()
    assert interp.process(build_data_packet(payload=ONE_SAMPLE)) is None
    assert _counts(interp) == (0, 1)


# --- unsupported-but-valid format: only the affected data is dropped ----------


def test_unsupported_context_emitted_data_dropped_then_reenabled() -> None:
    interp = Interpreter()

    # the unsupported context is still emitted as a context record, so
    # downstream knows the stream is present-but-unsupported
    unsupported = standard_context_fields(
        payload_format_u64=encode_payload_format_u64(real_complex_code=0b10)
    )
    record = interp.process(build_context_packet(fields=unsupported, counter=0))
    assert record["type"] == "context"
    assert record["context"]["supported"] is False
    assert _counts(interp) == (0, 0)

    # its data packets are dropped-and-counted as POLICY drops, not errors
    assert interp.process(build_data_packet(payload=ONE_SAMPLE, counter=1)) is None
    assert interp.process(build_data_packet(payload=ONE_SAMPLE, counter=2)) is None
    assert _counts(interp) == (0, 2)

    # a later supported context re-enables the flow
    supported = interp.process(
        build_context_packet(fields=standard_context_fields(), counter=3)
    )
    assert supported["context"]["supported"] is True
    data = interp.process(build_data_packet(payload=ONE_SAMPLE, counter=4))
    assert data["type"] == "data"
    assert data["data"] == ONE_SAMPLE
    assert _counts(interp) == (0, 2)  # nothing new dropped


# --- datagram-length hardening -------------------------------------------------


def test_oversized_datagram_trims_to_declared_size_via_process() -> None:
    interp = _primed()
    payload = encode_iq_float32([complex(1, 2), complex(3, 4)])
    glued = build_data_packet(payload=payload) + build_data_packet(
        payload=encode_iq_float32([complex(9, 9)]), counter=1
    )
    record = interp.process(glued)
    assert record["type"] == "data"
    assert record["data"] == payload  # second packet's bytes never ingested
    assert _counts(interp) == (0, 0)


def test_undersized_datagram_dropped_and_counted_not_short_read() -> None:
    interp = _primed()
    raw = build_data_packet(payload=encode_iq_float32([complex(1, 2)] * 4))
    assert interp.process(raw[:-6]) is None  # valid flags, truncated payload
    assert _counts(interp) == (1, 0)  # truncation surfaces as a caught error


def test_truncated_packet_does_not_erase_the_gap_signal() -> None:
    """A dropped-for-truncation packet must NOT advance _last_counter: the
    next good packet has to flag the discontinuity, or the buffer would
    stitch across the missing samples with no gap flush."""
    interp = _primed()
    r0 = interp.process(build_data_packet(payload=ONE_SAMPLE, counter=0))
    assert r0["metadata"]["gap_before"] is False

    lost = build_data_packet(payload=ONE_SAMPLE, counter=1)
    assert interp.process(lost[:-6]) is None  # counter 1 truncated -> dropped

    r2 = interp.process(build_data_packet(payload=ONE_SAMPLE, counter=2))
    assert r2["metadata"]["gap_before"] is True  # 0 -> 2: gap survives


def test_oversized_context_datagram_never_decodes_trailing_bytes() -> None:
    """The CIF walk is bounded by the DECLARED packet size: glued trailing
    bytes must not be readable as context fields."""
    interp = Interpreter()
    good = build_context_packet(fields=standard_context_fields())
    record = interp.process(good + b"\xde\xad\xbe\xef" * 8)
    assert record["type"] == "context"  # trailing garbage ignored entirely
    assert record["context"]["rf_hz"] == 97_300_000.0
    assert record["context"]["bytes_per_sample"] == 8

    # a context whose CIF claims a field beyond its declared end is dropped
    # as truncation even when the DATAGRAM has bytes there (glued packet)
    forged = bytearray(good)
    hdr_words = struct.unpack(">I", good[:4])[0] & 0xFFFF
    forged[2:4] = (hdr_words - 2).to_bytes(2, "big")  # declare 8 bytes fewer
    assert interp.process(bytes(forged) + b"\x00" * 64) is None
    assert interp.errors == 1


def test_non_bytes_input_is_caught_by_the_guard() -> None:
    """The never-propagate guard must itself never raise — not even on a
    stray non-bytes queue item (no __len__)."""
    interp = _primed()
    for junk in (None, 12345, object()):
        assert interp.process(junk) is None
    assert _counts(interp) == (3, 0)


# --- rate-limited logging --------------------------------------------------------


def test_repeated_failures_log_rate_limited(caplog) -> None:
    interp = _primed()
    raw = bytearray(build_data_packet(payload=ONE_SAMPLE))
    raw[0] = (0x2 << 4) | (raw[0] & 0x0F)  # unknown packet type
    with caplog.at_level(logging.WARNING, logger="sceptre_pipeline.interpreter"):
        for _ in range(50):
            assert interp.process(bytes(raw)) is None
    hits = [r for r in caplog.records if "unhandled packet type" in r.getMessage()]
    assert len(hits) == 5  # first 5 per reason, then every 1000th
    assert interp.dropped == 50  # every drop still counted


# --- foreign/random byte stress: alive, counted, bounded log ----------------------


def test_random_datagram_stress_keeps_interpreter_alive(caplog) -> None:
    rng = random.Random(2750)
    interp = _primed()
    errors0, dropped0 = _counts(interp)

    nones = 0
    records = 0
    with caplog.at_level(logging.WARNING, logger="sceptre_pipeline.interpreter"):
        for _ in range(500):
            kind = rng.randrange(3)
            if kind == 0:  # pure noise, arbitrary length
                raw = rng.randbytes(rng.randrange(0, 120))
            elif kind == 1:  # plausible word0, garbage remainder
                w0 = rng.getrandbits(32)
                raw = struct.pack(">I", w0) + rng.randbytes(rng.randrange(0, 96))
            else:  # a valid packet, randomly truncated
                good = build_data_packet(payload=ONE_SAMPLE, counter=rng.randrange(16))
                raw = good[: rng.randrange(0, len(good))]
            result = interp.process(raw)  # must never raise
            if result is None:
                nones += 1
            else:
                records += 1

    # every None landed on exactly one counter — nothing vanished silently
    errors, dropped = _counts(interp)
    assert (errors - errors0) + (dropped - dropped0) == nones
    assert nones + records == 500

    # the log stayed bounded (rate-limited), nowhere near one line per datagram
    assert len(caplog.records) < 120

    # and the per-stream maps stayed bounded: garbage headers wearing random
    # stream_ids never created tracked-stream state
    assert len(interp._contexts) <= interp.max_streams
    assert len(interp._last_counter) <= len(interp._contexts)

    # and the interpreter still works afterwards
    follow_up = interp.process(build_data_packet(payload=ONE_SAMPLE, counter=0))
    assert follow_up["type"] == "data"
    assert follow_up["data"] == ONE_SAMPLE
