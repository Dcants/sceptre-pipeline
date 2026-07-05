"""Header parse, context decode, and data trim for the Sceptre VITA-49.2 subset.

All layout facts follow docs/implementation-plan.md Appendix A as amended by
Stage 2.5 (flag-driven header) and Stage 2.75 (live hardening; empirically
verified against the reference recordings, overriding the PDF where they
conflict). Key corrections honored here:

- Stage 2.5: the header is FLAG-DRIVEN — length and every field offset derive
  from the word0 class_id/trailer/TSI/TSF flags. The 8-byte Class ID (when
  present) sits BEFORE the timestamps, so the recordings have a 28-byte header
  (int_ts at 16, frac_ps at 20) while default-config Sceptre has a 20-byte one
  (timestamps at 8/12). One code path handles both.
- C2: num_samples comes from the trimmed body length (1020), not the PDF
  formula (1021, which injects the tail of the header as a garbage sample).
- Stage 2.75: live bytes must DEGRADE GRACEFULLY, never crash Thread B or
  silently corrupt. ``decode_payload_format`` classifies formats as
  supported/unsupported instead of raising; ``Interpreter.process`` catches
  every per-packet failure (counted on ``errors``/``dropped``, rate-limited
  logging); context is PER-STREAM (``_contexts[stream_id]``, bounded by
  ``max_streams``) because the Sceptre format multiplexes streams on one
  socket; the CIF0 walk covers the full fixed-size field set and stops safely
  on variable/unknown bits without discarding already-decoded fields.
"""

from __future__ import annotations

import logging
import struct
from dataclasses import dataclass
from typing import Any

from .formats import (
    CIF_BANDWIDTH,
    CIF_DATA_PAYLOAD_FORMAT,
    CIF_GAIN,
    CIF_RF_REF_FREQ,
    CIF_SAMPLE_RATE,
    CLASS_ID_SIZE,
    CONTEXT_FIELD_CHANGE_BIT,
    FIELD_SIZE,
    FRAC_TS_SIZE,
    INT_TS_SIZE,
    ITEM_FORMAT_DTYPES,
    PACKET_TYPE_IF_CONTEXT,
    PACKET_TYPE_IF_DATA,
    PACKET_TYPES_WITH_STREAM_ID,
    RADIX_FREQ_HZ,
    RADIX_GAIN_DB,
    RC_TYPE_COMPLEX_CARTESIAN,
    RC_TYPE_REAL,
    STREAM_ID_SIZE,
    TRAILER_SIZE,
    W0_CLASS_ID_BIT,
    W0_COUNTER_SHIFT,
    W0_PACKET_SIZE_MASK,
    W0_PACKET_TYPE_SHIFT,
    W0_TRAILER_BIT,
    W0_TSF_SHIFT,
    W0_TSI_SHIFT,
    WORD0_SIZE,
    header_length,
)

logger = logging.getLogger(__name__)

# Bound on distinct stream_ids tracked per Interpreter: a spoofed/foreign
# stream_id flood must not grow the per-stream maps without limit.
DEFAULT_MAX_STREAMS = 64


class _RateLimitedLog:
    """Per-reason log limiter: first ``first_n`` occurrences log, then every
    ``every``-th. Keys are fixed reason strings, so the state stays bounded."""

    def __init__(self, first_n: int = 5, every: int = 1000) -> None:
        self._first_n = first_n
        self._every = every
        self._counts: dict[str, int] = {}

    def should_log(self, reason: str) -> bool:
        n = self._counts.get(reason, 0) + 1
        self._counts[reason] = n
        return n <= self._first_n or n % self._every == 0


# Unsupported payload-format words are logged once per distinct word, with a
# hard cap on the tracking set so a flood of unique bad words stays bounded.
_UNSUPPORTED_WORDS_LOGGED: set[int] = set()
_UNSUPPORTED_WORDS_CAP = 64
_unsupported_cap_announced = False

# Rate limiter for module-level (non-Interpreter) warnings; keyed per CIF bit
# (<= 32 keys) so one noisy bit cannot silence warnings about another.
_module_log = _RateLimitedLog()


@dataclass(frozen=True)
class Header:
    """A VITA-49 header parsed flag-first: every offset derives from word0."""

    packet_type: int
    class_id: bytes | None  # 8-byte Class ID (OUI + class codes), None if absent
    has_trailer: bool
    tsi: int
    tsf: int
    packet_counter: int
    packet_size: int  # in 32-bit words, header included
    stream_id: int | None  # present on packet types 1/3/4/5
    int_ts: int | None  # UTC seconds; None when TSI = 0
    frac_ps: int | None  # picoseconds; None when TSF = 0
    header_len: int  # bytes; a pure function of the word0 flags

    @property
    def has_class_id(self) -> bool:
        return self.class_id is not None

    @property
    def timestamp(self) -> float:
        return (self.int_ts or 0) + (self.frac_ps or 0) / 1e12


def parse_header(b: bytes) -> Header:
    if len(b) < WORD0_SIZE:
        raise ValueError(
            f"packet too short for VITA-49 word0: {len(b)} < {WORD0_SIZE} bytes"
        )
    word0 = int.from_bytes(b[:WORD0_SIZE], "big")
    packet_type = (word0 >> W0_PACKET_TYPE_SHIFT) & 0xF
    has_class_id = bool((word0 >> W0_CLASS_ID_BIT) & 1)
    has_trailer = bool((word0 >> W0_TRAILER_BIT) & 1)
    tsi = (word0 >> W0_TSI_SHIFT) & 0x3
    tsf = (word0 >> W0_TSF_SHIFT) & 0x3
    has_stream_id = packet_type in PACKET_TYPES_WITH_STREAM_ID

    expected_len = header_length(
        has_stream_id=has_stream_id, has_class_id=has_class_id, tsi=tsi, tsf=tsf
    )
    if len(b) < expected_len:
        raise ValueError(
            f"packet too short for its word0 flags: {len(b)} < {expected_len} bytes"
        )

    off = WORD0_SIZE
    stream_id = None
    if has_stream_id:
        stream_id = int.from_bytes(b[off : off + STREAM_ID_SIZE], "big")
        off += STREAM_ID_SIZE
    class_id = None
    if has_class_id:  # the Class ID sits BEFORE the timestamps
        class_id = bytes(b[off : off + CLASS_ID_SIZE])
        off += CLASS_ID_SIZE
    int_ts = None
    if tsi:
        int_ts = int.from_bytes(b[off : off + INT_TS_SIZE], "big")
        off += INT_TS_SIZE
    frac_ps = None
    if tsf:
        frac_ps = int.from_bytes(b[off : off + FRAC_TS_SIZE], "big")
        off += FRAC_TS_SIZE

    return Header(
        packet_type=packet_type,
        class_id=class_id,
        has_trailer=has_trailer,
        tsi=tsi,
        tsf=tsf,
        packet_counter=(word0 >> W0_COUNTER_SHIFT) & 0xF,
        packet_size=word0 & W0_PACKET_SIZE_MASK,
        stream_id=stream_id,
        int_ts=int_ts,
        frac_ps=frac_ps,
        header_len=off,
    )


def _log_unsupported_format(u: int, fields: dict[str, Any]) -> None:
    global _unsupported_cap_announced
    if u in _UNSUPPORTED_WORDS_LOGGED:
        return
    if len(_UNSUPPORTED_WORDS_LOGGED) >= _UNSUPPORTED_WORDS_CAP:
        if not _unsupported_cap_announced:
            _unsupported_cap_announced = True
            logger.warning(
                "seen %d distinct unsupported payload-format words; further "
                "ones will not be logged",
                _UNSUPPORTED_WORDS_CAP,
            )
        return
    _UNSUPPORTED_WORDS_LOGGED.add(u)
    logger.warning(
        "unsupported data payload format 0x%016x: packing_method=%d rc_type=%d "
        "item_format=0b%s event_tag=%d channel_tag=%d packing_bits=%d "
        "item_bits=%d; the stream's data packets will be dropped",
        u,
        fields["packing_method"],
        fields["rc_type"],
        format(fields["data_item_format"], "05b"),
        fields["event_tag"],
        fields["channel_tag"],
        fields["item_packing_bits"],
        fields["item_bits"],
    )


def decode_payload_format(u: int) -> dict[str, Any]:
    """Classify a Data Packet Payload Format word (exact 64-bit windows).

    Never raises: a layout we cannot decode (link-efficient packing,
    event/channel tags, in-container padding, complex-polar, reserved or
    VRT formats, non-byte-aligned sizes) returns ``supported=False`` with
    ``component_dtype=None``/``bytes_per_sample=None`` so the caller can
    drop-and-count that stream's data instead of crashing Thread B.
    """
    packing_method = (u >> 63) & 1
    rc_type = (u >> 61) & 0b11
    data_item_format = (u >> 56) & 0x1F
    event_tag = (u >> 52) & 0x7
    channel_tag = (u >> 48) & 0xF
    item_packing_bits = ((u >> 38) & 0x3F) + 1  # Item Packing Field Size - 1
    item_bits = ((u >> 32) & 0x3F) + 1  # Data Item Size - 1

    # (format, bits) membership in ITEM_FORMAT_DTYPES implies both
    # data_item_format in SUPPORTED_ITEM_FORMATS and item_bits in
    # SUPPORTED_ITEM_BITS, plus their cross-consistency (a float32 code with
    # 8-bit items has no dtype and must classify unsupported).
    component_dtype = ITEM_FORMAT_DTYPES.get((data_item_format, item_bits))
    supported = (
        packing_method == 0  # link-efficient packing not supported
        and rc_type in (RC_TYPE_REAL, RC_TYPE_COMPLEX_CARTESIAN)
        and component_dtype is not None
        and item_packing_bits == item_bits  # no in-container padding
        and event_tag == 0
        and channel_tag == 0
    )
    is_complex = rc_type == RC_TYPE_COMPLEX_CARTESIAN
    bytes_per_sample: int | None
    if supported:
        bytes_per_sample = (2 if is_complex else 1) * (item_bits // 8)
    else:
        component_dtype = None
        bytes_per_sample = None

    fmt: dict[str, Any] = {
        "is_complex": is_complex,
        "rc_type": rc_type,
        "data_item_format": data_item_format,
        "packing_method": packing_method,
        "item_bits": item_bits,
        "item_packing_bits": item_packing_bits,
        "component_bits": item_bits,  # legacy Stage 2 name
        "event_tag": event_tag,
        "channel_tag": channel_tag,
        "component_dtype": component_dtype,
        "bytes_per_sample": bytes_per_sample,
        "supported": supported,
    }
    if not supported:
        _log_unsupported_format(u, fmt)
    return fmt


def decode_context(b: bytes, hdr: Header) -> dict[str, Any]:
    """Decode an IF context packet into a typed dict.

    Walks CIF0 bits 31 -> 0 in descending order, advancing by FIELD_SIZE for
    EVERY present fixed-size bit (decoding only the ones we use) so unexpected
    optional fields cannot desync the offsets. Bit 31 (Context Field Change
    Indicator) occupies zero payload bytes. A variable-length, CIF-enable, or
    reserved/unknown bit stops the walk WITHOUT discarding the fields already
    decoded from higher bits. Truncation raises ValueError — the Interpreter's
    process() guard catches it and counts it on ``errors``.
    """
    offset = hdr.header_len
    if len(b) < offset + 4:
        raise ValueError(f"context packet truncated before CIF: {len(b)} bytes")
    cif = struct.unpack_from(">I", b, offset)[0]
    offset += 4

    ctx: dict[str, Any] = {}
    for bit in range(31, -1, -1):
        if not (cif >> bit) & 1:
            continue
        if bit == CONTEXT_FIELD_CHANGE_BIT:  # indicator only: zero bytes
            ctx["context_field_change"] = True
            continue
        size = FIELD_SIZE.get(bit)
        if size is None:
            if _module_log.should_log(f"cif0-stop-bit-{bit}"):
                logger.warning(
                    "CIF0 bit %d is variable/unknown; stopping walk, "
                    "%d fields decoded",
                    bit,
                    len(ctx),
                )
            break
        if offset + size > len(b):
            raise ValueError(
                f"context packet truncated inside CIF bit {bit} field "
                f"({len(b) - offset} of {size} bytes)"
            )
        field = b[offset : offset + size]
        if bit == CIF_BANDWIDTH:
            ctx["bandwidth_hz"] = struct.unpack(">q", field)[0] / RADIX_FREQ_HZ
        elif bit == CIF_RF_REF_FREQ:
            ctx["rf_hz"] = struct.unpack(">q", field)[0] / RADIX_FREQ_HZ
        elif bit == CIF_GAIN:
            stage2, stage1 = struct.unpack(">hh", field)  # bits 31:16, 15:0
            ctx["gain_db"] = stage2 / RADIX_GAIN_DB + stage1 / RADIX_GAIN_DB
        elif bit == CIF_SAMPLE_RATE:
            ctx["sample_rate_hz"] = struct.unpack(">q", field)[0] / RADIX_FREQ_HZ
        elif bit == CIF_DATA_PAYLOAD_FORMAT:
            fmt = decode_payload_format(struct.unpack(">Q", field)[0])
            ctx["supported"] = fmt["supported"]
            ctx["component_dtype"] = fmt["component_dtype"]
            ctx["bytes_per_sample"] = fmt["bytes_per_sample"]
            ctx["is_complex"] = fmt["is_complex"]
            ctx["data_item_format"] = fmt["data_item_format"]
        # other fixed-size fields (30/28/26/25/24/22/20/.../10): advance only
        offset += size
    return ctx


def trim_data(b: bytes, hdr: Header, bytes_per_sample: int) -> tuple[bytes, int]:
    """Trim a data packet to its sample payload (pure; no format assertions).

    Returns ``(body, num_samples)`` where ``body`` is exactly
    ``num_samples * bytes_per_sample`` bytes: the flag-driven header skipped,
    the trailer (when flagged) dropped, and any word-alignment padding smaller
    than one sample floored away. A datagram longer than the declared
    ``packet_size`` (concatenated packets / jumbo frame) is trimmed to exactly
    ``packet_size * 4`` bytes so trailing bytes are never ingested as samples;
    a shorter one raises ValueError (truncation — caught by the process()
    guard) rather than silently short-reading. Purity matters: format
    supportedness lives in ``decode_payload_format``, never here.
    """
    declared = hdr.packet_size * 4
    if len(b) < declared:
        raise ValueError(
            f"data packet truncated: {len(b)} bytes < declared packet_size "
            f"{hdr.packet_size} words ({declared} bytes)"
        )
    if len(b) > declared and _module_log.should_log("trim-length-mismatch"):
        logger.warning(
            "packet length %d bytes > header packet_size %d words (%d bytes); "
            "ignoring the trailing bytes",
            len(b),
            hdr.packet_size,
            declared,
        )
    start = hdr.header_len
    # clamp so a forged packet_size smaller than the header can never make the
    # end precede the start (a negative end would slice from the datagram tail)
    end = max(start, min(len(b), declared) - (TRAILER_SIZE if hdr.has_trailer else 0))
    body = b[start:end]
    num_samples = len(body) // bytes_per_sample
    return body[: num_samples * bytes_per_sample], num_samples


class Interpreter:
    """Parses raw packets into typed records; owns per-stream context.

    The Sceptre format multiplexes multiple streams on one socket, so context
    is tracked per ``stream_id`` (a single-stream capture is the N=1 case).
    Live bytes can be arbitrarily malformed: ``process`` never lets a parse
    failure propagate (it would kill Thread B) — exceptions are counted on
    ``errors``, policy drops (no context yet, unsupported format, unknown
    packet type, stream flood) on ``dropped``, both with rate-limited logging.
    Both counters are single-writer (Thread B); GIL-atomic int reads make
    them safe to read from other threads without a lock.
    """

    def __init__(self, max_streams: int = DEFAULT_MAX_STREAMS) -> None:
        self.max_streams = max_streams
        self.errors = 0  # datagrams dropped because parsing raised
        self.dropped = 0  # datagrams dropped by policy
        self._contexts: dict[int, dict[str, Any]] = {}
        self._last_counter: dict[int, int] = {}
        self._streams: set[int] = set()  # admitted ids; bounds both maps
        self._log = _RateLimitedLog()

    def context_for(self, stream_id: int) -> dict[str, Any] | None:
        """The most recent context adopted for ``stream_id`` (None if none)."""
        return self._contexts.get(stream_id)

    def process(self, raw: bytes) -> dict[str, Any] | None:
        try:
            return self._process(raw)
        except Exception as exc:
            self.errors += 1
            if self._log.should_log("unparseable"):
                logger.warning(
                    "dropping unparseable datagram (%d bytes): %s", len(raw), exc
                )
            return None

    def _process(self, raw: bytes) -> dict[str, Any] | None:
        hdr = parse_header(raw)

        if hdr.packet_type not in (PACKET_TYPE_IF_DATA, PACKET_TYPE_IF_CONTEXT):
            self.dropped += 1
            if self._log.should_log("unknown-packet-type"):
                logger.warning(
                    "unhandled packet type %d; dropping packet", hdr.packet_type
                )
            return None

        # types 1 and 4 always carry a stream_id; bound the per-stream maps
        stream_id = hdr.stream_id
        if stream_id not in self._streams:
            if len(self._streams) >= self.max_streams:
                self.dropped += 1
                if self._log.should_log("stream-flood"):
                    logger.warning(
                        "new stream 0x%08x beyond max_streams=%d; dropping "
                        "packet (spoofed-stream flood protection)",
                        stream_id,
                        self.max_streams,
                    )
                return None
            self._streams.add(stream_id)

        if hdr.packet_type == PACKET_TYPE_IF_CONTEXT:
            ctx = decode_context(raw, hdr)
            # an unsupported context is still adopted and emitted: downstream
            # must know the stream is present-but-unsupported
            self._contexts[stream_id] = ctx
            return {
                "type": "context",
                "metadata": self._metadata(hdr, ctx, gap_before=False),
                "context": ctx,
                "data": None,
            }

        ctx = self._contexts.get(stream_id)
        if ctx is None:  # startup: no context for THIS stream yet
            self.dropped += 1
            if self._log.should_log("no-context"):
                logger.warning(
                    "data packet on stream %s before first context packet; "
                    "dropping until context arrives",
                    stream_id,
                )
            return None
        if ctx.get("supported") is False or ctx.get("bytes_per_sample") is None:
            self.dropped += 1
            if self._log.should_log("unsupported-data"):
                logger.warning(
                    "dropping data packet on stream %s: its context payload "
                    "format is unsupported or absent",
                    stream_id,
                )
            return None

        prev = self._last_counter.get(stream_id)
        gap_before = prev is not None and hdr.packet_counter != (prev + 1) % 16
        if gap_before and self._log.should_log("counter-gap"):
            logger.warning(
                "packet counter gap on stream %s: %d -> %d",
                stream_id,
                prev,
                hdr.packet_counter,
            )
        self._last_counter[stream_id] = hdr.packet_counter
        body, _num_samples = trim_data(raw, hdr, ctx["bytes_per_sample"])
        return {
            "type": "data",
            "metadata": self._metadata(hdr, ctx, gap_before=gap_before),
            "context": None,
            "data": body,
        }

    @staticmethod
    def _metadata(
        hdr: Header, ctx: dict[str, Any], gap_before: bool
    ) -> dict[str, Any]:
        return {
            "stream_id": hdr.stream_id,
            "counter": hdr.packet_counter,
            "timestamp": hdr.timestamp,
            "packet_size": hdr.packet_size,
            "dtype": ctx.get("component_dtype"),
            "bytes_per_sample": ctx.get("bytes_per_sample"),
            "is_complex": ctx.get("is_complex"),
            "gap_before": gap_before,
        }
