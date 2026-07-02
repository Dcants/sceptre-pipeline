"""Header parse, context decode, and data trim for the Sceptre VITA-49.2 subset.

All layout facts follow docs/implementation-plan.md Appendix A (empirically
verified against the reference recordings; overrides the PDF where they
conflict). Key corrections honored here:

- C1: class_id/trailer flags are SET on real packets — payload bounds derive
  from the flags, never a fixed offset 20.
- C2: num_samples comes from the trimmed body length (1020), not the PDF
  formula (1021, which injects the id-word as a garbage first sample).
- C3: bytes[20:28] is a per-packet picosecond counter — skipped, never
  validated as a constant.
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
    COMPONENT_DTYPE,
    DATA_ITEM_FORMAT_FLOAT,
    EXPECTED_BYTES_PER_SAMPLE,
    FIELD_SIZE,
    PACKET_TYPE_IF_CONTEXT,
    PACKET_TYPE_IF_DATA,
    RADIX_FREQ_HZ,
    RADIX_GAIN_DB,
)

logger = logging.getLogger(__name__)

HEADER_SIZE = 20  # bytes: word0 + stream_id + int_ts + frac_ts(u64)
ID_WORD_SIZE = 8  # the per-packet ps counter following the header (C3)
TRAILER_SIZE = 4


@dataclass(frozen=True)
class Header:
    """The 20-byte VITA-49 header with the verified word0 bit extraction."""

    packet_type: int
    class_id: bool
    trailer: bool
    tsi: int
    tsf: int
    packet_counter: int
    packet_size: int  # in 32-bit words
    stream_id: int
    int_ts: int  # UTC seconds
    frac_ts: int  # picoseconds

    @property
    def timestamp(self) -> float:
        return self.int_ts + self.frac_ts / 1e12


def parse_header(b: bytes) -> Header:
    if len(b) < HEADER_SIZE:
        raise ValueError(
            f"packet too short for VITA-49 header: {len(b)} < {HEADER_SIZE} bytes"
        )
    word0, stream_id, int_ts, frac_ts = struct.unpack(">IIIQ", b[:HEADER_SIZE])
    return Header(
        packet_type=(word0 >> 28) & 0xF,
        class_id=bool((word0 >> 27) & 1),
        trailer=bool((word0 >> 26) & 1),
        tsi=(word0 >> 22) & 0x3,
        tsf=(word0 >> 20) & 0x3,
        packet_counter=(word0 >> 16) & 0xF,
        packet_size=word0 & 0xFFFF,
        stream_id=stream_id,
        int_ts=int_ts,
        frac_ts=frac_ts,
    )


def _payload_start(hdr: Header) -> int:
    return HEADER_SIZE + (ID_WORD_SIZE if hdr.class_id else 0)


def decode_payload_format(u: int) -> dict[str, Any]:
    """Decode a Data Packet Payload Format word (exact 64-bit windows).

    Fails loudly on anything but the supported complex float32 layout —
    a wrong-format stream must never be silently misinterpreted.
    """
    is_complex = ((u >> 61) & 0b11) == 0b01  # 01 = complex cartesian
    data_item_format = (u >> 56) & 0x1F
    component_bits = ((u >> 38) & 0x3F) + 1  # Item Packing Field Size - 1
    item_bits = ((u >> 32) & 0x3F) + 1  # Data Item Size - 1

    if data_item_format != DATA_ITEM_FORMAT_FLOAT:
        raise ValueError(
            f"unsupported data item format 0b{data_item_format:05b}; only "
            f"IEEE-754 single float (0b{DATA_ITEM_FORMAT_FLOAT:05b}) is supported"
        )
    if item_bits != component_bits:
        raise ValueError(
            f"unsupported layout: item size {item_bits} bits packed in "
            f"{component_bits}-bit containers (in-container padding untested)"
        )
    component_bytes = component_bits // 8
    bytes_per_sample = (2 if is_complex else 1) * component_bytes
    if bytes_per_sample != EXPECTED_BYTES_PER_SAMPLE:
        raise ValueError(
            f"unsupported bytes_per_sample {bytes_per_sample}; expected "
            f"{EXPECTED_BYTES_PER_SAMPLE} (complex float32)"
        )
    return {
        "is_complex": is_complex,
        "data_item_format": data_item_format,
        "component_bits": component_bits,
        "bytes_per_sample": bytes_per_sample,
    }


def decode_context(b: bytes, hdr: Header) -> dict[str, Any]:
    """Decode an IF context packet into a typed dict.

    Walks CIF bits 31 -> 0 in descending order, advancing by FIELD_SIZE for
    EVERY present bit (decoding only the ones we use) so unexpected optional
    fields cannot desync the offsets.
    """
    offset = _payload_start(hdr)
    if len(b) < offset + 4:
        raise ValueError(f"context packet truncated before CIF: {len(b)} bytes")
    cif = struct.unpack_from(">I", b, offset)[0]
    offset += 4

    ctx: dict[str, Any] = {}
    for bit in range(31, -1, -1):
        if not (cif >> bit) & 1:
            continue
        size = FIELD_SIZE.get(bit)
        if size is None:
            logger.warning(
                "context CIF bit %d has unknown field size; stopping the walk "
                "(higher-bit fields already decoded remain valid)",
                bit,
            )
            break
        field = b[offset : offset + size]
        if len(field) != size:
            raise ValueError(
                f"context packet truncated inside CIF bit {bit} field "
                f"({len(field)} of {size} bytes)"
            )
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
            ctx["is_complex"] = fmt["is_complex"]
            ctx["bytes_per_sample"] = fmt["bytes_per_sample"]
            ctx["data_item_format"] = fmt["data_item_format"]
            ctx["component_dtype"] = COMPONENT_DTYPE
        # GPS (14) / ECEF (12): known size, not decoded — advance only
        offset += size
    return ctx


def trim_data(b: bytes, hdr: Header, bytes_per_sample: int) -> tuple[bytes, int]:
    """Trim a data packet to its sample payload (pure; no format assertions).

    Returns ``(body, num_samples)`` where ``body`` is exactly
    ``num_samples * bytes_per_sample`` bytes: id-word skipped, trailer dropped,
    and any word-alignment padding smaller than one sample floored away.
    """
    if len(b) != hdr.packet_size * 4:
        logger.warning(
            "packet length %d bytes != header packet_size %d words (%d bytes)",
            len(b),
            hdr.packet_size,
            hdr.packet_size * 4,
        )
    start = _payload_start(hdr)
    end = len(b) - (TRAILER_SIZE if hdr.trailer else 0)
    body = b[start:end]
    num_samples = len(body) // bytes_per_sample
    return body[: num_samples * bytes_per_sample], num_samples


class Interpreter:
    """Parses raw packets into typed records; owns the current context.

    Data packets arriving before the first context packet are dropped with a
    warning (startup edge case). Per-stream mod-16 counter gaps are flagged in
    ``metadata["gap_before"]`` so the buffer can break accumulation there.
    """

    def __init__(self) -> None:
        self._current_context: dict[str, Any] | None = None
        self._last_counter: dict[int, int] = {}
        self._warned_no_context = False

    @property
    def current_context(self) -> dict[str, Any] | None:
        return self._current_context

    def process(self, raw: bytes) -> dict[str, Any] | None:
        hdr = parse_header(raw)

        if hdr.packet_type == PACKET_TYPE_IF_CONTEXT:
            ctx = decode_context(raw, hdr)
            self._current_context = ctx
            return {
                "type": "context",
                "metadata": self._metadata(hdr, ctx, gap_before=False),
                "context": ctx,
                "data": None,
            }

        if hdr.packet_type == PACKET_TYPE_IF_DATA:
            if self._current_context is None:
                if not self._warned_no_context:
                    logger.warning(
                        "data packet on stream %d before first context packet; "
                        "dropping until context arrives",
                        hdr.stream_id,
                    )
                    self._warned_no_context = True
                return None
            prev = self._last_counter.get(hdr.stream_id)
            gap_before = prev is not None and hdr.packet_counter != (prev + 1) % 16
            if gap_before:
                logger.warning(
                    "packet counter gap on stream %d: %d -> %d",
                    hdr.stream_id,
                    prev,
                    hdr.packet_counter,
                )
            self._last_counter[hdr.stream_id] = hdr.packet_counter
            body, _num_samples = trim_data(
                raw, hdr, self._current_context["bytes_per_sample"]
            )
            return {
                "type": "data",
                "metadata": self._metadata(
                    hdr, self._current_context, gap_before=gap_before
                ),
                "context": None,
                "data": body,
            }

        logger.warning("unhandled packet type %d; dropping packet", hdr.packet_type)
        return None

    @staticmethod
    def _metadata(
        hdr: Header, ctx: dict[str, Any], gap_before: bool
    ) -> dict[str, Any]:
        return {
            "stream_id": hdr.stream_id,
            "counter": hdr.packet_counter,
            "timestamp": hdr.timestamp,
            "packet_size": hdr.packet_size,
            "dtype": ctx.get("component_dtype", COMPONENT_DTYPE),
            "bytes_per_sample": ctx.get("bytes_per_sample"),
            "is_complex": ctx.get("is_complex"),
            "gap_before": gap_before,
        }
