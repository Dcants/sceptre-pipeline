"""Wire-format constants for the Sceptre VITA-49.2 subset.

Empirically verified against the two reference recordings — see
docs/implementation-plan.md Appendix A as amended by Stage 2.5 (flag-driven
header model), which OVERRIDES the PDF/spec where they conflict. Notably: the
recordings carry class_id AND trailer flags SET, so their header is 28 bytes
with the 8-byte Class ID BEFORE the timestamps; default-config Sceptre
(class_id=0, trailer=0) has a 20-byte header with timestamps at offsets 8/12.
One flag-driven code path handles both.
"""

from __future__ import annotations

# VITA-49 packet types (word0 bits 28-31)
PACKET_TYPE_IF_DATA = 1
PACKET_TYPE_IF_CONTEXT = 4

# word0 flag extraction (Stage 2.5: every header offset derives from these)
W0_PACKET_TYPE_SHIFT = 28  # bits 28-31
W0_CLASS_ID_BIT = 27  # Class ID present
W0_TRAILER_BIT = 26  # trailer present (data packets)
W0_TSI_SHIFT = 22  # bits 22-23: integer-timestamp mode (0 = absent)
W0_TSF_SHIFT = 20  # bits 20-21: fractional-timestamp mode (0 = absent)
W0_COUNTER_SHIFT = 16  # bits 16-19: mod-16 packet counter
W0_PACKET_SIZE_MASK = 0xFFFF  # bits 0-15: packet size in 32-bit words

# packet types that carry a Stream ID word (Sceptre uses 1 and 4)
PACKET_TYPES_WITH_STREAM_ID = frozenset({1, 3, 4, 5})

# header/trailer field sizes in bytes
WORD0_SIZE = 4
STREAM_ID_SIZE = 4
CLASS_ID_SIZE = 8  # OUI + class codes — sits BEFORE the timestamps
INT_TS_SIZE = 4
FRAC_TS_SIZE = 8
TRAILER_SIZE = 4


def header_length(
    *, has_stream_id: bool, has_class_id: bool, tsi: int, tsf: int
) -> int:
    """Header length implied by the word0 flags — never a magic constant.

    20 for the default config (stream id + both timestamps), 28 when the
    Class ID is present, smaller when TSI/TSF disable a timestamp.
    """
    return (
        WORD0_SIZE
        + (STREAM_ID_SIZE if has_stream_id else 0)
        + (CLASS_ID_SIZE if has_class_id else 0)
        + (INT_TS_SIZE if tsi else 0)
        + (FRAC_TS_SIZE if tsf else 0)
    )

# Context Indicator Field bit numbers (32-bit CIF, bit 31 = MSB)
CIF_BANDWIDTH = 29
CIF_RF_REF_FREQ = 27
CIF_GAIN = 23
CIF_SAMPLE_RATE = 21
CIF_DATA_PAYLOAD_FORMAT = 15
CIF_FORMATTED_GPS = 14
CIF_ECEF_EPHEMERIS = 12

# Field size in bytes for every CIF bit we know how to walk past.
# The walk MUST advance past every present bit (decode only the ones used)
# so an unexpected optional field cannot desync the offsets.
FIELD_SIZE: dict[int, int] = {
    CIF_BANDWIDTH: 8,
    CIF_RF_REF_FREQ: 8,
    CIF_GAIN: 4,
    CIF_SAMPLE_RATE: 8,
    CIF_DATA_PAYLOAD_FORMAT: 8,
    CIF_FORMATTED_GPS: 44,
    CIF_ECEF_EPHEMERIS: 52,
}

# Data Packet Payload Format: Data Item Format code for IEEE-754 single float
DATA_ITEM_FORMAT_FLOAT = 0b01110

# complex cartesian float32 = 2 components x 4 bytes
EXPECTED_BYTES_PER_SAMPLE = 8

# numpy dtype of one wire component (big-endian float32, 4 bytes) — distinct
# from EXPECTED_BYTES_PER_SAMPLE (8): frombuffer counts components, 2/sample
COMPONENT_DTYPE = ">f4"

# fixed-point radix constants (verified standard-correct, Appendix A)
RADIX_FREQ_HZ = 2**20  # bandwidth / RF ref / sample rate: int64 / 2**20 Hz
RADIX_GAIN_DB = 128  # gain: int16 / 128 dB per stage
RADIX_GPS_DEG = 2**22  # formatted GPS lat/lon: / 2**22 degrees
RADIX_ECEF_POS_M = 2**5  # ECEF position: / 2**5 m
RADIX_ECEF_VEL_MS = 2**16  # ECEF velocity: / 2**16 m/s
