"""Wire-format constants for the Sceptre VITA-49.2 subset.

Empirically verified against the two reference recordings — see
docs/implementation-plan.md Appendix A, which OVERRIDES the PDF/spec where
they conflict (notably: class_id/trailer flags are SET on real packets).
"""

from __future__ import annotations

# VITA-49 packet types (word0 bits 0-3)
PACKET_TYPE_IF_DATA = 1
PACKET_TYPE_IF_CONTEXT = 4

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
