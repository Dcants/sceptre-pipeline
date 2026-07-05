"""Stage 2.75: dynamic Data Packet Payload Format classification.

``decode_payload_format`` is a CLASSIFIER: it maps every 64-bit format word to
``(is_complex, component_dtype, bytes_per_sample, supported)`` and NEVER
raises — unsupportable layouts return ``supported=False`` so live bytes can
degrade gracefully instead of killing Thread B. Only complex-float32 is
empirically confirmed against the recordings; the int/unsigned/float64 code
mappings follow the VITA-49.2 standard (see the plan's honesty note).
"""

from __future__ import annotations

import logging

import pytest
from conftest import encode_payload_format_u64

from sceptre_pipeline.formats import (
    ITEM_FORMAT_DTYPES,
    SUPPORTED_ITEM_BITS,
    SUPPORTED_ITEM_FORMATS,
)
from sceptre_pipeline.interpreter import decode_payload_format

RETURN_KEYS = {
    "is_complex",
    "rc_type",
    "data_item_format",
    "packing_method",
    "item_bits",
    "item_packing_bits",
    "component_bits",  # legacy Stage 2 alias of item_bits
    "event_tag",
    "channel_tag",
    "sample_component_repeat",
    "fraction_size",
    "repeat_count",
    "vector_size",
    "component_dtype",
    "bytes_per_sample",
    "supported",
}


# --- the recorded format is unchanged ----------------------------------------


def test_recorded_word_still_classifies_complex_float32() -> None:
    fmt = decode_payload_format(0x2E0007DF00000000)  # verified real value
    assert fmt["supported"] is True
    assert fmt["is_complex"] is True
    assert fmt["component_dtype"] == ">f4"
    assert fmt["bytes_per_sample"] == 8
    assert fmt["data_item_format"] == 0b01110


# --- every supported format decodes dynamically -------------------------------


@pytest.mark.parametrize(
    ("data_item_format", "item_bits", "dtype"),
    [(f, b, d) for (f, b), d in sorted(ITEM_FORMAT_DTYPES.items())],
    ids=lambda v: str(v),
)
@pytest.mark.parametrize(
    ("rc_code", "is_complex"), [(0b00, False), (0b01, True)], ids=["real", "complex"]
)
def test_supported_matrix(rc_code, is_complex, data_item_format, item_bits, dtype):
    word = encode_payload_format_u64(
        real_complex_code=rc_code,
        data_item_format=data_item_format,
        packing_bits=item_bits,
        item_bits=item_bits,
    )
    fmt = decode_payload_format(word)
    assert fmt["supported"] is True
    assert fmt["is_complex"] is is_complex
    assert fmt["component_dtype"] == dtype
    assert fmt["bytes_per_sample"] == (2 if is_complex else 1) * (item_bits // 8)
    assert set(fmt) == RETURN_KEYS


def test_parametrize_signature_matches_dtype_map() -> None:
    """The matrix above derives from ITEM_FORMAT_DTYPES; pin its content so a
    formats.py edit can't silently shrink the covered matrix."""
    assert ITEM_FORMAT_DTYPES == {
        (0b00000, 8): ">i1",
        (0b00000, 16): ">i2",
        (0b00000, 32): ">i4",
        (0b10000, 8): ">u1",
        (0b10000, 16): ">u2",
        (0b10000, 32): ">u4",
        (0b01110, 32): ">f4",
        (0b01111, 64): ">f8",
    }
    assert SUPPORTED_ITEM_FORMATS == {0b00000, 0b10000, 0b01110, 0b01111}
    assert SUPPORTED_ITEM_BITS == {8, 16, 32, 64}


# --- unsupported layouts degrade, never raise ----------------------------------


UNSUPPORTED_WORDS = {
    "12-bit-items": encode_payload_format_u64(
        data_item_format=0b00000, packing_bits=12, item_bits=12
    ),
    "link-efficient-packing": encode_payload_format_u64(packing_method=1),
    "event-tag": encode_payload_format_u64(event_tag=3),
    "channel-tag": encode_payload_format_u64(channel_tag=7),
    "in-container-padding": encode_payload_format_u64(
        data_item_format=0b00000, packing_bits=32, item_bits=16
    ),
    "complex-polar": encode_payload_format_u64(real_complex_code=0b10),
    "reserved-rc": encode_payload_format_u64(real_complex_code=0b11),
    "reserved-format-code": encode_payload_format_u64(data_item_format=0b00111),
    "ieee-half-uncertain": encode_payload_format_u64(
        data_item_format=0b01101, packing_bits=16, item_bits=16
    ),
    "float32-code-odd-bits": encode_payload_format_u64(
        packing_bits=64, item_bits=64  # float32 code with 64-bit items
    ),
    # non-default framing fields change the payload layout: never guess
    "sample-component-repeat": encode_payload_format_u64() | (1 << 55),
    "fraction-size": encode_payload_format_u64() | (0b0110 << 44),
    "repeat-count": encode_payload_format_u64() | (3 << 16),
    "vector-size": encode_payload_format_u64() | 15,
}


@pytest.mark.parametrize("word", UNSUPPORTED_WORDS.values(), ids=UNSUPPORTED_WORDS)
def test_unsupported_layouts_classify_without_raising(word: int) -> None:
    fmt = decode_payload_format(word)  # must not raise
    assert fmt["supported"] is False
    assert fmt["component_dtype"] is None
    assert fmt["bytes_per_sample"] is None
    assert set(fmt) == RETURN_KEYS


def test_unsupported_word_logged_once(caplog) -> None:
    # a word unique to this test: repeated sightings must not repeat the log
    word = encode_payload_format_u64(channel_tag=9)
    with caplog.at_level(logging.WARNING, logger="sceptre_pipeline.interpreter"):
        decode_payload_format(word)
        decode_payload_format(word)
        decode_payload_format(word)
    hits = [r for r in caplog.records if format(word, "016x") in r.getMessage()]
    assert len(hits) == 1
