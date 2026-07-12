"""Tests for the CLI demo consumer output (``demo_emit``) and shutdown lines."""

from __future__ import annotations

import logging

import numpy as np

from sceptre_pipeline.__main__ import demo_emit, main


def test_demo_emit_prints_bandwidth_alongside_sample_rate(capsys):
    unit = {
        "stream_id": 5,
        "num_samples": 3,
        "samples": np.zeros(3, dtype=np.complex64),
        "start_timestamp": 1.0,
        "context": {
            "rf_hz": 103_000_000.0,
            "sample_rate_hz": 10_000_000.0,
            "bandwidth_hz": 5_000_000.0,
        },
    }
    demo_emit(unit)
    out = capsys.readouterr().out
    assert "sample_rate_hz=10000000.0" in out
    assert "bandwidth_hz=5000000.0" in out


def test_demo_emit_bandwidth_none_when_absent(capsys):
    unit = {
        "stream_id": 5,
        "num_samples": 3,
        "samples": np.zeros(3, dtype=np.complex64),
        "start_timestamp": 1.0,
        "context": {"rf_hz": 103_000_000.0, "sample_rate_hz": 10_000_000.0},
    }
    demo_emit(unit)
    out = capsys.readouterr().out
    assert "bandwidth_hz=None" in out


def test_replay_run_omits_capture_line(single_frequency_path, caplog):
    # capture efficiency is meaningless on an unpaced replay (wall-clock is
    # decoupled from stream time), so main() must NOT log it there -- while the
    # throughput line is still emitted.
    with caplog.at_level(logging.INFO):
        rc = main(["--replay", str(single_frequency_path)])
    assert rc == 0
    messages = [record.getMessage() for record in caplog.records]
    assert any("pipeline throughput" in m for m in messages)
    assert not any(m.startswith("capture:") for m in messages)
