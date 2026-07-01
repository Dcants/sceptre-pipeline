"""Stage 0 smoke tests: the package installs and the two fixtures exist and load."""

from __future__ import annotations


def test_package_imports() -> None:
    import sceptre_pipeline

    assert sceptre_pipeline.__version__


def test_recordings_exist_and_load(
    single_frequency_path, change_frequency_path, load_capture
) -> None:
    for path in (single_frequency_path, change_frequency_path):
        assert path.is_file(), f"missing fixture: {path}"

        capture = load_capture(path)
        assert set(capture) == {"start_time_ns", "duration_seconds", "packets"}

        packets = capture["packets"]
        assert len(packets) == 3072  # both captures hold 3072 datagrams (Appendix A)

        ts_ns, ip, port, payload = packets[0]
        assert isinstance(ts_ns, int)
        assert isinstance(ip, str)
        assert isinstance(port, int)
        assert isinstance(payload, bytes) and len(payload) > 0
