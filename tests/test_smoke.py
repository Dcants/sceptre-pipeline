"""Stage 0 smoke tests: the package installs and the two fixtures exist and load."""

from __future__ import annotations

import ast
import sys
from pathlib import Path


def test_package_imports() -> None:
    import sceptre_pipeline

    assert sceptre_pipeline.__version__


def test_runtime_imports_stdlib_and_numpy_only() -> None:
    """The shipped library must import nothing but the stdlib and numpy."""
    import sceptre_pipeline

    package_dir = Path(sceptre_pipeline.__file__).parent
    allowed = set(sys.stdlib_module_names) | {"numpy", "sceptre_pipeline"}

    for module_path in package_dir.rglob("*.py"):
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                top_levels = [alias.name.split(".")[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level > 0:  # relative import within the package
                    continue
                top_levels = [node.module.split(".")[0]] if node.module else []
            else:
                continue
            for top in top_levels:
                assert top in allowed, (
                    f"{module_path.name} imports {top!r}, which is neither "
                    "stdlib nor numpy"
                )


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
