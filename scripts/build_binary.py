"""Build a one-file sceptre-pipeline executable with PyInstaller.

PyInstaller cannot cross-compile: run this ON the OS you are building FOR
(Windows produces ``dist/sceptre-pipeline.exe``, Linux/macOS produce
``dist/sceptre-pipeline``). The binary embeds the interpreter and numpy, so
the target machine needs no Python at all.

Build outputs (``build/``, ``dist/``, the generated ``.spec``) are gitignored;
binaries are per-machine deployment artifacts and are never committed.

Requires the dev extra (``pip install -e .[dev]``): PyInstaller does the
freezing, and numpy must be importable for it to be bundled.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# The frozen app needs a real script as its entry point; generate it under the
# gitignored build/ tree so the repo carries only this builder.
ENTRY_SOURCE = '''\
import sys

from sceptre_pipeline.__main__ import main

if __name__ == "__main__":
    sys.exit(main())
'''


def main() -> int:
    try:
        import PyInstaller  # noqa: F401
    except ImportError:
        print(
            "PyInstaller is not installed. Run: pip install -e .[dev]",
            file=sys.stderr,
        )
        return 1

    entry_dir = PROJECT_ROOT / "build" / "pyinstaller-entry"
    entry_dir.mkdir(parents=True, exist_ok=True)
    entry = entry_dir / "sceptre_pipeline_binary.py"
    entry.write_text(ENTRY_SOURCE)

    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--onefile",
        "--name",
        "sceptre-pipeline",
        "--clean",
        "--noconfirm",
        # freeze the working-tree package, not a possibly-stale installed copy
        "--paths",
        str(PROJECT_ROOT / "src"),
        # keep the generated .spec out of the repo root
        "--specpath",
        str(PROJECT_ROOT / "build"),
        str(entry),
    ]
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    if result.returncode != 0:
        return result.returncode

    suffix = ".exe" if sys.platform == "win32" else ""
    artifact = PROJECT_ROOT / "dist" / f"sceptre-pipeline{suffix}"
    print(f"\nbuilt: {artifact}")
    print("Binaries are per-OS deployment artifacts - do not commit them.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
