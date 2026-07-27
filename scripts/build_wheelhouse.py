"""Populate ``wheelhouse/`` for air-gapped installs of sceptre-pipeline.

Run this on an internet-connected machine, then copy the repo *including*
``wheelhouse/`` to the target and install there with::

    pip install --no-index --find-links wheelhouse .

Two kinds of wheels are collected:

- **numpy**, cross-downloaded for every supported OS/arch x CPython version so
  one wheelhouse serves any target machine. Each (platform, version) combo is
  attempted independently; numpy occasionally drops or renames a tag, so a
  single missing combo is reported but tolerated. The script exits nonzero
  only when an entire platform family yields nothing — that platform would be
  uninstallable offline.
- **hatchling** (the build backend) and its dependencies, pure-Python, so the
  offline ``pip install`` can build this package under normal build isolation.

The wheelhouse is a deployment artifact, not source: it is gitignored and must
be regenerated for each bundle you prepare.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

WHEELHOUSE = Path(__file__).resolve().parent.parent / "wheelhouse"

PYTHON_VERSIONS = ("3.10", "3.11", "3.12", "3.13")

# Platform families -> candidate --platform tags, newest-compatible last.
# pip accepts repeated --platform flags and matches a wheel against any of
# them, which absorbs numpy's tag drift across releases (manylinux2014 vs
# manylinux_2_28, macOS deployment-target bumps).
PLATFORM_FAMILIES: dict[str, tuple[str, ...]] = {
    "windows-x86_64": ("win_amd64",),
    "linux-x86_64": (
        "manylinux2014_x86_64",
        "manylinux_2_17_x86_64",
        "manylinux_2_28_x86_64",
    ),
    "linux-aarch64": (
        "manylinux2014_aarch64",
        "manylinux_2_17_aarch64",
        "manylinux_2_28_aarch64",
    ),
    "macos-arm64": (
        "macosx_11_0_arm64",
        "macosx_12_0_arm64",
        "macosx_14_0_arm64",
    ),
    "macos-x86_64": (
        "macosx_10_9_x86_64",
        "macosx_10_13_x86_64",
        "macosx_12_0_x86_64",
        "macosx_13_0_x86_64",
        "macosx_14_0_x86_64",
    ),
}

# Pure-python build toolchain: hatchling resolves its own dependency tree
# (packaging, pathspec, pluggy, ...) with no platform restriction.
BUILD_TOOLCHAIN = ("hatchling",)


def download_numpy(family: str, tags: tuple[str, ...], py_version: str) -> bool:
    """Download one numpy (platform family, python version) combo.

    Returns True on success. Output is captured and only replayed on failure
    so the per-combo report stays readable.
    """
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "download",
        "numpy",
        "--only-binary=:all:",
        "--implementation",
        "cp",
        "--python-version",
        py_version,
        "-d",
        str(WHEELHOUSE),
    ]
    for tag in tags:
        cmd += ["--platform", tag]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        # surface pip's complaint (last lines carry the resolver error)
        tail = "\n".join(result.stderr.strip().splitlines()[-3:])
        print(f"    pip said: {tail}")
    return result.returncode == 0


def download_toolchain() -> bool:
    """Download hatchling + dependencies as universal (py3-none-any) wheels."""
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "download",
        *BUILD_TOOLCHAIN,
        "-d",
        str(WHEELHOUSE),
    ]
    result = subprocess.run(cmd)
    return result.returncode == 0


def main() -> int:
    WHEELHOUSE.mkdir(exist_ok=True)
    print(f"wheelhouse: {WHEELHOUSE}")

    results: dict[str, dict[str, bool]] = {}
    for family, tags in PLATFORM_FAMILIES.items():
        results[family] = {}
        for py_version in PYTHON_VERSIONS:
            print(f"numpy  {family}  py{py_version} ...")
            ok = download_numpy(family, tags, py_version)
            results[family][py_version] = ok
            print(f"    {'ok' if ok else 'FAILED'}")

    print("build toolchain (hatchling + deps) ...")
    toolchain_ok = download_toolchain()
    print(f"    {'ok' if toolchain_ok else 'FAILED'}")

    print("\n=== wheelhouse report ===")
    dead_families = []
    for family, by_version in results.items():
        marks = "  ".join(
            f"py{v}:{'ok' if ok else 'FAIL'}" for v, ok in by_version.items()
        )
        print(f"{family:16s} {marks}")
        if not any(by_version.values()):
            dead_families.append(family)
    print(f"{'toolchain':16s} {'ok' if toolchain_ok else 'FAIL'}")

    wheel_count = len(list(WHEELHOUSE.glob("*.whl")))
    print(f"\n{wheel_count} wheels in {WHEELHOUSE}")

    if dead_families:
        print(
            "ERROR: no numpy wheel downloaded for platform(s): "
            + ", ".join(dead_families),
            file=sys.stderr,
        )
        return 1
    if not toolchain_ok:
        print(
            "ERROR: build toolchain download failed; offline installs "
            "cannot build the package without it",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
