#!/usr/bin/env python3
"""Capture raw UDP datagrams to a pickle in the project-root recordings/ dir.

CLI and on-disk schema are unchanged from the original script; the capture
path now reuses sceptre_pipeline.recorder.Recorder.
"""

import argparse
import socket
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

try:
    from sceptre_pipeline.recorder import Recorder, default_recording_path
except ImportError:  # package not installed; run straight from the source tree
    sys.path.insert(0, str(PROJECT_ROOT / "src"))
    from sceptre_pipeline.recorder import Recorder, default_recording_path


def default_output_filename() -> Path:
    return default_recording_path(PROJECT_ROOT)


def receive_udp(
    host: str,
    port: int,
    duration: float,
    output_path: Path,
    buffer_size: int = 65_535,
) -> None:
    recorder = Recorder()

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
        udp_socket.bind((host, port))

        print(f"Waiting for UDP data on {host}:{port}...")

        try:
            data, address = udp_socket.recvfrom(buffer_size)
        except KeyboardInterrupt:
            print("\nStopped before receiving any packets.")
            return

        capture_start_ns = time.time_ns()
        deadline = time.monotonic() + duration

        recorder.append(capture_start_ns, address, data)

        print(
            f"First packet received from {address[0]}:{address[1]} "
            f"({len(data)} bytes)"
        )
        print(f"Capturing for {duration:g} seconds...")

        try:
            while True:
                remaining = deadline - time.monotonic()

                if remaining <= 0:
                    break

                udp_socket.settimeout(remaining)

                try:
                    data, address = udp_socket.recvfrom(buffer_size)
                except socket.timeout:
                    break

                recorder.append(time.time_ns(), address, data)

                print(
                    f"Packet {len(recorder)}: "
                    f"{len(data)} bytes from "
                    f"{address[0]}:{address[1]}"
                )

        except KeyboardInterrupt:
            print("\nCapture interrupted. Saving packets received so far...")

    recorder.save(output_path, capture_start_ns, duration)

    print(f"\nSaved {len(recorder)} packets")
    print(f"Payload bytes: {recorder.total_bytes:,}")
    print(f"File: {output_path.resolve()}")


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Interface to listen on",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="UDP port",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=5.0,
        help="Capture duration after the first packet",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output .pkl file",
    )

    args = parser.parse_args()

    if not 0 <= args.port <= 65_535:
        parser.error("--port must be between 0 and 65535")

    if args.duration <= 0:
        parser.error("--duration must be greater than zero")

    receive_udp(
        host=args.host,
        port=args.port,
        duration=args.duration,
        output_path=args.output or default_output_filename(),
    )


if __name__ == "__main__":
    main()
