#!/usr/bin/env python3

import argparse
import pickle
import socket
import time
from datetime import datetime
from pathlib import Path


def default_output_filename() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path(f"udp_capture_{timestamp}.pkl")


def receive_udp(
    host: str,
    port: int,
    duration: float,
    output_path: Path,
    buffer_size: int = 65_535,
) -> None:
    packets: list[tuple[int, str, int, bytes]] = []

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

        packets.append(
            (
                capture_start_ns,
                address[0],
                address[1],
                data,
            )
        )

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

                packets.append(
                    (
                        time.time_ns(),
                        address[0],
                        address[1],
                        data,
                    )
                )

                print(
                    f"Packet {len(packets)}: "
                    f"{len(data)} bytes from "
                    f"{address[0]}:{address[1]}"
                )

        except KeyboardInterrupt:
            print("\nCapture interrupted. Saving packets received so far...")

    capture = {
        "start_time_ns": capture_start_ns,
        "duration_seconds": duration,
        "packets": packets,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("wb") as file:
        pickle.dump(
            capture,
            file,
            protocol=pickle.HIGHEST_PROTOCOL,
        )

    total_payload_bytes = sum(len(packet[3]) for packet in packets)

    print(f"\nSaved {len(packets)} packets")
    print(f"Payload bytes: {total_payload_bytes:,}")
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