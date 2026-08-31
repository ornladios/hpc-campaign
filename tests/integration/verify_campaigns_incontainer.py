#!/usr/bin/env python3

import socket
import time

from tests.integration.verify_campaigns import main as verify_campaigns


def check_port(host: str, port: int, timeout: float = 15) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.1)


def main() -> None:
    for host, port in (
        ("s3-service.docker.hpc-campaign", 9000),
        ("https-service.docker.hpc-campaign", 443),
        ("ssh-service.docker.hpc-campaign", 22),
        ("xrootd-service.docker.hpc-campaign", 8080),
    ):
        check_port(host, port)
    check_port("localhost", 30000)
    verify_campaigns()


if __name__ == "__main__":
    main()
