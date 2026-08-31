#!/usr/bin/env python3

import argparse
import http.server
import os
import shutil
import socketserver
import ssl
from pathlib import Path
from typing import BinaryIO


class RangeRequestHandler(http.server.SimpleHTTPRequestHandler):
    """Static file handler with the byte ranges ADIOS remote reads require."""

    protocol_version = "HTTP/1.1"
    byte_range: tuple[int, int] | None = None

    def send_head(self) -> BinaryIO | None:
        range_header = self.headers.get("Range")
        path = Path(self.translate_path(self.path))
        if not range_header or not path.is_file():
            self.byte_range = None
            return super().send_head()

        try:
            unit, requested_range = range_header.strip().split("=", 1)
            start_text, end_text = requested_range.split("-", 1)
            if unit != "bytes" or "," in requested_range:
                raise ValueError
            size = path.stat().st_size
            if start_text:
                start = int(start_text)
                end = int(end_text) if end_text else size - 1
            else:
                suffix_length = int(end_text)
                start = max(0, size - suffix_length)
                end = size - 1
            end = min(end, size - 1)
            if start < 0 or start > end:
                raise ValueError
        except (OSError, ValueError):
            self.send_error(http.HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
            return None

        file_object = path.open("rb")
        self.byte_range = (start, end)
        self.send_response(http.HTTPStatus.PARTIAL_CONTENT)
        self.send_header("Content-type", self.guess_type(str(path)))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Content-Length", str(end - start + 1))
        self.send_header("Last-Modified", self.date_time_string(path.stat().st_mtime))
        self.end_headers()
        self.wfile.flush()
        return file_object

    def copyfile(self, source: BinaryIO, outputfile: BinaryIO) -> None:
        if self.byte_range is None:
            shutil.copyfileobj(source, outputfile)
            return
        start, end = self.byte_range
        source.seek(start)
        remaining = end - start + 1
        while remaining:
            chunk = source.read(min(64 * 1024, remaining))
            if not chunk:
                break
            outputfile.write(chunk)
            remaining -= len(chunk)


class ThreadingHTTPSServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve integration-test data over HTTPS")
    parser.add_argument("--directory", required=True)
    parser.add_argument("--port", type=int, default=443)
    parser.add_argument("--certificate", required=True)
    parser.add_argument("--key", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    os.chdir(args.directory)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(args.certificate, args.key)

    with ThreadingHTTPSServer(("0.0.0.0", args.port), RangeRequestHandler) as server:
        server.socket = context.wrap_socket(server.socket, server_side=True)
        print(f"Serving {args.directory} over HTTPS on port {args.port}", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
