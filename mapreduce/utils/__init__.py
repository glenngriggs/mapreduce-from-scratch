"""Utils package.

This package is for code shared by the Manager and the Worker.
"""

from __future__ import annotations

import json
import logging
import pathlib
import socket
import time
from typing import Callable, Dict, Any, Optional, List

from .ordered_dict import ThreadSafeOrderedDict  # re-export

LOGGER = logging.getLogger(__name__)

# ---------- JSON helpers ----------


class PathJSONEncoder(json.JSONEncoder):
    """JSON encoder that knows how to serialize pathlib.Path."""

    def default(self, o: Any) -> Any:
        """Return a JSON-serializable form of *o*.

        If *o* is a pathlib.Path, return its string path; otherwise, defer to
        the base encoder.
        """
        if isinstance(o, pathlib.Path):
            return str(o)
        return super().default(o)


def _json_dumps(d: Dict[str, Any]) -> bytes:
    return json.dumps(d, cls=PathJSONEncoder).encode("utf-8")


def _json_loads(data: bytes) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(data.decode("utf-8"))
    except json.JSONDecodeError:
        return None

# ---------- TCP helpers ----------


def tcp_send(host: str, port: int, message: Dict[str, Any]) -> None:
    """Open a short-lived TCP connection, send a single JSON message, close."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        sock.connect((host, int(port)))
        sock.sendall(_json_dumps(message))


def tcp_server(
    host: str,
    port: int,
    signals: Dict[str, Any],
    handle_func: Callable[[Dict[str, Any]], None],
) -> None:
    """
    Lifetime TCP server: one listen() for the whole process.

    - Accept with 1s timeout so we can check shutdown flag.
    - Read until client closes; then parse JSON once.
    - Ignore invalid JSON per spec.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, int(port)))
        sock.listen()
        sock.settimeout(1)  # avoid busy-wait
        while not signals.get("stop"):
            try:
                client, _ = sock.accept()
            except socket.timeout:
                continue
            client.settimeout(1)
            with client:
                chunks: List[bytes] = []
                while True:
                    try:
                        data = client.recv(4096)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    chunks.append(data)
            raw = b"".join(chunks)
            if not raw:
                continue
            msg = _json_loads(raw)
            if msg is None:
                continue
            try:
                handle_func(msg)
            except (
                ValueError,
                TypeError,
                KeyError,
                AttributeError,
                LookupError,
                OSError,
                RuntimeError,
                ConnectionError,
            ) as exc:
                LOGGER.exception("TCP handler raised: %s", exc)

# ---------- UDP helpers (heartbeats) ----------


def udp_send(host: str, port: int, message: Dict[str, Any]) -> None:
    """Fire-and-forget UDP JSON."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect((host, int(port)))
        sock.sendall(_json_dumps(message))


def udp_server(
    host: str,
    port: int,
    signals: Dict[str, Any],
    handle_func: Callable[[Dict[str, Any]], None],
) -> None:
    """
    Lifetime UDP server.

    - recv with 1s timeout so we can check shutdown flag.
    - Each datagram is a whole JSON message.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, int(port)))
        sock.settimeout(1)
        while not signals.get("stop"):
            try:
                data = sock.recv(4096)
            except socket.timeout:
                continue
            msg = _json_loads(data)
            if msg is None:
                continue
            try:
                handle_func(msg)
            except (
                ValueError,
                TypeError,
                KeyError,
                AttributeError,
                LookupError,
                OSError,
                RuntimeError,
                ConnectionError,
            ) as exc:
                LOGGER.exception("UDP handler raised: %s", exc)
