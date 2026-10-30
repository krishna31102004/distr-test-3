"""Four-byte network-order framing adapted from our MP1/MP2 protocol.py.

The legacy `send_frame`/`recv_frame` functions retain their original simple
timeout semantics for compatibility. Batch 3 RPC uses the deadline-aware helpers
with an absolute monotonic deadline; `rpc.py` adds the approved inner JSON/body
lengths and validates them before receiving a value. One request uses one connection.
"""

import struct
import time

HEADER = struct.Struct("!I")


def _validate_limit(max_bytes: int) -> None:
    """Require an explicit cap representable by the four-byte header."""
    if type(max_bytes) is not int or not 0 <= max_bytes <= 0xFFFFFFFF:
        raise ValueError("max_bytes must be an integer in 0..2**32-1")


def recv_exact(sock, size: int) -> bytes:
    """Read exactly size bytes, accepting fragmented reads; raise on early EOF."""
    if type(size) is not int or size < 0:
        raise ValueError("size must be a nonnegative integer")
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("socket closed before expected data arrived")
        data.extend(chunk)
    return bytes(data)


def send_frame(sock, payload: bytes, *, max_bytes: int) -> None:
    """Send raw bytes including empty bytes; reject oversized data before writing."""
    _validate_limit(max_bytes)
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes")
    if len(payload) > max_bytes:
        raise ValueError("frame exceeds max_bytes")
    sock.sendall(HEADER.pack(len(payload)))
    sock.sendall(payload)


def recv_frame(sock, *, max_bytes: int) -> bytes:
    """Read one bounded frame; reject an oversized header before reading its body.

    After malformed/oversized/truncated frames or timeouts, the caller must close
    the connection: the next frame boundary may no longer be trustworthy.
    """
    _validate_limit(max_bytes)
    length, = HEADER.unpack(recv_exact(sock, HEADER.size))
    if length > max_bytes:
        raise ValueError("frame exceeds max_bytes")
    return recv_exact(sock, length)


def _remaining(deadline: float) -> float:
    """Return time left on an absolute monotonic deadline or raise timeout."""
    if type(deadline) not in (int, float):
        raise ValueError("deadline must be an absolute monotonic number")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("operation deadline expired")
    return remaining


def recv_exact_deadline(sock, size: int, *, deadline: float) -> bytes:
    """Read an exact byte count while enforcing one deadline across fragments."""
    if type(size) is not int or size < 0:
        raise ValueError("size must be a nonnegative integer")
    data = bytearray()
    while len(data) < size:
        sock.settimeout(_remaining(deadline))
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("socket closed before expected data arrived")
        data.extend(chunk)
    return bytes(data)


def send_all_deadline(sock, data: bytes, *, deadline: float) -> None:
    """Send bytes using the remaining portion of a single absolute deadline."""
    if not isinstance(data, bytes):
        raise TypeError("data must be bytes")
    sock.settimeout(_remaining(deadline))
    sock.sendall(data)


def recv_frame_deadline(sock, *, max_bytes: int, deadline: float) -> bytes:
    """Read one ordinary length-prefixed frame under a total monotonic deadline."""
    _validate_limit(max_bytes)
    length, = HEADER.unpack(recv_exact_deadline(sock, HEADER.size, deadline=deadline))
    if length > max_bytes:
        raise ValueError("frame exceeds max_bytes")
    return recv_exact_deadline(sock, length, deadline=deadline)


def send_frame_deadline(sock, payload: bytes, *, max_bytes: int, deadline: float) -> None:
    """Send one ordinary length-prefixed frame under a total monotonic deadline."""
    _validate_limit(max_bytes)
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes")
    if len(payload) > max_bytes:
        raise ValueError("frame exceeds max_bytes")
    send_all_deadline(sock, HEADER.pack(len(payload)), deadline=deadline)
    send_all_deadline(sock, payload, deadline=deadline)
