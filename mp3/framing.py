"""Bounded byte framing adapted from our MP1/MP2 protocol.py.

The four-byte network-order length is a reusable prototype, not approval of
the final RPC transport/schema (KD-06). Callers MUST supply a finite socket
timeout and a frame cap; this module never changes socket settings. A socket
inactivity timeout does not bound an entire fragmented frame: future RPC callers
must separately enforce an overall monotonic operation deadline (KD-08), such as
through deadline-aware socket IO or an approved extension to this framing API.
Use one reader and serialized writers per connection if connections are shared.
"""

import struct

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
