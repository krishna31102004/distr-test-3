import struct
import time

HEADER = struct.Struct("!I")


def _validate_limit(max_bytes: int) -> None:
    if type(max_bytes) is not int or not 0 <= max_bytes <= 0xFFFFFFFF:
        raise ValueError("max_bytes must be an integer in 0..2**32-1")


def recv_exact(sock, size: int) -> bytes:
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
    _validate_limit(max_bytes)
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes")
    if len(payload) > max_bytes:
        raise ValueError("frame exceeds max_bytes")
    sock.sendall(HEADER.pack(len(payload)))
    sock.sendall(payload)


def recv_frame(sock, *, max_bytes: int) -> bytes:
    _validate_limit(max_bytes)
    length, = HEADER.unpack(recv_exact(sock, HEADER.size))
    if length > max_bytes:
        raise ValueError("frame exceeds max_bytes")
    return recv_exact(sock, length)


def _remaining(deadline: float) -> float:
    if type(deadline) not in (int, float):
        raise ValueError("deadline must be an absolute monotonic number")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("operation deadline expired")
    return remaining


def recv_exact_deadline(sock, size: int, *, deadline: float) -> bytes:
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


def send_all_deadline(sock, data: bytes, *, deadline: float, before_send=None) -> None:
    if not isinstance(data, bytes):
        raise TypeError("data must be bytes")
    sock.settimeout(_remaining(deadline))
    if before_send is not None:
        before_send()
    sock.sendall(data)


def recv_frame_deadline(sock, *, max_bytes: int, deadline: float) -> bytes:
    _validate_limit(max_bytes)
    length, = HEADER.unpack(recv_exact_deadline(sock, HEADER.size, deadline=deadline))
    if length > max_bytes:
        raise ValueError("frame exceeds max_bytes")
    return recv_exact_deadline(sock, length, deadline=deadline)


def send_frame_deadline(sock, payload: bytes, *, max_bytes: int, deadline: float) -> None:
    _validate_limit(max_bytes)
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes")
    if len(payload) > max_bytes:
        raise ValueError("frame exceeds max_bytes")
    send_all_deadline(sock, HEADER.pack(len(payload)), deadline=deadline)
    send_all_deadline(sock, payload, deadline=deadline)
