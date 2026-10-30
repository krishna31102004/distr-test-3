"""No VM connections: fragmented fake sockets and local socket pairs only."""

import socket
import struct
import unittest

from mp3.config import MAX_VALUE_BYTES
from mp3.framing import recv_exact, recv_frame, send_frame


class FragmentSocket:
    def __init__(self, data=b"", fragment=1):
        self.data = data
        self.fragment = fragment
        self.sent = bytearray()
        self.reads = []

    def recv(self, size):
        self.reads.append(size)
        chunk = self.data[:min(size, self.fragment)]
        self.data = self.data[len(chunk):]
        return chunk

    def sendall(self, data):
        self.sent.extend(data)


class FramingTests(unittest.TestCase):
    def test_arbitrary_bytes_and_empty(self):
        for payload in (b"", b"\x00\xff\n\r", bytes(range(256))):
            with self.subTest(length=len(payload)):
                sock = FragmentSocket()
                send_frame(sock, payload, max_bytes=512)
                sock.data = bytes(sock.sent)
                self.assertEqual(recv_frame(sock, max_bytes=512), payload)

    def test_one_mib_with_fragmentation(self):
        payload = bytes(range(256)) * (MAX_VALUE_BYTES // 256)
        sock = FragmentSocket(struct.pack("!I", len(payload)) + payload, fragment=997)
        self.assertEqual(recv_frame(sock, max_bytes=MAX_VALUE_BYTES), payload)

    def test_multiple_frames(self):
        sock = FragmentSocket()
        for value in (b"one", b"", b"three"):
            send_frame(sock, value, max_bytes=10)
        sock.data = bytes(sock.sent)
        for value in (b"one", b"", b"three"):
            self.assertEqual(recv_frame(sock, max_bytes=10), value)

    def test_truncated_header_and_body(self):
        for data in (b"", b"\x00\x00", struct.pack("!I", 5) + b"ab"):
            with self.subTest(data=data), self.assertRaises(ConnectionError):
                recv_frame(FragmentSocket(data), max_bytes=10)

    def test_oversized_header_does_not_read_body(self):
        sock = FragmentSocket(struct.pack("!I", 0xFFFFFFFF) + b"unread", fragment=4)
        with self.assertRaises(ValueError):
            recv_frame(sock, max_bytes=MAX_VALUE_BYTES)
        self.assertEqual(sock.data, b"unread")

    def test_send_rejects_before_any_write(self):
        sock = FragmentSocket()
        with self.assertRaises(ValueError):
            send_frame(sock, b"abc", max_bytes=2)
        self.assertEqual(sock.sent, b"")

    def test_bad_limits_and_types(self):
        for limit in (-1, True, 2**32, 1.2):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                recv_frame(FragmentSocket(), max_bytes=limit)
        with self.assertRaises(TypeError):
            send_frame(FragmentSocket(), "text", max_bytes=10)
        for size in (-1, True):
            with self.assertRaises(ValueError):
                recv_exact(FragmentSocket(), size)

    def test_real_socket_pair_and_timeout(self):
        left, right = socket.socketpair()
        with left, right:
            left.settimeout(0.2)
            right.settimeout(0.05)
            send_frame(left, b"\x00\xff", max_bytes=10)
            self.assertEqual(recv_frame(right, max_bytes=10), b"\x00\xff")
            with self.assertRaises(socket.timeout):
                recv_frame(right, max_bytes=10)
