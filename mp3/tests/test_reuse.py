"""Characterize selected original behavior without starting MP1/MP2 services."""

import importlib.util
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

from mp3 import legacy_membership as membership
from mp3.framing import recv_frame, send_frame

ROOT = Path(__file__).resolve().parents[2]


def load_reference(relative):
    """Load a side-effect-free reference module under a unique module name."""
    spec = importlib.util.spec_from_file_location("phase0_reference", ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    # Prevent __pycache__ writes in the immutable reference directories.
    with patch("sys.dont_write_bytecode", True):
        spec.loader.exec_module(module)
    return module


class ReuseTests(unittest.TestCase):
    def test_legacy_copy_is_byte_identical(self):
        self.assertEqual((ROOT / "mp2/membership.py").read_bytes(),
                         (ROOT / "mp3/legacy_membership.py").read_bytes())

    def test_both_original_protocols_interoperate(self):
        for path in ("mp1/protocol.py", "mp2/protocol.py"):
            original = load_reference(path)
            left, right = socket.socketpair()
            with self.subTest(path=path), left, right:
                left.settimeout(0.5)
                right.settimeout(0.5)
                for payload in (b"", b"\x00\xff\n"):
                    original.send_msg(left, payload)
                    self.assertEqual(recv_frame(right, max_bytes=10), payload)
                    send_frame(right, payload, max_bytes=10)
                    self.assertEqual(original.recv_msg(left), payload)

    def test_heartbeat_and_snapshot_are_independent(self):
        group = membership.MembershipList("self")
        group.tick_heartbeat()
        snapshot = group.snapshot()
        self.assertEqual(snapshot[0]["heartbeat"], 1)
        snapshot[0]["heartbeat"] = 999
        self.assertEqual(group.snapshot()[0]["heartbeat"], 1)

    def test_failed_entry_rejects_restart_known_limitation(self):
        group = membership.MembershipList("self")
        group.merge([dict(node_id="peer", heartbeat=1, incarnation=0, status="FAILED")])
        group.merge([dict(node_id="peer", heartbeat=50, incarnation=999, status="ALIVE")])
        peer = next(row for row in group.snapshot() if row["node_id"] == "peer")
        self.assertEqual(peer["status"], "FAILED")
        self.assertNotIn("peer", group.alive_peers())
        self.assertIn("peer", group.all_known_peers())
        self.assertNotIn("peer", [row[0] for row in group.list_mem()])

    def test_refutation_increments_incarnation(self):
        group = membership.MembershipList("self")
        self.assertTrue(group.merge([dict(node_id="self", heartbeat=0,
                                         incarnation=3, status="SUSPECTED")]))
        self.assertEqual(group.snapshot()[0]["incarnation"], 4)

    def test_timeout_lifecycle_with_controlled_clock(self):
        with patch.object(membership.time, "time", return_value=100):
            group = membership.MembershipList("self")
            group.set_suspicion_mode(True)
            group.merge([dict(node_id="peer", heartbeat=1, incarnation=0, status="ALIVE")])
        with patch.object(membership.time, "time", return_value=102):
            group.run_timeout_check()
        peer = next(row for row in group.snapshot() if row["node_id"] == "peer")
        self.assertEqual(peer["status"], "SUSPECTED")
        with patch.object(membership.time, "time", return_value=104):
            group.run_timeout_check()
        peer = next(row for row in group.snapshot() if row["node_id"] == "peer")
        self.assertEqual(peer["status"], "FAILED")
