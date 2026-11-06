"""MP3 machine logs queried by the unchanged MP1 server and client (KD-17).

The original MP1 server runs as a separate process in the log directory and the
original MP1 client function queries it. Nothing in mp1/ is imported or modified here.
"""

import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from mp3.tests.test_rr import RRCluster, delayed, version

MP1 = Path(__file__).resolve().parents[2] / "mp1"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Mp1LogQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.cluster = RRCluster(cls.root, log_vm="vm01")  # writes machine.1.log here
        # Only vm01 writes machine.1.log, so use a key that vm01 owns (it then logs the
        # replica receive/respond lines as well as the coordinator's read_repair event).
        key = next(f"mp1-key-{i}" for i in range(200)
                   if "vm01" in cls.cluster.owners(f"mp1-key-{i}"))
        cls.key = key
        owners = cls.cluster.owners(key)
        cls.cluster.install(owners[0], key, version(5), b"five")
        cls.cluster.install(owners[1], key, version(1), b"one")
        with delayed({owners[0]: 0.4}):
            result = cls.cluster.get(key, read_quorum=2)
            cls.report = result.propagation.wait(timeout=20)
        cls.port = free_port()
        cls.server = subprocess.Popen(
            [sys.executable, "-B", str(MP1 / "server.py"), "1", str(cls.port)],
            cwd=cls.root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            try:
                socket.create_connection(("127.0.0.1", cls.port), timeout=0.3).close()
                return
            except OSError:
                time.sleep(0.05)
        cls.tearDownClass()
        raise AssertionError("MP1 server did not start")

    @classmethod
    def tearDownClass(cls):
        server = getattr(cls, "server", None)
        if server is not None:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
        cls.cluster.stop()
        cls.temp.cleanup()

    def query(self, pattern: str) -> dict:
        code = ("import sys, json; sys.path.insert(0, sys.argv[1]); import client; "
                "print(json.dumps(client.query_machine('127.0.0.1', int(sys.argv[2]), sys.argv[3])))")
        done = subprocess.run([sys.executable, "-B", "-c", code, str(MP1), str(self.port), pattern],
                              capture_output=True, text=True, timeout=30, cwd=self.root)
        self.assertEqual(done.returncode, 0, done.stderr)
        return json.loads(done.stdout)

    def test_unchanged_mp1_client_finds_the_read_repair_event(self):
        result = self.query("read_repair")
        self.assertTrue(result["success"], result)
        self.assertEqual(result["filename"], "machine.1.log")
        self.assertEqual(result["count"], 1)
        event = json.loads(result["lines"][0])
        self.assertEqual(event["request_id"], self.report.operation_id)
        self.assertEqual(event["maximum_version"], "eventual:5:vm02/1")

    def test_count_option_and_replica_receive_respond_lines(self):
        received = self.query("-c receivedget")
        responded = self.query("-c respondedget")
        self.assertTrue(received["success"] and responded["success"])
        self.assertEqual(received["count"], responded["count"])
        self.assertGreaterEqual(received["count"], 1)  # vm01 served the read as an owner
        by_key = self.query(f"'receivedget {self.key}'")
        self.assertGreaterEqual(by_key["count"], 1)
        self.assertEqual(self.query(f"'receivedget {self.key}-other'")["count"], 0)

    def test_query_by_repair_owner_and_status(self):
        repaired = self.query('-F \'"status": "REPAIRED"\'')
        self.assertEqual(repaired["count"], 1)
        self.assertEqual(self.query("no-such-text-anywhere")["count"], 0)

    def test_no_value_bytes_reach_the_queryable_log(self):
        self.assertEqual(self.query("-c 'five'")["count"], 0)


if __name__ == "__main__":
    unittest.main()
