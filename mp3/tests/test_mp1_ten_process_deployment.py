"""Ten-process loopback check of the unchanged MP1 client/server deployment contract."""

from __future__ import annotations

import json
import os
from pathlib import Path
import random
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from mp3.ring import load_manifest


REPO = Path(__file__).resolve().parents[2]
MP1 = REPO / "mp1"
GEN_MANIFEST = REPO / "mp3" / "deploy" / "gen_manifest.py"


def free_manifest_base() -> int:
    """Find a currently free 40-port block matching gen_manifest's local layout."""
    slots = list(range(43000, 54000, 40))
    random.SystemRandom().shuffle(slots)
    for base in slots:
        sockets = []
        try:
            for index in range(10):
                for offset in range(4):
                    sock = socket.socket()
                    sockets.append(sock)
                    sock.bind(("127.0.0.1", base + index * 4 + offset))
        except OSError:
            for sock in sockets:
                sock.close()
            continue
        for sock in sockets:
            sock.close()
        return base
    raise RuntimeError("could not find a free local manifest port block")


class MP1TenProcessDeploymentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="mp1-ten-local-")
        self.root = Path(self.temp.name)
        self.generated = self.root / "generated"
        self.run_dirs = []
        self.servers: list[subprocess.Popen] = []
        base = free_manifest_base()
        subprocess.run([
            sys.executable, "-B", str(GEN_MANIFEST), "--target", "local",
            "--base-port", str(base), "--out", str(self.generated),
        ], check=True, capture_output=True, text=True, timeout=10,
            env={**os.environ, "MP3_TARGET": "local"})
        self.manifest_path = self.generated / "manifest.json"
        self.manifest = load_manifest(self.manifest_path)

    def tearDown(self):
        for server in reversed(self.servers):
            if server.poll() is None:
                server.terminate()
                try:
                    server.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=3)
        self.temp.cleanup()

    def test_original_client_queries_all_ten_deployed_servers_at_manifest_ports(self):
        machines = []
        for number in range(1, 11):
            vm_id = f"vm{number:02d}"
            endpoint = self.manifest.node(vm_id).log_query
            run_dir = self.root / vm_id / "run"
            run_dir.mkdir(parents=True)
            self.run_dirs.append(run_dir)
            # Match deploy_mp: copy, do not adapt, the original server and framing module.
            for name in ("server.py", "protocol.py"):
                source = (MP1 / name).read_bytes()
                (run_dir / name).write_bytes(source)
                self.assertEqual((run_dir / name).read_bytes(), source)
            (run_dir / f"machine.{number}.log").write_text(
                f"release_match_marker vm={number}\n"
                f"release_count_marker vm={number}\n"
                f"release_count_marker vm={number}\n",
                encoding="utf-8")
            server = subprocess.Popen(
                [sys.executable, "-B", "server.py", str(number), str(endpoint.port)],
                cwd=run_dir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.servers.append(server)
            machines.append(f"{endpoint.host}:{endpoint.port}\n")

        client_dir = self.root / "client"
        client_dir.mkdir()
        (client_dir / "machines.txt").write_text("".join(machines), encoding="utf-8")
        deadline = time.monotonic() + 8
        for machine in machines:
            host, port = machine.strip().split(":")
            while True:
                try:
                    with socket.create_connection((host, int(port)), timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        self.fail(f"MP1 server did not listen at {host}:{port}")
                    time.sleep(0.02)

        match = self._run_unchanged_client(client_dir, "release_match_marker")
        self.assertIn("10 machines responded, 0 failed", match)
        self.assertIn("Total matches across all machines: 10", match)

        count = self._run_unchanged_client(client_dir, "-c release_count_marker")
        self.assertIn("10 machines responded, 0 failed", count)
        self.assertIn("Total matches across all machines: 20", count)

        no_match = self._run_unchanged_client(client_dir, "release_no_such_marker_xyz")
        self.assertIn("10 machines responded, 0 failed", no_match)
        self.assertIn("Total matches across all machines: 0", no_match)

    def _run_unchanged_client(self, cwd: Path, pattern: str) -> str:
        result = subprocess.run(
            [sys.executable, "-B", str(MP1 / "client.py")],
            input=pattern + "\n", cwd=cwd, capture_output=True, text=True,
            timeout=15, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout


if __name__ == "__main__":
    unittest.main()
