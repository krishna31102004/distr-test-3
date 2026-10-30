"""Loopback KV RPC, replica storage integration, resource bounds, and recovery."""

from concurrent.futures import ThreadPoolExecutor
import io
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import struct
import sys
import tempfile
import threading
import time
import uuid
import unittest
from unittest.mock import patch

from mp3.config import MAX_VALUE_BYTES
from mp3.framing import HEADER
from mp3.membership import GossipEnvelope, Member, MemberState, MembershipService, MembershipTable
from mp3.models import ConsistencyMode, Context, LocalStatus, Version
from mp3.replica import ReplicaService
from mp3.ring import Endpoint, Manifest
from mp3.rpc import (
    MAX_RPC_PAYLOAD, ReplicaClient, ReplicaRequest, ReplicaResponse, RpcOperation,
    RpcOutcomeKind, RpcStatus, encode_response, new_request, receive_request,
    receive_response, send_request,
)
from mp3.storage import open_store


ROOT = Path(__file__).resolve().parents[2]
CTX = Context(1, ConsistencyMode.EVENTUAL_NO_RR)


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_manifest():
    endpoints = {}
    used = set()
    for number in range(1, 11):
        ports = []
        while len(ports) < 3:
            port = unused_port()
            if port not in used:
                used.add(port)
                ports.append(port)
        vm = f"vm{number:02d}"
        endpoints[vm] = {
            "kv": Endpoint("127.0.0.1", ports[0]),
            "membership": Endpoint("127.0.0.1", ports[1]),
            "log_query": Endpoint("127.0.0.1", ports[2]),
        }
    return Manifest.from_endpoints(endpoints)


def membership_table(manifest, self_id, generation, peer_generations):
    table = MembershipTable(manifest, self_id)
    table.set_local_generation(generation)
    rows = {row.vm_id: row for row in table.snapshot()}
    rows[self_id] = Member(self_id, generation, 0, 0, MemberState.ALIVE)
    peers = dict(peer_generations)
    sender = next((f"vm{i:02d}" for i in range(1, 11)
                   if f"vm{i:02d}" != self_id and f"vm{i:02d}" in peers), None)
    if sender is None:
        sender = next(f"vm{i:02d}" for i in range(1, 11)
                      if f"vm{i:02d}" != self_id)
        peers[sender] = 1
    for vm_id, peer_generation in peers.items():
        rows[vm_id] = Member(vm_id, peer_generation, 0, 1, MemberState.ALIVE)
    envelope = GossipEnvelope(manifest.fingerprint, sender, peers[sender], tuple(rows.values()))
    table.merge_gossip(envelope)
    return table


class RpcEnvironment(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.manifest = make_manifest()
        self.client_table = membership_table(self.manifest, "vm01", 1, {"vm04": 1})
        self.client = ReplicaClient(self.manifest, self.client_table)
        self.store = open_store(Path(self.temp.name) / "vm04.sqlite", bootstrap=CTX,
                                vm_id="vm04")
        self.addCleanup(self.store.close)
        generation = self.store.start_incarnation()
        self.replica_table = membership_table(self.manifest, "vm04", generation,
                                              {"vm01": 1})
        self.output = io.StringIO()
        self.service = ReplicaService(self.store, self.manifest, self.replica_table,
                                      workers=8, max_pending=32,
                                      event_stream=self.output)
        self.service.start()
        self.addCleanup(self.service.stop)
        self.target = self.manifest.node("vm04")

    def make_request(self, op, key, *, version=None, value=b"", epoch=1,
                     sender_generation=1, target_id="vm04", manifest_id=None):
        return ReplicaRequest(
            request_id=str(uuid.uuid4()),
            manifest_id=manifest_id or self.manifest.fingerprint,
            sender_id="vm01", sender_generation=sender_generation,
            target_id=target_id, epoch=epoch, mode=CTX.mode, op=op,
            phase="local", key=key, version=version, value=value,
        )

    def call(self, request, *, timeout=3.0):
        return self.client.call_replica(self.target, request,
                                        deadline=time.monotonic() + timeout)

    def raw_call(self, request):
        deadline = time.monotonic() + 2
        with socket.create_connection((self.target.kv.host, self.target.kv.port), timeout=1) as sock:
            send_request(sock, request, self.manifest, deadline=deadline)
            sock.shutdown(socket.SHUT_WR)
            return receive_response(sock, self.manifest, deadline=deadline)

    def response(self, request, **kwargs):
        outcome = self.call(request, **kwargs)
        self.assertEqual(outcome.kind, RpcOutcomeKind.RESPONSE, outcome.error)
        self.assertIsNotNone(outcome.response)
        return outcome.response


class ReplicaIntegrationTests(RpcEnvironment):
    def test_actual_tcp_read_install_outcomes_and_empty_vs_missing(self):
        missing = self.response(self.make_request(RpcOperation.READ_LOCAL, "photo"))
        self.assertEqual((missing.status, missing.present, missing.version, missing.value),
                         (RpcStatus.NOT_FOUND, False, None, b""))

        v7 = Version("eventual", 7, "vm01/1")
        installed = self.response(self.make_request(
            RpcOperation.INSTALL_IF_NEWER, "photo", version=v7, value=b"image bytes"))
        self.assertEqual((installed.status, installed.stored_version, installed.durable),
                         (RpcStatus.INSTALLED, v7, True))
        found = self.response(self.make_request(RpcOperation.READ_LOCAL, "photo"))
        self.assertEqual((found.status, found.present, found.version, found.value),
                         (RpcStatus.FOUND, True, v7, b"image bytes"))

        stale = self.response(self.make_request(
            RpcOperation.INSTALL_IF_NEWER, "photo",
            version=Version("eventual", 6, "vm01/1"), value=b"older"))
        self.assertEqual((stale.status, stale.stored_version, stale.durable),
                         (RpcStatus.SUPERSEDED, v7, True))
        conflict = self.response(self.make_request(
            RpcOperation.INSTALL_IF_NEWER, "photo", version=v7, value=b"different"))
        self.assertEqual((conflict.status, conflict.stored_version, conflict.durable),
                         (RpcStatus.VERSION_CONFLICT, v7, False))
        unchanged = self.response(self.make_request(
            RpcOperation.INSTALL_IF_NEWER, "photo", version=v7, value=b"image bytes"))
        self.assertEqual((unchanged.status, unchanged.stored_version, unchanged.durable),
                         (RpcStatus.UNCHANGED, v7, True))

        empty_version = Version("eventual", 8, "vm01/1")
        self.response(self.make_request(RpcOperation.INSTALL_IF_NEWER, "empty",
                                        version=empty_version, value=b""))
        empty = self.response(self.make_request(RpcOperation.READ_LOCAL, "empty"))
        self.assertEqual((empty.status, empty.present, empty.value),
                         (RpcStatus.FOUND, True, b""))

    def test_client_connect_uses_cached_addresses_and_shared_candidate_deadline(self):
        resolved = self.client._addresses[self.target.vm_id]
        self.client._addresses[self.target.vm_id] = (
            (socket.AF_INET, socket.SOCK_STREAM, 0,
             ("127.0.0.1", unused_port())),
            *resolved,
        )
        request = self.make_request(RpcOperation.READ_LOCAL, "cached-dns")
        with patch("mp3.rpc.socket.getaddrinfo", side_effect=AssertionError("DNS in RPC")), \
                patch("mp3.rpc.socket.create_connection",
                      side_effect=AssertionError("create_connection resets candidate timeout")):
            response = self.response(request)
        self.assertEqual(response.status, RpcStatus.NOT_FOUND)

    def test_actual_tcp_unicode_nul_key_and_full_binary_value(self):
        key = "π\x00photo"
        value = bytes(range(256)) * (MAX_VALUE_BYTES // 256)
        version = Version("eventual", 9, "vm01/1")
        write = self.response(self.make_request(
            RpcOperation.INSTALL_IF_NEWER, key, version=version, value=value), timeout=5)
        self.assertEqual((write.status, write.value_len, write.durable),
                         (RpcStatus.INSTALLED, 0, True))
        read = self.response(self.make_request(RpcOperation.READ_LOCAL, key), timeout=5)
        self.assertEqual((read.status, read.value, read.version),
                         (RpcStatus.FOUND, value, version))

    def test_stale_epoch_returns_current_context_and_does_not_mutate(self):
        request = self.make_request(RpcOperation.INSTALL_IF_NEWER, "old-mode",
                                    version=Version("eventual", 1, "vm01/1"),
                                    value=b"never", epoch=2)
        response = self.response(request)
        self.assertEqual((response.status, response.epoch, response.mode),
                         (RpcStatus.STALE_EPOCH, 1, ConsistencyMode.EVENTUAL_NO_RR))
        self.assertFalse(response.durable)
        self.assertEqual(self.store.read_local("old-mode", context=CTX).status,
                         LocalStatus.NOT_FOUND)

    def test_zero_epoch_returns_stale_context_under_request_deadline(self):
        request = self.make_request(RpcOperation.READ_LOCAL, "zero-epoch", epoch=0)
        response = self.response(request)
        self.assertEqual((response.status, response.epoch, response.mode),
                         (RpcStatus.STALE_EPOCH, CTX.epoch, CTX.mode))

    def test_invalid_sender_generation_and_wrong_target_do_not_mutate(self):
        wrong_generation = self.make_request(
            RpcOperation.INSTALL_IF_NEWER, "bad-generation",
            version=Version("eventual", 1, "vm01/1"), value=b"x", sender_generation=2)
        out = self.raw_call(wrong_generation)
        self.assertEqual((out.status, out.error),
                         (RpcStatus.ERROR, "INVALID_REQUEST"))

        wrong_target = self.make_request(
            RpcOperation.INSTALL_IF_NEWER, "bad-target",
            version=Version("eventual", 1, "vm01/1"), value=b"x", target_id="vm05")
        out = self.raw_call(wrong_target)
        self.assertEqual((out.status, out.error), (RpcStatus.ERROR, "INVALID_REQUEST"))
        self.assertEqual(self.store.read_local("bad-generation", context=CTX).status,
                         LocalStatus.NOT_FOUND)
        self.assertEqual(self.store.read_local("bad-target", context=CTX).status,
                         LocalStatus.NOT_FOUND)

    def test_wrong_manifest_is_closed_before_storage_mutation(self):
        from mp3.rpc import _encode_payload
        malformed = self.make_request(
            RpcOperation.INSTALL_IF_NEWER, "bad-manifest",
            version=Version("eventual", 1, "vm01/1"), value=b"x",
            manifest_id="0" * 64)
        payload = _encode_payload(malformed.to_metadata(), malformed.value)
        with socket.create_connection((self.target.kv.host, self.target.kv.port), timeout=1) as sock:
            sock.sendall(HEADER.pack(len(payload)) + payload)
            sock.shutdown(socket.SHUT_WR)
            self.assertEqual(sock.recv(1), b"")
        self.assertEqual(self.store.read_local("bad-manifest", context=CTX).status,
                         LocalStatus.NOT_FOUND)

    def test_receipt_and_response_terminal_lines_do_not_log_values(self):
        value = b"secret value bytes"
        self.response(self.make_request(
            RpcOperation.INSTALL_IF_NEWER, "safe-key",
            version=Version("eventual", 1, "vm01/1"), value=value))
        text = self.output.getvalue()
        self.assertIn(": receivedput safe-key", text)
        self.assertIn(": respondedput safe-key", text)
        self.assertNotIn(value.decode(), text)

    def test_committed_write_with_lost_ack_is_reported_unknown(self):
        request = self.make_request(
            RpcOperation.INSTALL_IF_NEWER, "ack-lost",
            version=Version("eventual", 12, "vm01/1"), value=b"committed")
        with patch("mp3.replica.send_response", side_effect=ConnectionResetError("ack dropped")):
            outcome = self.call(request)
        self.assertEqual(outcome.kind, RpcOutcomeKind.ERROR)
        self.assertIsNone(outcome.response)
        self.assertTrue(outcome.completion_unknown)
        stored = self.store.read_local("ack-lost", context=CTX)
        self.assertEqual((stored.status, stored.record.value),
                         (LocalStatus.FOUND, b"committed"))

    def test_client_timeout_after_commit_marks_completion_unknown(self):
        request = self.make_request(
            RpcOperation.INSTALL_IF_NEWER, "late-ack",
            version=Version("eventual", 13, "vm01/1"), value=b"committed before deadline")
        from mp3.replica import send_response as real_send_response

        def delayed_response(*args, **kwargs):
            time.sleep(0.25)
            return real_send_response(*args, **kwargs)

        with patch("mp3.replica.send_response", side_effect=delayed_response):
            outcome = self.call(request, timeout=0.08)
        self.assertEqual(outcome.kind, RpcOutcomeKind.TIMEOUT)
        self.assertTrue(outcome.completion_unknown)
        stored = self.store.read_local("late-ack", context=CTX)
        self.assertEqual((stored.status, stored.record.value),
                         (LocalStatus.FOUND, b"committed before deadline"))

    def test_concurrent_clients_progress_for_distinct_and_same_keys(self):
        def put(index):
            key = "shared" if index < 8 else f"other-{index}"
            version = Version("eventual", index + 1, "vm01/1")
            return self.call(self.make_request(RpcOperation.INSTALL_IF_NEWER, key,
                                               version=version, value=str(index).encode()))

        with ThreadPoolExecutor(max_workers=12) as pool:
            outcomes = list(pool.map(put, range(16)))
        self.assertTrue(all(outcome.kind == RpcOutcomeKind.RESPONSE for outcome in outcomes))
        result = self.store.read_local("shared", context=CTX)
        self.assertEqual(result.status, LocalStatus.FOUND)
        self.assertEqual(result.record.version.number, 8)
        self.assertEqual(result.record.value, b"7")

    def test_membership_gossip_uses_separate_service_while_kv_load_runs(self):
        member_service = MembershipService(self.target.membership, self.replica_table,
                                           workers=1, max_pending=2)
        member_service.start()
        self.addCleanup(member_service.stop)

        def read(index):
            return self.call(self.make_request(RpcOperation.READ_LOCAL, f"load-{index}"))

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(read, index) for index in range(16)]
            from mp3.membership import exchange_gossip
            response = exchange_gossip(self.target.membership, self.client_table.envelope(),
                                       self.manifest, timeout=1.0)
            self.client_table.merge_gossip(response)
            outcomes = [future.result(timeout=3) for future in futures]
        self.assertTrue(all(outcome.kind == RpcOutcomeKind.RESPONSE for outcome in outcomes))
        self.assertEqual(response.sender_id, "vm04")

    def test_shutdown_is_bounded_and_joins_worker_threads(self):
        report = self.service.stop(timeout=2)
        self.assertEqual(report.incomplete_requests, 0)
        self.assertTrue(report.workers_joined)
        self.assertFalse(any(thread.name.startswith("mp3-kv-handler-vm04")
                             for thread in threading.enumerate()))

    def test_full_worker_and_queue_returns_explicit_overloaded(self):
        self.service.stop(timeout=2)
        self.service = ReplicaService(self.store, self.manifest, self.replica_table,
                                      workers=1, max_pending=1, request_timeout=3,
                                      overload_reply_timeout=0.5,
                                      event_stream=self.output)
        self.service.start()
        self.addCleanup(self.service.stop)

        slow = socket.create_connection((self.target.kv.host, self.target.kv.port), timeout=1)
        slow.sendall(HEADER.pack(MAX_RPC_PAYLOAD))
        limit = time.monotonic() + 1
        while self.service._outstanding < 1 and time.monotonic() < limit:
            time.sleep(0.005)
        self.assertEqual(self.service._outstanding, 1)

        with ThreadPoolExecutor(max_workers=1) as pool:
            queued = pool.submit(self.call,
                                 self.make_request(RpcOperation.READ_LOCAL, "queued"),
                                 timeout=2)
            limit = time.monotonic() + 1
            while self.service._queue.qsize() < 1 and time.monotonic() < limit:
                time.sleep(0.005)
            self.assertEqual(self.service._queue.qsize(), 1)
            overloaded = self.call(self.make_request(RpcOperation.READ_LOCAL, "overload"),
                                    timeout=2)
            self.assertEqual(overloaded.kind, RpcOutcomeKind.OVERLOADED)
            self.assertEqual(overloaded.response.error, "OVERLOADED")
            slow.close()
            queued_outcome = queued.result(timeout=3)
        self.assertEqual(queued_outcome.kind, RpcOutcomeKind.RESPONSE)

    def test_service_receive_deadline_bounds_slow_fragmented_header(self):
        self.service.stop(timeout=2)
        self.service = ReplicaService(self.store, self.manifest, self.replica_table,
                                      workers=1, max_pending=1, request_timeout=0.15)
        self.service.start()
        self.addCleanup(self.service.stop)
        started = time.monotonic()
        with socket.create_connection((self.target.kv.host, self.target.kv.port), timeout=1) as sock:
            for byte in HEADER.pack(MAX_RPC_PAYLOAD):
                try:
                    sock.sendall(bytes((byte,)))
                except OSError:
                    break
                time.sleep(0.08)
            sock.settimeout(0.4)
            try:
                self.assertEqual(sock.recv(1), b"")
            except (ConnectionResetError, socket.timeout):
                pass
        self.assertLess(time.monotonic() - started, 0.6)

    def test_storage_lock_wait_uses_service_deadline_and_cannot_commit_late(self):
        self.service.stop(timeout=2)
        self.service = ReplicaService(self.store, self.manifest, self.replica_table,
                                      workers=1, max_pending=1, request_timeout=0.2)
        self.service.start()
        self.addCleanup(self.service.stop)
        self.store.timeout = 3.0
        blocker = sqlite3.connect(self.store.path, isolation_level=None)
        blocker.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        request = self.make_request(
            RpcOperation.INSTALL_IF_NEWER, "deadline-lock",
            version=Version("eventual", 1, "vm01/1"), value=b"must not install late")
        outcome = self.call(request, timeout=1.0)
        elapsed = time.monotonic() - started
        blocker.execute("ROLLBACK")
        blocker.close()
        self.assertLess(elapsed, 0.8)
        self.assertNotEqual(outcome.kind, RpcOutcomeKind.RESPONSE)
        self.assertEqual(self.store.read_local("deadline-lock", context=CTX).status,
                         LocalStatus.NOT_FOUND)


class RpcFailureTests(unittest.TestCase):
    def setUp(self):
        self.manifest = make_manifest()
        self.client_table = membership_table(self.manifest, "vm01", 1, {"vm04": 1})
        self.client = ReplicaClient(self.manifest, self.client_table)
        self.target = self.manifest.node("vm04")

    def request(self):
        return new_request(manifest=self.manifest, sender_id="vm01", sender_generation=1,
                           target_id="vm04", epoch=1, mode=CTX.mode,
                           op=RpcOperation.READ_LOCAL, key="key")

    def test_connection_refused_is_unreachable_not_not_found(self):
        result = self.client.call_replica(self.target, self.request(),
                                          deadline=time.monotonic() + 0.5)
        self.assertEqual(result.kind, RpcOutcomeKind.UNREACHABLE)
        self.assertIsNone(result.response)

    def test_nonfinite_and_unrepresentable_deadlines_return_outcomes(self):
        request = self.request()
        for deadline in (float("inf"), float("nan"), 10**10000, 1e300):
            with self.subTest(deadline_type=type(deadline).__name__):
                result = self.client.call_replica(self.target, request, deadline=deadline)
                self.assertEqual(result.kind, RpcOutcomeKind.ERROR)
                self.assertIsNone(result.response)

    def _serve_once(self, responder):
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.target.kv.host, self.target.kv.port))
        listener.listen(1)
        errors = []

        def serve():
            try:
                connection, _ = listener.accept()
                with connection:
                    request = receive_request(connection, self.manifest,
                                              deadline=time.monotonic() + 2)
                    responder(connection, request)
            except Exception as error:
                errors.append(error)
            finally:
                listener.close()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        return thread, errors

    def test_lost_response_after_send_is_unknown_error(self):
        thread, errors = self._serve_once(lambda connection, request: None)
        result = self.client.call_replica(self.target, self.request(),
                                          deadline=time.monotonic() + 1)
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertFalse(errors)
        self.assertEqual(result.kind, RpcOutcomeKind.ERROR)
        self.assertIsNone(result.response)

    def test_wrong_request_id_and_wrong_generation_in_responses_are_invalid(self):
        def responder(request_id, generation):
            def respond(connection, request):
                response = ReplicaResponse(
                    request_id=request_id or request.request_id,
                    manifest_id=self.manifest.fingerprint, sender_id="vm04",
                    sender_generation=generation, target_id="vm01", epoch=1, mode=CTX.mode,
                    op=RpcOperation.READ_LOCAL, phase="local", status=RpcStatus.NOT_FOUND,
                    present=False)
                from mp3.rpc import send_response
                send_response(connection, response, self.manifest, deadline=time.monotonic() + 1)
            return respond

        cases = (("00000000-0000-4000-8000-000000000001", 1), (None, 2))
        for request_id, generation in cases:
            thread, errors = self._serve_once(responder(request_id, generation))
            result = self.client.call_replica(self.target, self.request(),
                                              deadline=time.monotonic() + 1)
            thread.join(timeout=2)
            self.assertFalse(errors)
            self.assertEqual(result.kind, RpcOutcomeKind.INVALID_RESPONSE)
            if generation != 1:
                self.assertTrue(result.completion_unknown)

    def test_slow_fragmented_response_cannot_reset_total_deadline(self):
        def trickle(connection, request):
            response = ReplicaResponse(
                request.request_id, self.manifest.fingerprint, "vm04", 1, "vm01", 1,
                CTX.mode, RpcOperation.READ_LOCAL, "local", RpcStatus.NOT_FOUND, False)
            payload = encode_response(response, self.manifest)
            frame = struct.pack("!I", len(payload)) + payload
            for byte in frame:
                try:
                    connection.sendall(bytes((byte,)))
                    time.sleep(0.025)
                except OSError:
                    return

        thread, _errors = self._serve_once(trickle)
        deadline = time.monotonic() + 0.18
        start = time.monotonic()
        result = self.client.call_replica(self.target, self.request(), deadline=deadline)
        elapsed = time.monotonic() - start
        thread.join(timeout=1)
        self.assertEqual(result.kind, RpcOutcomeKind.TIMEOUT)
        self.assertLess(elapsed, 0.45)


class ReplicaProcessRecoveryTests(unittest.TestCase):
    def test_tcp_install_survives_kill9_and_process_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = make_manifest()
            path = Path(directory) / "vm04.sqlite"
            script = r'''
import sys, time
from mp3.membership import GossipEnvelope, Member, MemberState, MembershipTable
from mp3.models import ConsistencyMode, Context
from mp3.replica import ReplicaService
from mp3.ring import Manifest
from mp3.storage import open_store
manifest = Manifest.from_json(sys.argv[2])
path = sys.argv[1]
boot = sys.argv[3] == "bootstrap"
store = open_store(path, bootstrap=Context(1, ConsistencyMode.EVENTUAL_NO_RR) if boot else None,
                   vm_id="vm04")
generation = store.start_incarnation()
table = MembershipTable(manifest, "vm04")
table.set_local_generation(generation)
rows = {row.vm_id: row for row in table.snapshot()}
rows["vm01"] = Member("vm01", 1, 0, 1, MemberState.ALIVE)
rows["vm02"] = Member("vm02", 1, 0, 1, MemberState.ALIVE)
table.merge_gossip(GossipEnvelope(manifest.fingerprint, "vm02", 1, tuple(rows.values())))
service = ReplicaService(store, manifest, table, workers=2, max_pending=4)
service.start()
print("READY " + str(generation), flush=True)
while True:
    time.sleep(1)
'''
            env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")

            def start(mode):
                proc = subprocess.Popen(
                    [sys.executable, "-B", "-c", script, str(path), manifest.to_json(), mode],
                    cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True)
                ready = proc.stdout.readline().strip()
                if not ready.startswith("READY "):
                    stderr = proc.stderr.read()
                    proc.kill()
                    raise AssertionError(f"replica process failed to start: {ready} {stderr}")
                return proc, int(ready.split()[1])

            client_table = membership_table(manifest, "vm01", 1, {"vm04": 1})
            client = ReplicaClient(manifest, client_table)
            target = manifest.node("vm04")

            first, gen1 = start("bootstrap")
            try:
                version = Version("eventual", 99, "vm01/1")
                request = new_request(
                    manifest=manifest, sender_id="vm01", sender_generation=1,
                    target_id="vm04", epoch=1, mode=CTX.mode,
                    op=RpcOperation.INSTALL_IF_NEWER, key="crash-proof", version=version,
                    value=b"durable over tcp")
                result = client.call_replica(target, request, deadline=time.monotonic() + 2)
                self.assertEqual(result.kind, RpcOutcomeKind.RESPONSE)
                self.assertEqual(result.response.status, RpcStatus.INSTALLED)
                self.assertTrue(result.response.durable)
                self.assertEqual(gen1, 1)
            finally:
                os.kill(first.pid, signal.SIGKILL)
                first.wait(timeout=3)
                first.stdout.close()
                first.stderr.close()

            second, gen2 = start("recover")
            try:
                # Teach the caller the new process generation before dispatch.
                rows = {row.vm_id: row for row in client_table.snapshot()}
                rows["vm04"] = Member("vm04", gen2, 0, 1, MemberState.ALIVE)
                rows["vm02"] = Member("vm02", 1, 0, 1, MemberState.ALIVE)
                client_table.merge_gossip(GossipEnvelope(
                    manifest.fingerprint, "vm02", 1, tuple(rows.values())))
                read = new_request(
                    manifest=manifest, sender_id="vm01", sender_generation=1,
                    target_id="vm04", epoch=1, mode=CTX.mode,
                    op=RpcOperation.READ_LOCAL, key="crash-proof")
                result = client.call_replica(target, read, deadline=time.monotonic() + 2)
                self.assertEqual(gen2, 2)
                self.assertEqual(result.kind, RpcOutcomeKind.RESPONSE)
                self.assertEqual((result.response.status, result.response.value),
                                 (RpcStatus.FOUND, b"durable over tcp"))
            finally:
                os.kill(second.pid, signal.SIGKILL)
                second.wait(timeout=3)
                second.stdout.close()
                second.stderr.close()


if __name__ == "__main__":
    unittest.main()
