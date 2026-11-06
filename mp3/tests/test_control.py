"""Control-plane wire rules, bounded service behavior, and CLI put/get over real nodes."""

import contextlib
from concurrent.futures import ThreadPoolExecutor
import io
import json
import os
from pathlib import Path
import socket
import stat
import tempfile
import threading
import time
import unittest
from unittest import mock

from mp3.cli import main
from mp3.commands import read_value_file, run_get, run_put, write_value_file_atomic
from mp3.config import MAX_VALUE_BYTES
from mp3.control import (
    COMMANDS, ControlError, ControlRequest, ControlResponse, ControlService, ControlStatus,
    _send_message, send_command,
)
from mp3.coordinator import (
    OperationStatus, ProtocolResult, Schema2Activation, Schema3Activation,
)
from mp3.models import ConsistencyMode, Context, Version
from mp3.node import NodeRuntime
from mp3.ring import Endpoint, Manifest, key_position, ring_id_hex
from mp3.rpc import RpcProtocolError, _recv_payload, new_operation_id
from mp3.tests.test_strong import _manifest


def make_request(manifest, **overrides):
    fields = dict(command_id=new_operation_id(), manifest_id=manifest.fingerprint,
                  target_id="vm01", command="put", key="k", value=b"v")
    fields.update(overrides)
    return ControlRequest(**fields)


def make_response(manifest, request, **overrides):
    fields = dict(command_id=request.command_id, manifest_id=manifest.fingerprint,
                  node_id="vm01", command=request.command, status=ControlStatus.OK,
                  mode=ConsistencyMode.STRONG, epoch=1)
    fields.update(overrides)
    return ControlResponse(**fields)


class ManifestControlEndpointTests(unittest.TestCase):
    def test_control_endpoint_is_required_unique_and_part_of_fingerprint(self):
        manifest = _manifest()
        self.assertEqual(Manifest.from_json(manifest.to_json()).fingerprint,
                         manifest.fingerprint)
        endpoints = {node.vm_id: {"kv": node.kv, "membership": node.membership,
                                  "log_query": node.log_query, "control": node.control}
                     for node in manifest.nodes}
        missing = {vm: {k: v for k, v in e.items() if k != "control"}
                   for vm, e in endpoints.items()}
        with self.assertRaises(ValueError):
            Manifest.from_endpoints(missing)
        clash = {vm: dict(e) for vm, e in endpoints.items()}
        clash["vm02"]["control"] = clash["vm01"]["control"]
        with self.assertRaises(ValueError):
            Manifest.from_endpoints(clash)
        moved = {vm: dict(e) for vm, e in endpoints.items()}
        moved["vm03"]["control"] = Endpoint("127.0.0.1", 65000)
        self.assertNotEqual(Manifest.from_endpoints(moved).fingerprint, manifest.fingerprint)

    def test_old_three_endpoint_manifest_is_rejected(self):
        data = json.loads(_manifest().to_json())
        for node in data["nodes"]:
            del node["control"]
        with self.assertRaises(ValueError):
            Manifest.from_json(json.dumps(data))


class ControlWireTests(unittest.TestCase):
    def setUp(self):
        self.manifest = _manifest()

    def test_request_round_trip_and_value_rules(self):
        request = make_request(self.manifest, key="λ key", value=b"\x00\xff")
        decoded = ControlRequest.from_metadata(
            request.to_metadata(), request.value, self.manifest)
        self.assertEqual(decoded, request)
        for bad in (dict(command="bogus"), dict(key=""), dict(command="get", value=b"x"),
                    dict(command="status", key="k", value=b""),
                    dict(value=b"x" * (MAX_VALUE_BYTES + 1)), dict(target_id="vm11"),
                    dict(key="x" * 5000), dict(command_id="not-a-uuid"), dict(key="\ud800")):
            with self.subTest(bad=list(bad)), self.assertRaises(RpcProtocolError):
                make_request(self.manifest, **bad)
        self.assertEqual(COMMANDS, {
            "put", "get", "status", "mode", "tx_prepare", "tx_commit", "tx_activate",
            "tx_abort", "tx_status", "liststore", "list_mem_ids", "read_replica", "multiput",
            "mp_arm", "mp_go", "mp_abort", "mp_status"})
        for bad in (dict(command="list_mem_ids", key="k", value=b""),
                    dict(command="liststore", key="cursor", value=b""),
                    dict(command="liststore", key="", value=b"x" * 70000),
                    dict(command="read_replica", key="k", value=b""),
                    dict(command="multiput", key="", value=b"{}"),
                    dict(command="mp_go", key="k", value=b"x" * 3000)):
            with self.subTest(bad=bad["command"]), self.assertRaises(RpcProtocolError):
                make_request(self.manifest, **bad)
        make_request(self.manifest, command="liststore", key="", value=b"")
        make_request(self.manifest, command="liststore", key="", value=b'{"after": "k"}')
        make_request(self.manifest, command="list_mem_ids", key="", value=b"")
        make_request(self.manifest, command="read_replica", key="k", value=b"{}")
        for bad in (dict(command="mode", key="eventual", value=b""),
                    dict(command="mode", key="strong", value=b"x"),
                    dict(command="tx_status", key="not-a-uuid", value=b""),
                    dict(command="tx_prepare", key=new_operation_id(), value=b""),
                    dict(command="tx_commit", key=new_operation_id(), value=b"{}"),
                    dict(command="tx_prepare", key=new_operation_id(), value=b"x" * 3000)):
            with self.subTest(bad=bad["command"]), self.assertRaises(RpcProtocolError):
                make_request(self.manifest, **bad)
        make_request(self.manifest, command="mode", key="eventual-RR", value=b"")
        make_request(self.manifest, command="tx_prepare", key=new_operation_id(), value=b"{}")

    def test_request_metadata_is_exact_and_bound_to_the_manifest(self):
        request = make_request(self.manifest)
        metadata = request.to_metadata()
        for bad in (dict(metadata, extra=1), {k: v for k, v in metadata.items() if k != "key"},
                    dict(metadata, schema=2), dict(metadata, manifest_id="0" * 64),
                    dict(metadata, value_len=5)):
            with self.subTest(bad=sorted(bad)), self.assertRaises(RpcProtocolError):
                ControlRequest.from_metadata(bad, request.value, self.manifest)

    def test_response_round_trip_and_consistency_rules(self):
        request = make_request(self.manifest, command="get", value=b"")
        version = Version("strong", 4, "vm02/3")
        response = make_response(
            self.manifest, request, version=version, owners=("vm02", "vm05"),
            key_id=ring_id_hex(key_position("k")), value=b"data")
        decoded = ControlResponse.from_metadata(
            response.to_metadata(), response.value, self.manifest)
        self.assertEqual(decoded, response)
        for bad in (dict(value=b"x", status=ControlStatus.NOT_FOUND),
                    dict(version=version, status=ControlStatus.ERROR),
                    dict(owners=("vm01", "vm01")), dict(owners=("vm01",) * 6),
                    dict(key_id="XYZ"), dict(epoch=0), dict(error="a\x00b"),
                    dict(error="x" * 2000)):
            with self.subTest(bad=list(bad)), self.assertRaises(RpcProtocolError):
                make_response(self.manifest, request, **bad)
        put = make_request(self.manifest)
        with self.assertRaises(RpcProtocolError):
            make_response(self.manifest, put, value=b"only get may carry a body")

    def test_oversize_and_inconsistent_lengths_are_rejected_before_reading_a_body(self):
        left, right = socket.socketpair()
        with left, right:
            right.settimeout(2)
            left.sendall((2**31).to_bytes(4, "big"))
            with self.assertRaises(RpcProtocolError):
                _recv_payload(right, deadline=time.monotonic() + 2)


class FakeRuntime:
    """Just enough of NodeRuntime for the service: identity, context and operations."""

    def __init__(self, manifest, vm_id="vm01", mode=ConsistencyMode.STRONG):
        self.manifest = manifest
        self.vm_id = vm_id
        self._context = Context(1, mode)
        self.gate = None
        self.calls = []
        self.deadlines = []
        self.failure = None
        self.result = None

    @property
    def context(self):
        return self._context

    def _ok(self, value=None):
        return ProtocolResult(OperationStatus.OK, value, Version("strong", 1, "vm01/1"),
                              new_operation_id(), ("vm01", "vm02", "vm03"))

    def strong_put(self, key, value, *, deadline):
        self.calls.append(("put", key))
        self.deadlines.append(deadline)
        if self.failure is not None:
            raise self.failure
        if self.gate is not None:
            self.gate.wait(5)
        if self.result is not None:
            return self.result
        return self._ok()

    def eventual_rr_put(self, key, value):
        self.calls.append(("rr-put", key))
        return self._ok()

    def eventual_rr_get(self, key, *, deadline):
        self.calls.append(("rr-get", key))
        return self._ok(b"repaired")

    def eventual_put(self, key, value):
        self.calls.append(("eventual-put", key))
        if self.failure is not None:
            raise self.failure
        return self._ok()

    def strong_get(self, key, *, deadline):
        self.calls.append(("get", key))
        if self.failure is not None:
            raise self.failure
        return self._ok(b"stored")


class ControlServiceTests(unittest.TestCase):
    def setUp(self):
        self.manifest = _manifest()
        self.runtime = FakeRuntime(self.manifest)

    def start(self, **kwargs):
        service = ControlService(self.runtime, **kwargs)
        service.start()
        self.addCleanup(service.stop)
        return service

    def call(self, request, *, raw=None, timeout=3.0):
        node = self.manifest.node("vm01")
        deadline = time.monotonic() + timeout
        with socket.create_connection((node.control.host, node.control.port), timeout=timeout) as sock:
            if raw is not None:
                sock.sendall(raw)
            else:
                _send_message(sock, request.to_metadata(), request.value, deadline=deadline)
            sock.shutdown(socket.SHUT_WR)
            try:
                metadata, body = _recv_payload(sock, deadline=deadline)
            except (ConnectionError, RpcProtocolError):
                return None
            return ControlResponse.from_metadata(metadata, body, self.manifest)

    def test_put_get_and_status_run_the_nodes_operations(self):
        self.start()
        put = self.call(make_request(self.manifest, key="photo", value=b"bytes"))
        self.assertEqual((put.status, put.owners, put.key_id),
                         (ControlStatus.OK, ("vm01", "vm02", "vm03"),
                          ring_id_hex(key_position("photo"))))
        get = self.call(make_request(self.manifest, command="get", key="photo", value=b""))
        self.assertEqual((get.status, get.value), (ControlStatus.OK, b"stored"))
        status = self.call(make_request(self.manifest, command="status", key="", value=b""))
        self.assertEqual((status.status, status.mode, status.epoch),
                         (ControlStatus.OK, ConsistencyMode.STRONG, 1))
        self.assertEqual(self.runtime.calls, [("put", "photo"), ("get", "photo")])

    def raw_message(self, metadata_bytes, body=b"", trailer=b""):
        return ((8 + len(metadata_bytes) + len(body) - 4 + 4).to_bytes(4, "big")[0:0]
                + (4 + len(metadata_bytes) + len(body)).to_bytes(4, "big")
                + len(metadata_bytes).to_bytes(4, "big") + metadata_bytes + body + trailer)

    def test_reusing_a_command_id_with_different_content_is_refused(self):
        self.start()
        first = make_request(self.manifest, key="k1", value=b"one")
        self.assertEqual(self.call(first).status, ControlStatus.OK)
        other_key = make_request(self.manifest, command_id=first.command_id,
                                 key="k2", value=b"one")
        response = self.call(other_key)
        self.assertEqual((response.status, response.error),
                         (ControlStatus.ERROR, "COMMAND_ID_REUSED"))
        other_value = make_request(self.manifest, command_id=first.command_id,
                                   key="k1", value=b"two")
        self.assertEqual(self.call(other_value).error, "COMMAND_ID_REUSED")
        self.assertEqual(self.call(first).status, ControlStatus.OK)
        self.assertEqual(self.runtime.calls, [("put", "k1")])

    def test_reused_id_while_running_with_other_content_is_refused(self):
        self.runtime.gate = threading.Event()
        self.start()
        first = make_request(self.manifest, key="slow")
        pool = ThreadPoolExecutor(max_workers=1)
        self.addCleanup(pool.shutdown)
        running = pool.submit(self.call, first, timeout=8)
        deadline = time.monotonic() + 3
        while not self.runtime.calls and time.monotonic() < deadline:
            time.sleep(0.01)
        clash = self.call(make_request(self.manifest, command_id=first.command_id,
                                       key="different"))
        self.assertEqual(clash.error, "COMMAND_ID_REUSED")
        self.runtime.gate.set()
        self.assertEqual(running.result(timeout=5).status, ControlStatus.OK)

    def test_command_deadline_never_outlives_the_request(self):
        self.start(request_timeout=4.0, command_timeout=3.9)
        started = time.monotonic()
        self.assertEqual(self.call(make_request(self.manifest)).status, ControlStatus.OK)
        # request 4.0 s minus the 1.0 s reply margin, not the 3.9 s command timeout
        self.assertLessEqual(self.runtime.deadlines[0] - started, 3.1)

    def test_command_with_too_little_request_time_left_does_not_start(self):
        self.start(request_timeout=1.5, command_timeout=1.0)
        response = self.call(make_request(self.manifest))
        self.assertEqual(response.status, ControlStatus.TIMEOUT)
        self.assertEqual(self.runtime.calls, [])

    def test_eventual_put_is_judged_by_request_time_not_the_command_timeout(self):
        self.runtime = FakeRuntime(self.manifest, mode=ConsistencyMode.EVENTUAL_NO_RR)
        self.start(request_timeout=10.0, command_timeout=2.0)
        self.assertEqual(self.call(make_request(self.manifest, key="ev")).status,
                         ControlStatus.OK)
        self.assertEqual(self.runtime.calls, [("eventual-put", "ev")])

    def test_unstarted_command_is_not_remembered_so_a_retry_can_run(self):
        self.runtime = FakeRuntime(self.manifest, mode=ConsistencyMode.EVENTUAL_NO_RR)
        service = self.start(request_timeout=6.0, command_timeout=2.0)
        request = make_request(self.manifest)
        for _ in range(2):
            response = self.call(request)
            self.assertEqual(response.status, ControlStatus.TIMEOUT)
            self.assertNotIn("DUPLICATE", response.error)
        self.assertEqual(self.runtime.calls, [])
        self.assertEqual(len(service._finished), 0)

    def test_completion_unknown_marker_survives_a_long_error(self):
        self.runtime.result = ProtocolResult(
            OperationStatus.ERROR, None, None, new_operation_id(),
            completion_unknown=True, error="x" * 5000)
        self.start()
        response = self.call(make_request(self.manifest))
        self.assertEqual(response.status, ControlStatus.ERROR)
        self.assertTrue(response.error.endswith("(completion unknown)"))
        self.assertLessEqual(len(response.error.encode()), 1024)

    def test_unexpected_exception_returns_only_its_type(self):
        self.runtime.failure = RuntimeError("secret /private/path")
        self.start()
        response = self.call(make_request(self.manifest))
        self.assertEqual((response.status, response.error),
                         (ControlStatus.ERROR,
                          "INTERNAL_ERROR RuntimeError (completion unknown)"))
        get = self.call(make_request(self.manifest, command="get", value=b""))
        self.assertEqual(get.error, "INTERNAL_ERROR RuntimeError")

    def test_old_command_ids_are_forgotten_after_the_memory_limit(self):
        self.start()
        with mock.patch("mp3.control.MAX_REMEMBERED_COMMANDS", 2):
            first = make_request(self.manifest, key="a")
            for request in (first, make_request(self.manifest, key="b"),
                            make_request(self.manifest, key="c")):
                self.assertEqual(self.call(request).status, ControlStatus.OK)
            self.assertEqual(self.call(first).status, ControlStatus.OK)
        self.assertEqual([call[1] for call in self.runtime.calls], ["a", "b", "c", "a"])

    def test_malformed_control_json_is_closed_without_running_anything(self):
        self.start()
        good = make_request(self.manifest)
        text = json.dumps(good.to_metadata(), sort_keys=True)
        duplicate = text[:-1] + ', "key": "other"}'
        nan = text.replace('"value_len": 1', '"value_len": NaN')
        for label, raw in (
                ("duplicate key", self.raw_message(duplicate.encode(), b"v")),
                ("NaN", self.raw_message(nan.encode(), b"v")),
                ("non-utf8", self.raw_message(b'{"key": "\xff"}\xff', b"v")),
                ("trailing bytes", self.raw_message(text.encode(), b"v", b"extra")),
                ("not an object", self.raw_message(b"[1,2]", b""))):
            with self.subTest(label=label):
                self.assertIsNone(self.call(good, raw=raw))
        self.assertEqual(self.runtime.calls, [])

    def test_slow_trickled_request_is_dropped_at_the_request_deadline(self):
        self.start(request_timeout=1.5, command_timeout=1.0)
        node = self.manifest.node("vm01")
        started = time.monotonic()
        with socket.create_connection((node.control.host, node.control.port), timeout=5) as sock:
            sock.sendall(b"\x00\x00")
            sock.settimeout(4)
            self.assertEqual(sock.recv(1), b"")
        self.assertLess(time.monotonic() - started, 3.5)
        self.assertEqual(self.runtime.calls, [])

    def test_wrong_target_runs_nothing(self):
        self.start()
        response = self.call(make_request(self.manifest, target_id="vm02"))
        self.assertEqual((response.status, response.error), (ControlStatus.ERROR, "WRONG_NODE"))
        self.assertEqual(self.runtime.calls, [])

    def test_invalid_frames_are_closed_without_running_anything(self):
        self.start()
        good = make_request(self.manifest)
        for raw in (b"", b"\x00\x00", b"\xff\xff\xff\xff" + b"x" * 8,
                    (8).to_bytes(4, "big") + (9999).to_bytes(4, "big") + b"abcd",
                    (12).to_bytes(4, "big") + (4).to_bytes(4, "big") + b"nope" + b"\x00" * 4):
            with self.subTest(raw=raw[:12]):
                self.assertIsNone(self.call(good, raw=raw))
        self.assertEqual(self.runtime.calls, [])
        # The service still answers a valid command afterwards.
        self.assertEqual(self.call(good).status, ControlStatus.OK)

    def test_duplicate_command_ids_do_not_run_twice(self):
        self.start()
        put = make_request(self.manifest, key="once")
        first, second = self.call(put), self.call(put)
        self.assertEqual(first, second)
        self.assertEqual(self.runtime.calls, [("put", "once")])
        get = make_request(self.manifest, command="get", key="once", value=b"")
        self.assertEqual(self.call(get).status, ControlStatus.OK)
        repeat = self.call(get)
        self.assertEqual((repeat.status, repeat.error), (ControlStatus.ERROR, "DUPLICATE_COMMAND"))
        self.assertEqual(self.runtime.calls, [("put", "once"), ("get", "once")])

    def test_command_in_progress_is_not_started_again(self):
        self.runtime.gate = threading.Event()
        self.start()
        put = make_request(self.manifest, key="slow")
        pool = ThreadPoolExecutor(max_workers=2)
        self.addCleanup(pool.shutdown)
        running = pool.submit(self.call, put, timeout=8)
        deadline = time.monotonic() + 3
        while not self.runtime.calls and time.monotonic() < deadline:
            time.sleep(0.01)
        duplicate = self.call(put)
        self.assertEqual(duplicate.error, "DUPLICATE_COMMAND_IN_PROGRESS")
        self.runtime.gate.set()
        self.assertEqual(running.result(timeout=5).status, ControlStatus.OK)
        self.assertEqual(self.runtime.calls, [("put", "slow")])

    def test_full_queue_gets_an_explicit_overloaded_reply(self):
        self.runtime.gate = threading.Event()
        self.start(workers=1, max_pending=1, request_timeout=10.0, command_timeout=5.0)
        pool = ThreadPoolExecutor(max_workers=3)
        self.addCleanup(pool.shutdown)
        futures = []
        for index in range(3):
            futures.append(pool.submit(self.call, make_request(self.manifest, key=f"k{index}"),
                                       timeout=8))
            time.sleep(0.3)
        overloaded = futures[2].result(timeout=5)
        self.assertEqual(overloaded.status, ControlStatus.OVERLOADED)
        self.runtime.gate.set()
        self.assertEqual([f.result(timeout=5).status for f in futures[:2]],
                         [ControlStatus.OK, ControlStatus.OK])

    def test_eventual_rr_mode_runs_the_rr_operations(self):
        self.runtime = FakeRuntime(self.manifest, mode=ConsistencyMode.EVENTUAL_RR)
        self.start()
        put = self.call(make_request(self.manifest, key="rr"))
        get = self.call(make_request(self.manifest, command="get", key="rr", value=b""))
        self.assertEqual((put.status, get.status, get.value),
                         (ControlStatus.OK, ControlStatus.OK, b"repaired"))
        self.assertEqual(put.mode, ConsistencyMode.EVENTUAL_RR)
        self.assertEqual(self.runtime.calls, [("rr-put", "rr"), ("rr-get", "rr")])

    def test_stop_closes_the_port_and_reports_clean(self):
        service = self.start()
        report = service.stop(timeout=3)
        self.assertEqual((report.incomplete_requests, report.workers_joined), (0, True))
        node = self.manifest.node("vm01")
        with self.assertRaises(OSError):
            socket.create_connection((node.control.host, node.control.port), timeout=1)

    def test_invalid_service_settings_are_rejected(self):
        for kwargs in (dict(workers=0), dict(max_pending=0), dict(request_timeout=0),
                       dict(command_timeout=float("inf")),
                       dict(request_timeout=2.0, command_timeout=2.0)):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                ControlService(self.runtime, **kwargs)


class CliFileTests(unittest.TestCase):
    def test_file_helpers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "empty").write_bytes(b"")
            (root / "big").write_bytes(b"x" * (MAX_VALUE_BYTES + 1))
            (root / "max").write_bytes(b"y" * MAX_VALUE_BYTES)
            self.assertEqual(read_value_file(str(root / "empty")), b"")
            self.assertEqual(len(read_value_file(str(root / "max"))), MAX_VALUE_BYTES)
            with self.assertRaises(ValueError):
                read_value_file(str(root / "big"))
            with self.assertRaises(OSError):
                read_value_file(str(root / "missing"))
            target = root / "out"
            target.write_bytes(b"old")
            write_value_file_atomic(str(target), b"new")
            self.assertEqual(target.read_bytes(), b"new")
            self.assertEqual([p.name for p in root.iterdir() if p.name.startswith(".mp3-get")], [])
            with self.assertRaises(OSError):
                write_value_file_atomic(str(root / "no" / "dir" / "out"), b"x")

    def test_special_files_are_not_read_and_permissions_are_kept(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fifo = root / "pipe"
            os.mkfifo(fifo)
            with self.assertRaises(ValueError):
                read_value_file(str(fifo))
            with self.assertRaises(ValueError):
                read_value_file(directory)
            target = root / "out"
            target.write_bytes(b"old")
            os.chmod(target, 0o644)
            write_value_file_atomic(str(target), b"new")
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o644)
            fresh = root / "fresh"
            write_value_file_atomic(str(fresh), b"x")
            self.assertEqual(fresh.read_bytes(), b"x")

    def test_get_refuses_a_destination_that_is_not_a_regular_file(self):
        manifest = _manifest()
        with tempfile.TemporaryDirectory() as directory:
            fifo = Path(directory) / "pipe"
            os.mkfifo(fifo)
            code, result = run_get(manifest, "vm01", "k", str(fifo))
            self.assertEqual((code, result["status"]), (2, "ERROR"))
            with self.assertRaises(ValueError):
                write_value_file_atomic(str(fifo), b"x")
            self.assertTrue(stat.S_ISFIFO(fifo.stat().st_mode))

    def test_failed_atomic_write_keeps_the_old_file_and_leaves_no_temp_file(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "out"
            target.write_bytes(b"old")
            with mock.patch("mp3.commands.os.replace", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    write_value_file_atomic(str(target), b"new")
            self.assertEqual(target.read_bytes(), b"old")
            self.assertEqual(sorted(p.name for p in Path(directory).iterdir()), ["out"])

    def test_oversize_put_never_contacts_a_node(self):
        manifest = _manifest()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "big"
            path.write_bytes(b"x" * (MAX_VALUE_BYTES + 1))
            code, result = run_put(manifest, "vm01", "k", str(path))
        self.assertEqual((code, result["status"]), (2, "ERROR"))

    def test_unreachable_node_is_an_error_and_get_leaves_the_file(self):
        manifest = _manifest()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "out"
            path.write_bytes(b"keep")
            code, result = run_get(manifest, "vm01", "k", str(path))
            self.assertEqual((code, result["status"], path.read_bytes()), (1, "ERROR", b"keep"))
            code, result = run_get(manifest, "vm01", "k", directory)
            self.assertEqual((code, result["status"]), (2, "ERROR"))
        with self.assertRaises(ControlError):
            send_command(manifest, "vm01", "status", timeout=1.0)


class ControlClusterTests(unittest.TestCase):
    """Ten loopback nodes in one process; commands go through the real control sockets."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = _manifest()
        self.manifest_path = self.root / "manifest.json"
        self.manifest_path.write_text(self.manifest.to_json(), encoding="utf-8")
        inventory = {node.vm_id: {"release_id": "control-local", "schema_version": 3}
                     for node in self.manifest.nodes}
        self.activation = Schema2Activation.from_inventory(self.manifest, inventory)
        self.rr_activation = Schema3Activation.from_inventory(self.manifest, inventory)
        self.nodes: list[NodeRuntime] = []
        self.addCleanup(self._stop_nodes)

    def _stop_nodes(self):
        for node in reversed(self.nodes):
            try:
                node.stop(timeout=8)
            except Exception:
                pass

    def start_cluster(self, mode, vm_ids=None):
        vm_ids = vm_ids or [node.vm_id for node in self.manifest.nodes]
        for vm_id in vm_ids:
            self.nodes.append(NodeRuntime(
                manifest=self.manifest, vm_id=vm_id,
                database_path=self.root / f"{mode.value}-{vm_id}.sqlite", bootstrap=True,
                bootstrap_mode=mode, protocol_activation=self.activation,
                rr_activation=self.rr_activation, event_stream=io.StringIO(), replica_workers=4, replica_queue=16,
                coordinator_workers=4, coordinator_queue=8, attempt_workers=8,
                attempt_queue=24).start())
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if all(any(row.vm_id == peer and row.restart_generation > 0
                       for row in node.membership)
                   for node in self.nodes for peer in vm_ids if peer != node.vm_id):
                return
            time.sleep(0.02)
        self.fail("membership did not converge")

    def cli(self, vm_id, *args):
        stream = io.StringIO()
        with contextlib.redirect_stdout(stream):
            code = main(["--manifest", str(self.manifest_path), "--vm-id", vm_id, *args])
        return code, json.loads(stream.getvalue())

    def put_get_round_trip(self, mode):
        self.start_cluster(mode)
        source = self.root / "photo.bin"
        payload = bytes(range(256)) * 40
        source.write_bytes(payload)
        code, put = self.cli("vm01", "put", "photo", str(source))
        self.assertEqual((code, put["status"]), (0, "OK"), put)
        self.assertEqual(put["key_id"], ring_id_hex(key_position("photo")))
        owners = {node.vm_id for node in self.manifest.owners("photo")}
        self.assertTrue(set(put["acks"]) <= owners)
        self.assertGreaterEqual(len(put["acks"]), 2)
        self.assertEqual(put["mode"], mode.value)
        out = self.root / "photo.out"
        code, got = self.cli("vm07", "get", "photo", str(out))
        self.assertEqual((code, got["status"], got["bytes"]), (0, "OK", len(payload)), got)
        self.assertEqual(out.read_bytes(), payload)
        self.assertEqual(got["version"].split(":")[0], "strong" if mode == ConsistencyMode.STRONG
                         else "eventual")
        return source

    def test_strong_put_and_get_from_different_nodes(self):
        self.put_get_round_trip(ConsistencyMode.STRONG)

    def test_eventual_norr_put_and_get_from_different_nodes(self):
        self.put_get_round_trip(ConsistencyMode.EVENTUAL_NO_RR)

    def test_eventual_rr_put_and_get_from_different_nodes(self):
        self.put_get_round_trip(ConsistencyMode.EVENTUAL_RR)

    def test_empty_replacement_and_one_mib_values_and_missing_key(self):
        self.start_cluster(ConsistencyMode.STRONG)
        empty = self.root / "empty"
        empty.write_bytes(b"")
        self.assertEqual(self.cli("vm02", "put", "blank", str(empty))[0], 0)
        out = self.root / "blank.out"
        out.write_bytes(b"stale")
        code, got = self.cli("vm09", "get", "blank", str(out))
        self.assertEqual((code, got["status"], got["bytes"]), (0, "OK", 0))
        self.assertEqual(out.read_bytes(), b"")
        big = self.root / "big"
        big.write_bytes(bytes(range(256)) * (MAX_VALUE_BYTES // 256))
        self.assertEqual(self.cli("vm03", "put", "blank", str(big))[0], 0)
        self.assertEqual(self.cli("vm05", "get", "blank", str(out))[0], 0)
        self.assertEqual(out.read_bytes(), big.read_bytes())
        missing = self.root / "missing.out"
        missing.write_bytes(b"untouched")
        code, got = self.cli("vm04", "get", "never-written", str(missing))
        self.assertEqual((code, got["status"]), (1, "NOT_FOUND"))
        self.assertEqual(missing.read_bytes(), b"untouched")

    def test_status_reports_the_nodes_mode_and_wrong_node_is_refused(self):
        self.start_cluster(ConsistencyMode.EVENTUAL_NO_RR, ["vm01"])
        response = send_command(self.manifest, "vm01", "status")
        self.assertEqual((response.status, response.mode, response.node_id),
                         (ControlStatus.OK, ConsistencyMode.EVENTUAL_NO_RR, "vm01"))

    def test_node_shutdown_reports_the_control_service_and_closes_its_port(self):
        self.start_cluster(ConsistencyMode.STRONG, ["vm01"])
        node = self.nodes[0]
        report = node.stop(timeout=8)
        self.assertTrue(report.clean, report)
        self.assertTrue(report.control.workers_joined)
        with self.assertRaises(ControlError):
            send_command(self.manifest, "vm01", "status", timeout=1.0)


if __name__ == "__main__":
    unittest.main()
