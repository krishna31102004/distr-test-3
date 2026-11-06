"""C5 wire schema, binary framing, strict validation, and deadline primitives."""

from dataclasses import replace
import socket
import struct
import threading
import time
import unittest
import uuid

from mp3.config import MAX_VALUE_BYTES
from mp3.models import ConsistencyMode, Version
from mp3.ring import Endpoint, Manifest
from mp3.coordinator import Schema2Activation
from mp3.rpc import (
    MAX_JSON_BYTES, MAX_RPC_PAYLOAD, PROTOCOL_SCHEMA_VERSION, ReplicaRequest, ReplicaResponse, RpcOperation,
    RpcProtocolError, RpcStatus, _encode_json, encode_request, encode_response,
    new_operation_id, new_request, receive_request, receive_response, send_request, send_response,
)


def make_manifest():
    endpoints = {}
    for number in range(1, 11):
        vm = f"vm{number:02d}"
        base = 43000 + number * 4
        endpoints[vm] = {
            "kv": Endpoint("127.0.0.1", base),
            "membership": Endpoint("127.0.0.1", base + 1),
            "log_query": Endpoint("127.0.0.1", base + 2),
            "control": Endpoint("127.0.0.1", base + 3),
        }
    return Manifest.from_endpoints(endpoints)


def request(manifest, *, op=RpcOperation.INSTALL_IF_NEWER, value=b"\x00\xff",
            version=None, key="λ\x00key"):
    if op == RpcOperation.INSTALL_IF_NEWER and version is None:
        version = Version("eventual", 7, "vm01/1")
    return new_request(manifest=manifest, sender_id="vm01", sender_generation=1,
                       target_id="vm04", epoch=1,
                       mode=ConsistencyMode.EVENTUAL_NO_RR, op=op,
                       key=key, version=version, value=value)


def transfer(send_fn, receive_fn):
    left, right = socket.socketpair()
    errors = []

    def send():
        try:
            send_fn(left)
            left.shutdown(socket.SHUT_WR)
        except Exception as error:  # captured for assertion in test thread
            errors.append(error)
        finally:
            left.close()

    worker = threading.Thread(target=send, daemon=True)
    worker.start()
    try:
        result = receive_fn(right)
    finally:
        right.close()
    worker.join(timeout=2)
    if worker.is_alive():
        raise AssertionError("frame sender did not finish")
    if errors:
        raise errors[0]
    return result


def raw_payload(metadata, body=b""):
    encoded = _encode_json(metadata)
    payload = struct.pack("!I", len(encoded)) + encoded + body
    return struct.pack("!I", len(payload)) + payload


class RpcCodecTests(unittest.TestCase):
    def setUp(self):
        self.manifest = make_manifest()

    def test_request_round_trip_preserves_unicode_empty_nul_and_raw_bytes(self):
        req = request(self.manifest, value=b"\x00\xff\xfe", key="λ\x00name")

        def sender(sock):
            send_request(sock, req, self.manifest, deadline=time.monotonic() + 2)

        got = transfer(sender, lambda sock: receive_request(
            sock, self.manifest, deadline=time.monotonic() + 2))
        self.assertEqual(got, req)
        self.assertEqual(got.value, b"\x00\xff\xfe")

        empty_key = request(self.manifest, value=b"", key="")
        empty_got = transfer(lambda sock: send_request(
            sock, empty_key, self.manifest, deadline=time.monotonic() + 2),
            lambda sock: receive_request(sock, self.manifest, deadline=time.monotonic() + 2))
        self.assertEqual((empty_got.key, empty_got.value), ("", b""))

    def test_full_one_mib_response_round_trip_is_binary_safe(self):
        value = bytes(range(256)) * (MAX_VALUE_BYTES // 256)
        response = ReplicaResponse(
            request_id=str(uuid.uuid4()), manifest_id=self.manifest.fingerprint,
            sender_id="vm04", sender_generation=3, target_id="vm01", epoch=1,
            mode=ConsistencyMode.EVENTUAL_NO_RR, op=RpcOperation.READ_LOCAL,
            phase="local", status=RpcStatus.FOUND, present=True,
            version=Version("eventual", 123, "vm04/3"), value=value,
        )
        encoded = encode_response(response, self.manifest)
        self.assertEqual(len(encoded), 4 + len(_encode_json(response.to_metadata())) + MAX_VALUE_BYTES)
        self.assertLessEqual(len(encoded), MAX_RPC_PAYLOAD)

        got = transfer(lambda sock: send_response(
            sock, response, self.manifest, deadline=time.monotonic() + 3),
            lambda sock: receive_response(sock, self.manifest, deadline=time.monotonic() + 3))
        self.assertEqual(got, response)
        self.assertEqual(got.value, value)

    def test_exact_json_and_outer_payload_limits(self):
        base = request(self.manifest, value=b"x" * MAX_VALUE_BYTES, key="")
        base_len = len(_encode_json(base.to_metadata()))
        full = replace(base, key="k" * (MAX_JSON_BYTES - base_len))
        encoded = encode_request(full, self.manifest)
        self.assertEqual(len(_encode_json(full.to_metadata())), MAX_JSON_BYTES)
        self.assertEqual(len(encoded), MAX_RPC_PAYLOAD)
        with self.assertRaises(RpcProtocolError):
            encode_request(replace(full, key=full.key + "k"), self.manifest)

    def test_very_large_canonical_version_round_trips_within_metadata_cap(self):
        version = Version("eventual", 10 ** 14999, "vm01/1")
        req = request(self.manifest, value=b"", version=version)
        payload = encode_request(req, self.manifest)
        got = transfer(lambda sock: send_request(
            sock, req, self.manifest, deadline=time.monotonic() + 2),
            lambda sock: receive_request(sock, self.manifest, deadline=time.monotonic() + 2))
        self.assertEqual(got.version, version)
        self.assertLessEqual(len(payload), MAX_RPC_PAYLOAD)

    def test_found_empty_value_is_not_not_found(self):
        found = ReplicaResponse(
            str(uuid.uuid4()), self.manifest.fingerprint, "vm04", 1, "vm01", 1,
            ConsistencyMode.EVENTUAL_NO_RR, RpcOperation.READ_LOCAL, "local",
            RpcStatus.FOUND, True, Version("eventual", 1, "vm04/1"), value=b"")
        self.assertEqual(found.status, RpcStatus.FOUND)
        self.assertTrue(found.present)
        self.assertEqual(found.value_len, 0)
        missing = replace(found, status=RpcStatus.NOT_FOUND, present=False, version=None)
        self.assertFalse(missing.present)

    def test_canonical_schema_and_decimal_fields(self):
        req = request(self.manifest)
        metadata = req.to_metadata()
        self.assertEqual(set(metadata), {
            "schema", "request_id", "manifest_id", "sender_id", "sender_generation",
            "target_id", "epoch", "mode", "op", "phase", "key", "version", "value_len"})
        self.assertEqual(metadata["schema"], 1)
        self.assertEqual(metadata["sender_generation"], "1")
        self.assertEqual(metadata["epoch"], "1")
        self.assertEqual(metadata["version"]["number"], "7")

    def test_schema2_phase_round_trip_and_operation_correlation(self):
        operation_id = new_operation_id()
        one = new_request(
            manifest=self.manifest, sender_id="vm01", sender_generation=1,
            target_id="vm04", epoch=1, mode=ConsistencyMode.STRONG,
            op=RpcOperation.READ_LOCAL, key="k", phase="strong_query",
            schema=PROTOCOL_SCHEMA_VERSION, operation_id=operation_id)
        two = new_request(
            manifest=self.manifest, sender_id="vm01", sender_generation=1,
            target_id="vm05", epoch=1, mode=ConsistencyMode.STRONG,
            op=RpcOperation.READ_LOCAL, key="k", phase="strong_query",
            schema=PROTOCOL_SCHEMA_VERSION, operation_id=operation_id)
        self.assertEqual(one.operation_id, two.operation_id)
        self.assertNotEqual(one.request_id, two.request_id)
        metadata = one.to_metadata()
        self.assertIn("operation_id", metadata)
        got = transfer(lambda sock: send_request(
            sock, one, self.manifest, deadline=time.monotonic() + 2),
            lambda sock: receive_request(
                sock, self.manifest, deadline=time.monotonic() + 2))
        self.assertEqual(got, one)

        response = ReplicaResponse(
            request_id=one.request_id, manifest_id=self.manifest.fingerprint,
            sender_id="vm04", sender_generation=4, target_id="vm01", epoch=1,
            mode=ConsistencyMode.STRONG, op=RpcOperation.READ_LOCAL,
            phase="strong_query", status=RpcStatus.NOT_FOUND, present=False,
            schema=PROTOCOL_SCHEMA_VERSION, operation_id=operation_id)
        self.assertEqual(response.operation_id, one.operation_id)
        with self.assertRaises(RpcProtocolError):
            replace(response, operation_id=new_operation_id()).validate_for(
                one, self.manifest.node("vm04"), self.manifest, 4)

    def test_schema2_rejects_bad_phase_mode_ids_and_extra_fields(self):
        operation_id = new_operation_id()
        base = new_request(
            manifest=self.manifest, sender_id="vm01", sender_generation=1,
            target_id="vm04", epoch=1, mode=ConsistencyMode.STRONG,
            op=RpcOperation.READ_LOCAL, key="k", phase="strong_query",
            schema=PROTOCOL_SCHEMA_VERSION, operation_id=operation_id)
        metadata = base.to_metadata()
        for malformed in (
            dict(metadata, surprise=True),
            dict(metadata, operation_id="not-a-uuid"),
            dict(metadata, phase="eventual_read"),
            dict(metadata, sender_generation="0"),
            {key: value for key, value in metadata.items() if key != "operation_id"},
        ):
            with self.subTest(malformed=malformed):
                with self.assertRaises(RpcProtocolError):
                    ReplicaRequest.from_metadata(malformed, b"", self.manifest)
        with self.assertRaises(RpcProtocolError):
            new_request(
                manifest=self.manifest, sender_id="vm01", sender_generation=1,
                target_id="vm04", epoch=1, mode=ConsistencyMode.EVENTUAL_RR,
                op=RpcOperation.READ_LOCAL, key="k", phase="eventual_read",
                schema=PROTOCOL_SCHEMA_VERSION, operation_id=operation_id)

    def test_schema2_inventory_requires_all_ten_on_same_release(self):
        inventory = {
            f"vm{index:02d}": {"release_id": "reviewed-build", "schema_version": 2}
            for index in range(1, 11)
        }
        activation = Schema2Activation.from_inventory(self.manifest, inventory)
        self.assertTrue(activation.enabled_for(self.manifest))
        with self.assertRaises(ValueError):
            Schema2Activation.from_inventory(self.manifest, {
                key: value for key, value in inventory.items() if key != "vm10"})
        mixed = dict(inventory)
        mixed["vm10"] = {"release_id": "older-build", "schema_version": 2}
        with self.assertRaises(ValueError):
            Schema2Activation.from_inventory(self.manifest, mixed)

    def test_rejects_wrong_uuid_schema_fields_and_integer_coercions(self):
        req = request(self.manifest)
        base = req.to_metadata()
        for metadata in (
            dict(base, request_id="not-a-uuid"),
            dict(base, schema=True),
            dict(base, surprise="field"),
            {key: value for key, value in base.items() if key != "version"},
            dict(base, epoch=True),
            dict(base, sender_generation="01"),
            dict(base, value_len=True),
        ):
            with self.subTest(metadata=metadata):
                with self.assertRaises(RpcProtocolError):
                    ReplicaRequest.from_metadata(metadata, req.value, self.manifest)

    def test_rejects_bad_version_kind_writer_and_request_combinations(self):
        req = request(self.manifest)
        base = req.to_metadata()
        bad_versions = (
            {"kind": "strong", "number": "7", "writer_id": "vm01/1"},
            {"kind": "eventual", "number": "07", "writer_id": "vm01/1"},
            {"kind": "eventual", "number": "7", "writer_id": "vm11/1"},
            {"kind": "eventual", "number": "7", "writer_id": "vm01/01"},
        )
        for version in bad_versions:
            with self.subTest(version=version), self.assertRaises(RpcProtocolError):
                ReplicaRequest.from_metadata(dict(base, version=version), req.value, self.manifest)
        with self.assertRaises(RpcProtocolError):
            request(self.manifest, op=RpcOperation.READ_LOCAL, value=b"unexpected")
        with self.assertRaises(RpcProtocolError):
            request(self.manifest, op=RpcOperation.READ_LOCAL, version=Version("eventual", 1, "vm01/1"))
        with self.assertRaises(RpcProtocolError):
            request(self.manifest, version=Version("strong", 1, "vm01/1"))
        with self.assertRaises(RpcProtocolError):
            request(self.manifest, value=b"x" * (MAX_VALUE_BYTES + 1))

    def test_duplicate_json_fields_and_malformed_utf8_are_rejected(self):
        req = request(self.manifest)
        metadata = _encode_json(req.to_metadata())
        duplicate = metadata[:-1] + b',"schema":1}'
        payload = struct.pack("!I", 4 + len(duplicate) + len(req.value)) + struct.pack("!I", len(duplicate)) + duplicate + req.value
        with self.assertRaises(RpcProtocolError):
            transfer(lambda sock: (sock.sendall(payload), sock.shutdown(socket.SHUT_WR)),
                     lambda sock: receive_request(sock, self.manifest, deadline=time.monotonic() + 2))
        bad_text = b'{"schema":1,"key":"\xff"}'
        payload = struct.pack("!I", 4 + len(bad_text)) + struct.pack("!I", len(bad_text)) + bad_text
        with self.assertRaises(RpcProtocolError):
            transfer(lambda sock: (sock.sendall(payload), sock.shutdown(socket.SHUT_WR)),
                     lambda sock: receive_request(sock, self.manifest, deadline=time.monotonic() + 2))

    def test_oversized_outer_and_json_lengths_rejected_before_body_read(self):
        left, right = socket.socketpair()
        try:
            left.sendall(struct.pack("!I", MAX_RPC_PAYLOAD + 1))
            with self.assertRaises(RpcProtocolError):
                receive_request(right, self.manifest, deadline=time.monotonic() + 1)
        finally:
            left.close()
            right.close()

        left, right = socket.socketpair()
        try:
            outer = 4 + MAX_JSON_BYTES + 1
            left.sendall(struct.pack("!II", outer, MAX_JSON_BYTES + 1))
            with self.assertRaises(RpcProtocolError):
                receive_request(right, self.manifest, deadline=time.monotonic() + 1)
        finally:
            left.close()
            right.close()

    def test_truncated_body_and_trailing_bytes_fail_closed(self):
        req = request(self.manifest)
        payload = encode_request(req, self.manifest)
        frame = struct.pack("!I", len(payload)) + payload
        with self.assertRaises(ConnectionError):
            transfer(lambda sock: (sock.sendall(frame[:-1]), sock.shutdown(socket.SHUT_WR)),
                     lambda sock: receive_request(sock, self.manifest, deadline=time.monotonic() + 2))
        with self.assertRaises(RpcProtocolError):
            transfer(lambda sock: (sock.sendall(frame + b"x"), sock.shutdown(socket.SHUT_WR)),
                     lambda sock: receive_request(sock, self.manifest, deadline=time.monotonic() + 2))

    def test_response_contradictory_status_and_stored_version_evidence_rejected(self):
        with self.assertRaises(RpcProtocolError):
            ReplicaResponse(
                str(uuid.uuid4()), self.manifest.fingerprint, "vm04", 1, "vm01", 1,
                ConsistencyMode.EVENTUAL_NO_RR, RpcOperation.READ_LOCAL, "local",
                RpcStatus.FOUND, False)
        requested = request(self.manifest, value=b"v", version=Version("eventual", 7, "vm01/1"))
        target = self.manifest.node("vm04")
        bad = ReplicaResponse(
            requested.request_id, self.manifest.fingerprint, "vm04", 1, "vm01", 1,
            ConsistencyMode.EVENTUAL_NO_RR, RpcOperation.INSTALL_IF_NEWER, "local",
            RpcStatus.SUPERSEDED, False, stored_version=Version("eventual", 6, "vm01/1"),
            durable=True)
        with self.assertRaises(RpcProtocolError):
            bad.validate_for(requested, target, self.manifest, 1)


if __name__ == "__main__":
    unittest.main()
