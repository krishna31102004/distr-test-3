"""Schema-3 Eventual-RR wire rules, activation gates, and replica admission (KD-13/15)."""

from concurrent.futures import Future
import io
from pathlib import Path
import socket
import tempfile
import time
import unittest

from mp3.attempts import AttemptLease
from mp3.coordinator import (
    BoundedExecutor, OwnerCall, OwnerDispatchError, OwnerOutcome, RpcCompletion,
    Schema2Activation, Schema3Activation, collect_distinct, dispatch_owner,
)
from mp3.models import ConsistencyMode, Context, Version
from mp3.replica import ReplicaService
from mp3.rpc import (
    SCHEMA_VERSION, ReplicaClient, ReplicaRequest, ReplicaResponse, RpcOperation,
    RpcOutcome, RpcOutcomeKind, RpcProtocolError, RpcStatus, new_operation_id,
    new_request, receive_response, send_request,
)
from mp3.storage import open_store
from mp3.tests.test_replica import make_manifest as make_replica_manifest, membership_table
from mp3.tests.test_coordinator import manifest_fixture
from mp3.tests.test_rpc import make_manifest as make_wire_manifest

RR = ConsistencyMode.EVENTUAL_RR
RR_CONTEXT = Context(1, RR)


def rr_request(manifest, *, op=RpcOperation.READ_LOCAL, phase="eventual_rr_read",
               mode=RR, key="rr-key", version=None, value=b"", target="vm04",
               schema=3, operation_id=None):
    return new_request(manifest=manifest, sender_id="vm01", sender_generation=1,
                       target_id=target, epoch=1, mode=mode, op=op, key=key,
                       version=version, value=value, phase=phase, schema=schema,
                       operation_id=operation_id or new_operation_id())


def inventory(manifest, *, schema=3, release="local", overrides=None):
    nodes = {node.vm_id: {"release_id": release, "schema_version": schema}
             for node in manifest.nodes}
    for vm_id, record in (overrides or {}).items():
        nodes[vm_id] = record
    return nodes


class Schema3WireTests(unittest.TestCase):
    def setUp(self):
        self.manifest = make_wire_manifest()

    def test_read_and_repair_round_trip_through_metadata(self):
        version = Version("eventual", 9, "vm02/1")
        for request in (
                rr_request(self.manifest),
                rr_request(self.manifest, op=RpcOperation.INSTALL_IF_NEWER,
                           phase="eventual_rr_repair", version=version, value=b"\x00v"),
                rr_request(self.manifest, op=RpcOperation.INSTALL_IF_NEWER,
                           phase="eventual_put", version=version, value=b"")):
            with self.subTest(phase=request.phase):
                decoded = ReplicaRequest.from_metadata(
                    request.to_metadata(), request.value, self.manifest)
                self.assertEqual(decoded, request)
                self.assertEqual(decoded.schema, 3)

    def test_schema3_requires_rr_mode_and_known_phase(self):
        version = Version("eventual", 1, "vm02/1")
        for mode in (ConsistencyMode.STRONG, ConsistencyMode.EVENTUAL_NO_RR):
            with self.subTest(mode=mode), self.assertRaises(RpcProtocolError):
                rr_request(self.manifest, mode=mode)
        for phase in ("strong_query", "eventual_read", "local", "eventual_rr_extra"):
            with self.subTest(phase=phase), self.assertRaises(RpcProtocolError):
                rr_request(self.manifest, phase=phase)
        with self.assertRaises(RpcProtocolError):
            # A read phase cannot carry an install operation.
            rr_request(self.manifest, op=RpcOperation.INSTALL_IF_NEWER,
                       phase="eventual_rr_read", version=version)

    def test_schema2_and_schema1_cannot_use_rr_phases(self):
        with self.assertRaises(RpcProtocolError):
            rr_request(self.manifest, schema=2, mode=ConsistencyMode.EVENTUAL_NO_RR)
        with self.assertRaises(RpcProtocolError):
            rr_request(self.manifest, schema=1, mode=RR)
        with self.assertRaises(RpcProtocolError):
            rr_request(self.manifest, schema=4)

    def test_read_rejects_version_and_body_and_repair_requires_version(self):
        version = Version("eventual", 3, "vm02/1")
        with self.assertRaises(RpcProtocolError):
            rr_request(self.manifest, version=version)
        with self.assertRaises(RpcProtocolError):
            rr_request(self.manifest, value=b"body")
        with self.assertRaises(RpcProtocolError):
            rr_request(self.manifest, op=RpcOperation.INSTALL_IF_NEWER,
                       phase="eventual_rr_repair")

    def test_schema3_metadata_requires_operation_id_and_exact_fields(self):
        request = rr_request(self.manifest)
        metadata = request.to_metadata()
        missing = dict(metadata)
        del missing["operation_id"]
        extra = dict(metadata, surprise=1)
        for bad in (missing, extra):
            with self.subTest(fields=sorted(bad)), self.assertRaises(RpcProtocolError):
                ReplicaRequest.from_metadata(bad, b"", self.manifest)

    def test_schema1_requests_are_unchanged(self):
        request = new_request(
            manifest=self.manifest, sender_id="vm01", sender_generation=1,
            target_id="vm04", epoch=1, mode=ConsistencyMode.EVENTUAL_NO_RR,
            op=RpcOperation.READ_LOCAL, key="k")
        self.assertEqual(request.schema, SCHEMA_VERSION)
        self.assertNotIn("operation_id", request.to_metadata())


class Schema3ActivationTests(unittest.TestCase):
    def setUp(self):
        self.manifest = make_wire_manifest()

    def test_requires_every_node_to_advertise_schema_three(self):
        activation = Schema3Activation.from_inventory(
            self.manifest, inventory(self.manifest))
        self.assertTrue(activation.enabled_for(self.manifest))
        with self.assertRaises(ValueError):
            Schema3Activation.from_inventory(self.manifest, inventory(
                self.manifest, overrides={"vm07": {"release_id": "local", "schema_version": 2}}))
        with self.assertRaises(ValueError):
            Schema3Activation.from_inventory(self.manifest, inventory(
                self.manifest, overrides={"vm07": {"release_id": "other", "schema_version": 3}}))
        incomplete = inventory(self.manifest)
        del incomplete["vm10"]
        with self.assertRaises(ValueError):
            Schema3Activation.from_inventory(self.manifest, incomplete)

    def test_schema2_activation_still_accepts_a_schema3_inventory(self):
        activation = Schema2Activation.from_inventory(self.manifest, inventory(self.manifest))
        self.assertTrue(activation.enabled_for(self.manifest))
        schema2_only = Schema2Activation.from_inventory(
            self.manifest, inventory(self.manifest, schema=2))
        self.assertTrue(schema2_only.enabled_for(self.manifest))

    def test_activation_belongs_to_one_manifest(self):
        activation = Schema3Activation.from_inventory(self.manifest, inventory(self.manifest))
        self.assertFalse(activation.enabled_for(make_replica_manifest()))
        self.assertFalse(activation.enabled_for(None))


class Schema3ReplicaTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.manifest = make_replica_manifest()
        self.activation = Schema3Activation.from_inventory(
            self.manifest, inventory(self.manifest))
        self.store = open_store(Path(self.temp.name) / "vm04.sqlite",
                                bootstrap=RR_CONTEXT, vm_id="vm04")
        self.addCleanup(self.store.close)
        generation = self.store.start_incarnation()
        self.replica_table = membership_table(self.manifest, "vm04", generation, {"vm01": 1})
        self.client_table = membership_table(self.manifest, "vm01", 1, {"vm04": 1})
        self.target = self.manifest.node("vm04")
        self.key = next(f"owned-{i}" for i in range(500)
                        if "vm04" in {n.vm_id for n in self.manifest.owners(f"owned-{i}")})
        self.other_key = next(f"other-{i}" for i in range(500)
                              if "vm04" not in {n.vm_id for n in self.manifest.owners(f"other-{i}")})

    def start_service(self, *, activation):
        service = ReplicaService(self.store, self.manifest, self.replica_table,
                                 workers=4, max_pending=8, event_stream=io.StringIO(),
                                 rr_activation=activation)
        service.start()
        self.addCleanup(service.stop)

    def raw_call(self, request):
        deadline = time.monotonic() + 2
        with socket.create_connection((self.target.kv.host, self.target.kv.port),
                                      timeout=1) as sock:
            send_request(sock, request, self.manifest, deadline=deadline)
            sock.shutdown(socket.SHUT_WR)
            return receive_response(sock, self.manifest, deadline=deadline)

    def client(self, *, activation):
        return ReplicaClient(self.manifest, self.client_table, rr_activation=activation)

    def test_replica_without_activation_rejects_schema3_before_touching_storage(self):
        self.start_service(activation=None)
        response = self.raw_call(rr_request(
            self.manifest, op=RpcOperation.INSTALL_IF_NEWER, phase="eventual_rr_repair",
            key=self.key, version=Version("eventual", 5, "vm02/1"), value=b"x"))
        self.assertEqual((response.status, response.error), (RpcStatus.ERROR, "SCHEMA3_DISABLED"))
        self.assertEqual(self.store.read_local(self.key, context=RR_CONTEXT).status.value,
                         "NOT_FOUND")

    def test_client_without_activation_fails_before_sending(self):
        self.start_service(activation=self.activation)
        outcome = self.client(activation=None).call_replica(
            self.target, rr_request(self.manifest, key=self.key),
            deadline=time.monotonic() + 2)
        self.assertEqual(outcome.kind, RpcOutcomeKind.ERROR)
        self.assertIn("SCHEMA3_DISABLED", outcome.error)

    def test_non_owner_is_rejected_for_schema3(self):
        self.start_service(activation=self.activation)
        response = self.raw_call(rr_request(self.manifest, key=self.other_key))
        self.assertEqual((response.status, response.error),
                         (RpcStatus.ERROR, "NOT_PRESCRIBED_OWNER"))

    def test_read_put_and_repair_phases_work_end_to_end(self):
        self.start_service(activation=self.activation)
        client = self.client(activation=self.activation)

        def call(request):
            outcome = client.call_replica(self.target, request, deadline=time.monotonic() + 3)
            self.assertEqual(outcome.kind, RpcOutcomeKind.RESPONSE, outcome.error)
            return outcome.response

        self.assertEqual(call(rr_request(self.manifest, key=self.key)).status,
                         RpcStatus.NOT_FOUND)
        first = Version("eventual", 5, "vm02/1")
        stored = call(rr_request(self.manifest, op=RpcOperation.INSTALL_IF_NEWER,
                                 phase="eventual_put", key=self.key, version=first,
                                 value=b"first"))
        self.assertEqual(stored.status, RpcStatus.INSTALLED)
        found = call(rr_request(self.manifest, key=self.key))
        self.assertEqual((found.status, found.version, found.value),
                         (RpcStatus.FOUND, first, b"first"))
        newer = Version("eventual", 9, "vm03/1")
        repaired = call(rr_request(self.manifest, op=RpcOperation.INSTALL_IF_NEWER,
                                   phase="eventual_rr_repair", key=self.key,
                                   version=newer, value=b"newer"))
        self.assertEqual(repaired.status, RpcStatus.INSTALLED)
        older = call(rr_request(self.manifest, op=RpcOperation.INSTALL_IF_NEWER,
                                phase="eventual_rr_repair", key=self.key,
                                version=first, value=b"first"))
        self.assertEqual((older.status, older.stored_version), (RpcStatus.SUPERSEDED, newer))
        self.assertEqual(call(rr_request(self.manifest, key=self.key)).value, b"newer")

    def test_attempt_lease_is_allowed_for_rr_phases_only(self):
        self.start_service(activation=self.activation)
        client = self.client(activation=self.activation)
        lease = AttemptLease(foreground_seconds=3.0, continuation_seconds=3.0, max_lifetime=6.0)
        outcome = client.call_replica(
            self.target, rr_request(self.manifest, key=self.key),
            deadline=lease.hard_deadline, attempt_lease=lease)
        self.assertEqual(outcome.kind, RpcOutcomeKind.RESPONSE, outcome.error)
        self.assertEqual(outcome.response.status, RpcStatus.NOT_FOUND)
        schema1 = new_request(
            manifest=self.manifest, sender_id="vm01", sender_generation=1,
            target_id="vm04", epoch=1, mode=RR, op=RpcOperation.READ_LOCAL, key=self.key)
        refused = client.call_replica(self.target, schema1, deadline=lease.hard_deadline,
                                      attempt_lease=lease)
        self.assertEqual(refused.kind, RpcOutcomeKind.ERROR)


class Schema3CoordinatorTests(unittest.TestCase):
    """Sender-side owner check and quorum collection accept schema 3 like schema 2."""

    def setUp(self):
        self.manifest = manifest_fixture()
        self.key = "alpha"
        self.owners = self.manifest.owners(self.key)
        self.operation_id = new_operation_id()

    def rr_call(self, owner, *, number):
        request = new_request(
            manifest=self.manifest, sender_id="vm01", sender_generation=2,
            target_id=owner.vm_id, epoch=1, mode=RR, op=RpcOperation.READ_LOCAL,
            key=self.key, phase="eventual_rr_read", schema=3,
            operation_id=self.operation_id)
        response = ReplicaResponse(
            request_id=request.request_id, manifest_id=self.manifest.fingerprint,
            sender_id=owner.vm_id, sender_generation=3, target_id="vm01", epoch=1,
            mode=RR, op=RpcOperation.READ_LOCAL, phase="eventual_rr_read",
            status=RpcStatus.FOUND, present=True,
            version=Version("eventual", number, "vm02/1"), value=b"v",
            schema=3, operation_id=self.operation_id)
        future = Future()
        future.set_result(RpcCompletion(
            RpcOutcome(RpcOutcomeKind.RESPONSE, response), time.monotonic()))
        return OwnerCall(owner, request, 3, future)

    def test_collector_qualifies_distinct_schema3_owner_replies(self):
        calls = [self.rr_call(owner, number=index + 1)
                 for index, owner in enumerate(self.owners)]
        result = collect_distinct(
            calls, owners=self.owners, manifest=self.manifest, required=2,
            operation_id=self.operation_id, phase="eventual_rr_read",
            context=RR_CONTEXT, deadline=time.monotonic() + 2)
        self.assertTrue(result.reached)
        self.assertEqual(len({row.owner_id for row in result.qualifying}), 2)

    def test_dispatch_refuses_a_schema3_request_to_a_non_owner(self):
        owner_ids = {node.vm_id for node in self.owners}
        stranger = next(node for node in self.manifest.nodes if node.vm_id not in owner_ids)
        request = new_request(
            manifest=self.manifest, sender_id="vm01", sender_generation=2,
            target_id=stranger.vm_id, epoch=1, mode=RR, op=RpcOperation.READ_LOCAL,
            key=self.key, phase="eventual_rr_read", schema=3,
            operation_id=self.operation_id)
        executor = BoundedExecutor(workers=1, max_pending=1, name="schema3-preflight")
        try:
            with self.assertRaises(OwnerDispatchError) as rejected:
                dispatch_owner(object(), stranger, request, manifest=self.manifest,
                               deadline=time.monotonic() + 2, expected_generation=1,
                               executor=executor)
            self.assertEqual(rejected.exception.failure.outcome,
                             OwnerOutcome.NOT_PRESCRIBED_OWNER)
            self.assertEqual(executor.counts, (0, 0, 0))
        finally:
            self.assertTrue(executor.shutdown(timeout=2).clean)


if __name__ == "__main__":
    unittest.main()
