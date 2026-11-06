"""Eventual-RR (KD-13): first-R client reply, bounded comparison, one conditional repair."""

import io
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock

from mp3.coordinator import OperationStatus, Schema2Activation, Schema3Activation
from mp3 import eventual
from mp3.eventual import EventualRRCoordinator, ReadRepairHandle, RepairStatus
from mp3.models import ConsistencyMode, Context, LocalStatus, Record, Version
from mp3.node import NodeRuntime
from mp3.rpc import ReplicaClient
from mp3.tests.test_strong import _manifest

RR = ConsistencyMode.EVENTUAL_RR
CONTEXT = Context(1, RR)
SECRET = b"SECRET-VALUE-do-not-log-123"


def version(number, writer="vm02/1"):
    return Version("eventual", number, writer)


class RRCluster:
    """Start ten in-process nodes in Eventual-RR mode on loopback."""

    def __init__(self, root: Path, *, read_quorum=2, write_quorum=2, log_vm=None):
        self.root = root
        self.manifest = _manifest()
        inventory = {node.vm_id: {"release_id": "rr-local", "schema_version": 3}
                     for node in self.manifest.nodes}
        self.schema2 = Schema2Activation.from_inventory(self.manifest, inventory)
        self.schema3 = Schema3Activation.from_inventory(self.manifest, inventory)
        self.nodes: dict[str, NodeRuntime] = {}
        self.log_path = root / "machine.1.log"
        for node in self.manifest.nodes:
            self.nodes[node.vm_id] = NodeRuntime(
                manifest=self.manifest, vm_id=node.vm_id,
                database_path=root / f"{node.vm_id}.sqlite", bootstrap=True,
                bootstrap_mode=RR, protocol_activation=self.schema2,
                rr_activation=self.schema3, event_stream=io.StringIO(),
                log_path=self.log_path if node.vm_id == log_vm else None,
                replica_workers=4, replica_queue=16, coordinator_workers=4,
                coordinator_queue=8, attempt_workers=16, attempt_queue=48,
                read_quorum=read_quorum, write_quorum=write_quorum).start()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if all(any(row.vm_id == peer and row.restart_generation > 0
                       for row in node.membership)
                   for node in self.nodes.values() for peer in self.nodes
                   if peer != node.vm_id):
                return
            time.sleep(0.02)
        self.stop()
        raise AssertionError("membership did not converge")

    def stop(self):
        for node in reversed(tuple(self.nodes.values())):
            try:
                node.stop(timeout=8)
            except Exception:
                pass

    def owners(self, key):
        return [node.vm_id for node in self.manifest.owners(key)]

    def install(self, vm_id, key, ver, value):
        result = self.nodes[vm_id].store.install_if_newer(key, Record(ver, value),
                                                          context=CONTEXT)
        assert result.status == LocalStatus.INSTALLED, result
        return result

    def local(self, vm_id, key):
        return self.nodes[vm_id].store.read_local(key, context=CONTEXT)

    def get(self, key, *, via="vm01", **kwargs):
        return self.nodes[via].eventual_rr_get(
            key, deadline=time.monotonic() + 10, **kwargs)


def delayed(slow: dict, *, before=None):
    """Wrap ReplicaClient.call_replica: delay chosen read targets, run a hook before sends."""
    original = ReplicaClient.call_replica

    def wrapper(client, target, request, **kwargs):
        if request.phase == "eventual_rr_read" and target.vm_id in slow:
            time.sleep(slow[target.vm_id])
        if before is not None:
            before(target, request)
        return original(client, target, request, **kwargs)

    return mock.patch.object(ReplicaClient, "call_replica", wrapper)


class EventualRRSharedClusterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.cluster = RRCluster(Path(cls.temp.name), log_vm="vm01")

    @classmethod
    def tearDownClass(cls):
        cls.cluster.stop()
        cls.temp.cleanup()

    def report(self, result):
        report = result.propagation.wait(timeout=20)
        self.assertIsNotNone(report, "read repair did not finish")
        return report

    def test_put_then_get_round_trip_with_nothing_to_repair(self):
        key = "rr-roundtrip"
        put = self.cluster.nodes["vm03"].eventual_rr_put(key, b"hello")
        self.assertEqual(put.status, OperationStatus.OK, put.error)
        self.assertGreaterEqual(len(put.qualifying_owners), 2)
        # W=2 acknowledges early; wait until the background fanout reaches every owner.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not all(
                self.cluster.local(vm, key).status == LocalStatus.FOUND
                for vm in self.cluster.owners(key)):
            time.sleep(0.05)
        got = self.cluster.get(key, via="vm08")
        self.assertEqual((got.status, got.value, got.version),
                         (OperationStatus.OK, b"hello", put.version))
        report = self.report(got)
        self.assertEqual(report.repairs, ())
        self.assertEqual(report.maximum_version, put.version)
        self.assertEqual(sorted(report.reachable), sorted(self.cluster.owners(key)))

    def test_client_gets_first_r_and_background_repairs_to_the_maximum(self):
        key = "rr-stale"
        owners = self.cluster.owners(key)
        newest = version(7)
        self.cluster.install(owners[0], key, newest, b"newest")
        self.cluster.install(owners[1], key, version(2), b"older")
        self.cluster.install(owners[2], key, version(2), b"older")
        self.cluster.install(owners[4], key, version(1), b"oldest")
        with delayed({owners[0]: 0.8}):
            result = self.cluster.get(key, read_quorum=2)
            # The newest copy answered too late for the first R replies.
            self.assertNotEqual(result.version, newest)
            report = self.report(result)
        self.assertEqual(report.maximum_version, newest)
        repaired = {repair.owner_id: repair.status for repair in report.repairs}
        self.assertEqual(repaired, {vm: RepairStatus.REPAIRED for vm in owners[1:]})
        for vm in owners:
            local = self.cluster.local(vm, key)
            self.assertEqual((local.record.version, local.record.value), (newest, b"newest"))
        # The returned client result did not change after repair.
        self.assertNotEqual(result.version, newest)

    def test_first_r_absent_returns_not_found_but_later_copy_still_repairs(self):
        key = "rr-late-found"
        owners = self.cluster.owners(key)
        self.cluster.install(owners[2], key, version(7), b"late")
        with delayed({owners[2]: 0.8}):
            result = self.cluster.get(key, read_quorum=2)
            self.assertEqual(result.status, OperationStatus.NOT_FOUND)
            report = self.report(result)
        self.assertEqual(report.returned_status, OperationStatus.NOT_FOUND)
        self.assertEqual(report.maximum_version, version(7))
        self.assertEqual({r.owner_id for r in report.repairs},
                         {owners[0], owners[1], owners[3], owners[4]})
        self.assertTrue(all(r.status == RepairStatus.REPAIRED for r in report.repairs))
        for vm in owners:
            self.assertEqual(self.cluster.local(vm, key).record.value, b"late")

    def test_all_absent_is_not_found_and_sends_no_repair(self):
        key = "rr-nothing-anywhere"
        result = self.cluster.get(key)
        self.assertEqual(result.status, OperationStatus.NOT_FOUND)
        report = self.report(result)
        self.assertEqual((report.repairs, report.maximum_version), ((), None))
        for vm in self.cluster.owners(key):
            self.assertEqual(self.cluster.local(vm, key).status, LocalStatus.NOT_FOUND)

    def test_repair_never_overwrites_a_newer_concurrent_write(self):
        key = "rr-race"
        owners = self.cluster.owners(key)
        self.cluster.install(owners[0], key, version(7), b"seven")
        self.cluster.install(owners[1], key, version(2), b"two")
        target, newer = owners[1], version(9, "vm03/1")

        def inject(node, request):
            if request.phase == "eventual_rr_repair" and node.vm_id == target:
                self.cluster.install(target, key, newer, b"nine")

        with delayed({owners[0]: 0.6}, before=inject):
            result = self.cluster.get(key, read_quorum=2)
            report = self.report(result)
        outcome = {repair.owner_id: repair for repair in report.repairs}[target]
        self.assertEqual(outcome.status, RepairStatus.SUPERSEDED_NEWER)
        self.assertEqual(outcome.stored_version, newer)
        self.assertEqual(self.cluster.local(target, key).record.value, b"nine")

    def test_equal_version_with_different_bytes_fails_closed_without_repair(self):
        key = "rr-conflict"
        owners = self.cluster.owners(key)
        for vm in owners[2:]:
            self.cluster.install(vm, key, version(3), b"three")
        self.cluster.install(owners[0], key, version(7), b"seven-A")
        self.cluster.install(owners[1], key, version(7), b"seven-B")
        with delayed({owners[0]: 0.6, owners[1]: 0.6}):
            result = self.cluster.get(key, read_quorum=2)
            report = self.report(result)
        self.assertEqual(result.status, OperationStatus.OK)
        self.assertTrue(report.conflict)
        self.assertEqual(report.repairs, ())
        self.assertEqual(self.cluster.local(owners[2], key).record.value, b"three")

    def test_a_get_through_the_coordinator_quorum_settings(self):
        key = "rr-quorum-5"
        owners = self.cluster.owners(key)
        self.cluster.install(owners[0], key, version(4), b"four")
        result = self.cluster.get(key, read_quorum=5)
        self.assertEqual((result.status, result.version), (OperationStatus.OK, version(4)))
        report = self.report(result)
        self.assertEqual({r.owner_id for r in report.repairs}, set(owners[1:]))

    def test_machine_log_records_version_reachable_responses_and_repairs(self):
        key = "rr-logged"
        owners = self.cluster.owners(key)
        self.cluster.install(owners[0], key, version(6), SECRET)
        self.cluster.install(owners[1], key, version(1), b"old")
        with delayed({owners[0]: 0.5}):
            report = self.report(self.cluster.get(key, read_quorum=2))
        text = self.cluster.log_path.read_text(encoding="utf-8")
        self.assertNotIn(SECRET.decode(), text)
        events = [json.loads(line) for line in text.splitlines() if line.strip()]
        mine = [e for e in events if e.get("event") == "read_repair" and e.get("key") == key]
        self.assertEqual(len(mine), 1)
        event = mine[0]
        self.assertEqual(event["node_id"], "vm01")
        self.assertEqual(event["request_id"], report.operation_id)
        self.assertEqual(sorted(event["reachable"]), sorted(owners))
        self.assertEqual(len(event["responses"]), 5)
        self.assertEqual(event["maximum_version"], "eventual:6:vm02/1")
        self.assertIsNotNone(event["returned_version"] or event["returned_status"])
        self.assertTrue(event["repairs"])
        self.assertTrue(all(r["status"] == "REPAIRED" for r in event["repairs"]))
        self.assertNotIn("\n", json.dumps(event))

    def test_report_is_logged_before_waiters_are_released(self):
        key = "rr-order"
        owners = self.cluster.owners(key)
        self.cluster.install(owners[0], key, version(3), b"three")
        order = []
        real_log, real_finish = eventual.log_read_repair, ReadRepairHandle.finish

        def spy_log(*args, **kwargs):
            order.append("log")
            return real_log(*args, **kwargs)

        def spy_finish(handle, report):
            order.append("finish")
            return real_finish(handle, report)

        with mock.patch.object(eventual, "log_read_repair", spy_log), \
                mock.patch.object(ReadRepairHandle, "finish", spy_finish):
            self.report(self.cluster.get(key, read_quorum=5))
        self.assertEqual(order, ["log", "finish"])

    def test_key_with_quotes_and_control_characters_stays_one_log_line(self):
        key = 'rr "quoted"\nnewline\x01tab\t\u2603'
        report = self.report(self.cluster.get(key))
        lines = self.cluster.log_path.read_text(encoding="utf-8").splitlines()
        events = [json.loads(line) for line in lines if line.strip()]
        mine = [e for e in events if e.get("event") == "read_repair"
                and e.get("request_id") == report.operation_id]
        self.assertEqual(len(mine), 1)
        self.assertEqual(mine[0]["key"], key)

    def test_a_failure_in_the_background_keeps_the_client_result_and_finishes_the_report(self):
        key = "rr-background-failure"
        owners = self.cluster.owners(key)
        self.cluster.install(owners[0], key, version(4), b"four")
        with mock.patch.object(EventualRRCoordinator, "_compare_and_repair",
                               side_effect=RuntimeError("boom")):
            result = self.cluster.get(key, read_quorum=5)
            report = self.report(result)
        self.assertEqual(result.status, OperationStatus.OK)
        self.assertEqual(report.returned_status, OperationStatus.OK)
        self.assertEqual(report.returned_version, version(4))
        self.assertIn("internal error", report.incomplete_reason)

    def test_no_background_capacity_means_no_repair_but_a_normal_client_result(self):
        key = "rr-no-capacity"
        owners = self.cluster.owners(key)
        self.cluster.install(owners[0], key, version(4), b"four")
        self.cluster.install(owners[1], key, version(1), b"one")
        background = self.cluster.nodes["vm01"].coordinator_resources.background
        with mock.patch.object(background, "submit", return_value=None):
            result = self.cluster.get(key, read_quorum=2)
            report = self.report(result)
        self.assertIn(result.status, (OperationStatus.OK, OperationStatus.NOT_FOUND))
        self.assertEqual(report.repairs, ())
        self.assertIn("background capacity unavailable", report.incomplete_reason)

    def test_repair_is_skipped_after_a_mode_change_or_past_the_twenty_second_limit(self):
        node = self.cluster.nodes["vm01"]
        coordinator = EventualRRCoordinator(node)
        other = Context(2, RR)
        skipped = coordinator._repair(other, "op", "k", ["vm02"], version(1), b"v",
                                      time.monotonic())
        self.assertEqual([(r.owner_id, r.status) for r in skipped],
                         [("vm02", RepairStatus.NOT_ATTEMPTED)])
        self.assertIn("mode or epoch", skipped[0].error)
        late = coordinator._repair(node.context, "op", "k", ["vm02"], version(1), b"v",
                                   time.monotonic() - 19.5)
        self.assertEqual(late[0].status, RepairStatus.NOT_ATTEMPTED)
        self.assertIn("20-second", late[0].error)
        self.assertEqual(coordinator._repair(node.context, "op", "k", [], None, b"",
                                             time.monotonic()), [])

    def test_background_work_is_bounded_and_ends_within_twenty_seconds_of_admission(self):
        key = "rr-bounded"
        owners = self.cluster.owners(key)
        self.cluster.install(owners[0], key, version(4), b"four")
        admitted = time.monotonic()
        report = self.report(self.cluster.get(key, read_quorum=5))
        self.assertLess(time.monotonic() - admitted, 20.0)
        self.assertTrue(report.repairs)


    def test_get_from_a_stored_value_never_logs_value_bytes_in_replica_lines(self):
        key = "rr-replica-lines"
        self.cluster.install(self.cluster.owners(key)[0], key, version(2), SECRET)
        self.report(self.cluster.get(key, read_quorum=5))
        self.assertNotIn(SECRET.decode(), self.cluster.log_path.read_text(encoding="utf-8"))


class EventualRRFailureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def cluster(self, **kwargs):
        cluster = RRCluster(Path(self.temp.name), **kwargs)
        self.addCleanup(cluster.stop)
        return cluster

    def test_an_unreachable_owner_is_reported_not_replaced_and_not_repaired(self):
        cluster = self.cluster()
        key = "rr-down-owner"
        owners = cluster.owners(key)
        # Every live owner holds a copy, so the first R replies are always FOUND.
        for vm in (owners[0], owners[1], owners[2]):
            cluster.install(vm, key, version(5), b"five")
        cluster.install(owners[4], key, version(1), b"one")
        down = owners[3]
        cluster.nodes[down].stop(timeout=8)
        via = next(vm for vm in cluster.nodes if vm != down)  # a coordinator that is up
        result = cluster.get(key, via=via, read_quorum=2)
        self.assertEqual(result.status, OperationStatus.OK)
        report = result.propagation.wait(timeout=20)
        row = {reply.owner_id: reply for reply in report.replies}[down]
        self.assertFalse(row.valid)
        self.assertNotIn(down, report.reachable)
        self.assertEqual({r.owner_id for r in report.repairs}, {owners[4]})
        self.assertNotIn(down, {r.owner_id for r in report.repairs})

    def test_when_r_is_unreachable_the_get_errors_and_no_repair_is_scheduled(self):
        cluster = self.cluster()
        key = "rr-r-impossible"
        owners = cluster.owners(key)
        cluster.install(owners[0], key, version(5), b"five")
        for vm in owners[1:]:
            cluster.nodes[vm].stop(timeout=8)
        via = next(vm for vm in cluster.nodes if vm not in owners[1:])  # still running
        result = cluster.get(key, via=via, read_quorum=2)
        self.assertIn(result.status, (OperationStatus.ERROR, OperationStatus.TIMEOUT))
        self.assertNotEqual(result.status, OperationStatus.NOT_FOUND)
        report = result.propagation.wait(timeout=20)
        self.assertEqual(report.repairs, ())
        self.assertIn("no repair", report.incomplete_reason)

    def test_wrong_mode_and_missing_activation_are_refused(self):
        cluster = self.cluster()
        node = cluster.nodes["vm01"]
        with mock.patch.object(node, "rr_activation", None):
            result = node.eventual_rr_get("k", deadline=time.monotonic() + 3)
        self.assertEqual((result.status, result.error),
                         (OperationStatus.ERROR, "SCHEMA3_DISABLED"))
        for bad in (0, 6, True):
            with self.assertRaises(ValueError):
                from mp3.eventual import EventualRRCoordinator
                EventualRRCoordinator(node, read_quorum=bad)

    def test_node_rejects_invalid_quorums(self):
        manifest = _manifest()
        for r, w in ((0, 2), (2, 6), (True, 2)):
            with self.subTest(r=r, w=w), self.assertRaises(ValueError):
                NodeRuntime(manifest=manifest, vm_id="vm01",
                            database_path=Path(self.temp.name) / "q.sqlite", bootstrap=True,
                            read_quorum=r, write_quorum=w)


class StructuredLogTests(unittest.TestCase):
    def test_formatter_rejects_a_field_that_replaces_a_base_key(self):
        import logging
        from mp3.eventlog import JsonFormatter
        record = logging.LogRecord("t", logging.INFO, __file__, 1, "read_repair", (), None)
        record.fields = {"event": "forged"}
        with self.assertRaises(ValueError):
            JsonFormatter().format(record)
        record.fields = {"key": "k"}
        self.assertEqual(json.loads(JsonFormatter().format(record))["key"], "k")


if __name__ == "__main__":
    unittest.main()
