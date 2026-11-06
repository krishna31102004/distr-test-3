"""Cluster-wide consistency-mode changes on ten loopback nodes (KD-16)."""

import contextlib
import io
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest

from mp3.cli import main
from mp3.control import ControlStatus, send_command
from mp3.coordinator import Schema2Activation, Schema3Activation
from mp3.models import ConsistencyMode as M, Context, LocalStatus, Record, Version
from mp3.node import NodeRuntime
from mp3.rpc import new_operation_id
from mp3.tests.test_strong import _manifest
from mp3.transition import (
    COORDINATOR_ID, PROBE_ID, TransitionFault, TransitionManager,
)

STRONG, NORR, RR = M.STRONG, M.EVENTUAL_NO_RR, M.EVENTUAL_RR


def wait_for(predicate, timeout=25.0, description="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {description}")


class ModeCluster:
    """Ten in-process nodes with real control and KV sockets and separate databases."""

    def __init__(self, root: Path, mode=STRONG):
        self.root = root
        self.manifest = _manifest()
        inventory = {node.vm_id: {"release_id": "mode-local", "schema_version": 3}
                     for node in self.manifest.nodes}
        self.schema2 = Schema2Activation.from_inventory(self.manifest, inventory)
        self.schema3 = Schema3Activation.from_inventory(self.manifest, inventory)
        self.manifest_path = root / "manifest.json"
        self.manifest_path.write_text(self.manifest.to_json(), encoding="utf-8")
        self.nodes: dict[str, NodeRuntime] = {}
        for node in self.manifest.nodes:
            self.nodes[node.vm_id] = self._make(node.vm_id, bootstrap=True, mode=mode)
        wait_for(self.membership_converged, 12, "membership")

    def _make(self, vm_id, *, bootstrap, mode=None):
        return NodeRuntime(
            manifest=self.manifest, vm_id=vm_id, database_path=self.root / f"{vm_id}.sqlite",
            bootstrap=bootstrap, bootstrap_mode=mode if bootstrap else None,
            protocol_activation=self.schema2, rr_activation=self.schema3,
            event_stream=io.StringIO(), replica_workers=4, replica_queue=16,
            coordinator_workers=4, coordinator_queue=8, attempt_workers=16,
            attempt_queue=48).start()

    def running(self):
        return [node for node in self.nodes.values() if node.store is not None and node.ready]

    def membership_converged(self):
        live = self.running()
        return all(any(row.vm_id == peer and row.restart_generation > 0 for row in node.membership)
                   for node in live for peer in self.nodes if peer != node.vm_id)

    def stop(self):
        for node in reversed(tuple(self.nodes.values())):
            try:
                node.stop(timeout=8)
            except Exception:
                pass

    def revive(self, vm_id):
        """Start a stopped node again from its database, as a fresh process would.

        The membership module refuses a second start of one database inside one Python
        process, so the guard is cleared here to stand in for the new process."""
        from mp3 import membership
        path = str((self.root / f"{vm_id}.sqlite").resolve())
        with membership._START_LOCK:
            membership._STARTED_STORE_PATHS.discard(path)
        self.nodes[vm_id] = self._make(vm_id, bootstrap=False)

    def restart(self, vm_id):
        self.nodes[vm_id].stop(timeout=8)
        self.revive(vm_id)

    def request(self, via, mode, *, timeout=60.0):
        return send_command(self.manifest, via, "mode", mode.value, timeout=timeout)

    def put(self, via, key, value):
        return send_command(self.manifest, via, "put", key, value)

    def get(self, via, key):
        return send_command(self.manifest, via, "get", key)

    def contexts(self):
        return {vm: node.store.current_context() for vm, node in self.nodes.items()
                if node.store is not None}

    def phases(self):
        return {vm: node.store.local_phase() for vm, node in self.nodes.items()
                if node.store is not None}

    def settled(self, mode, epoch):
        return (all(c == Context(epoch, mode) for c in self.contexts().values())
                and set(self.phases().values()) == {"ACTIVE"})

    def replicated(self, key):
        for vm in (node.vm_id for node in self.manifest.owners(key)):
            node = self.nodes[vm]
            if node.store.read_local(key, context=node.store.current_context()).status \
                    != LocalStatus.FOUND:
                return False
        return True


class ModeTestCase(unittest.TestCase):
    def cluster(self, mode=STRONG):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        cluster = ModeCluster(Path(temp.name), mode)
        self.addCleanup(cluster.stop)
        return cluster


class ModeChangeTests(ModeTestCase):
    def test_all_six_directed_transitions_obey_the_data_rules(self):
        cluster = self.cluster(STRONG)
        owners_before = [n.vm_id for n in cluster.manifest.owners("alpha")]
        steps = [  # (via, target, data policy)
            ("vm07", NORR, "clear"), ("vm03", RR, "retain"), ("vm10", NORR, "retain"),
            ("vm05", STRONG, "clear"), ("vm02", RR, "clear"), ("vm09", STRONG, "clear")]
        epoch = 1
        key = "alpha"
        value = b"value"
        mode = STRONG
        for via, target, policy in steps:
            self.assertEqual(cluster.put(via, key, value).status, ControlStatus.OK)
            wait_for(lambda: cluster.replicated(key), 10, "replication to all owners")
            reply = cluster.request(via, target)
            epoch += 1
            self.assertEqual((reply.status, reply.error), (ControlStatus.OK, None), reply.error)
            self.assertEqual((reply.mode, reply.epoch), (target, epoch))
            self.assertTrue(cluster.settled(target, epoch), (cluster.contexts(), cluster.phases()))
            got = cluster.get("vm04", key)
            if policy == "retain":
                self.assertEqual((got.status, got.value), (ControlStatus.OK, value))
            else:
                self.assertEqual(got.status, ControlStatus.NOT_FOUND)
                with_records = [vm for vm, node in cluster.nodes.items() if node.store.read_local(
                    key, context=node.store.current_context()).status == LocalStatus.FOUND]
                self.assertEqual(with_records, [])
            mode = target
            # Membership, identities and ring owners are untouched.
            for node in cluster.nodes.values():
                rows = node.membership
                self.assertEqual(len(rows), 10)
                self.assertTrue(all(row.restart_generation > 0 for row in rows))
            self.assertEqual([n.vm_id for n in cluster.manifest.owners("alpha")], owners_before)
        self.assertEqual(mode, STRONG)
        self.assertEqual(epoch, 7)

    def test_same_mode_reports_no_change_without_a_new_epoch(self):
        cluster = self.cluster(STRONG)
        reply = cluster.request("vm04", STRONG)
        self.assertEqual((reply.status, reply.epoch), (ControlStatus.NO_CHANGE, 1))
        self.assertTrue(cluster.settled(STRONG, 1))
        self.assertIsNone(cluster.nodes[COORDINATOR_ID].store.journal_open())

    def test_concurrent_requests_for_one_target_make_one_change(self):
        cluster = self.cluster(STRONG)
        replies = {}

        def ask(via):
            replies[via] = cluster.request(via, NORR)

        threads = [threading.Thread(target=ask, args=(via,)) for via in ("vm02", "vm08", "vm05")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(70)
        self.assertEqual(len(replies), 3)
        self.assertTrue(all(r.status in (ControlStatus.OK, ControlStatus.NO_CHANGE)
                            for r in replies.values()), replies)
        self.assertIn(ControlStatus.OK, {r.status for r in replies.values()})
        self.assertTrue(cluster.settled(NORR, 2))  # exactly one epoch step

    def test_a_different_target_while_one_is_running_gets_busy(self):
        cluster = self.cluster(STRONG)
        entered, release = threading.Event(), threading.Event()

        def hold(stage):
            if stage == "after_prepare":
                entered.set()
                release.wait(30)

        cluster.nodes[COORDINATOR_ID].transition.fault_hook = hold
        first = {}
        thread = threading.Thread(
            target=lambda: first.update(reply=cluster.request("vm06", NORR)))
        thread.start()
        self.assertTrue(entered.wait(20))
        busy = cluster.request("vm09", RR)
        self.assertEqual(busy.status, ControlStatus.BUSY)
        release.set()
        thread.join(70)
        self.assertEqual(first["reply"].status, ControlStatus.OK)
        self.assertTrue(cluster.settled(NORR, 2))

    def test_unreachable_node_blocks_the_change_before_anything_is_fenced(self):
        cluster = self.cluster(STRONG)
        cluster.put("vm01", "keep", b"kept")
        cluster.nodes["vm08"].stop(timeout=8)
        reply = cluster.request("vm02", NORR)
        self.assertEqual(reply.status, ControlStatus.TIMEOUT)
        self.assertIn("vm08", reply.error)
        for vm, node in cluster.nodes.items():
            if vm != "vm08":
                self.assertEqual(node.store.local_phase(), "ACTIVE")
                self.assertEqual(node.store.current_context(), Context(1, STRONG))
        self.assertIsNone(cluster.nodes[COORDINATOR_ID].store.journal_open())

    def test_cli_consistency_mode_command(self):
        cluster = self.cluster(STRONG)

        def run(*args):
            stream = io.StringIO()
            with contextlib.redirect_stdout(stream):
                code = main(["--manifest", str(cluster.manifest_path), "--vm-id", "vm03", *args])
            return code, json.loads(stream.getvalue())

        code, result = run("consistency_mode", "eventual-noRR")
        self.assertEqual((code, result["status"], result["mode"], result["epoch"]),
                         (0, "OK", "eventual-noRR", 2))
        self.assertEqual(result["transition"]["target_mode"], "eventual-noRR")
        code, result = run("consistency_mode", "eventual-noRR")
        self.assertEqual((code, result["status"]), (0, "NO_CHANGE"))
        code, result = run("consistency_mode", "bogus")
        self.assertEqual(code, 2)


class AbortTests(ModeTestCase):
    def test_a_refusing_node_aborts_the_change_and_burns_the_epoch(self):
        cluster = self.cluster(STRONG)
        cluster.put("vm01", "keep", b"kept")
        wait_for(lambda: cluster.replicated("keep"), 10, "replication")
        cluster.nodes["vm05"].rr_activation = None  # vm05 cannot speak Eventual-RR
        reply = cluster.request("vm02", RR)
        self.assertEqual(reply.status, ControlStatus.ERROR)
        self.assertIn("aborted", reply.error)
        wait_for(lambda: cluster.settled(STRONG, 1), 15, "every node back to Strong epoch 1")
        got = cluster.get("vm07", "keep")
        self.assertEqual((got.status, got.value), (ControlStatus.OK, b"kept"))
        self.assertEqual(cluster.put("vm03", "after", b"abort").status, ControlStatus.OK)
        cluster.nodes["vm05"].rr_activation = cluster.schema3
        retry = cluster.request("vm02", RR)
        self.assertEqual((retry.status, retry.epoch), (ControlStatus.OK, 3))  # epoch 2 was burned


class CrashRecoveryTests(ModeTestCase):
    def crash_at(self, cluster, stage):
        """Make vm01's driver stop at `stage` exactly once, as a crash would."""
        manager = cluster.nodes[COORDINATOR_ID].transition
        fired = threading.Event()

        def hook(name):
            if name == stage and not fired.is_set():
                fired.set()
                raise TransitionFault(stage)

        manager.fault_hook = hook
        return fired

    def test_a_decision_to_commit_is_replayed_and_never_undone(self):
        cluster = self.cluster(STRONG)
        cluster.put("vm01", "gone", b"cleared")
        self.crash_at(cluster, "after_commit_decided")
        cluster.nodes[COORDINATOR_ID].transition.pause_recovery()  # no live recovery: a true crash
        reply = cluster.request("vm04", NORR)
        self.assertNotEqual(reply.status, ControlStatus.OK)
        self.assertEqual(cluster.nodes[COORDINATOR_ID].store.journal_open().state,
                         "COMMIT_DECIDED")
        self.assertEqual(set(cluster.phases().values()), {"PREPARED"})
        self.assertEqual(cluster.get("vm05", "gone").status, ControlStatus.ERROR)  # fenced
        cluster.restart(COORDINATOR_ID)  # vm01 comes back and replays the decision
        wait_for(lambda: cluster.settled(NORR, 2), 30, "commit replay to finish")
        self.assertEqual(cluster.get("vm05", "gone").status, ControlStatus.NOT_FOUND)

    def test_a_crash_before_the_decision_aborts_on_recovery(self):
        cluster = self.cluster(STRONG)
        cluster.put("vm01", "safe", b"intact")
        wait_for(lambda: cluster.replicated("safe"), 10, "replication")
        self.crash_at(cluster, "after_prepare")
        cluster.nodes[COORDINATOR_ID].transition.pause_recovery()
        cluster.request("vm04", NORR)
        self.assertEqual(cluster.nodes[COORDINATOR_ID].store.journal_open().state, "PREPARING")
        self.assertEqual(set(cluster.phases().values()), {"PREPARED"})
        cluster.restart(COORDINATOR_ID)
        wait_for(lambda: cluster.settled(STRONG, 1), 30, "abort on recovery")
        got = cluster.get("vm06", "safe")
        self.assertEqual((got.status, got.value), (ControlStatus.OK, b"intact"))

    def test_a_crash_after_global_active_finishes_activation(self):
        cluster = self.cluster(STRONG)
        self.crash_at(cluster, "after_global_active")
        cluster.nodes[COORDINATOR_ID].transition.pause_recovery()
        cluster.request("vm04", RR)
        self.assertEqual(cluster.nodes[COORDINATOR_ID].store.journal_open().state,
                         "GLOBAL_ACTIVE")
        self.assertEqual(set(cluster.phases().values()), {"COMMITTED"})
        cluster.restart(COORDINATOR_ID)
        wait_for(lambda: cluster.settled(RR, 2), 30, "activation replay")

    def test_a_live_coordinator_recovers_a_failed_driver_by_itself(self):
        cluster = self.cluster(STRONG)
        self.crash_at(cluster, "after_commit")
        reply = cluster.request("vm04", NORR)
        self.assertNotEqual(reply.status, ControlStatus.OK)
        wait_for(lambda: cluster.settled(NORR, 2), 30, "background recovery")

    def test_a_participant_that_crashes_after_prepare_reconciles(self):
        cluster = self.cluster(STRONG)
        cluster.put("vm01", "gone", b"cleared")
        manager = cluster.nodes[COORDINATOR_ID].transition

        def hook(stage):
            if stage == "after_commit_decided" and cluster.nodes["vm04"].ready:
                cluster.nodes["vm04"].stop(timeout=8)  # vm04 dies after PREPARED

        manager.fault_hook = hook
        reply = cluster.request("vm02", NORR)
        self.assertEqual(reply.status, ControlStatus.PENDING)  # decided, not yet confirmed
        self.assertIn("vm04", reply.error)
        manager.fault_hook = None
        cluster.revive("vm04")
        wait_for(lambda: cluster.settled(NORR, 2), 40, "participant reconciliation")
        self.assertEqual(cluster.get("vm05", "gone").status, ControlStatus.NOT_FOUND)

    def test_a_node_that_missed_activate_stays_fenced_until_it_is_told(self):
        cluster = self.cluster(STRONG)
        manager = cluster.nodes[COORDINATOR_ID].transition
        silenced = {"vm06"}
        real = manager.participant_message

        def deaf(command, transition_id, payload):
            return real(command, transition_id, payload)

        # Drop ACTIVATE to vm06 only by making it refuse until we allow it.
        gate = threading.Event()
        victim = cluster.nodes["vm06"].transition
        original = victim.participant_message

        def refuse_activate(command, transition_id, payload):
            if command == "tx_activate" and not gate.is_set():
                return ControlStatus.ERROR, {}, "dropped for the test"
            return original(command, transition_id, payload)

        victim.participant_message = refuse_activate
        reply = cluster.request("vm04", NORR)
        self.assertEqual(reply.status, ControlStatus.PENDING)
        self.assertEqual(cluster.phases()["vm06"], "COMMITTED")
        others = [vm for vm in cluster.nodes if vm not in silenced]
        self.assertTrue(all(cluster.phases()[vm] == "ACTIVE" for vm in others))
        # Active nodes serve the target mode; the fenced node returns an explicit refusal.
        self.assertEqual(cluster.nodes["vm06"].store.read_local(
            "k", context=Context(2, NORR)).status, LocalStatus.TRANSITIONING)
        gate.set()  # vm01's recovery loop replays ACTIVATE with the same transition id
        wait_for(lambda: cluster.settled(NORR, 2), 30, "late activation")


class FencingTests(ModeTestCase):
    def test_old_epoch_writes_cannot_resurrect_cleared_data(self):
        cluster = self.cluster(STRONG)
        cluster.put("vm01", "ghost", b"strong-era")
        wait_for(lambda: cluster.replicated("ghost"), 10, "replication")
        self.assertEqual(cluster.request("vm02", NORR).status, ControlStatus.OK)
        store = cluster.nodes["vm04"].store
        old = Context(1, STRONG)
        late = store.install_if_newer("ghost", Record(Version("strong", 9, "vm01/1"), b"late"),
                                      context=old)
        self.assertEqual(late.status, LocalStatus.STALE_EPOCH)
        self.assertEqual(store.read_local("ghost", context=old).status, LocalStatus.STALE_EPOCH)
        self.assertEqual(cluster.get("vm03", "ghost").status, ControlStatus.NOT_FOUND)

    def test_fenced_nodes_answer_with_explicit_errors_not_not_found(self):
        cluster = self.cluster(STRONG)
        entered, release = threading.Event(), threading.Event()

        def hold(stage):
            if stage == "after_prepare":
                entered.set()
                release.wait(30)

        cluster.nodes[COORDINATOR_ID].transition.fault_hook = hold
        thread = threading.Thread(target=lambda: cluster.request("vm04", NORR))
        thread.start()
        self.assertTrue(entered.wait(20))
        reply = cluster.get("vm05", "anything")
        self.assertEqual(reply.status, ControlStatus.ERROR)
        self.assertIn("TRANSITIONING", reply.error)
        put = cluster.put("vm05", "anything", b"x")
        self.assertEqual(put.status, ControlStatus.ERROR)
        release.set()
        thread.join(70)
        self.assertTrue(cluster.settled(NORR, 2))


class MessageValidationTests(ModeTestCase):
    def payload(self, **overrides):
        data = {"coordinator": "vm01", "from_epoch": 1, "from_mode": "strong",
                "target_epoch": 2, "target_mode": "eventual-noRR", "budget": 5.0}
        data.update(overrides)
        return json.dumps(data).encode()

    def test_bad_prepare_payloads_are_refused_without_fencing(self):
        cluster = self.cluster(STRONG)
        manager = cluster.nodes["vm05"].transition
        for bad in (self.payload(coordinator="vm02"), self.payload(extra=1),
                    self.payload(target_epoch=1), self.payload(from_mode="bogus"),
                    self.payload(target_mode="strong"), b"not json", b"[]"):
            with self.subTest(bad=bad[:30]):
                status, _detail, error = manager.participant_message(
                    "tx_prepare", new_operation_id(), bad)
                self.assertEqual(status, ControlStatus.ERROR)
                self.assertTrue(error)
        self.assertEqual(cluster.nodes["vm05"].store.local_phase(), "ACTIVE")

    def test_prepare_for_an_unavailable_target_mode_is_refused(self):
        cluster = self.cluster(STRONG)
        node = cluster.nodes["vm05"]
        node.rr_activation = None
        status, _detail, error = node.transition.participant_message(
            "tx_prepare", new_operation_id(), self.payload(target_mode="eventual-RR"))
        self.assertEqual(status, ControlStatus.ERROR)
        self.assertIn("not activated", error)

    def test_activate_is_refused_unless_vm01_recorded_the_global_decision(self):
        cluster = self.cluster(STRONG)
        node = cluster.nodes["vm05"]
        forged = new_operation_id()
        status, _d, _e = node.transition.participant_message(
            "tx_prepare", forged, self.payload())
        self.assertEqual(status, ControlStatus.OK)
        status, _d, error = node.transition.participant_message("tx_activate", forged, b"")
        self.assertEqual(status, ControlStatus.ERROR)
        self.assertIn("not verified", error)
        status, detail, _e = node.transition.participant_message("tx_abort", forged, b"")
        self.assertEqual((status, detail["phase"]), (ControlStatus.OK, "ABORTED"))
        self.assertEqual(node.store.local_phase(), "ACTIVE")

    def test_status_probe_and_unknown_transition_status(self):
        cluster = self.cluster(STRONG)
        reply = send_command(cluster.manifest, "vm03", "tx_status", PROBE_ID)
        self.assertEqual((reply.status, json.loads(reply.detail)["local_phase"]),
                         (ControlStatus.OK, "ACTIVE"))
        reply = send_command(cluster.manifest, COORDINATOR_ID, "tx_status", new_operation_id())
        detail = json.loads(reply.detail)
        self.assertEqual((detail["phase"], detail["decision"]), ("NONE", "UNKNOWN"))

    def test_only_vm01_coordinates_but_any_node_accepts_the_command(self):
        cluster = self.cluster(STRONG)
        self.assertIsInstance(cluster.nodes["vm09"].transition, TransitionManager)
        self.assertEqual(cluster.nodes["vm09"].transition.vm_id, "vm09")
        reply = cluster.request("vm09", RR)  # forwarded to vm01
        self.assertEqual((reply.status, reply.mode), (ControlStatus.OK, RR))


class ReviewFixTests(ModeTestCase):
    """Behaviors found by the independent review: forged messages, orphans, retries."""

    def payload(self, **overrides):
        data = {"coordinator": "vm01", "from_epoch": 1, "from_mode": "strong",
                "target_epoch": 2, "target_mode": "eventual-noRR", "budget": 5.0}
        data.update(overrides)
        return json.dumps(data).encode()

    def test_a_forged_commit_cannot_clear_a_node_and_the_orphan_is_released(self):
        cluster = self.cluster(STRONG)
        cluster.put("vm01", "precious", b"data")
        wait_for(lambda: cluster.replicated("precious"), 10, "replication")
        node = cluster.nodes["vm05"]
        forged = new_operation_id()
        status, _d, _e = node.transition.participant_message(
            "tx_prepare", forged, self.payload())
        self.assertEqual(status, ControlStatus.OK)  # a stray PREPARE can only fence
        status, _d, error = node.transition.participant_message("tx_commit", forged, b"")
        self.assertEqual(status, ControlStatus.ERROR)
        self.assertIn("COMMIT decision is not verified", error)
        self.assertEqual(node.store.current_context(), Context(1, STRONG))
        # vm01 has never heard of this ID, so recovery aborts it and the node reopens.
        wait_for(lambda: cluster.settled(STRONG, 1), 15, "orphan released")
        self.assertEqual(cluster.get("vm05", "precious").value, b"data")

    def test_a_stray_abort_does_not_hide_the_open_transition(self):
        cluster = self.cluster(STRONG)
        node = cluster.nodes["vm05"]
        node.transition.pause_recovery()  # keep recovery out of the way for this check
        forged = new_operation_id()
        node.transition.participant_message("tx_prepare", forged, self.payload())
        status, _d, error = node.transition.participant_message(
            "tx_abort", new_operation_id(), b"")
        self.assertEqual(status, ControlStatus.ERROR)
        self.assertEqual(node.store.open_transition().transition_id, forged)
        node.transition.participant_message("tx_abort", forged, b"")
        self.assertEqual(node.store.local_phase(), "ACTIVE")

    def test_vm01_retries_its_own_failed_abort(self):
        cluster = self.cluster(STRONG)
        cluster.nodes["vm05"].rr_activation = None  # forces an abort
        manager = cluster.nodes[COORDINATOR_ID].transition
        real = manager.participant_message
        failed = []

        def flaky(command, transition_id, payload):
            if command == "tx_abort" and not failed:
                failed.append(transition_id)
                return ControlStatus.ERROR, {}, "storage busy (test)"
            return real(command, transition_id, payload)

        manager.participant_message = flaky
        reply = cluster.request("vm02", RR)
        self.assertEqual(reply.status, ControlStatus.ERROR)
        self.assertTrue(failed)
        wait_for(lambda: cluster.settled(STRONG, 1), 20, "vm01 settles its own row")

    def test_an_acknowledgement_must_report_the_expected_phase(self):
        cluster = self.cluster(STRONG)
        victim = cluster.nodes["vm07"].transition
        real = victim.participant_message

        def wrong_phase(command, transition_id, payload):
            status, detail, error = real(command, transition_id, payload)
            if command == "tx_prepare":
                return status, {"phase": "ACTIVE"}, error
            return status, detail, error

        victim.participant_message = wrong_phase
        reply = cluster.request("vm02", NORR)
        self.assertEqual(reply.status, ControlStatus.ERROR)
        self.assertIn("aborted", reply.error)
        wait_for(lambda: cluster.settled(STRONG, 1), 20, "back to Strong")

    def test_a_base_exception_in_the_driver_still_closes_the_run(self):
        cluster = self.cluster(STRONG)
        manager = cluster.nodes[COORDINATOR_ID].transition

        class Boom(BaseException):
            pass

        def hook(stage):
            if stage == "before_prepare":
                manager.fault_hook = None
                raise Boom()

        manager.fault_hook = hook
        with self.assertRaises(Boom):
            manager.request_mode(NORR, new_operation_id())
        self.assertTrue(manager._current.done.is_set())
        self.assertFalse(manager._drive_lock.locked())
        wait_for(lambda: cluster.settled(STRONG, 1), 25, "recovery aborts the open journal")

    def test_only_a_few_mode_requests_may_wait_on_one_node(self):
        cluster = self.cluster(STRONG)
        entered, release = threading.Event(), threading.Event()

        def hold(stage):
            if stage == "after_prepare":
                entered.set()
                release.wait(40)

        cluster.nodes[COORDINATOR_ID].transition.fault_hook = hold
        replies = {}
        threads = []
        for via in ("vm02", "vm03", "vm04"):
            thread = threading.Thread(
                target=lambda via=via: replies.update({via: cluster.request(via, NORR)}))
            thread.start()
            threads.append(thread)
            if via == "vm02":
                self.assertTrue(entered.wait(20))
            time.sleep(0.3)
        extra = cluster.request("vm05", NORR)
        self.assertEqual(extra.status, ControlStatus.BUSY)
        self.assertIn("too many", extra.error)
        release.set()
        for thread in threads:
            thread.join(70)
        self.assertTrue(all(r.status == ControlStatus.OK for r in replies.values()), replies)
        self.assertTrue(cluster.settled(NORR, 2))


if __name__ == "__main__":
    unittest.main()
