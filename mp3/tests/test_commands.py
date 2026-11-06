"""The five remaining commands (ls, liststore, getfromreplica, list_mem_ids, multiput)
and an end-to-end pass of all eight commands in every mode, on ten loopback nodes."""

import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock

from mp3 import multiput as multiput_module
from mp3.cli import main
from mp3.control import ControlStatus, send_command
from mp3.models import (
    ConsistencyMode as M, Context, LocalStatus, Record, Version,
)
from mp3.multiput import ARMED, DONE, NOT_STARTED, STARTED, UNKNOWN, MultiputManager
from mp3.node import NodeRuntime
from mp3.ring import key_position, ring_id_hex
from mp3.rpc import ReplicaClient, RpcOperation, new_operation_id
from mp3.storage import TransitioningError, open_store
from mp3.control import ControlService
from mp3.rpc import RpcOutcome, RpcOutcomeKind
from mp3.tests.test_control import FakeRuntime
from mp3.tests.test_strong import _manifest
from mp3.tests.test_transition import ModeCluster, wait_for

STRONG, NORR, RR = M.STRONG, M.EVENTUAL_NO_RR, M.EVENTUAL_RR


class Runner:
    """Run the real CLI against a cluster and parse every JSON line it prints."""

    def __init__(self, cluster):
        self.cluster = cluster

    def __call__(self, vm_id, *args, node=True):
        stream = io.StringIO()
        flags = ["--manifest", str(self.cluster.manifest_path)]
        if node:
            flags += ["--vm-id", vm_id]
        with contextlib.redirect_stdout(stream):
            code = main([*flags, *args])
        lines = [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]
        return code, lines


class ClusterCase(unittest.TestCase):
    def cluster(self, mode=STRONG):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        cluster = ModeCluster(Path(temp.name), mode)
        self.addCleanup(cluster.stop)
        self.root = Path(temp.name)
        self.cli = Runner(cluster)
        return cluster

    def write(self, name, data):
        path = self.root / name
        path.write_bytes(data)
        return path


class LsTests(unittest.TestCase):
    def test_ls_needs_only_the_manifest_and_matches_the_ring(self):
        manifest = _manifest()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_text(manifest.to_json(), encoding="utf-8")
            stream = io.StringIO()
            with contextlib.redirect_stdout(stream), mock.patch.dict(os.environ, {}, clear=True):
                code = main(["--manifest", str(path), "ls", "my key"])  # no node is running
        result = json.loads(stream.getvalue())
        self.assertEqual((code, result["status"]), (0, "OK"))
        self.assertEqual(result["key_id"], ring_id_hex(key_position("my key")))
        expected = manifest.owners("my key")
        self.assertEqual([row["vm"] for row in result["owners"]], [n.vm_id for n in expected])
        self.assertEqual(len(result["owners"]), 5)
        for row, node in zip(result["owners"], expected):
            self.assertEqual(row["ring_id"], ring_id_hex(node.ring_id))
            self.assertEqual(row["address"], f"{node.kv.host}:{node.kv.port}")


class StorageListingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = open_store(Path(self.temp.name) / "vm04.sqlite",
                                bootstrap=Context(1, NORR), vm_id="vm04")
        self.addCleanup(self.store.close)
        self.store.start_incarnation()

    def fill(self, keys):
        context = self.store.current_context()
        for key in keys:
            version = self.store.allocate_eventual_version(context=context)
            self.store.install_if_newer(key, Record(version, b"v"), context=context)

    def test_pages_are_ordered_bounded_and_resumable(self):
        keys = [f"k{i:03d}" for i in range(23)] + ["snow☃", "nul\x00key", "with space"]
        self.fill(keys)
        seen, cursor, pages = [], None, 0
        while True:
            page, cursor = self.store.list_keys(after=cursor, max_keys=7)
            self.assertLessEqual(len(page), 7)
            seen.extend(page)
            pages += 1
            if cursor is None:
                break
        self.assertEqual(seen, sorted(keys, key=lambda k: k.encode("utf-8")))
        self.assertEqual(pages, 4)
        self.assertEqual(len(set(seen)), len(keys))

    def test_byte_budget_and_empty_store(self):
        self.assertEqual(self.store.list_keys(), ([], None))
        self.fill([f"{'x' * 40}{i}" for i in range(10)])
        page, cursor = self.store.list_keys(max_bytes=200)
        self.assertTrue(0 < len(page) < 10)
        self.assertEqual(cursor, page[-1])
        rest, end = self.store.list_keys(after=cursor, max_bytes=10_000)
        self.assertEqual(len(page) + len(rest), 10)
        self.assertIsNone(end)
        self.assertEqual(len(self.store.list_keys(max_bytes=1)[0]), 1)  # always progresses

    def test_a_fenced_node_refuses_to_list(self):
        from mp3.models import TransitionInfo
        self.fill(["a"])
        transition = TransitionInfo(new_operation_id(), "vm01", Context(1, NORR),
                                    Context(2, RR))
        self.store.prepare_transition(transition)
        with self.assertRaises(TransitioningError):
            self.store.list_keys()
        with self.assertRaises(ValueError):
            self.store.list_keys(max_keys=0)


class ListStoreCommandTests(ClusterCase):
    def test_liststore_prints_every_key_with_its_ring_id_over_several_pages(self):
        cluster = self.cluster(STRONG)
        node = cluster.nodes["vm03"]
        context = node.store.current_context()
        keys = [f"key-{i:02d}" for i in range(12)] + ["uni☃", "has space"]
        for index, key in enumerate(keys):
            version = Version("strong", index + 1, "vm01/1")
            node.store.install_if_newer(key, Record(version, b"v"), context=context)
        small = lambda self, after=None: self.store.list_keys(after=after, max_keys=5)
        with mock.patch.object(NodeRuntime, "list_store", small):
            code, lines = self.cli("vm03", "liststore")
        self.assertEqual(code, 0)
        summary, rows = lines[-1], lines[:-1]
        self.assertEqual((summary["status"], summary["keys"], summary["node"]),
                         ("OK", len(keys), "vm03"))
        self.assertEqual(summary["node_ring_id"],
                         ring_id_hex(cluster.manifest.node("vm03").ring_id))
        self.assertEqual([r["key"] for r in rows], sorted(keys, key=lambda k: k.encode()))
        for row in rows:
            self.assertEqual(row["key_id"], ring_id_hex(key_position(row["key"])))

    def test_an_empty_store_lists_nothing(self):
        cluster = self.cluster(STRONG)
        code, lines = self.cli("vm05", "liststore")
        self.assertEqual((code, lines[-1]["keys"], len(lines)), (0, 0, 1))


class ListMemIdsTests(ClusterCase):
    def test_all_ten_members_sorted_by_ring_id_including_a_suspected_one(self):
        cluster = self.cluster(STRONG)
        cluster.nodes["vm06"].stop(timeout=8)
        wait_for(lambda: any(row[0] == "vm06" and row[4].value == "SUSPECTED"
                             for row in cluster.nodes["vm02"].list_mem_ids()),
                 20, "vm06 suspected")
        code, lines = self.cli("vm02", "list_mem_ids")
        self.assertEqual(code, 0)
        members = lines[0]["members"]
        self.assertEqual(len(members), 10)
        ring_ids = [row["ring_id"] for row in members]
        self.assertEqual(ring_ids, sorted(ring_ids))  # fixed-width hex sorts numerically
        by_vm = {row["vm"]: row for row in members}
        self.assertEqual(by_vm["vm06"]["status"], "SUSPECTED")
        self.assertEqual(by_vm["vm02"]["status"], "ALIVE")
        for vm, row in by_vm.items():
            self.assertEqual(row["ring_id"], ring_id_hex(cluster.manifest.node(vm).ring_id))
        self.assertEqual(lines[0]["node_ring_id"],
                         ring_id_hex(cluster.manifest.node("vm02").ring_id))


class GetFromReplicaTests(ClusterCase):
    def setUp(self):
        self.calls = []

    def test_reads_exactly_one_replica_without_quorum_or_repair(self):
        cluster = self.cluster(STRONG)
        key = "replica-key"
        owners = [n.vm_id for n in cluster.manifest.owners(key)]
        stranger = next(vm for vm in cluster.nodes if vm not in owners)
        payload = bytes(range(200))
        self.assertEqual(cluster.put("vm01", key, payload).status, ControlStatus.OK)
        wait_for(lambda: cluster.replicated(key), 10, "replication")
        original = ReplicaClient.call_replica

        def spy(client, target, request, **kwargs):
            self.calls.append((target.vm_id, request.op, request.phase, request.schema))
            return original(client, target, request, **kwargs)

        out = self.root / "copy.bin"
        with mock.patch.object(ReplicaClient, "call_replica", spy):
            code, lines = self.cli("vm01", "getfromreplica", owners[2], key, str(out))
        result = lines[-1]
        self.assertEqual((code, result["status"], result["replica"]), (0, "OK", owners[2]))
        self.assertEqual(out.read_bytes(), payload)
        self.assertEqual(result["bytes"], len(payload))
        self.assertTrue(result["version"].startswith("strong:"))
        self.assertEqual(self.calls, [(owners[2], RpcOperation.READ_LOCAL, "local", 1)])

        # By KV address, and a non-owner has no copy: NOT_FOUND, file untouched, nothing installed.
        node = cluster.manifest.node(owners[0])
        out2 = self.root / "by-address.bin"
        self.assertEqual(self.cli("vm04", "getfromreplica",
                                  f"{node.kv.host}:{node.kv.port}", key, str(out2))[0], 0)
        self.assertEqual(out2.read_bytes(), payload)
        absent = self.write("absent.bin", b"keep")
        code, lines = self.cli("vm01", "getfromreplica", stranger, key, str(absent))
        self.assertEqual((code, lines[-1]["status"]), (1, "NOT_FOUND"))
        self.assertEqual(absent.read_bytes(), b"keep")
        store = cluster.nodes[stranger].store
        self.assertEqual(store.read_local(key, context=store.current_context()).status,
                         LocalStatus.NOT_FOUND)

    def test_empty_value_unavailable_target_and_bad_address(self):
        cluster = self.cluster(STRONG)
        key = "empty-replica"
        owners = [n.vm_id for n in cluster.manifest.owners(key)]
        self.assertEqual(cluster.put("vm01", key, b"").status, ControlStatus.OK)
        wait_for(lambda: cluster.replicated(key), 10, "replication")
        out = self.write("empty.bin", b"stale")
        code, lines = self.cli("vm01", "getfromreplica", owners[1], key, str(out))
        self.assertEqual((code, lines[-1]["status"], lines[-1]["bytes"]), (0, "OK", 0))
        self.assertEqual(out.read_bytes(), b"")
        down = next(vm for vm in owners if vm != "vm01")
        cluster.nodes[down].stop(timeout=8)
        untouched = self.write("untouched.bin", b"as-is")
        via = next(vm for vm in cluster.nodes if vm != down)
        code, lines = self.cli(via, "getfromreplica", down, key, str(untouched))
        self.assertEqual(code, 1)
        self.assertIn(lines[-1]["status"], ("ERROR", "TIMEOUT"))
        self.assertEqual(untouched.read_bytes(), b"as-is")
        code, lines = self.cli(via, "getfromreplica", "no-such-host:1", key, str(untouched))
        self.assertEqual((code, lines[-1]["status"]), (2, "ERROR"))
        code, lines = self.cli(via, "getfromreplica", owners[0], key, str(self.root))
        self.assertEqual(code, 2)  # a directory is not a valid destination


class MultiputTests(ClusterCase):
    def test_all_ready_then_one_concurrent_launch_and_a_per_vm_report(self):
        cluster = self.cluster(STRONG)
        key = "contended"
        mine = self.write("vm03.bin", b"V3" * 100)
        cluster.nodes["vm03"].multiput_file = mine  # this VM puts its own file
        launches = []
        original = MultiputManager._go

        def record(manager, multiput_id, key_):
            launches.append((manager.vm_id, time.monotonic()))
            return original(manager, multiput_id, key_)

        with mock.patch.object(MultiputManager, "_go", record):
            code, lines = self.cli("vm01", "multiput", key, "VM3", "VM5", "vm08")
        result = lines[-1]
        self.assertEqual((code, result["status"]), (0, "OK"), result)
        self.assertEqual(result["key_id"], ring_id_hex(key_position(key)))
        nodes = {row["vm"]: row for row in result["nodes"]}
        self.assertEqual(sorted(nodes), ["vm03", "vm05", "vm08"])
        for row in nodes.values():
            self.assertEqual((row["state"], row["status"]), (DONE, "OK"))
            self.assertTrue(row["version"].startswith("strong:"))
            self.assertGreater(row["put_ms"], 0)
            self.assertIn("go_ms", row)
        self.assertEqual(nodes["vm03"]["bytes"], 200)
        self.assertEqual(nodes["vm03"]["sha256"], hashlib.sha256(mine.read_bytes()).hexdigest()[:16])
        self.assertEqual(nodes["vm05"]["bytes"], 1024)  # random bytes by default
        # One GO per VM, all sent together (same process, so the times are comparable here).
        self.assertEqual(sorted(vm for vm, _ in launches), ["vm03", "vm05", "vm08"])
        times = [t for _, t in launches]
        self.assertLess(max(times) - min(times), 1.0)
        # The key now holds exactly one of the three values.
        final = cluster.get("vm02", key)
        self.assertEqual(final.status, ControlStatus.OK)
        self.assertIn(hashlib.sha256(final.value).hexdigest()[:16],
                      {row["sha256"] for row in nodes.values()})

    def test_the_initiator_may_be_one_of_the_selected_vms(self):
        cluster = self.cluster(STRONG)
        code, lines = self.cli("vm02", "multiput(shared, VM2, VM9)")
        self.assertEqual((code, lines[-1]["status"]), (0, "OK"))
        self.assertEqual({row["vm"] for row in lines[-1]["nodes"]}, {"vm02", "vm09"})

    def test_a_vm_that_is_not_ready_aborts_everyone_and_nothing_is_written(self):
        cluster = self.cluster(STRONG)
        cluster.nodes["vm08"].stop(timeout=8)
        code, lines = self.cli("vm01", "multiput", "never-written", "VM3", "VM5", "VM8")
        result = lines[-1]
        self.assertEqual((code, result["status"]), (1, "ERROR"))
        rows = {row["vm"]: row for row in result["nodes"]}
        self.assertEqual((rows["vm03"]["state"], rows["vm05"]["state"]), (NOT_STARTED, NOT_STARTED))
        self.assertEqual(rows["vm08"]["state"], UNKNOWN)  # no reply is never "not started"
        self.assertEqual(cluster.get("vm02", "never-written").status, ControlStatus.NOT_FOUND)
        for vm in ("vm03", "vm05"):
            states = {e.state for e in cluster.nodes[vm].multiput._entries.values()}
            self.assertEqual(states, {"ABORTED"})

    def test_invalid_selections_are_refused(self):
        cluster = self.cluster(STRONG)
        manager = cluster.nodes["vm01"].multiput
        for vms in ([], ["vm01", "vm01"], [f"vm{i:02d}" for i in range(1, 11)] + ["vm01"],
                    ["vm99"]):
            status, _detail, error = manager.coordinate("k", vms)
            self.assertEqual(status, ControlStatus.ERROR, vms)
            self.assertTrue(error)
        code, lines = self.cli("vm01", "multiput", "k", "VM1", "VM1")
        self.assertEqual(code, 2)


class MultiputParticipantTests(ClusterCase):
    def payload(self, multiput_id):
        return json.dumps({"multiput_id": multiput_id}).encode()

    def test_go_needs_a_live_arm_and_never_follows_an_abort(self):
        cluster = self.cluster(STRONG)
        manager = cluster.nodes["vm04"].multiput
        mid = new_operation_id()
        status, detail, _e = manager.participant_message("mp_arm", "k", self.payload(mid))
        self.assertEqual((status, detail["state"]), (ControlStatus.OK, ARMED))
        again = manager.participant_message("mp_arm", "k", self.payload(mid))
        self.assertEqual(again[1]["sha256"], detail["sha256"])  # idempotent ARM
        reuse = manager.participant_message("mp_arm", "other-key", self.payload(mid))
        self.assertEqual(reuse[0], ControlStatus.ERROR)
        self.assertEqual(manager.participant_message("mp_abort", "k", self.payload(mid))[1]["state"],
                         NOT_STARTED)
        go = manager.participant_message("mp_go", "k", self.payload(mid))
        self.assertEqual((go[0], go[1]["state"]), (ControlStatus.ERROR, NOT_STARTED))
        # A GO for an ID never armed, and a GO that arrives after an ABORT tombstone.
        unknown = new_operation_id()
        self.assertEqual(manager.participant_message("mp_go", "k", self.payload(unknown))[1]["state"],
                         UNKNOWN)
        manager.participant_message("mp_abort", "k", self.payload(unknown))
        self.assertEqual(manager.participant_message("mp_go", "k", self.payload(unknown))[0],
                         ControlStatus.ERROR)
        self.assertEqual(manager.participant_message("mp_status", "k", b"{}")[0],
                         ControlStatus.ERROR)  # no multiput_id

    def test_an_expired_arm_lease_refuses_go(self):
        cluster = self.cluster(STRONG)
        manager = cluster.nodes["vm04"].multiput
        mid = new_operation_id()
        with mock.patch.object(multiput_module, "ARM_LEASE_SECONDS", 0.05):
            manager.participant_message("mp_arm", "k", self.payload(mid))
            time.sleep(0.2)
            status, detail, error = manager.participant_message("mp_go", "k", self.payload(mid))
        self.assertEqual((status, detail["state"]), (ControlStatus.ERROR, NOT_STARTED))
        self.assertIn("lease", error)

    def test_a_duplicate_go_never_starts_a_second_put(self):
        cluster = self.cluster(STRONG)
        node = cluster.nodes["vm04"]
        mid = new_operation_id()
        with mock.patch.object(node, "strong_put", wraps=node.strong_put) as put:
            node.multiput.participant_message("mp_arm", "dup-key", self.payload(mid))
            first = node.multiput.participant_message("mp_go", "dup-key", self.payload(mid))
            second = node.multiput.participant_message("mp_go", "dup-key", self.payload(mid))
            self.assertEqual((first[0], second[0]), (ControlStatus.OK, ControlStatus.OK))
            wait_for(lambda: node.multiput.participant_message(
                "mp_status", "dup-key", self.payload(mid))[1]["state"] == DONE, 15, "put done")
            node.multiput.participant_message("mp_go", "dup-key", self.payload(mid))
            self.assertEqual(put.call_count, 1)
        final = node.multiput.participant_message("mp_status", "dup-key", self.payload(mid))[1]
        self.assertEqual(final["result"]["status"], "OK")

    def test_remembered_multiputs_are_bounded(self):
        cluster = self.cluster(STRONG)
        manager = cluster.nodes["vm04"].multiput
        with mock.patch.object(multiput_module, "MAX_REMEMBERED", 3):
            for _ in range(8):
                mid = new_operation_id()
                manager.participant_message("mp_arm", "k", self.payload(mid))
                manager.participant_message("mp_abort", "k", self.payload(mid))
        self.assertLessEqual(len(manager._entries), 4)

    def test_a_put_during_a_mode_change_reports_transitioning(self):
        cluster = self.cluster(STRONG)
        node = cluster.nodes["vm04"]
        mid = new_operation_id()
        node.multiput.participant_message("mp_arm", "k", self.payload(mid))
        with mock.patch.object(node, "transition_phase", return_value="PREPARED"):
            node.multiput.participant_message("mp_go", "k", self.payload(mid))
            wait_for(lambda: node.multiput.participant_message(
                "mp_status", "k", self.payload(mid))[1]["state"] == NOT_STARTED, 10,
                "put refused")
        view = node.multiput.participant_message("mp_status", "k", self.payload(mid))[1]
        # The put never ran, so it is NOT_STARTED, with the reason attached.
        self.assertEqual((view["result"]["status"], view["result"]["error"]),
                         ("ERROR", "TRANSITIONING"))


class AllEightCommandsTests(ClusterCase):
    def test_every_command_in_every_mode(self):
        cluster = self.cluster(STRONG)
        modes = [STRONG, NORR, RR]
        for index, mode in enumerate(modes):
            key = f"transcript-{mode.value}"
            payload = os.urandom(300)
            source = self.write(f"in-{index}.bin", payload)
            # 1 put
            code, lines = self.cli("vm03", "put", key, str(source))
            self.assertEqual((code, lines[-1]["status"], lines[-1]["mode"]),
                             (0, "OK", mode.value), lines)
            wait_for(lambda: cluster.replicated(key), 15, "replication to all owners")
            # 2 get
            out = self.root / f"out-{index}.bin"
            code, lines = self.cli("vm08", "get", key, str(out))
            self.assertEqual((code, out.read_bytes()), (0, payload))
            # 3 ls
            code, lines = self.cli("vm08", "ls", key, node=False)
            owners = [n.vm_id for n in cluster.manifest.owners(key)]
            self.assertEqual([r["vm"] for r in lines[-1]["owners"]], owners)
            # 4 liststore (on an owner)
            code, lines = self.cli(owners[0], "liststore")
            self.assertIn(key, [row.get("key") for row in lines])
            # 5 getfromreplica
            copy = self.root / f"copy-{index}.bin"
            code, lines = self.cli("vm01", "getfromreplica", owners[1], key, str(copy))
            self.assertEqual((code, copy.read_bytes(), lines[-1]["replica"]),
                             (0, payload, owners[1]))
            # 6 list_mem_ids
            code, lines = self.cli("vm05", "list_mem_ids")
            ids = [row["ring_id"] for row in lines[0]["members"]]
            self.assertEqual((len(ids), ids == sorted(ids)), (10, True))
            # 7 multiput
            code, lines = self.cli("vm02", "multiput", f"{key}-mp", "VM4", "VM6")
            self.assertEqual((code, lines[-1]["status"]), (0, "OK"), lines[-1])
            # 8 consistency_mode (to the next mode)
            if index + 1 < len(modes):
                nxt = modes[index + 1]
                code, lines = self.cli("vm09", "consistency_mode", nxt.value)
                self.assertEqual((code, lines[-1]["status"], lines[-1]["mode"]),
                                 (0, "OK", nxt.value), lines)
                self.assertTrue(cluster.settled(nxt, index + 2))


class ReviewFixTests(ClusterCase):
    def payload(self, multiput_id):
        return json.dumps({"multiput_id": multiput_id}).encode()

    def test_armed_values_expire_and_only_a_few_may_be_held(self):
        cluster = self.cluster(STRONG)
        manager = cluster.nodes["vm04"].multiput
        with mock.patch.object(multiput_module, "MAX_ARMED", 2), \
                mock.patch.object(multiput_module, "ARM_LEASE_SECONDS", 0.3):
            ids = [new_operation_id() for _ in range(3)]
            self.assertEqual(manager.participant_message("mp_arm", "k", self.payload(ids[0]))[0],
                             ControlStatus.OK)
            self.assertEqual(manager.participant_message("mp_arm", "k", self.payload(ids[1]))[0],
                             ControlStatus.OK)
            status, _d, error = manager.participant_message("mp_arm", "k", self.payload(ids[2]))
            self.assertEqual(status, ControlStatus.ERROR)
            self.assertIn("too many", error)
            time.sleep(0.5)  # both leases ran out: the values are freed and slots reopen
            self.assertEqual(manager.participant_message("mp_status", "k", self.payload(ids[0]))[1]
                             ["state"], NOT_STARTED)
            self.assertTrue(all(e.value == b"" for e in manager._entries.values()
                                if e.state == "EXPIRED"))
            self.assertEqual(manager.participant_message("mp_arm", "k", self.payload(ids[2]))[0],
                             ControlStatus.OK)

    def script(self, replies):
        """Replace the network fan-out with scripted per-command replies."""
        log = []

        def fanout(manager, command, key, vms, raw, timeout):
            log.append((command, tuple(vms)))
            return replies(command, list(vms), log)

        return log, mock.patch.object(MultiputManager, "_fanout", fanout)

    @staticmethod
    def done(vm, **extra):
        result = {"status": "OK", "put_ms": 3.0, "version": "strong:1:vm03/1", "acks": ["vm01"]}
        return (True, {"state": DONE, "result": result, **extra}, None, 1.0)

    def manager(self):
        cluster = self.cluster(STRONG)
        return cluster.nodes["vm01"].multiput

    def test_a_go_whose_reply_was_lost_still_reports_the_finished_put(self):
        gos = []

        def replies(command, vms, log):
            if command == "mp_arm":
                return {vm: (True, {"state": ARMED, "bytes": 1, "sha256": "aa"}, None, 1.0)
                        for vm in vms}
            if command == "mp_go" and not gos:
                gos.append(1)  # vm03's reply is lost; vm05 starts normally
                return {"vm03": (False, {}, "timed out", 5.0),
                        "vm05": (True, {"state": STARTED}, None, 1.0)}
            return {vm: self.done(vm) for vm in vms}

        manager = self.manager()
        log, patched = self.script(replies)
        with patched:
            status, detail, error = manager.coordinate("k", ["vm03", "vm05"])
        self.assertEqual((status, error), (ControlStatus.OK, None))
        rows = {r["vm"]: r for r in detail["nodes"]}
        self.assertEqual({r["status"] for r in rows.values()}, {"OK"})
        self.assertEqual([c for c, _ in log].count("mp_go"), 2)  # one idempotent resend

    def test_a_vm_that_lost_its_arm_is_not_started_and_does_not_stall_the_report(self):
        def replies(command, vms, log):
            if command == "mp_arm":
                return {vm: (True, {"state": ARMED}, None, 1.0) for vm in vms}
            if command == "mp_go":
                return {"vm03": self.done("vm03"),
                        "vm05": (False, {"state": UNKNOWN}, "unknown multiput", 1.0)}
            return {vm: self.done(vm) for vm in vms}

        manager = self.manager()
        log, patched = self.script(replies)
        began = time.monotonic()
        with patched:
            status, detail, _error = manager.coordinate("k", ["vm03", "vm05"])
        self.assertLess(time.monotonic() - began, 3.0)
        rows = {r["vm"]: r for r in detail["nodes"]}
        self.assertEqual((rows["vm05"]["state"], rows["vm03"]["state"]), (NOT_STARTED, DONE))
        self.assertEqual(status, ControlStatus.ERROR)

    def test_unknown_after_a_lost_go_stays_unknown_and_is_reported_not_waited_on(self):
        calls = []

        def replies(command, vms, log):
            calls.append(command)
            if command == "mp_arm":
                return {vm: (True, {"state": ARMED}, None, 1.0) for vm in vms}
            if command == "mp_go":
                if calls.count("mp_go") == 1:
                    return {"vm03": self.done("vm03"), "vm05": (False, {}, "timed out", 5.0)}
                return {vm: (False, {"state": UNKNOWN}, "unknown multiput", 1.0) for vm in vms}
            return {vm: self.done(vm) for vm in vms}

        manager = self.manager()
        log, patched = self.script(replies)
        began = time.monotonic()
        with patched:
            status, detail, error = manager.coordinate("k", ["vm03", "vm05"])
        self.assertLess(time.monotonic() - began, 3.0)
        rows = {r["vm"]: r for r in detail["nodes"]}
        self.assertEqual(rows["vm05"]["state"], UNKNOWN)
        self.assertEqual(status, ControlStatus.ERROR)
        self.assertIn("UNKNOWN", error)
        self.assertIn("vm05", error)

    def test_a_go_that_never_arrived_is_resent_while_the_node_is_still_armed(self):
        counts = {"go": 0}

        def replies(command, vms, log):
            if command == "mp_arm":
                return {vm: (True, {"state": ARMED}, None, 1.0) for vm in vms}
            if command == "mp_go":
                counts["go"] += 1
                if counts["go"] <= 2:
                    return {vm: (False, {}, "lost", 5.0) for vm in vms}
                return {vm: (True, {"state": STARTED}, None, 1.0) for vm in vms}
            if command == "mp_status":
                if counts["go"] <= 2:
                    return {vm: (True, {"state": ARMED}, None, 1.0) for vm in vms}
                return {vm: self.done(vm) for vm in vms}
            return {}

        manager = self.manager()
        log, patched = self.script(replies)
        with patched:
            status, _detail, _error = manager.coordinate("k", ["vm03"])
        self.assertEqual(status, ControlStatus.OK)
        self.assertEqual(counts["go"], 3)

    def test_liststore_cursor_travels_in_the_body_so_long_keys_page_correctly(self):
        cluster = self.cluster(STRONG)
        node = cluster.nodes["vm03"]
        context = node.store.current_context()
        keys = sorted(f"{i}" + "\x01" * 2800 for i in range(5))
        for index, key in enumerate(keys):
            node.store.install_if_newer(key, Record(Version("strong", index + 1, "vm01/1"), b"v"),
                                        context=context)
        small = lambda self, after=None: self.store.list_keys(after=after, max_keys=2)
        with mock.patch.object(NodeRuntime, "list_store", small):
            code, lines = self.cli("vm03", "liststore")
        self.assertEqual((code, lines[-1]["status"], lines[-1]["keys"]), (0, "OK", 5))
        self.assertEqual([row["key"] for row in lines[:-1]], keys)

    def test_a_bad_liststore_cursor_is_refused(self):
        cluster = self.cluster(STRONG)
        reply = send_command(cluster.manifest, "vm03", "liststore", "", b"not json")
        self.assertEqual((reply.status, reply.error), (ControlStatus.ERROR, "bad liststore cursor"))
        reply = send_command(cluster.manifest, "vm03", "liststore", "", b'{"after": 5}')
        self.assertEqual(reply.status, ControlStatus.ERROR)

    def test_reply_parsing_never_loops_or_crashes_on_a_bad_page(self):
        from types import SimpleNamespace
        from mp3.commands import run_list_mem_ids, run_liststore
        manifest = _manifest()
        pages = iter([
            {"keys": ["a", "b"], "next": "b"}, {"keys": ["c"], "next": "b"}])

        def fake(manifest_, vm_id, command, key="", value=b"", **kwargs):
            return SimpleNamespace(status=ControlStatus.OK, node_id=vm_id,
                                   value=json.dumps(next(pages)).encode(),
                                   detail=json.dumps({"node_ring_id": "ab"}))

        emitted = []
        with mock.patch("mp3.commands.send_command", fake):
            code, result = run_liststore(manifest, "vm01", emitted.append)
        self.assertEqual((code, result["status"]), (1, "ERROR"))
        self.assertIn("did not advance", result["message"])
        bad = SimpleNamespace(status=ControlStatus.OK, node_id="vm01", value=b"",
                              detail="[1, 2]")
        with mock.patch("mp3.commands.send_command", lambda *a, **k: bad):
            code, result = run_list_mem_ids(manifest, "vm01")
        self.assertEqual((code, result["status"]), (1, "ERROR"))

    def test_ambiguous_hosts_and_odd_ports_do_not_resolve(self):
        from mp3.commands import resolve_replica
        manifest = _manifest()
        node = manifest.node("vm04")
        self.assertEqual(resolve_replica(manifest, f"{node.kv.host}:{node.kv.port}"), "vm04")
        for bad in (node.kv.host, f"{node.kv.host}:\u0663", f"{node.kv.host}:1", "nope", ""):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                resolve_replica(manifest, bad)


class ControlLimitsTests(unittest.TestCase):
    """Bounded control workers and request-time clamping, with a fake node."""

    def setUp(self):
        self.manifest = _manifest()
        self.runtime = FakeRuntime(self.manifest)

    def service(self, **kwargs):
        service = ControlService(self.runtime, **kwargs)
        service.start()
        self.addCleanup(service.stop)
        return service

    def test_a_multiput_is_clamped_to_the_time_left_in_the_request(self):
        seen = []

        class Manager:
            def coordinate(self, key, vms, *, budget=None):
                seen.append(budget)
                return ControlStatus.OK, {"nodes": []}, None

        self.runtime.multiput = Manager()
        self.service(request_timeout=12.0, command_timeout=5.0)
        payload = json.dumps({"vms": ["vm01"]}).encode()
        reply = send_command(self.manifest, "vm01", "multiput", "k", payload, timeout=10)
        self.assertEqual(reply.status, ControlStatus.OK)
        self.assertTrue(9.0 < seen[0] < 11.0, seen)

    def test_a_multiput_with_too_little_request_time_does_not_start(self):
        seen = []

        class Manager:
            def coordinate(self, key, vms, *, budget=None):
                seen.append(budget)
                return ControlStatus.OK, {"nodes": []}, None

        self.runtime.multiput = Manager()
        self.service(request_timeout=6.0, command_timeout=2.0)
        payload = json.dumps({"vms": ["vm01"]}).encode()
        reply = send_command(self.manifest, "vm01", "multiput", "k", payload, timeout=10)
        self.assertEqual(reply.status, ControlStatus.TIMEOUT)
        self.assertEqual(seen, [])

    def test_replica_reads_cannot_fill_every_control_worker(self):
        gate = threading.Event()

        def call_replica(target, op, key, **kwargs):
            gate.wait(10)
            return RpcOutcome(RpcOutcomeKind.TIMEOUT, None, "slow replica")

        self.runtime.call_replica = call_replica
        self.service(request_timeout=30.0, command_timeout=10.0)
        payload = json.dumps({"target": "vm02"}).encode()
        replies = []
        threads = [threading.Thread(target=lambda: replies.append(send_command(
            self.manifest, "vm01", "read_replica", "k", payload, timeout=25)))
            for _ in range(3)]
        for thread in threads:
            thread.start()
        time.sleep(0.5)
        extra = send_command(self.manifest, "vm01", "read_replica", "k", payload, timeout=10)
        self.assertEqual(extra.status, ControlStatus.BUSY)
        # Other commands still get a worker.
        self.assertEqual(send_command(self.manifest, "vm01", "status").status, ControlStatus.OK)
        gate.set()
        for thread in threads:
            thread.join(15)
        self.assertEqual(len(replies), 3)


if __name__ == "__main__":
    unittest.main()
