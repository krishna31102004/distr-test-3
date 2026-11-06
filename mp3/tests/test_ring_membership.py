"""Local tests for the fixed ring and restart-aware MP2-style membership adapter."""

import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import unittest

from mp3.membership import (
    GossipEnvelope, Member, MemberState, MembershipRuntime, MembershipService,
    MembershipTable,
)
from mp3.models import ConsistencyMode, Context, Record, Version
from mp3.ring import (
    Endpoint, Manifest, RingNode, key_position, load_manifest, node_position,
    ring_id_hex, select_successors,
)
from mp3.storage import open_store


ROOT = Path(__file__).resolve().parents[2]


def make_manifest():
    endpoints = {}
    for number in range(1, 11):
        vm_id = f"vm{number:02d}"
        base = 41000 + number * 4
        endpoints[vm_id] = {
            "kv": Endpoint("127.0.0.1", base),
            "membership": Endpoint("127.0.0.1", base + 1),
            "log_query": Endpoint("127.0.0.1", base + 2),
            "control": Endpoint("127.0.0.1", base + 3),
        }
    return Manifest.from_endpoints(endpoints)


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_local_network_manifest():
    endpoints = {}
    used = set()
    for number in range(1, 11):
        vm_id = f"vm{number:02d}"
        ports = []
        while len(ports) < 4:
            port = unused_port()
            if port not in used:
                used.add(port)
                ports.append(port)
        endpoints[vm_id] = {
            "kv": Endpoint("127.0.0.1", ports[0]),
            "membership": Endpoint("127.0.0.1", ports[1]),
            "log_query": Endpoint("127.0.0.1", ports[2]),
            "control": Endpoint("127.0.0.1", ports[3]),
        }
    return Manifest.from_endpoints(endpoints)


def envelope_for(manifest, sender, sender_generation, rows):
    return GossipEnvelope(manifest.fingerprint, sender, sender_generation, tuple(rows))


def default_rows(manifest, *, generation=0, heartbeat=0, state=MemberState.ALIVE):
    return [Member(node.vm_id, generation if node.vm_id == "vm02" else 0,
                   0, heartbeat if node.vm_id == "vm02" else 0,
                   state if node.vm_id == "vm02" else MemberState.ALIVE)
            for node in manifest.nodes]


def replace_row(rows, member):
    index = next(i for i, row in enumerate(rows) if row.vm_id == member.vm_id)
    rows[index] = member


class FakeClock:
    def __init__(self, start=0.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class RingTests(unittest.TestCase):
    def test_sha256_known_vectors_and_fixed_node_positions(self):
        self.assertEqual(key_position("photo"), int.from_bytes(
            hashlib.sha256(b"mp3/key/photo").digest(), "big"))
        self.assertEqual(node_position("vm06"), int.from_bytes(
            hashlib.sha256(b"mp3/node/vm06").digest(), "big"))
        self.assertEqual(ring_id_hex(1), "0" * 63 + "1")
        self.assertEqual(len(ring_id_hex(node_position("vm01"))), 64)
        self.assertEqual(hashlib.sha256(b"").hexdigest(),
                         "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        self.assertEqual(ring_id_hex(key_position("photo")),
                         "935ba85381249f6e2d550d84d10f37a80b1336ed7e2da3c1748ce9817fa027b4")
        with self.assertRaises(ValueError):
            node_position("vm1")
        with self.assertRaises(ValueError):
            key_position("\ud800")

    def test_clockwise_wraparound_and_inclusive_exact_position(self):
        nodes = [RingNode(f"vm{i:02d}", i * 10) for i in range(1, 11)]
        self.assertEqual([n.position for n in select_successors(nodes, 100, include_equal=True)],
                         [100, 10, 20, 30, 40])
        self.assertEqual([n.position for n in select_successors(nodes, 50, include_equal=True)],
                         [50, 60, 70, 80, 90])
        self.assertEqual([n.position for n in select_successors(nodes, 50, include_equal=False)],
                         [60, 70, 80, 90, 100])
        with self.assertRaises(ValueError):
            select_successors(nodes + [RingNode("duplicate", 10)], 1, include_equal=True)

    def test_manifest_is_immutable_canonical_and_order_independent(self):
        manifest = make_manifest()
        reversed_mapping = json.loads(manifest.to_json())
        reversed_mapping["nodes"].reverse()
        reversed_manifest = Manifest.from_mapping(reversed_mapping)
        self.assertEqual(manifest.fingerprint, reversed_manifest.fingerprint)
        self.assertEqual(manifest.to_json(), reversed_manifest.to_json())
        self.assertEqual(tuple(node.vm_id for node in manifest.nodes),
                         tuple(f"vm{i:02d}" for i in range(1, 11)))
        self.assertEqual(len(manifest.owners("photo")), 5)
        self.assertEqual(len({node.vm_id for node in manifest.owners("photo")}), 5)
        self.assertEqual(manifest.owners("photo"), reversed_manifest.owners("photo"))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_text(manifest.to_json(), encoding="utf-8")
            self.assertEqual(load_manifest(path).fingerprint, manifest.fingerprint)
        with self.assertRaises(Exception):
            manifest.nodes = ()
        with self.assertRaises((AttributeError, TypeError)):
            manifest._by_id.clear()

    def test_manifest_rejects_bad_ids_hashes_endpoints_duplicates_and_shape(self):
        manifest = make_manifest()
        raw = json.loads(manifest.to_json())
        raw["nodes"][0]["vm_id"] = "vm1"
        with self.assertRaises(ValueError):
            Manifest.from_mapping(raw)
        raw = json.loads(manifest.to_json())
        raw["nodes"][0]["ring_id"] += 1
        with self.assertRaises(ValueError):
            Manifest.from_mapping(raw)
        raw = json.loads(manifest.to_json())
        raw["nodes"][1]["kv"] = raw["nodes"][0]["kv"]
        with self.assertRaises(ValueError):
            Manifest.from_mapping(raw)
        with self.assertRaises(ValueError):
            Manifest.from_json('{"nodes": [], "nodes": []}')
        with self.assertRaises(ValueError):
            Endpoint("", 1234)
        with self.assertRaises(ValueError):
            Endpoint("host", True)

    def test_owner_selection_matches_in_separate_process(self):
        manifest = make_manifest()
        code = (
            "from mp3.ring import Manifest; import json; "
            f"m=Manifest.from_json({manifest.to_json()!r}); "
            "print(json.dumps([n.vm_id for n in m.owners('photo')]))"
        )
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        result = subprocess.run([sys.executable, "-B", "-c", code], cwd=ROOT,
                                env=env, capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(result.stdout),
                         [node.vm_id for node in manifest.owners("photo")])

    def test_failed_membership_does_not_change_prescribed_owners(self):
        manifest = make_manifest()
        clock = FakeClock()
        table = MembershipTable(manifest, "vm01", clock=clock)
        table.set_local_generation(1)
        before = table.owners("photo")
        clock.advance(2.5)
        suspected = table.check_suspicions()
        self.assertEqual(len(suspected), 9)
        self.assertEqual([m.status for m in table.snapshot() if m.vm_id != "vm01"],
                         [MemberState.SUSPECTED] * 9)
        self.assertEqual([member.vm_id for member in table.snapshot()],
                         [node.vm_id for node in sorted(manifest.nodes,
                                                        key=lambda node: node.ring_id)])
        self.assertEqual(table.owners("photo"), before)
        self.assertEqual(len(table.list_mem_ids()), 10)
        rows = table.list_mem_ids()
        self.assertEqual([row[5] for row in rows], sorted(node.ring_id for node in manifest.nodes))
        self.assertEqual([row[0] for row in rows],
                         [node.vm_id for node in sorted(manifest.nodes,
                                                        key=lambda node: node.ring_id)])


class MembershipTests(unittest.TestCase):
    def setUp(self):
        self.manifest = make_manifest()
        self.clock = FakeClock()
        self.table = MembershipTable(self.manifest, "vm01", clock=self.clock)
        self.table.set_local_generation(1)

    def merge_vm02(self, generation, incarnation=0, heartbeat=1,
                   status=MemberState.ALIVE, sender="vm03", sender_generation=1):
        rows = [Member(node.vm_id, 0, 0, 0, MemberState.ALIVE)
                for node in self.manifest.nodes]
        replace_row(rows, next(row for row in self.table.snapshot() if row.vm_id == self.table.self_id))
        replace_row(rows, Member("vm02", generation, incarnation, heartbeat, status))
        replace_row(rows, Member("vm03", sender_generation, 0, 1, MemberState.ALIVE))
        return self.table.merge_gossip(envelope_for(
            self.manifest, sender, sender_generation, rows))

    def test_gossip_roundtrip_and_schema_validation(self):
        rows = list(self.table.snapshot())
        replace_row(rows, Member("vm02", 3, 2, 8, MemberState.ALIVE))
        envelope = envelope_for(self.manifest, "vm02", 3, rows)
        decoded = GossipEnvelope.from_bytes(envelope.to_bytes(), self.manifest)
        self.assertEqual(decoded, envelope)
        with self.assertRaises(ValueError):
            GossipEnvelope.from_bytes(envelope.to_bytes() + b"x", self.manifest)
        # Corrupt the fingerprint and then the fixed ten-row shape.
        bad = json.loads(envelope.to_bytes())
        bad["manifest_id"] = "0" * 64
        with self.assertRaises(ValueError):
            GossipEnvelope.from_bytes(json.dumps(bad).encode(), self.manifest)
        bad = json.loads(envelope.to_bytes())
        bad["members"].pop()
        with self.assertRaises(ValueError):
            GossipEnvelope.from_bytes(json.dumps(bad).encode(), self.manifest)
        bad = json.loads(envelope.to_bytes())
        bad["members"][0]["heartbeat"] = "9" * 257
        with self.assertRaises(ValueError):
            GossipEnvelope.from_bytes(json.dumps(bad).encode(), self.manifest)

    def test_suspicion_is_retained_and_new_generation_recovers(self):
        self.merge_vm02(3)
        self.clock.advance(2.5)
        self.assertIn("vm02", self.table.check_suspicions())
        row = next(member for member in self.table.snapshot() if member.vm_id == "vm02")
        self.assertEqual(row.status, MemberState.SUSPECTED)
        self.assertEqual(row.restart_generation, 3)
        self.merge_vm02(4, heartbeat=0)
        row = next(member for member in self.table.snapshot() if member.vm_id == "vm02")
        self.assertEqual((row.restart_generation, row.status), (4, MemberState.ALIVE))

    def test_delayed_old_generation_cannot_override_recovery(self):
        self.merge_vm02(8, heartbeat=1)
        self.merge_vm02(9, heartbeat=0)
        self.merge_vm02(8, heartbeat=10**30, status=MemberState.SUSPECTED)
        row = next(member for member in self.table.snapshot() if member.vm_id == "vm02")
        self.assertEqual((row.restart_generation, row.heartbeat, row.status),
                         (9, 0, MemberState.ALIVE))

    def test_equal_incarnation_suspicion_wins_and_newer_incarnation_refutes(self):
        self.merge_vm02(2, incarnation=4, heartbeat=2)
        self.merge_vm02(2, incarnation=4, heartbeat=99, status=MemberState.SUSPECTED)
        row = next(member for member in self.table.snapshot() if member.vm_id == "vm02")
        self.assertEqual(row.status, MemberState.SUSPECTED)
        self.merge_vm02(2, incarnation=4, heartbeat=100, status=MemberState.ALIVE)
        row = next(member for member in self.table.snapshot() if member.vm_id == "vm02")
        self.assertEqual(row.status, MemberState.SUSPECTED)
        self.merge_vm02(2, incarnation=5, heartbeat=0, status=MemberState.ALIVE)
        row = next(member for member in self.table.snapshot() if member.vm_id == "vm02")
        self.assertEqual((row.incarnation, row.status), (5, MemberState.ALIVE))

    def test_self_suspicion_is_refuted_with_a_strictly_newer_incarnation(self):
        rows = list(self.table.snapshot())
        replace_row(rows, Member("vm01", 1, 0, 12, MemberState.SUSPECTED))
        replace_row(rows, Member("vm03", 1, 0, 1, MemberState.ALIVE))
        self.table.merge_gossip(envelope_for(self.manifest, "vm03", 1, rows))
        self.assertEqual(next(row for row in self.table.snapshot() if row.vm_id == "vm01"),
                         Member("vm01", 1, 1, 0, MemberState.ALIVE))

    def test_newer_self_generation_claim_fails_closed(self):
        rows = list(self.table.snapshot())
        replace_row(rows, Member("vm01", 2, 0, 0, MemberState.ALIVE))
        replace_row(rows, Member("vm03", 1, 0, 1, MemberState.ALIVE))
        with self.assertRaises(RuntimeError):
            self.table.merge_gossip(envelope_for(self.manifest, "vm03", 1, rows))

    def test_invalid_newer_self_generation_rejection_is_atomic(self):
        # vm10 sorts after vm01..vm09, so applying updates before validating the
        # self-generation claim would partially accept vm01's newer generation.
        table = MembershipTable(self.manifest, "vm10", clock=self.clock)
        table.set_local_generation(1)
        before = table.snapshot()
        rows = list(before)
        replace_row(rows, Member("vm01", 7, 0, 12, MemberState.ALIVE))
        replace_row(rows, Member("vm10", 2, 0, 0, MemberState.ALIVE))
        replace_row(rows, Member("vm02", 1, 0, 1, MemberState.ALIVE))
        with self.assertRaises(RuntimeError):
            table.merge_gossip(envelope_for(self.manifest, "vm02", 1, rows))
        self.assertEqual(table.snapshot(), before)

    def test_concurrent_merges_are_serialized_and_generation_monotonic(self):
        errors = []
        barrier = threading.Barrier(10)

        def update(generation):
            rows = [Member(node.vm_id, 0, 0, 0, MemberState.ALIVE)
                    for node in self.manifest.nodes]
            rows[0] = Member("vm01", 1, 0, 0, MemberState.ALIVE)
            rows[1] = Member("vm02", generation, 0, generation, MemberState.ALIVE)
            sender = "vm03"
            rows[2] = Member(sender, 1, 0, generation, MemberState.ALIVE)
            try:
                barrier.wait()
                self.table.merge_gossip(envelope_for(self.manifest, sender, 1, rows))
            except Exception as error:
                errors.append(error)

        threads = [threading.Thread(target=update, args=(generation,))
                   for generation in range(1, 11)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)
        self.assertFalse(errors)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        row = next(member for member in self.table.snapshot() if member.vm_id == "vm02")
        self.assertEqual(row.restart_generation, 10)

    def test_target_rotation_is_bounded_and_does_not_need_introducer(self):
        targets = self.table.next_targets(3)
        self.assertEqual(len(targets), 3)
        self.assertNotIn("vm01", [node.vm_id for node in targets])
        second = self.table.next_targets(3)
        self.assertEqual(len({node.vm_id for node in targets + second}), 6)
        self.assertLessEqual(len(self.table.next_targets(3)), 3)

    def test_service_rejects_unbounded_or_invalid_worker_configuration(self):
        with self.assertRaises(ValueError):
            MembershipService(self.manifest.node("vm01").membership, self.table,
                              workers=2, max_pending=1)
        with self.assertRaises(ValueError):
            MembershipService(self.manifest.node("vm01").membership, self.table,
                              workers=1, max_pending=2, io_timeout=0)
        with self.assertRaises(ValueError):
            MembershipService(self.manifest.node("vm01").membership, self.table,
                              workers=1, max_pending=2, io_timeout=float("inf"))
        with self.assertRaises(ValueError):
            MembershipTable(self.manifest, "vm01", suspicion_seconds=float("nan"))

    def test_repeated_sigkill_restart_uses_durable_generation_and_keeps_values(self):
        context = Context(1, ConsistencyMode.STRONG)
        manifest_json = self.manifest.to_json()
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "vm06.sqlite"
            with open_store(db, bootstrap=context, vm_id="vm06") as store:
                installed = store.install_if_newer(
                    "durable", Record(Version("strong", 7, "vm06/1"), b"kept"),
                    context=context)
                self.assertEqual(installed.status.value, "INSTALLED")
            code = (
                "import os,time; from mp3.ring import Manifest; "
                "from mp3.membership import MembershipRuntime; "
                "from mp3.storage import open_store; "
                "s=open_store(os.environ['MP3_DB'], vm_id='vm06'); "
                "r=MembershipRuntime(s, Manifest.from_json(os.environ['MP3_MANIFEST'])); "
                "r.start(background=False, listen=False); print(r.generation, flush=True); "
                "time.sleep(30)"
            )
            env = dict(os.environ, MP3_DB=str(db), MP3_MANIFEST=manifest_json,
                       PYTHONDONTWRITEBYTECODE="1")
            generations = []
            for expected in (1, 2):
                child = subprocess.Popen([sys.executable, "-B", "-c", code], cwd=ROOT,
                                         env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                         text=True)
                try:
                    actual = int(child.stdout.readline().strip())
                    generations.append(actual)
                    self.assertEqual(actual, expected)
                finally:
                    child.send_signal(signal.SIGKILL)
                    child.wait(timeout=5)
                    child.stdout.close()
                    child.stderr.close()
            self.assertEqual(generations, [1, 2])
            with open_store(db, vm_id="vm06") as recovered:
                runtime = MembershipRuntime(recovered, self.manifest)
                runtime.start(background=False, listen=False)
                self.assertEqual(runtime.generation, 3)
                result = recovered.read_local("durable", context=context)
                self.assertEqual(result.record.value, b"kept")
                runtime.stop()

    def test_tcp_gossip_recovers_without_introducer(self):
        manifest = make_local_network_manifest()
        tables = {vm_id: MembershipTable(manifest, vm_id)
                  for vm_id in ("vm02", "vm03")}
        for table in tables.values():
            table.set_local_generation(1)
        services = [MembershipService(manifest.node(vm_id).membership, table)
                    for vm_id, table in tables.items()]
        context = Context(1, ConsistencyMode.STRONG)
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "vm06.sqlite"
            with open_store(db, bootstrap=context, vm_id="vm06") as store:
                store.start_incarnation()  # VM6's prior process before its simulated crash.
                runtime = MembershipRuntime(store, manifest, fanout=3, rpc_timeout=0.25)
                runtime.start(background=False, listen=False)
                # Both directly reachable peers currently believe VM6's old process died.
                for table in tables.values():
                    rows = list(table.snapshot())
                    index = next(i for i, row in enumerate(rows) if row.vm_id == "vm06")
                    rows[index] = Member("vm06", 1, 0, 0, MemberState.SUSPECTED)
                    other = "vm03" if table.self_id == "vm02" else "vm02"
                    other_index = next(i for i, row in enumerate(rows) if row.vm_id == other)
                    rows[other_index] = Member(other, 1, 0, 1, MemberState.ALIVE)
                    table.merge_gossip(envelope_for(manifest, other, 1, rows))
                for service in services:
                    service.start()
                try:
                    # VM01 is the unavailable introducer. VM6's rotating sample is
                    # VM01, VM02, VM03; direct peer exchanges recover generation 2.
                    outcomes = runtime.gossip_round()
                    self.assertTrue(any(item.startswith("vm01:unreachable")
                                        for item in outcomes), outcomes)
                    for table in tables.values():
                        recovered = next(row for row in table.snapshot()
                                         if row.vm_id == "vm06")
                        self.assertEqual((recovered.restart_generation, recovered.status),
                                         (2, MemberState.ALIVE))
                    self.assertEqual([node.vm_id for node in runtime.table.owners("photo")],
                                     [node.vm_id for node in manifest.owners("photo")])
                finally:
                    runtime.stop()
                    for service in services:
                        service.stop()

    def test_service_start_requires_generation_before_network_admission(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "vm01.sqlite"
            context = Context(1, ConsistencyMode.STRONG)
            with open_store(db, bootstrap=context, vm_id="vm01") as store:
                table = MembershipTable(self.manifest, "vm01")
                service = MembershipService(Endpoint("127.0.0.1", 41999), table)
                with self.assertRaises(RuntimeError):
                    service.start()
                service.stop()

    def test_runtime_reserves_only_one_generation_per_store_per_process(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "vm01.sqlite"
            context = Context(1, ConsistencyMode.STRONG)
            with open_store(db, bootstrap=context, vm_id="vm01") as store:
                first = MembershipRuntime(store, self.manifest)
                second = MembershipRuntime(store, self.manifest)
                first.start(background=False, listen=False)
                with self.assertRaises(RuntimeError):
                    second.start(background=False, listen=False)
                self.assertEqual(store.start_incarnation(), 2)
                first.stop()
                second.stop()


if __name__ == "__main__":
    unittest.main()
