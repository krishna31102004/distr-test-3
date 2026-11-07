"""Deterministic unit coverage for the reportable benchmark pipeline."""

from __future__ import annotations

from collections import Counter
import csv
import json
from types import SimpleNamespace
import tempfile
import hashlib
from pathlib import Path
from unittest.mock import patch
import unittest

from mp3.control import ControlStatus
from mp3.models import ConsistencyMode, Version
from mp3.ring import Endpoint, Manifest
from mp3.benchmarks.matrix import full_matrix, seed_for
from mp3.benchmarks.metrics import (complete_checkpoint, sample_summary, trial_summary,
                                    validate_real_matrix)
from mp3.benchmarks.report import (_validate_indexed_keys,
                                   _validate_replica_content_checks,
                                   _verify_summary_metrics, load_summaries,
                                   write_validated_csvs)
from mp3.benchmarks.workload import Workload, preload_value, unique_value
from mp3.benchmarks.worker import validate_client_configuration


def _benchmark_manifest() -> Manifest:
    service_names = ("kv", "membership", "log_query", "control")
    endpoints = {
        f"vm{number:02d}": {
            service: Endpoint("127.0.0.1", 20_000 + number * 10 + offset)
            for offset, service in enumerate(service_names)
        }
        for number in range(1, 11)
    }
    return Manifest.from_endpoints(endpoints)


def _preload_reply(*, status=ControlStatus.OK, version=None, value=b"", detail=""):
    return SimpleNamespace(status=status, version=version, value=value, error=None,
                           detail=detail)


class PreloadAllOwnersTests(unittest.TestCase):
    def _run_preload(self, temporary: str, *, mismatch_owner: str | None = None,
                     missing_owner: str | None = None):
        from mp3.benchmarks import preload as preload_module

        manifest = _benchmark_manifest()
        key = "bench-key-000000"
        value = preload_value("runABC", 0)
        owners = tuple(node.vm_id for node in manifest.owners(key))
        if mismatch_owner == "first":
            mismatch_owner = owners[0]
        if missing_owner == "first":
            missing_owner = owners[0]
        read_targets = []

        def fake_send(_manifest, _coordinator, command, request_key="", body=b"", **_kwargs):
            if command == "put":
                return _preload_reply(version=Version("eventual", 1, "vm01/1"))
            if command == "read_replica":
                owner = json.loads(body.decode("utf-8"))["target"]
                read_targets.append(owner)
                if owner == missing_owner:
                    return _preload_reply(status=ControlStatus.NOT_FOUND)
                actual = b"wrong" if owner == mismatch_owner else value
                # Per-owner versions may legitimately differ; content is the condition.
                number = int(owner[-2:])
                return _preload_reply(version=Version("eventual", number,
                                                       f"{owner}/1"), value=actual)
            if command == "liststore":
                return _preload_reply(value=json.dumps(
                    {"keys": [key], "next": None}).encode("utf-8"))
            self.fail(f"unexpected control command: {command}")

        report_path = Path(temporary) / "preload.json"
        evidence_path = Path(temporary) / "content.jsonl"
        presence_path = Path(temporary) / "presence.jsonl"
        with patch.object(preload_module, "load_manifest", return_value=manifest), \
                patch.object(preload_module, "send_command", side_effect=fake_send):
            report = preload_module.preload(
                manifest_path="unused", run_id="runABC", mode="eventual-noRR",
                key_count=1, max_attempts=1, workers=1, report_path=report_path,
                evidence_path=evidence_path, presence_path=presence_path)
        return report, manifest, owners, read_targets, evidence_path

    def test_preload_reads_exact_value_from_every_prescribed_owner_without_version_equality(self):
        with tempfile.TemporaryDirectory() as temporary:
            report, _manifest, owners, read_targets, evidence_path = self._run_preload(temporary)
            self.assertTrue(report["complete"])
            self.assertEqual(read_targets, list(owners))
            content_row = json.loads(evidence_path.read_text(encoding="utf-8"))
            self.assertEqual([check["owner"] for check in content_row["replica_content_checks"]],
                             list(owners))
            self.assertEqual(len({check["version"]
                                  for check in content_row["replica_content_checks"]}), 5)
            _validate_replica_content_checks(
                content_row, list(owners),
                hashlib.sha256(preload_value("runABC", 0)).hexdigest())

    def test_preload_rejects_missing_or_wrong_owner_values(self):
        with tempfile.TemporaryDirectory() as temporary:
            report, _manifest, owners, read_targets, _evidence = self._run_preload(
                temporary, mismatch_owner="first")
            self.assertEqual(read_targets, list(owners))
            self.assertFalse(report["complete"])
            self.assertIn("bench-key-000000", report["failed_keys"])

        with tempfile.TemporaryDirectory() as temporary:
            report, _manifest, owners, read_targets, _evidence = self._run_preload(
                temporary, missing_owner="first")
            self.assertEqual(read_targets, list(owners))
            self.assertFalse(report["complete"])
            self.assertIn("bench-key-000000", report["failed_keys"])

    def test_report_validator_requires_exact_value_evidence_for_all_five_owners(self):
        with tempfile.TemporaryDirectory() as temporary:
            report, _manifest, owners, _read_targets, evidence_path = self._run_preload(temporary)
            self.assertTrue(report["complete"])
            row = json.loads(evidence_path.read_text(encoding="utf-8"))
            row["replica_content_checks"] = row["replica_content_checks"][:-1]
            expected_hash = hashlib.sha256(preload_value("runABC", 0)).hexdigest()
            with self.assertRaisesRegex(ValueError, "all five owners"):
                _validate_replica_content_checks(row, list(owners), expected_hash)


class BenchmarkWorkloadTests(unittest.TestCase):
    def test_value_is_exactly_1024_bytes_and_operation_values_are_unique(self):
        values = [unique_value("runA", "vm01", index) for index in range(100)]
        self.assertTrue(all(len(value) == 1024 for value in values))
        self.assertEqual(len(set(values)), 100)
        self.assertEqual(len(preload_value("runA", 0)), 1024)

    def test_uniform_workload_is_repeatable_and_alternates_half_get_half_put(self):
        left, right = Workload(913, 1000), Workload(913, 1000)
        first = [left.operation("vm04", index) for index in range(20_000)]
        self.assertEqual(first, [right.operation("vm04", index) for index in range(20_000)])
        self.assertEqual(Counter(command for command, _ in first), {"get": 10_000, "put": 10_000})
        indices = [int(key.rsplit("-", 1)[1]) for _command, key in first]
        self.assertGreater(len(set(indices)), 990)
        self.assertTrue(all(0 <= index < 1000 for index in indices))

    def test_hot_99_percent_applies_independently_to_get_and_put(self):
        workload = Workload(47, 100_000, "hot99")
        for command in ("get", "put"):
            rows = [workload.operation("vm01", index)
                    for index in range(2_000) if workload.operation("vm01", index)[0] == command]
            hot = sum(int(key.rsplit("-", 1)[1]) < 10 for _cmd, key in rows)
            self.assertEqual(len(rows), 1000)
            self.assertEqual(hot, 990)
            self.assertEqual(len(set(key for _cmd, key in rows if int(key[-6:]) >= 10)), 10)

    def test_invalid_workload_parameters_are_rejected(self):
        for kwargs in ({"seed": -1}, {"seed": True},
                       {"seed": 1, "key_count": 1, "distribution": "hot99"},
                       {"seed": 1, "distribution": "other"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                Workload(**kwargs)


class BenchmarkMatrixAndMetricsTests(unittest.TestCase):
    def test_full_matrix_has_45_exp1_and_six_exp2_trials(self):
        matrix = full_matrix()
        self.assertEqual(len(matrix), 51)
        self.assertEqual(sum(row.experiment == "exp1" for row in matrix), 45)
        self.assertEqual(sum(row.experiment == "exp2" for row in matrix), 6)
        self.assertEqual({row.duration_seconds for row in matrix}, {180})
        self.assertEqual(len({row.trial_id for row in matrix}), 51)

    def test_comparable_variants_share_seeds_and_base_seed_changes_them(self):
        rows = [row for row in full_matrix() if row.experiment == "exp1"
                and row.clients == 4 and row.trial == 2]
        self.assertEqual(len({row.seed for row in rows}), 1)
        self.assertNotEqual(seed_for(1, "exp1", 1), seed_for(2, "exp1", 1))

    def test_short_full_trial_duration_is_rejected(self):
        with self.assertRaises(ValueError):
            full_matrix(duration_seconds=179)

    def test_metric_aggregation_uses_operation_level_latency_and_success_only_throughput(self):
        spec = {"trial_id": "smoke", "duration_seconds": 4, "clients": 2}
        result = trial_summary(
            spec=spec,
            operation_rows=[{"status": "OK", "latency_ms": 10},
                            {"status": "ERROR", "latency_ms": 30}],
            workers=[{"actual_elapsed_seconds": 4}, {"actual_elapsed_seconds": 5}],
            measurement_interval_seconds=5,
            environment={"hostname": "local"},
            preload_evidence={"complete": True}, target="local_loopback",
            not_for_report=True)
        self.assertEqual(result["successful_throughput_ops_sec"], 0.2)
        self.assertEqual(result["mean_latency_ms_all_terminal"], 20)
        self.assertEqual(result["mean_latency_ms_successful"], 10)
        self.assertEqual(result["failed_operations"], 1)

    def test_sample_standard_deviation(self):
        self.assertEqual(sample_summary((1, 2, 3)), {"mean": 2.0, "sample_sd": 1.0, "n": 3})

    def test_worker_refuses_wrong_live_mode_or_quorums(self):
        reply = SimpleNamespace(status=ControlStatus.OK,
                                 mode=ConsistencyMode.EVENTUAL_NO_RR,
                                 detail='{"read_quorum":1,"write_quorum":2,"generation":4}')
        details = validate_client_configuration(
            reply, mode="eventual-noRR", read_quorum=1, write_quorum=2)
        self.assertEqual(details["generation"], 4)
        with self.assertRaisesRegex(RuntimeError, "mode/R/W"):
            validate_client_configuration(reply, mode="eventual-noRR",
                                          read_quorum=2, write_quorum=2)
        with self.assertRaisesRegex(RuntimeError, "mode/R/W"):
            validate_client_configuration(reply, mode="strong",
                                          read_quorum=None, write_quorum=None)

    def test_checkpoint_requires_complete_duration_and_raw_operation_files(self):
        spec = {"trial_id": "exp1-strong-rxwx-c1-t1", "experiment": "exp1",
                "mode": "strong", "read_quorum": None, "write_quorum": None,
                "clients": 1, "trial": 1, "seed": 1, "duration_seconds": 180}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "trials" / "vm01.jsonl"
            raw.parent.mkdir()
            run_id = "runABC"
            workload = Workload(1, 100_000, "uniform")
            get_command, get_key = workload.operation("vm01", 0)
            put_command, put_key = workload.operation("vm01", 1)
            put_hash = hashlib.sha256(unique_value(run_id, "vm01", 1)).hexdigest()
            operations = [
                {"run_id": run_id, "client_id": "vm01", "operation_index": 0,
                 "seed": 1,
                 "command": get_command, "key": get_key, "status": "OK", "latency_ms": 10,
                 "value_bytes": 1024, "value_sha256": "a" * 64},
                {"run_id": run_id, "client_id": "vm01", "operation_index": 1,
                 "seed": 1,
                 "command": put_command, "key": put_key, "status": "OK", "latency_ms": 20,
                 "value_bytes": 1024, "value_sha256": put_hash},
            ]
            raw.write_text("".join(json.dumps(row) + "\n" for row in operations),
                           encoding="utf-8")
            worker = {"client_id": "vm01", "mode": "strong", "seed": 1,
                      "read_quorum": None, "write_quorum": None,
                      "not_for_report": False, "actual_elapsed_seconds": 180,
                      "operations": 2}
            summary = trial_summary(
                spec=spec, operation_rows=operations, workers=[worker],
                measurement_interval_seconds=180,
                environment={"hostname": "host", "client_hosts": ["host-vm01"],
                             "gate_release_skew_upper_seconds": 0.0},
                preload_evidence={"run_id": run_id, "complete": True,
                                  "key_count": 100_000, "replication_factor": 5,
                                  "all_five_owners_verified": True, "mode": "strong"},
                target="remote_university_vm")
            summary.update({"spec": spec, "target": "remote_university_vm",
                            "worker_count": 1, "not_for_report": False,
                            "raw_files": [str(raw.relative_to(root))], "workers": [worker]})
            self.assertTrue(complete_checkpoint(summary, root, spec))
            summary["environment"]["client_hosts"] = ["127.0.0.1"]
            self.assertFalse(complete_checkpoint(summary, root, spec))
            summary["environment"]["client_hosts"] = ["host-vm01"]
            summary["workers"][0]["actual_elapsed_seconds"] = 179
            self.assertFalse(complete_checkpoint(summary, root, spec))
            summary["workers"][0]["actual_elapsed_seconds"] = 180
            rows = [json.loads(line) for line in raw.read_text().splitlines()]
            rows[1]["value_sha256"] = "b" * 64
            raw.write_text("".join(json.dumps(row) + "\n" for row in rows))
            self.assertFalse(complete_checkpoint(summary, root, spec))
            raw.write_text("".join(json.dumps(row) + "\n" for row in operations))
            raw.unlink()
            self.assertFalse(complete_checkpoint(summary, root, spec))

    def test_local_or_incomplete_results_cannot_become_final_report_data(self):
        with self.assertRaisesRegex(ValueError, "run.json"):
            load_summaries(Path(tempfile.mkdtemp()))
        with self.assertRaisesRegex(ValueError, "all 51"):
            validate_real_matrix([])

    def test_real_matrix_rejects_worker_shorter_than_required_duration(self):
        spec = full_matrix()[0]
        row = {
            "spec": {**spec.__dict__, "trial_id": spec.trial_id},
            "target": "remote_university_vm", "not_for_report": False,
            "worker_count": spec.clients,
            "workers": [{"actual_elapsed_seconds": 179.999}],
        }
        with self.assertRaisesRegex(ValueError, "below the required measured duration"):
            validate_real_matrix([row])

    def test_raw_metric_recomputation_rejects_summary_tampering(self):
        row = {
            "measurement_interval_seconds": 2.0,
                "environment": {"gate_release_skew_upper_seconds": 0.0},
                "workers": [{"actual_elapsed_seconds": 2.0}],
            "operations": 2, "raw_operation_count": 2,
            "successful_operations": 1, "failed_operations": 1,
            "status_counts": {"OK": 1, "ERROR": 1},
            "mean_latency_ms_all_terminal": 20.0,
            "mean_latency_ms_successful": 10.0,
            "successful_throughput_ops_sec": 0.5,
            "_verified_operations": [
                {"status": "OK", "latency_ms": 10.0},
                {"status": "ERROR", "latency_ms": 30.0},
            ],
        }
        _verify_summary_metrics(Path("trial.json"), row)
        row["successful_operations"] = 2
        row["_verified_operations"] = [
            {"status": "OK", "latency_ms": 10.0},
            {"status": "ERROR", "latency_ms": 30.0},
        ]
        with self.assertRaisesRegex(ValueError, "successful_operations"):
            _verify_summary_metrics(Path("trial.json"), row)

    def test_preload_index_validation_rejects_duplicates_and_holes(self):
        good = [{"index": i, "key": f"bench-{i:06d}"} for i in range(3)]
        self.assertEqual(_validate_indexed_keys(good, key_count=3,
                                                key_prefix="bench-"),
                         {f"bench-{i:06d}": i for i in range(3)})
        duplicate = [*good[:2], {"index": 1, "key": "bench-000001"}]
        with self.assertRaisesRegex(ValueError, "duplicate"):
            _validate_indexed_keys(duplicate, key_count=3, key_prefix="bench-")

    def test_validated_trial_csv_and_three_trial_configuration_statistics(self):
        summaries = []
        for trial, throughput, latency in ((1, 10.0, 4.0), (2, 12.0, 6.0), (3, 14.0, 8.0)):
            summaries.append({
                "spec": {"trial_id": f"e1-{trial}", "experiment": "exp1",
                         "mode": "strong", "read_quorum": None, "write_quorum": None,
                         "clients": 1, "trial": trial, "seed": 425 + trial,
                         "duration_seconds": 180},
                "operations": 1800, "successful_operations": 1800,
                "failed_operations": 0, "successful_throughput_ops_sec": throughput,
                "mean_latency_ms_all_terminal": latency,
                "mean_latency_ms_successful": latency,
                "measurement_interval_seconds": 181.0,
                "workers": [{"actual_elapsed_seconds": 180.5}],
            })
        with tempfile.TemporaryDirectory() as temporary:
            trial_csv, aggregate_csv = write_validated_csvs(summaries, temporary)
            with trial_csv.open(encoding="utf-8", newline="") as stream:
                trials = list(csv.DictReader(stream))
            with aggregate_csv.open(encoding="utf-8", newline="") as stream:
                aggregates = list(csv.DictReader(stream))
        self.assertEqual(len(trials), 3)
        self.assertEqual(len(aggregates), 1)
        self.assertEqual(float(aggregates[0]["throughput_mean_ops_sec"]), 12.0)
        self.assertEqual(float(aggregates[0]["throughput_sample_sd"]), 2.0)
        self.assertEqual(float(aggregates[0]["latency_mean_ms"]), 6.0)
        self.assertEqual(float(aggregates[0]["latency_sample_sd_ms"]), 2.0)
        self.assertEqual(float(trials[0]["measurement_interval_seconds"]), 181.0)
        self.assertEqual(float(trials[0]["minimum_worker_elapsed_seconds"]), 180.5)


if __name__ == "__main__":
    unittest.main()
