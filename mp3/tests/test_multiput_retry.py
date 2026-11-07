"""Multiput must not turn version-supersession into false durable W acknowledgments."""
import unittest
from types import SimpleNamespace
from unittest import mock

from mp3.models import ConsistencyMode, Version
from mp3.multiput import MultiputManager, _Entry
from mp3 import multiput as module


class DummyRuntime:
    vm_id = "vm04"
    manifest = object()

    def __init__(self, mode=ConsistencyMode.EVENTUAL_RR):
        self.context = SimpleNamespace(mode=mode)
        self.phase = "ACTIVE"

    def transition_phase(self):
        return self.phase


def result(status, error=None):
    return SimpleNamespace(
        status=SimpleNamespace(value=status), error=error,
        version=Version("eventual", 123, "vm04/1") if status == "OK" else None,
        qualifying_owners=("vm03", "vm08") if status == "OK" else (),
    )


class MultiputRetryTests(unittest.TestCase):
    def setUp(self):
        self.runtime = DummyRuntime()
        self.manager = MultiputManager(self.runtime)
        self.id = "first-go"
        self.manager._entries[self.id] = _Entry("same-key", "GO_ACCEPTED", b"same-value")
        self.addCleanup(self.manager.stop)

    def run_with_results(self, outcomes):
        with mock.patch.object(self.manager, "_put", side_effect=outcomes) as put, \
             mock.patch.object(module.time, "sleep"):
            self.manager._run_put(self.id)
        return self.manager._entries[self.id].result, put

    def test_superseded_retries_same_value_with_fresh_quorum(self):
        output, put = self.run_with_results([
            result("ERROR", "eventual_put got 1 qualifying replies; failures=vm07:SUPERSEDED"),
            result("OK"),
        ])
        self.assertEqual((output["status"], output["attempts"]), ("OK", 2))
        self.assertEqual(put.call_args_list, [mock.call("same-key", b"same-value")] * 2)
        self.assertEqual(output["acks"], ["vm03", "vm08"])

    def test_unreachable_not_retried(self):
        output, put = self.run_with_results([result("ERROR", "vm03:UNREACH")])
        self.assertEqual((output["status"], output["attempts"]), ("ERROR", 1))
        self.assertEqual(put.call_count, 1)

    def test_uncertain_timeout_not_retried(self):
        output, put = self.run_with_results([result("TIMEOUT", "SUPERSEDED elsewhere")])
        self.assertEqual((output["status"], output["attempts"]), ("TIMEOUT", 1))
        self.assertEqual(put.call_count, 1)

    def test_strong_never_retried(self):
        self.runtime.context.mode = ConsistencyMode.STRONG
        output, put = self.run_with_results([result("ERROR", "vm07:SUPERSEDED")])
        self.assertEqual((output["status"], output["attempts"]), ("ERROR", 1))
        self.assertEqual(put.call_count, 1)

    def test_repeated_supersession_exhausts_explicitly(self):
        with mock.patch.object(module, "MULTIPUT_MAX_EVENTUAL_ATTEMPTS", 3):
            output, put = self.run_with_results([result("ERROR", "SUPERSEDED")] * 3)
        self.assertEqual((output["status"], output["attempts"]), ("ERROR", 3))
        self.assertEqual(put.call_count, 3)

    def test_mode_transition_stops_retries(self):
        def supersede_then_transition(key, value):
            self.runtime.phase = "PREPARE"
            return result("ERROR", "SUPERSEDED")
        output, put = self.run_with_results(supersede_then_transition)
        self.assertEqual((output["status"], output["attempts"]), ("ERROR", 1))
        self.assertEqual(put.call_count, 1)


if __name__ == "__main__":
    unittest.main()
