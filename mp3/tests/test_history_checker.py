"""Synthetic sanity checks for the bounded Strong register-history checker."""

import unittest

from mp3.coordinator import OperationStatus, ProtocolResult
from mp3.models import Version
from mp3.rpc import new_operation_id
from mp3.tests.history_checker import HistoryEntry, check_register_history


class HistoryCheckerTests(unittest.TestCase):
    def result(self, status, *, value=None, version=None, unknown=False):
        return ProtocolResult(status, value, version, new_operation_id(),
                              completion_unknown=unknown)

    def test_accepts_valid_register_order_and_ambiguous_write_effect(self):
        initial = self.result(OperationStatus.OK, version=Version("strong", 1, "vm01/1"))
        write = self.result(OperationStatus.OK, version=Version("strong", 2, "vm02/1"))
        read = self.result(OperationStatus.OK, value=b"b", version=write.version)
        rows = [
            HistoryEntry(initial.operation_id, "vm01", "k", "put", b"a", 0.0, 1.0, initial),
            HistoryEntry(write.operation_id, "vm02", "k", "put", b"b", 2.0, 3.0, write),
            HistoryEntry(read.operation_id, "vm03", "k", "get", None, 3.0, 4.0, read),
        ]
        self.assertTrue(check_register_history(rows))

        uncertain = self.result(OperationStatus.TIMEOUT, unknown=True)
        after_uncertain = self.result(
            OperationStatus.OK, value=b"maybe", version=Version("strong", 3, "vm04/1"))
        uncertain_rows = [
            HistoryEntry(uncertain.operation_id, "vm01", "k", "put", b"maybe",
                         5.0, 7.0, uncertain),
            HistoryEntry(after_uncertain.operation_id, "vm02", "k", "get", None,
                         7.0, 8.0, after_uncertain),
        ]
        self.assertTrue(check_register_history(uncertain_rows))

    def test_rejects_real_time_violation_and_wrong_read_value(self):
        first = self.result(OperationStatus.OK, version=Version("strong", 1, "vm01/1"))
        wrong_read = self.result(OperationStatus.OK, value=b"not-a", version=first.version)
        rows = [
            HistoryEntry(first.operation_id, "vm01", "k", "put", b"a", 0.0, 1.0, first),
            HistoryEntry(wrong_read.operation_id, "vm02", "k", "get", None,
                         2.0, 3.0, wrong_read),
        ]
        self.assertFalse(check_register_history(rows))

        valid_read = self.result(OperationStatus.OK, value=b"a", version=first.version)
        invalid_real_time = [rows[0], HistoryEntry(
            valid_read.operation_id, "vm02", "k", "get", None, 2.0, 3.0, valid_read)]
        self.assertTrue(check_register_history(invalid_real_time))

        absent = self.result(OperationStatus.NOT_FOUND)
        absent_after_write = [rows[0], HistoryEntry(
            absent.operation_id, "vm02", "k", "get", None, 2.0, 3.0, absent)]
        self.assertFalse(check_register_history(absent_after_write))

    def test_rejects_completed_strong_write_that_moves_tag_backward(self):
        high_version = Version("strong", 9, "vm01/1")
        low_version = Version("strong", 8, "vm02/1")
        high = self.result(OperationStatus.OK, version=high_version)
        low = self.result(OperationStatus.OK, version=low_version)
        read = self.result(OperationStatus.OK, value=b"low", version=low_version)
        history = [
            HistoryEntry(high.operation_id, "vm01", "k", "put", b"high",
                         0.0, 1.0, high),
            HistoryEntry(low.operation_id, "vm02", "k", "put", b"low",
                         2.0, 3.0, low),
            HistoryEntry(read.operation_id, "vm03", "k", "get", None,
                         4.0, 5.0, read),
        ]
        self.assertFalse(check_register_history(history))


if __name__ == "__main__":
    unittest.main()
