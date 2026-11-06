"""Durable mode-transition state in the local store (KD-16): fence, commit, journal."""

from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid
from unittest import mock

from mp3.models import (
    ConsistencyMode as M, Context, LocalStatus, Record, TransitionInfo, Version,
    mode_change_clears_data,
)
from mp3.storage import StorageError, TransitionRefused, open_store

STRONG, NORR, RR = M.STRONG, M.EVENTUAL_NO_RR, M.EVENTUAL_RR


def info(source, target, *, epoch_from=1, epoch_to=2, transition_id=None):
    return TransitionInfo(transition_id or str(uuid.uuid4()), "vm01",
                          Context(epoch_from, source), Context(epoch_to, target))


class StoreCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "vm04.sqlite"

    def open(self, mode=STRONG, **kwargs):
        store = open_store(self.path, bootstrap=Context(1, mode), vm_id="vm04", **kwargs)
        self.addCleanup(store.close)
        store.start_incarnation()
        return store

    def strong_record(self, store, key="k", value=b"v"):
        context = store.current_context()
        version = store.allocate_strong_version(context=context, observed_max=None)
        assert store.install_if_newer(key, Record(version, value), context=context).status \
            == LocalStatus.INSTALLED
        return version


class FenceTests(StoreCase):
    def test_prepared_node_serves_nothing_and_changes_nothing(self):
        store = self.open()
        self.strong_record(store)
        context = store.current_context()
        store.prepare_transition(info(STRONG, NORR))
        self.assertEqual(store.local_phase(), "PREPARED")
        read = store.read_local("k", context=context)
        self.assertEqual((read.status, read.record), (LocalStatus.TRANSITIONING, None))
        version = Version("strong", 99, "vm04/1")
        write = store.install_if_newer("k", Record(version, b"x"), context=context)
        self.assertEqual(write.status, LocalStatus.TRANSITIONING)
        self.assertFalse(write.durable)
        with self.assertRaises(StorageError):
            store.allocate_strong_version(context=context, observed_max=None)
        with self.assertRaises(StorageError):
            store.allocate_eventual_version(context=context)
        self.assertEqual(store.current_context(), context)  # diagnostics still work

    def test_fence_holds_through_committed_and_lifts_only_on_activate(self):
        store = self.open()
        transition = info(STRONG, NORR)
        store.prepare_transition(transition)
        store.commit_transition(transition.transition_id)
        new = Context(2, NORR)
        self.assertEqual(store.local_phase(), "COMMITTED")
        self.assertEqual(store.read_local("k", context=new).status, LocalStatus.TRANSITIONING)
        store.activate_transition(transition.transition_id)
        self.assertEqual(store.local_phase(), "ACTIVE")
        self.assertEqual(store.read_local("k", context=new).status, LocalStatus.NOT_FOUND)
        self.assertEqual(store.read_local("k", context=Context(1, STRONG)).status,
                         LocalStatus.STALE_EPOCH)


class PrepareTests(StoreCase):
    def test_prepare_is_idempotent_and_rejects_conflicts(self):
        store = self.open()
        first = info(STRONG, NORR)
        self.assertEqual(store.prepare_transition(first), "PREPARED")
        self.assertEqual(store.prepare_transition(first), "PREPARED")
        clash = TransitionInfo(first.transition_id, "vm01", Context(1, STRONG), Context(3, NORR))
        with self.assertRaises(TransitionRefused):
            store.prepare_transition(clash)
        with self.assertRaises(TransitionRefused):
            store.prepare_transition(info(STRONG, RR))  # another is already open

    def test_prepare_requires_the_expected_current_context(self):
        store = self.open()
        with self.assertRaises(TransitionRefused):
            store.prepare_transition(info(STRONG, NORR, epoch_from=5, epoch_to=6))
        with self.assertRaises(TransitionRefused):
            store.prepare_transition(info(NORR, RR))

    def test_info_validation(self):
        for bad in (dict(coordinator="vm02"), dict(epoch_to=1), dict(target=STRONG),
                    dict(transition_id="nope"), dict(phase="DONE")):
            kwargs = dict(transition_id=str(uuid.uuid4()), coordinator="vm01",
                          from_context=Context(1, STRONG), target_context=Context(2, NORR))
            if "epoch_to" in bad:
                kwargs["target_context"] = Context(1, NORR)
            elif "target" in bad:
                kwargs["target_context"] = Context(2, STRONG)
            else:
                kwargs.update(bad)
            with self.subTest(bad=list(bad)), self.assertRaises(ValueError):
                TransitionInfo(**kwargs)

    def test_a_stray_abort_cannot_shadow_the_transition_that_fences_the_node(self):
        store = self.open()
        transition = info(STRONG, NORR)
        store.prepare_transition(transition)
        with self.assertRaises(TransitionRefused):
            store.abort_transition(str(uuid.uuid4()))
        self.assertEqual(store.open_transition().transition_id, transition.transition_id)
        self.assertEqual(store.transition_state().transition_id, transition.transition_id)
        store.abort_transition(transition.transition_id)
        self.assertIsNone(store.open_transition())

    def test_aborted_transition_ids_are_tombstoned_against_delayed_prepare(self):
        store = self.open()
        transition = info(STRONG, NORR)
        store.prepare_transition(transition)
        self.assertEqual(store.abort_transition(transition.transition_id), "ABORTED")
        self.assertEqual(store.local_phase(), "ACTIVE")
        with self.assertRaises(TransitionRefused):
            store.prepare_transition(transition)
        unseen = info(STRONG, RR)
        self.assertEqual(store.abort_transition(unseen.transition_id), "ABORTED")
        with self.assertRaises(TransitionRefused):
            store.prepare_transition(unseen)
        # The old mode serves again after an abort.
        self.assertEqual(store.read_local("k", context=Context(1, STRONG)).status,
                         LocalStatus.NOT_FOUND)


class CommitTests(StoreCase):
    def test_strong_to_eventual_clears_records_and_version_metadata(self):
        store = self.open(STRONG)
        self.strong_record(store, "a")
        self.strong_record(store, "b")
        transition = info(STRONG, RR)
        store.prepare_transition(transition)
        store.commit_transition(transition.transition_id)
        store.activate_transition(transition.transition_id)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM records").fetchone()[0], 0)
            meta = dict(db.execute("SELECT name,value FROM meta"))
        self.assertEqual((meta["strong_counter"], meta["last_timestamp"]), ("0", "0"))
        self.assertEqual((meta["epoch"], meta["mode"]), ("2", RR.value))
        self.assertEqual(store.current_context(), Context(2, RR))

    def test_eventual_to_strong_clears_and_restarts_strong_numbering(self):
        store = self.open(NORR)
        context = store.current_context()
        version = store.allocate_eventual_version(context=context)
        store.install_if_newer("k", Record(version, b"e"), context=context)
        transition = info(NORR, STRONG)
        store.prepare_transition(transition)
        store.commit_transition(transition.transition_id)
        store.activate_transition(transition.transition_id)
        new = store.current_context()
        self.assertEqual(store.read_local("k", context=new).status, LocalStatus.NOT_FOUND)
        self.assertEqual(store.allocate_strong_version(context=new, observed_max=None).number, 1)

    def test_the_two_eventual_modes_keep_records_and_timestamps(self):
        store = self.open(NORR)
        context = store.current_context()
        version = store.allocate_eventual_version(context=context)
        store.install_if_newer("k", Record(version, b"kept"), context=context)
        with sqlite3.connect(self.path) as db:
            before = db.execute("SELECT value FROM meta WHERE name='last_timestamp'").fetchone()[0]
        for source, target, number in ((NORR, RR, 2), (RR, NORR, 3)):
            transition = info(source, target, epoch_from=number - 1, epoch_to=number)
            store.prepare_transition(transition)
            store.commit_transition(transition.transition_id)
            store.activate_transition(transition.transition_id)
        final = store.current_context()
        self.assertEqual(final, Context(3, NORR))
        found = store.read_local("k", context=final)
        self.assertEqual((found.status, found.record.value, found.record.version),
                         (LocalStatus.FOUND, b"kept", version))
        with sqlite3.connect(self.path) as db:
            after = db.execute("SELECT value FROM meta WHERE name='last_timestamp'").fetchone()[0]
        self.assertEqual(before, after)

    def test_commit_is_one_atomic_transaction(self):
        store = self.open(STRONG)
        self.strong_record(store)
        transition = info(STRONG, NORR)
        store.prepare_transition(transition)
        # Fail after the DELETE has run, before the context update commits.
        with mock.patch("mp3.storage._decimal", side_effect=RuntimeError("crash mid-commit")):
            with self.assertRaises(RuntimeError):
                store.commit_transition(transition.transition_id)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM records").fetchone()[0], 1)
            self.assertEqual(dict(db.execute("SELECT name,value FROM meta"))["epoch"], "1")
        self.assertEqual(store.local_phase(), "PREPARED")
        store.commit_transition(transition.transition_id)  # a retry still works
        self.assertEqual(store.local_phase(), "COMMITTED")

    def test_commit_activate_and_abort_ordering_rules(self):
        store = self.open()
        transition = info(STRONG, NORR)
        with self.assertRaises(TransitionRefused):
            store.commit_transition(transition.transition_id)  # never prepared
        with self.assertRaises(TransitionRefused):
            store.activate_transition(transition.transition_id)
        store.prepare_transition(transition)
        with self.assertRaises(TransitionRefused):
            store.activate_transition(transition.transition_id)  # not committed yet
        self.assertEqual(store.commit_transition(transition.transition_id), "COMMITTED")
        self.assertEqual(store.commit_transition(transition.transition_id), "COMMITTED")
        with self.assertRaises(TransitionRefused):
            store.abort_transition(transition.transition_id)  # no abort after commit
        self.assertEqual(store.activate_transition(transition.transition_id), "ACTIVE")
        self.assertEqual(store.activate_transition(transition.transition_id), "ACTIVE")
        with self.assertRaises(TransitionRefused):
            store.abort_transition(transition.transition_id)


class DurabilityTests(StoreCase):
    def test_state_survives_a_restart_at_every_phase(self):
        store = self.open(STRONG)
        self.strong_record(store)
        transition = info(STRONG, NORR)
        store.prepare_transition(transition)
        store.close()
        store = open_store(self.path, vm_id="vm04")
        self.addCleanup(store.close)
        self.assertEqual(store.local_phase(), "PREPARED")
        store.commit_transition(transition.transition_id)
        store.close()
        store = open_store(self.path, vm_id="vm04")
        self.addCleanup(store.close)
        self.assertEqual((store.local_phase(), store.current_context()),
                         ("COMMITTED", Context(2, NORR)))
        self.assertEqual(store.transition_state().phase, "COMMITTED")
        store.activate_transition(transition.transition_id)
        self.assertEqual(store.local_phase(), "ACTIVE")

    def test_an_older_database_without_transition_tables_upgrades_in_place(self):
        store = self.open(STRONG)
        self.strong_record(store)
        store.close()
        with sqlite3.connect(self.path) as db:
            for table in ("transition", "authority", "controller_journal"):
                db.execute(f"DROP TABLE {table}")
        store = open_store(self.path, vm_id="vm04")
        self.addCleanup(store.close)
        self.assertEqual(store.local_phase(), "ACTIVE")
        self.assertEqual(store.read_local("k", context=Context(1, STRONG)).status,
                         LocalStatus.FOUND)

    def test_terminal_history_is_bounded(self):
        store = self.open()
        for _ in range(80):
            store.abort_transition(str(uuid.uuid4()))
        with sqlite3.connect(self.path) as db:
            self.assertLessEqual(db.execute("SELECT COUNT(*) FROM transition").fetchone()[0], 64)


class EpochAndJournalTests(StoreCase):
    def test_epochs_only_go_up_and_burned_epochs_are_not_reused(self):
        store = self.open()
        self.assertEqual([store.allocate_transition_epoch() for _ in range(3)], [2, 3, 4])
        transition = info(STRONG, NORR, epoch_to=4)
        store.prepare_transition(transition)
        store.commit_transition(transition.transition_id)
        self.assertEqual(store.allocate_transition_epoch(), 5)
        store.close()
        reopened = open_store(self.path, vm_id="vm04")
        self.addCleanup(reopened.close)
        self.assertEqual(reopened.allocate_transition_epoch(), 6)

    def test_journal_follows_only_the_legal_path(self):
        store = self.open()
        transition_id, request = str(uuid.uuid4()), str(uuid.uuid4())
        source, target = Context(1, STRONG), Context(2, NORR)
        self.assertIsNone(store.journal_open())
        store.journal_start(transition_id, request, source, target)
        entry = store.journal_open()
        self.assertEqual((entry.transition_id, entry.state, entry.request_id, entry.target_context),
                         (transition_id, "PREPARING", request, target))
        with self.assertRaises(TransitionRefused):
            store.journal_start(str(uuid.uuid4()), request, source, target)
        with self.assertRaises(TransitionRefused):
            store.journal_advance(transition_id, "GLOBAL_ACTIVE")  # skips the commit decision
        store.journal_advance(transition_id, "COMMIT_DECIDED")
        store.journal_advance(transition_id, "COMMIT_DECIDED")  # idempotent
        with self.assertRaises(TransitionRefused):
            store.journal_advance(transition_id, "ABORT_DECIDED")  # irrevocable
        store.journal_advance(transition_id, "GLOBAL_ACTIVE")
        self.assertEqual(store.journal_open().state, "GLOBAL_ACTIVE")
        store.journal_advance(transition_id, "DONE")
        self.assertIsNone(store.journal_open())
        self.assertEqual(store.journal_get(transition_id).state, "DONE")
        self.assertIsNone(store.journal_get(str(uuid.uuid4())))

    def test_journal_survives_a_restart_and_an_abort_path(self):
        store = self.open()
        transition_id = str(uuid.uuid4())
        store.journal_start(transition_id, str(uuid.uuid4()), Context(1, STRONG), Context(2, RR))
        store.close()
        store = open_store(self.path, vm_id="vm04")
        self.addCleanup(store.close)
        self.assertEqual(store.journal_open().state, "PREPARING")
        store.journal_advance(transition_id, "ABORT_DECIDED")
        store.journal_advance(transition_id, "ABORTED")
        self.assertIsNone(store.journal_open())

    def test_clear_rule(self):
        self.assertTrue(mode_change_clears_data(STRONG, NORR))
        self.assertTrue(mode_change_clears_data(RR, STRONG))
        self.assertFalse(mode_change_clears_data(NORR, RR))
        self.assertFalse(mode_change_clears_data(RR, NORR))


if __name__ == "__main__":
    unittest.main()
