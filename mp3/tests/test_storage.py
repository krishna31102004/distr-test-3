"""Batch 1 tests: disposable SQLite state, deterministic faults and local processes."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from mp3.models import Context, ConsistencyMode as Mode, Version, Record, LocalStatus as S
from mp3.storage import open_store, StorageError, StaleEpochError

CTX = Context(1, Mode.EVENTUAL_NO_RR)
ROOT = Path(__file__).resolve().parents[2]


def rec(number, value=b'value', writer='vm01/1'):
    return Record(Version('eventual', number, writer), value)


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'kv.sqlite'
        self.store = open_store(self.path, bootstrap=CTX)
        self.addCleanup(self.store.close)

    def test_missing_empty_full_size_and_keys(self):
        self.assertEqual(self.store.read_local('missing', context=CTX).status, S.NOT_FOUND)
        for key, value in (('', b''), ('nul\x00key', b'\x00\xff'), ('λ\nkey', bytes(range(256))*4096)):
            self.assertEqual(self.store.install_if_newer(key, rec(1, value), context=CTX).status, S.INSTALLED)
            result = self.store.read_local(key, context=CTX)
            self.assertEqual(result.status, S.FOUND)
            self.assertEqual(result.record.value, value)

    def test_conditional_statuses_and_stored_evidence(self):
        for number, value, status in ((7,b'a',S.INSTALLED),(6,b'b',S.SUPERSEDED),
                                      (7,b'a',S.UNCHANGED),(7,b'b',S.VERSION_CONFLICT),(8,b'c',S.INSTALLED)):
            result = self.store.install_if_newer('k', rec(number,value), context=CTX)
            self.assertEqual(result.status,status)
            self.assertEqual(result.stored_version.number, max(7,number))
            self.assertEqual(result.durable,status != S.VERSION_CONFLICT)
        self.assertEqual(self.store.read_local('k',context=CTX).record,rec(8,b'c'))

    def test_concurrent_same_key_highest_wins(self):
        with ThreadPoolExecutor(max_workers=16) as pool:
            results=list(pool.map(lambda n:self.store.install_if_newer('race',rec(n,str(n).encode()),context=CTX), list(range(40,0,-1))))
        self.assertTrue(all(r.durable for r in results))
        self.assertEqual(self.store.read_local('race',context=CTX).record,rec(40,b'40'))

    def test_equal_counter_writer_tiebreak(self):
        a,b=rec(1,b'a','vm01/1'),rec(1,b'b','vm02/1')
        self.assertLess(a.version.compare(b.version),0)
        self.store.install_if_newer('k',b,context=CTX)
        self.assertEqual(self.store.install_if_newer('k',a,context=CTX).status,S.SUPERSEDED)

    def test_allocator_repeated_backward_and_concurrent(self):
        self.assertEqual(self.store.start_incarnation(),1)
        with patch('mp3.storage.time.time_ns',side_effect=[100,100,90]):
            versions=[self.store.allocate_eventual_version(context=CTX) for _ in range(3)]
        self.assertEqual([v.number for v in versions],[100,101,102])
        with patch('mp3.storage.time.time_ns',return_value=50), ThreadPoolExecutor(max_workers=12) as pool:
            versions=list(pool.map(lambda _:self.store.allocate_eventual_version(context=CTX),range(30)))
        self.assertEqual(sorted(v.number for v in versions),list(range(103,133)))

    def test_reopen_generation_and_timestamp(self):
        self.store.start_incarnation()
        with patch('mp3.storage.time.time_ns',return_value=100):
            first=self.store.allocate_eventual_version(context=CTX)
        self.store.install_if_newer('k',Record(first,b''),context=CTX)
        self.store.close()
        with open_store(self.path) as recovered:
            self.assertEqual(recovered.current_context(),CTX)
            self.assertEqual(recovered.start_incarnation(),2)
            with patch('mp3.storage.time.time_ns',return_value=10):
                second=recovered.allocate_eventual_version(context=CTX)
            self.assertEqual(second.number,101)
            self.assertEqual(second.writer_id,'vm01/2')
            self.assertEqual(recovered.read_local('k',context=CTX).record.value,b'')

    def test_epoch_and_mode_rejection(self):
        for context in (Context(2,Mode.EVENTUAL_NO_RR),Context(1,Mode.EVENTUAL_RR),Context(1,Mode.STRONG)):
            self.assertEqual(self.store.install_if_newer('k',rec(1),context=context).status,S.STALE_EPOCH)
            self.assertEqual(self.store.read_local('k',context=context).status,S.STALE_EPOCH)
            with self.assertRaises(StaleEpochError):
                self.store.allocate_eventual_version(context=context)
        self.assertEqual(self.store.read_local('k',context=CTX).status,S.NOT_FOUND)

    def test_epoch_check_against_persisted_not_cached_context(self):
        # Disposable boundary fixture, NOT an implemented cluster switch.
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE meta SET value='2' WHERE name='epoch'")
        result=self.store.install_if_newer('k',rec(1),context=CTX)
        self.assertEqual(result.status,S.STALE_EPOCH)
        self.assertEqual(result.context.epoch,2)

    def test_strong_local_tags_without_strong_allocator(self):
        self.store.close()
        path=Path(self.temp.name)/'strong.sqlite'
        ctx=Context(1,Mode.STRONG)
        with open_store(path,bootstrap=ctx) as strong:
            record=Record(Version('strong',2**15000,'vm02/1'),b'strong')
            self.assertEqual(strong.install_if_newer('k',record,context=ctx).status,S.INSTALLED)
            self.assertEqual(strong.read_local('k',context=ctx).record,record)
            with self.assertRaises(StorageError):strong.allocate_eventual_version(context=ctx)

    def test_bad_input_and_closed_store_errors(self):
        for key in (None,'bad\ud800'):
            self.assertEqual(self.store.read_local(key,context=CTX).status,S.ERROR)
        self.assertEqual(self.store.install_if_newer('k',None,context=CTX).status,S.ERROR)
        self.assertEqual(self.store.install_if_newer('k',Record(Version('strong',1,'vm01/1'),b''),context=CTX).status,S.ERROR)
        with self.assertRaises(ValueError):rec(1,b'x'*(1048576+1))
        with self.assertRaises(ValueError):Version('eventual',True,'vm01/1')
        with self.assertRaises(ValueError):Version('eventual',1,'bad')
        with self.assertRaises(ValueError):rec(1).version.compare(Version('strong',1,'vm01/1'))
        self.store.close()
        self.assertEqual(self.store.read_local('k',context=CTX).status,S.ERROR)

    def test_bootstrap_rejects_existing_missing_wrong_vm(self):
        with self.assertRaises(ValueError):open_store(self.path,timeout=True)
        with self.assertRaises(ValueError):open_store(self.path,timeout=10**1000)
        with self.assertRaises(StorageError):open_store(self.path,bootstrap=CTX)
        with self.assertRaises(StorageError):open_store(Path(self.temp.name)/'missing')
        with self.assertRaises(StorageError):open_store(self.path) # live process lock
        self.store.close()
        with self.assertRaises(StorageError):open_store(self.path,vm_id='vm02')
        with open_store(self.path) as recovered:self.assertEqual(recovered.current_context(),CTX)

    def test_invalid_database_and_metadata_fail_closed(self):
        self.store.close()
        bad=Path(self.temp.name)/'bad'
        bad.write_bytes(b'not sqlite')
        with self.assertRaises(StorageError):open_store(bad)
        self.assertEqual(bad.read_bytes(),b'not sqlite')
        with sqlite3.connect(self.path) as db:db.execute("UPDATE meta SET value='01' WHERE name='generation'")
        with self.assertRaises(StorageError):open_store(self.path)

    def test_sqlite_settings_and_lock_timeout(self):
        with self.store._transaction() as db:
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0],'wal')
            self.assertEqual(db.execute('PRAGMA synchronous').fetchone()[0],2)
        self.store.timeout=.05
        with sqlite3.connect(self.path,isolation_level=None) as db:
            db.execute('BEGIN IMMEDIATE')
            start=time.monotonic()
            result=self.store.install_if_newer('locked',rec(1),context=CTX)
            self.assertEqual(result.status,S.ERROR)
            self.assertFalse(result.durable)
            self.assertLess(time.monotonic()-start,1)
            db.execute('ROLLBACK')

    def test_commit_disk_error_rolls_back_no_durable_ack(self):
        original=sqlite3.connect
        class FailCommit(sqlite3.Connection):
            def execute(self,sql,*args):
                if sql=='COMMIT':raise sqlite3.OperationalError('injected SQLITE_FULL at commit')
                return super().execute(sql,*args)
        def connect(*args,**kwargs):return original(*args,**kwargs,factory=FailCommit)
        with patch('mp3.storage.sqlite3.connect',side_effect=connect):
            result=self.store.install_if_newer('lost',rec(1),context=CTX)
        self.assertEqual(result.status,S.ERROR)
        self.assertFalse(result.durable)
        self.assertEqual(self.store.read_local('lost',context=CTX).status,S.NOT_FOUND)

    def test_reader_progress_while_writer_waits_and_close_drains(self):
        self.store.install_if_newer('old',rec(1),context=CTX)
        self.store.timeout=.2
        with sqlite3.connect(self.path,isolation_level=None) as db, ThreadPoolExecutor(max_workers=2) as pool:
            db.execute('BEGIN IMMEDIATE')
            writer=pool.submit(self.store.install_if_newer,'new',rec(2),context=CTX)
            # The unrelated read must not wait behind an application-wide writer lock.
            reader=pool.submit(self.store.read_local,'old',context=CTX)
            self.assertEqual(reader.result(timeout=1).status,S.FOUND)
            self.assertEqual(writer.result(timeout=1).status,S.ERROR)
            db.execute('ROLLBACK')
        self.store.close()
        with open_store(self.path) as recovered:
            self.assertEqual(recovered.read_local('new',context=CTX).status,S.NOT_FOUND)

    def test_same_tag_conflicting_concurrent_values(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda value:self.store.install_if_newer('collision',rec(1,value),context=CTX), (b'a',b'b')))
        self.assertEqual({r.status for r in results},{S.INSTALLED,S.VERSION_CONFLICT})
        self.assertIn(self.store.read_local('collision',context=CTX).record.value,(b'a',b'b'))

    def test_crash_before_and_after_commit_and_allocator_commit(self):
        self.store.close()
        script=r'''
import os,signal,sqlite3,sys
from mp3.storage import open_store
from mp3.models import Context,ConsistencyMode,Version,Record
store=open_store(sys.argv[1]); mode=sys.argv[2]
original=sqlite3.connect
class Crash(sqlite3.Connection):
    def execute(self,sql,*args):
        if sql=='COMMIT' and mode=='before':os.kill(os.getpid(),signal.SIGKILL)
        result=super().execute(sql,*args)
        if sql=='COMMIT' and mode in ('after','allocate'):os.kill(os.getpid(),signal.SIGKILL)
        return result
sqlite3.connect=lambda *a,**kw:original(*a,**kw,factory=Crash)
ctx=Context(1,ConsistencyMode.EVENTUAL_NO_RR)
if mode=='allocate':store.allocate_eventual_version(context=ctx)
else:store.install_if_newer('crash',Record(Version('eventual',7,'vm01/1'),b'persist'),context=ctx)
'''
        for boundary in ('before','after','allocate'):
            if boundary=='allocate':
                with open_store(self.path) as store:store.start_incarnation()
            child=subprocess.run([sys.executable,'-B','-c',script,str(self.path),boundary],cwd=ROOT,capture_output=True,timeout=10)
            self.assertEqual(child.returncode,-9,child.stderr.decode())
            with open_store(self.path) as recovered:
                result=recovered.read_local('crash',context=CTX)
                self.assertEqual(result.status,S.NOT_FOUND if boundary=='before' else S.FOUND)
                if boundary=='allocate':
                    with sqlite3.connect(self.path) as db:last=int(db.execute("SELECT value FROM meta WHERE name='last_timestamp'").fetchone()[0])
                    with patch('mp3.storage.time.time_ns',return_value=0):next_version=recovered.allocate_eventual_version(context=CTX)
                    self.assertEqual(next_version.number,last+1)
