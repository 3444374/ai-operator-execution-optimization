"""Focused budget regressions: actual patched reserve and SQLite transactions."""
import ast
import hashlib
import importlib.util
import json
import multiprocessing as mp
import os
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
if (HERE/'send_counts.py').exists():
    COUNTS = HERE/'send_counts.py'
    HELPER = HERE/'sembench_pre_send_ledger.py'
else:
    CODE = HERE.parents[1]
    COUNTS = CODE/'src/experiments/send_counts.py'
    HELPER = CODE/'scripts/benchmarks/sembench_pre_send_ledger.py'
spec = importlib.util.spec_from_file_location('send_counts',COUNTS)
counts = importlib.util.module_from_spec(spec);spec.loader.exec_module(counts)


def load(root, *, limit=64, post_limit=192, global_limit=128, global_posts=384):
    marker=SimpleNamespace(request=lambda:None)
    state={'minimal_sync_public_cli_software':{'max_logical':limit,'max_posts':post_limit},
           'matrix':{'stages':{phase:[limit,post_limit,300] for phase in ['quality','single','dual']},
                     'hard_logical_total':global_limit,'hard_transmission_total':global_posts}}
    functions=[node for node in ast.parse(HELPER.read_text()).body if isinstance(node,ast.FunctionDef)
               and node.name in ['initialize','reserve','finish','assert_hooks']]
    hooks={'aiohttp':(marker,'request',marker.request)}
    hooks.update({'send_counts_'+name:(counts,name,getattr(counts,name)) for name in ['snapshot','verify_increment','initialize_counts','check_identity','mark_initialized']})
    values=dict(ROOT=Path(root),DB=Path(root)/'sends.sqlite',STATE=state,_send_counts=counts,
                sqlite3=sqlite3,time=time,os=os,hashlib=hashlib,json=json,_installed=True,_hooks=hooks)
    exec(compile(ast.Module(body=functions,type_ignores=[]),'actual-fixed-send-ledger','exec'),values)
    return values


def control(root,phase='single',deadline=None):
    with sqlite3.connect(Path(root)/'sends.sqlite') as db:
        db.execute('INSERT OR REPLACE INTO control VALUES(1,?,?,?,?,?,?,?)',
                   (phase,'slot','unit',time.time(),time.time()+300,0,deadline or time.monotonic_ns()+300_000_000_000))


def concurrent_worker(root,start,output):
    ctx=load(root,limit=23,global_limit=23)
    start.wait(10);success=0;refused=0
    for _ in range(20):
        try:ctx['reserve']('gateway','http://127.0.0.1/fixture',b'{"fixture":true}');success+=1
        except RuntimeError:refused+=1
    output.put((success,refused))


def crash_worker(root,committed):
    if committed:
        load(root)['reserve']('gateway','http://127.0.0.1/fixture',b'{}')
    else:
        db=sqlite3.connect(Path(root)/'sends.sqlite');db.execute('BEGIN IMMEDIATE')
        db.execute("INSERT INTO sends(phase,kind)VALUES('single','gateway')")
    os._exit(91)


class SendCounters(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory(prefix='semloom-counter-regression-')
        self.root=Path(self.directory.name)
        (self.root/'model-start.json').write_text(json.dumps({'deadline_monotonic_ns':time.monotonic_ns()+300_000_000_000}))
        self.ctx=load(self.root);self.ctx['initialize']();control(self.root)

    def tearDown(self):
        self.directory.cleanup()

    def snapshot(self):
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            db.execute('BEGIN IMMEDIATE');return counts.snapshot(db)

    def test_three_hop_reserve_and_failed_response_do_not_refund(self):
        seqs=[self.ctx['reserve'](kind,'http://127.0.0.1/fixture',b'{}') for kind in ['gateway','router','model']]
        self.ctx['finish'](seqs[0],error=OSError('fixture'))
        self.ctx['finish'](seqs[1],SimpleNamespace(status=503))
        self.assertEqual(self.snapshot()['single'],(3,1));self.assertEqual(self.snapshot()[counts.MODEL],(3,1))
        self.ctx['initialize']();self.assertEqual(self.snapshot()[counts.MODEL],(3,1))

    def test_phase_logical_and_transport_limits_are_checked_before_insert(self):
        for logical,posts,kinds in [(1,9,['gateway','gateway']),(9,1,['model','model'])]:
            ctx=load(self.root,limit=logical,post_limit=posts)
            ctx['reserve'](kinds[0],'http://127.0.0.1/fixture',b'{}')
            before=self.snapshot()
            with self.assertRaises(RuntimeError):ctx['reserve'](kinds[1],'http://127.0.0.1/fixture',b'{}')
            self.assertEqual(self.snapshot(),before)
            # Use a new phase for the next independent condition.
            control(self.root,'dual')

    def test_global_limits_across_phases(self):
        ctx=load(self.root,global_limit=1)
        ctx['reserve']('gateway','http://127.0.0.1/fixture',b'{}');control(self.root,'dual')
        with self.assertRaises(RuntimeError):ctx['reserve']('gateway','http://127.0.0.1/fixture',b'{}')
        ctx=load(self.root,global_posts=1)
        with self.assertRaises(RuntimeError):ctx['reserve']('model','http://127.0.0.1/fixture',b'{}')
        self.assertEqual(self.snapshot()[counts.MODEL],(1,1))

    def test_software_is_excluded_from_model_total(self):
        control(self.root,'software');ctx=load(self.root,global_limit=1,global_posts=1)
        for kind in ['gateway','fixture']:ctx['reserve'](kind,'http://127.0.0.1/fixture',b'{}')
        self.assertEqual(self.snapshot()['software'],(2,1));self.assertEqual(self.snapshot()[counts.MODEL],(0,0))
        control(self.root);ctx['reserve']('gateway','http://127.0.0.1/fixture',b'{}')
        self.assertEqual(self.snapshot()[counts.MODEL],(1,1))

    def test_expired_clock_and_replaced_hook_refuse_before_insert(self):
        control(self.root,deadline=time.monotonic_ns()-1)
        with self.assertRaises(TimeoutError):self.ctx['reserve']('gateway','http://127.0.0.1/fixture',b'{}')
        control(self.root)
        owner,attribute,expected=self.ctx['_hooks']['aiohttp'];owner.request=lambda:None
        with self.assertRaises(RuntimeError):self.ctx['reserve']('gateway','http://127.0.0.1/fixture',b'{}')
        self.assertEqual(self.snapshot()[counts.MODEL],(0,0))

    def test_no_delete_or_identity_mutation_but_finish_allowed(self):
        sequence=self.ctx['reserve']('gateway','http://127.0.0.1/fixture',b'{}')
        for sql in ['DELETE FROM sends',"UPDATE sends SET phase='software'", "UPDATE sends SET kind='model'",'UPDATE sends SET sequence=2']:
            with sqlite3.connect(self.root/'sends.sqlite') as db:
                with self.assertRaises(sqlite3.IntegrityError):db.execute(sql)
        self.ctx['finish'](sequence,SimpleNamespace(status=200));self.assertEqual(self.snapshot()[counts.MODEL],(1,1))

    def test_corrupt_counter_or_missing_trigger_never_resets(self):
        self.ctx['reserve']('gateway','http://127.0.0.1/fixture',b'{}')
        with sqlite3.connect(self.root/'sends.sqlite') as db:db.execute("UPDATE send_totals SET logical=0 WHERE scope IN ('single','__model__')")
        with self.assertRaises(RuntimeError):self.ctx['initialize']()
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            db.execute("UPDATE send_totals SET logical=1 WHERE scope IN ('single','__model__')")
            db.execute('DROP TRIGGER send_totals_insert')
        with self.assertRaises(RuntimeError):self.ctx['initialize']()
        with self.assertRaises(RuntimeError):self.ctx['reserve']('gateway','http://127.0.0.1/fixture',b'{}')
        with sqlite3.connect(self.root/'sends.sqlite') as db:self.assertEqual(db.execute('SELECT count(*) FROM sends').fetchone()[0],1)

    def test_offline_legacy_migration_preserves_rows_and_rejects_wrong_snapshot(self):
        legacy=self.root/'legacy.sqlite'
        with sqlite3.connect(legacy) as db:
            db.execute('CREATE TABLE sends(sequence INTEGER PRIMARY KEY,phase TEXT,kind TEXT)')
            db.executemany('INSERT INTO sends(phase,kind)VALUES(?,?)',[(phase,kind) for phase in counts.PHASES for kind in counts.KINDS])
        with sqlite3.connect(legacy) as db:
            db.execute('BEGIN IMMEDIATE');before=list(db.execute('SELECT * FROM sends'))
            with self.assertRaises(RuntimeError):counts.initialize_counts(db)
            with self.assertRaises(RuntimeError):counts.initialize_counts(db,migrate=True,expected_rows=15)
            counts.initialize_counts(db,migrate=True,expected_rows=16)
            self.assertEqual(list(db.execute('SELECT * FROM sends')),before)
            self.assertEqual(counts.snapshot(db)[counts.MODEL],(12,3))
        with sqlite3.connect(legacy) as db:
            db.execute('BEGIN IMMEDIATE');counts.initialize_counts(db)
            self.assertEqual(counts.snapshot(db)[counts.MODEL],(12,3))

    def test_process_death_rolls_back_uncommitted_and_retains_committed(self):
        ctx=mp.get_context('spawn')
        for committed,expected in [(False,(0,0)),(True,(1,1))]:
            child=ctx.Process(target=crash_worker,args=(str(self.root),committed));child.start();child.join(10)
            self.assertEqual(child.exitcode,91);self.ctx['initialize']();self.assertEqual(self.snapshot()[counts.MODEL],expected)

    def test_concurrent_processes_cannot_overdraw_last_allowance(self):
        ctx=mp.get_context('spawn');start=ctx.Event();output=ctx.Queue()
        children=[ctx.Process(target=concurrent_worker,args=(str(self.root),start,output)) for _ in range(4)]
        for child in children:child.start()
        start.set()
        for child in children:child.join(15);self.assertEqual(child.exitcode,0)
        results=[output.get(timeout=2) for _ in children]
        self.assertEqual(sum(x[0] for x in results),23);self.assertEqual(sum(x[1] for x in results),57)
        self.assertEqual(self.snapshot()[counts.MODEL],(23,23))

    def test_missing_or_replaced_database_is_not_reinitialized(self):
        database=self.root/'sends.sqlite';retained=self.root/'retained.sqlite'
        self.ctx['reserve']('gateway','http://127.0.0.1/fixture',b'{}');database.rename(retained)
        with self.assertRaises(RuntimeError):self.ctx['initialize']()
        self.assertFalse(database.exists())
        with self.assertRaises(RuntimeError):self.ctx['reserve']('gateway','http://127.0.0.1/fixture',b'{}')
        with sqlite3.connect(database) as db:db.execute('CREATE TABLE placeholder(id INTEGER)')
        with self.assertRaises(RuntimeError):self.ctx['initialize']()
        database.unlink();retained.rename(database);self.ctx['initialize']();self.assertEqual(self.snapshot()[counts.MODEL],(1,1))

    def test_commit_failure_does_not_return_a_send_or_spend_half_a_counter(self):
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            db.execute('CREATE TABLE commit_parent(id INTEGER PRIMARY KEY)')
            db.execute('CREATE TABLE commit_child(id INTEGER REFERENCES commit_parent(id) DEFERRABLE INITIALLY DEFERRED)')
            db.execute('CREATE TRIGGER test_commit_failure AFTER INSERT ON sends BEGIN INSERT INTO commit_child VALUES(9); END')
        def connect(*args,**kwargs):
            db=sqlite3.connect(*args,**kwargs);db.execute('PRAGMA foreign_keys=ON');return db
        self.ctx['sqlite3']=SimpleNamespace(connect=connect)
        with self.assertRaises(sqlite3.IntegrityError):self.ctx['reserve']('gateway','http://127.0.0.1/fixture',b'{}')
        self.assertEqual(self.snapshot()[counts.MODEL],(0,0))
        with sqlite3.connect(self.root/'sends.sqlite') as db:self.assertEqual(db.execute('SELECT count(*) FROM sends').fetchone()[0],0)

    def test_hot_reserve_does_not_scan_or_count_send_history(self):
        statements=[]
        def connect(*args,**kwargs):
            db=sqlite3.connect(*args,**kwargs);db.set_trace_callback(statements.append);return db
        self.ctx['sqlite3']=SimpleNamespace(connect=connect)
        self.ctx['reserve']('gateway','http://127.0.0.1/fixture',b'{}')
        self.assertFalse(any('count(' in statement.lower() and 'from sends' in statement.lower() for statement in statements))
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            plan=list(db.execute('EXPLAIN QUERY PLAN SELECT coalesce(max(sequence),0) FROM sends'))
            self.assertTrue(any('SEARCH sends' in row[-1] for row in plan))

    def test_private_harness_import_and_counter_fingerprint(self):
        validation=self.root/'validation';validation.mkdir()
        (validation/'pre_send_ledger.py').write_bytes(HELPER.read_bytes())
        (validation/'send_counts.py').write_bytes(COUNTS.read_bytes())
        files={name:hashlib.sha256((validation/name).read_bytes()).hexdigest() for name in ['pre_send_ledger.py','send_counts.py']}
        (self.root/'harness-identity.json').write_text(json.dumps({'files':files}))
        (self.root/'state.json').write_text(json.dumps(self.ctx['STATE']))
        environment=dict(os.environ,MODELRUN_ROOT=str(self.root),PYTHONPATH=str(validation),PYTHONDONTWRITEBYTECODE='1')
        code='import pre_send_ledger as ledger; ledger.initialize(); print(ledger.harness_identity())'
        first=subprocess.run([os.sys.executable,'-B','-c',code],env=environment,capture_output=True,text=True,timeout=10)
        self.assertEqual(first.returncode,0,first.stderr)
        (validation/'send_counts.py').write_text((validation/'send_counts.py').read_text()+'\n# changed identity\n')
        changed=subprocess.run([os.sys.executable,'-B','-c',code],env=environment,capture_output=True,text=True,timeout=10)
        self.assertNotEqual(changed.returncode,0);self.assertIn('differs from fixed identity',changed.stderr)


if __name__=='__main__':unittest.main(verbosity=2)
