from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
import weakref

from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.postgresql.query_inputs import QueryInputs
from src.experiments.postgresql.m2_source import InformationSource,SourceLimits


class Tokenizer:
    def apply_chat_template(self,messages,**kwargs):
        return list(range(len(messages[-1]['content'])+10))


class Cursor:
    def __init__(self,connection):self.cursor=connection.cursor()
    def __enter__(self):return self
    def __exit__(self,*_):self.cursor.close()
    def execute(self,statement,parameters=()):return self.cursor.execute(statement.replace('ONLY ',''),parameters)
    def fetchone(self):return self.cursor.fetchone()


class Connection:
    def __init__(self,raw):self.raw=raw
    def cursor(self,**_):return Cursor(self.raw)
    def execute(self,statement,parameters=()):return self.raw.execute(statement.replace('ONLY ','').replace('%s','?'),parameters)


class InformationSourceTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.root=Path(self.directory.name)
        self.raw=sqlite3.connect(':memory:');self.addCleanup(self.raw.close)
        self.raw.execute('CREATE TABLE inputs(source_position INTEGER,row_id TEXT,movie_id TEXT,review_text TEXT,review_id TEXT)')
        self.raw.executemany('INSERT INTO inputs VALUES(?,?,?,?,?)',[(i,str(i),'movie','x'*n,str(i)) for i,n in enumerate([5,2,7,1,4,3])])
        self.connection=Connection(self.raw);self.inputs=QueryInputs('movie','inputs',6,1024,1024)
        self.plan=SemanticMapPlan('classify','fixture',128)
        self.limits=SourceLimits(3,65536,65536,1048576,4096)

    def source(self,mode,*,reuse=None,identity=None,deadline=None):
        root=self.root/(mode+str(len(list(self.root.iterdir()))));root.mkdir()
        return InformationSource(self.connection,self.inputs,self.plan,Tokenizer(),mode=mode,limits=self.limits,root=root,
            identity=identity or {'input':'fixture'},deadline=deadline or time.monotonic()+30,emit=lambda e:None,reuse=reuse)

    def test_same_payloads_different_information_sets_and_paid_reads(self):
        expected={'stream_fifo':[0,1,2,3,4,5],'window_fifo':[0,1,2,3,4,5],
                  'window_length':[1,0,2,3,5,4],'global_length':[3,1,5,4,0,2]}
        for mode,order in expected.items():
            with self.subTest(mode=mode):
                source=self.source(mode);rows=list(source.rows())
                self.assertEqual([r['source_position'] for r in rows],order)
                self.assertEqual(len({r['source_example_id'] for r in rows}),6)
                self.assertLessEqual(source.metrics['peak_candidate_rows'],3)
                self.assertEqual(source.metrics['source_reads'],12 if mode=='global_length' else 6)
                self.assertEqual(source.metrics['tokenize_ns']==0,mode=='stream_fifo')

    def test_reuse_requires_identity_and_checks_payload_before_yield(self):
        source=self.source('global_length');first=list(source.rows());path=source.root/'metadata.sqlite'
        reuse=self.source('global_reuse',reuse=path)
        self.assertEqual(list(reuse.rows()),first)
        self.assertEqual(reuse.metrics['source_reads'],6)
        self.assertEqual(reuse.metrics['tokenize_ns'],0)
        with self.assertRaisesRegex(ValueError,'identity'):list(self.source('global_reuse',reuse=path,identity={'input':'other'}).rows())
        self.raw.execute("UPDATE inputs SET review_text='changed' WHERE source_position=3")
        with self.assertRaisesRegex(ValueError,'payload changed'):next(self.source('global_reuse',reuse=path).rows())

    def test_missing_row_and_overlong_work_fail_without_truncation(self):
        self.raw.execute('DELETE FROM inputs WHERE source_position=5')
        with self.assertRaisesRegex(ValueError,'incomplete'):list(self.source('global_length').rows())
        source=self.source('window_length');source.limits=SourceLimits(3,65536,65536,1048576,130)
        with self.assertRaisesRegex(ValueError,'context'):list(source.rows())

    def test_deadline_and_metadata_budget_stop_before_allocation_grows(self):
        with self.assertRaises(TimeoutError):list(self.source('global_length',deadline=time.monotonic()-1).rows())
        source=self.source('global_length')
        source.limits=SourceLimits(3,65536,65536,1048576,4096)
        source.metrics['metadata_logical_bytes']=65536
        with self.assertRaisesRegex(ValueError,'metadata exceeds'):list(source.rows())

    def test_metadata_contains_no_prompt_or_label(self):
        source=self.source('global_length');list(source.rows())
        with closing(sqlite3.connect(source.root/'metadata.sqlite')) as metadata:
            self.assertEqual([r[1] for r in metadata.execute('PRAGMA table_info(meta)')],['position','row_id','work','digest'])
            self.assertEqual(metadata.execute('SELECT count(*) FROM meta').fetchone()[0],6)

    def test_exhausted_payload_window_is_released_before_next_fetch(self):
        class Payload(dict):
            pass
        inputs = self.inputs
        alive = []
        peaks = []
        class TrackedInputs:
            max_rows = inputs.max_rows
            select_sql = inputs.select_sql
            @staticmethod
            def convert(raw):
                row = Payload(inputs.convert(raw))
                alive.append(weakref.ref(row))
                peaks.append(sum(value() is not None for value in alive))
                return row
        for mode in ('window_fifo', 'window_length'):
            source = self.source(mode)
            source.inputs = TrackedInputs()
            alive.clear()
            peaks.clear()
            for row in source.rows():
                self.assertIn('source_position', row)
            # The consumer may retain its last yielded row while the next window
            # is fetched; the exhausted list must not retain all three rows.
            self.assertLessEqual(max(peaks), self.limits.candidate_rows + 1)
