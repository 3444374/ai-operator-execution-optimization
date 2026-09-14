"""Projection integrity for a live, buffered, sequential gateway event stream."""
import json
from pathlib import Path
import tempfile
import unittest
from src.experiments.postgresql.persistent_gateway import complete_events, settled_query


class PersistentProjectionTests(unittest.TestCase):
    def test_partial_write_is_revisited_and_previous_queries_are_not_repeated(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'events.jsonl'
            first = b'{"event":"first"}\n'
            path.write_bytes(first+b'{"event":')
            rows, offset = complete_events(path, 0)
            self.assertEqual(rows,[dict(event='first')])
            self.assertEqual(offset,len(first))
            with path.open('ab') as out: out.write(b'"second"}\n')
            rows, end = complete_events(path, offset)
            self.assertEqual(rows,[dict(event='second')])
            self.assertEqual(end,path.stat().st_size)
            path.write_bytes(b'')
            with self.assertRaisesRegex(ValueError,'truncated'): complete_events(path,end)

    def test_eof_or_session_closure_alone_does_not_prove_job_cleanup(self):
        drain = [dict(event='core_job_drained',usage=dict(held_tasks=0,active_requests=0))]
        session = [dict(event='session_start',session_id=9),dict(event='session_end',session_id=9,termination='returned')]
        self.assertFalse(settled_query([],session))
        self.assertFalse(settled_query(drain,session[:1]))
        self.assertTrue(settled_query(drain,session))
        with self.assertRaises(ValueError): settled_query(drain*2,session)
        session[-1]['session_id']=10
        with self.assertRaises(ValueError): settled_query(drain,session)
        session[-1]['session_id']=9
        drain[0]['usage']['active_requests']=1
        with self.assertRaises(ValueError): settled_query(drain,session)
