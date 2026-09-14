"""Reject incomplete waiting accounts; preserve query preparation and upstream wait."""
import json
from pathlib import Path
import tempfile
import unittest

from tests.experiments import test_flow_timing
from src.experiments.postgresql.waiting_positions import analyze_waiting_positions


class WaitingPositionTests(unittest.TestCase):
    def fixture(self, root):
        test_flow_timing.FlowTimingTests().fixture(root)
        summary = dict(status='passed', manifest_sha256='a'*64, query_preparation_started_ns=1,
                       config=dict(arm='pg', task='map', movie_id=None))
        (root/'summary.json').write_text(json.dumps(summary))
        (root/'waiting-contract.json').write_text(json.dumps(dict(
            eligibility='sealed-immutable-full-scan-at-invocation', manifest_sha256='a'*64)))
        path = root/'q0/execution.json';execution = json.loads(path.read_text())
        execution.update(t_release_ns=5, t_query_terminal_ns=62, query_jct_seconds=57/1e9)
        path.write_text(json.dumps(execution))
        events = [json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()]
        for sequence, start, end, terminal in ((0,25,34,35),(1,26,28,29)):
            for kind, when in (('core_http_started',start),('core_http_finished',end),('core_terminal',terminal)):
                events.append(dict(event=kind,key=dict(session_id=4,sequence=sequence),monotonic_ns=when))
        (root/'events.jsonl').write_text('\n'.join(json.dumps(e) for e in events))

    def test_preparation_and_all_waiting_segments_are_included(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);self.fixture(root)
            result=analyze_waiting_positions(root,expected_rows=2,window=2)
            self.assertEqual(result['preparation_seconds'],4/1e9)
            self.assertEqual(result['preparation_to_eof_seconds'],61/1e9)
            self.assertEqual(result['sql_to_eof_seconds'],57/1e9)
            for row in result['rows']:
                self.assertEqual(sum(row['durations_ns'].values()),row['total_to_client_ns'])
            self.assertEqual(result['rows'][0]['durations_ns']['eligible_to_submit'],24)
            self.assertEqual(result['rows'][1]['durations_ns']['terminal_to_pg_ready'],1)

    def test_missing_duplicated_or_late_terminal_cannot_be_inferred(self):
        for change in ('missing','duplicate','late'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);self.fixture(root)
                path=root/'events.jsonl';events=[json.loads(s) for s in path.read_text().splitlines()]
                if change=='missing':events.pop()
                elif change=='duplicate':events.append(events[-1])
                else:events[-1]['monotonic_ns']=50
                path.write_text('\n'.join(json.dumps(e) for e in events))
                with self.assertRaises(ValueError):analyze_waiting_positions(root,expected_rows=2,window=2)

    def test_policy_dependent_eligibility_and_missing_preparation_are_rejected(self):
        for change in ('eligibility','preparation','source'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);self.fixture(root)
                path=root/('summary.json' if change=='preparation' else 'waiting-contract.json')
                value=json.loads(path.read_text())
                if change=='preparation':value.pop('query_preparation_started_ns')
                elif change=='eligibility':value['eligibility']='when-core-accepts'
                else:value['manifest_sha256']='b'*64
                path.write_text(json.dumps(value))
                with self.assertRaises(ValueError):analyze_waiting_positions(root,expected_rows=2,window=2)
