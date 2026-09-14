"""Check stage correlation without inferring row identities from completions."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from src.experiments.postgresql.flow_timing import analyze_flow


class FlowTimingTests(unittest.TestCase):
    def fixture(self, root):
        (root/'q0').mkdir()
        def write(name, value):
            (root/name).write_text(json.dumps(value))
        data = ''.join(json.dumps(dict(row=[name, 'answer'], received_ns=60+i))+'\n'
                       for i, name in enumerate(('alpha', 'beta'))).encode()
        (root/'q0/results.jsonl').write_bytes(data)
        write('q0/execution.json', dict(status='completed',query_status='completed',recorded_rows=2,
            recorded_bytes=len(data),results_sha256=hashlib.sha256(data).hexdigest(),
            t_release_ns=0,t_first_row_ns=60,query_jct_seconds=62/1e9))
        write('pg-backend.json', dict(backend_pid=17))
        write('flow-settings.json', dict(pause_input=-1,pause_ms=80,ready_first=False,
                                       clock_source='clock_gettime(CLOCK_MONOTONIC)'))
        events=[]
        for name, index, when in (
            ('child_ready',0,10),('task_prepared',0,12),('child_ready',1,14),('task_prepared',1,16),
            ('offer_accepted',0,20),('offer_accepted',1,21),('receive_begin',0,24),
            ('result_ready',1,30),('result_ready',0,40),('node_return',0,42),('row_release',0,43),
            ('node_return',1,45),('row_release',1,46)):
            events.append(dict(version=1,backend_pid=17,pump_id=1,event=name,input_index=index,monotonic_ns=when,
                has_sequence=name not in ('child_ready','task_prepared'),sequence=index,
                retained_rows=2,head_present=True,head_input_index=0,head_ready=name=='node_return'))
        bindings=[]
        for i,name in enumerate(('alpha','beta')):
            for phase in ('before_offer','accepted'):
                bindings.append('LOG: SEMLOOM_MAP_BINDING '+json.dumps(dict(version=1,backend_pid=17,
                    stream=1,offer=i+1,sequence=i,row_id=name,payload_digest='a'*64,phase=phase)))
        def save(events):
            (root/'q0-producer.log').write_text('\n'.join(bindings+[
                'LOG: SEMLOOM_FLOW_TRACE '+json.dumps(e) for e in events]))
        save(events)
        (root/'events.jsonl').write_text('\n'.join(json.dumps(dict(event='core_submitted',
            key=dict(session_id=4,sequence=i),monotonic_ns=25+i)) for i in range(2)))
        return events,save

    def test_out_of_order_completion_has_independently_bound_client_times(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);self.fixture(root)
            result=analyze_flow(root,expected_rows=2,window=2)
            self.assertEqual([r['row_id'] for r in result['rows']],['alpha','beta'])
            self.assertEqual(result['rows'][1]['ready_to_node_s'],15/1e9)
            self.assertEqual(result['first_input_to_submit_s'],15/1e9)

    def test_missing_duplicate_or_reordered_node_evidence_is_rejected(self):
        for kind in ('missing','duplicate','order','sequence'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);events,save=self.fixture(root)
                if kind=='missing':events.pop()
                elif kind=='duplicate':events.append(dict(events[-1]))
                elif kind=='order':events[-1]['monotonic_ns']=1
                else:events[-1]['sequence']=0
                save(events)
                with self.assertRaises(ValueError):analyze_flow(root,expected_rows=2,window=2)

    def test_unverified_clock_and_changed_result_bytes_are_rejected(self):
        for kind in ('clock','results'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);self.fixture(root)
                if kind=='clock':
                    p=root/'flow-settings.json';settings=json.loads(p.read_text());settings['clock_source']='unknown'
                    p.write_text(json.dumps(settings))
                else:
                    p=root/'q0/results.jsonl';p.write_text(p.read_text().replace('alpha','other'))
                with self.assertRaises(ValueError):analyze_flow(root,expected_rows=2,window=2)
